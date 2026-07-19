"""senlytd 데몬 조립(wiring) + 상시 소비 루프 — 헥사고날 코어 ↔ 포트 ↔ 어댑터 결선.

Dart `lib/app/daemon.dart` 소비 모델(동기 poll) 준용 — 스텁 제거(사용자 원칙 2026-07-10).

이 클래스는 **물리 하드웨어가 아니라** SSE→멱등→실행→역보고의 **상시 소비 루프**다.
유일 mock 은 물리 엔진(FakeEngineAdapter) 뿐 — 소비 루프 자체는 실구현한다.

소비 파이프라인(SoT §1-1·§9):
  BootRecovery.plan() → INTERRUPTED 잔여 보고(크래시 복구·과토출 0)
    → 상시 루프(stop 플래그까지):
        Dispatcher.poll_commandsets() + poll() 로 도착분 소비
          → PumpSequencer 가 FakeEngine 실행·JobReport 생성
          → 생성된 진행보고를 status_sink.report_status(주문 PENDING→PROCESSING→DONE|FAILED)
          → 봉투 전이(DELIVERED→RUNNING→DONE|FAILED)를 status_sink.report_command_set_transition
        멱등 ledger.check_and_claim 로 중복 토출 차단(IL-02).
    → heartbeat 10s 주기(SENLYT_HEARTBEAT_INTERVAL_MS) send_heartbeat(queueDepth 파생) + ship_trace 배치 flush(별도 스레드).
    → 네트워크 오류는 삼켜 루프 지속(다음 폴 재시도)·OQ(offline_queue)로 역보고 무손실(§10-6).

종료(shutdown): Sequencer drain → OQ flush → heartbeat 정지 → 자원 정리(우아한 종료).
"""

from __future__ import annotations

import queue
import secrets
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from ..core.command_set import (
    MAINTENANCE_COMMAND_SET_PREFIX,
    CommandSet,
    CommandSetStatus,
)
from ..core.order_status import DispensePhase
from ..core.pump_guard import StatusErrorCode
from ..core.wire_messages import Command, Heartbeat, StatusReport
from ..obs.log import (
    STAGE_DISPENSE_DONE,
    STAGE_ERROR,
    STAGE_PI_RECEIVED,
    STAGE_STATUS_REPORT,
    StructuredLogger,
)
from ..persistence.file_idempotency_ledger import FileIdempotencyLedger
from ..pipeline.boot_recovery import BootRecovery, RecoveryAction
from ..pipeline.pump_sequencer import PumpSequencer
from ..pipeline.recipe_resolver import RecipeResolver
from ..ports.command_source_port import CommandSourcePort
from ..ports.commandset_source_port import CommandSetSourcePort
from ..ports.engine_port import EnginePort
from ..ports.valve_port import ValvePort
from ..ports.status_sink_port import StatusSinkPort, TraceSpan
from .dispatcher import Dispatcher, RecipeInterpreter


# 로그→trace 스팬 버퍼 상한 — 서버 전송 실패가 계속되면 로그가 무한 누적하는 것을 막는다(전송
#   실패 자체가 로그를 만들면 피드백 폭주). 상한 초과분은 드롭하되 **드롭 건수를 세어**(silent 금지)
#   다음 flush 에 합성 WARN 으로 남긴다(dispense 스팬은 별도로 항상 append — 이 상한은 로그 스팬만).
_LOG_TRACE_BUFFER_CAP = 500

# severity 서열(DEBUG<INFO<WARN<ERROR) — ship_log_min_severity 게이트 비교용.
_SEVERITY_RANK = {"DEBUG": 0, "INFO": 1, "WARN": 2, "ERROR": 3}


def _now_iso_ms() -> str:
    """ISO8601 밀리초 Z — TraceSpan.ts / StatusReport.updatedAt 포맷(부록A P-3)."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


# 엔진 어댑터 클래스 → heartbeat `engine` 표기(§9-3).
#   ⚠️ **fake 도 반드시 보고한다**(2026-07-17 봉합) — 종전엔 Sy01bEngineAdapter 외 전부 None 이라
#   `includeIfNull:false` 로 키 자체가 빠졌고, admin 기기 카드가 online 인데 "엔진 —"으로 떴다.
#   실 Pi 라도 USB-RS485 어댑터 미장착이면 자동감지(bootstrap.build_engine)가 fake 로 떨어지는데,
#   그게 **정상적인 fake 구동**인지 **보고 누락**인지 운영자가 화면에서 구분할 수 없었다.
_ENGINE_WIRE_NAMES: dict[str, str] = {
    "Sy01bEngineAdapter": "sy01b",
    "FakeEnginePort": "fake",
}


def engine_wire_name(engine: EnginePort) -> str:
    """엔진 어댑터 → heartbeat engine 표기. 미지 어댑터(테스트 더블 등)는 클래스명 그대로.

    관측 필드라 **침묵(None)보다 이름**이 낫다 — 모르는 어댑터도 무엇이 붙었는지 보이게 한다.
    """
    return _ENGINE_WIRE_NAMES.get(type(engine).__name__, type(engine).__name__)


def _order_id_of(command_id: str) -> str:
    """합성키 `{orderId}:{attempt}` → orderId. 콜론 없으면(maintenance mnt-uuid) 그대로."""
    return command_id.rsplit(":", 1)[0]


def _attempt_of(command_id: str) -> int | None:
    """합성키 `{orderId}:{attempt}` → attempt(int). 파싱 불가면 None."""
    if ":" not in command_id:
        return None
    tail = command_id.rsplit(":", 1)[1]
    return int(tail) if tail.isdigit() else None


def _error_backoff_s(poll_interval_s: float, consecutive_errors: int) -> float:
    """폴 오류 연속 횟수 → 대기시간(지수 백오프·상한 60s) — 순수 함수(단위테스트용).

    (감사 P3 봉합·2026-07-15) 서버 다운/타임아웃 시 정확히 poll_interval 로 무한 재시도하던
    재연결 스톰을 지수 백오프로 완화한다. **오류에만 적용** — 정상 유휴는 호출측이 이 함수를
    쓰지 않고 poll_interval_s 그대로 대기한다(유휴 백오프는 주문 반응성을 해친다).
    """
    return min(poll_interval_s * (2.0 ** consecutive_errors), 60.0)


def _default_interpret(command: Command) -> list:
    """recipe==None 폴백 기본 해석기 — 명시 recipe 만 사용(recipeId/fragranceResult 해석 미주입 시).

    recipe 가 None 이면 빈 스텝 → RR 이 empty_recipe 로 CMD_VALIDATION_FAILED drop(토출 0·
    silent-success 금지). 실 폴백 해석(recipeId/flavorRecipe)이 필요하면 deps.interpret 주입.
    """
    return list(command.recipe) if command.recipe else []


@dataclass(frozen=True, slots=True)
class DaemonDeps:
    """데몬 의존성 묶음(포트 주입) — 상시 소비 루프 결선.

    필수: deviceId·command_source·status_sink·engine·ledger(crash-safe 파일 멱등).
    선택: resolver(pump_map)·commandset_source·interpret·시계/id 발급기·주기·logger.
    """

    device_id: str
    command_source: CommandSourcePort
    status_sink: StatusSinkPort
    engine: EnginePort
    ledger: FileIdempotencyLedger
    # 기주 밸브 포트(§9-1 v2·선택) — 미주입이면 valve 스텝 수신 시 fail-closed drop(토출 0).
    valve: "ValvePort | None" = None
    # RR pump_map(§9-1) — 미주입이면 빈 매핑(모든 스텝 unmapped drop·안전측).
    resolver: RecipeResolver | None = None
    # CommandSet 봉투 축(선택 — 미주입 시 기존 Command 축만 소비·무파괴).
    commandset_source: CommandSetSourcePort | None = None
    # recipe==None 폴백 해석(미주입 시 명시 recipe 만).
    interpret: RecipeInterpreter | None = None
    request_id_gen: Callable[[], str] | None = None
    now_iso: Callable[[], str] | None = None
    logger: StructuredLogger | None = None
    # 폴링 간격(초·SENLYT_POLL_INTERVAL_MS 파생). 유휴 시 이 간격만큼 대기(중단 신호 즉응).
    poll_interval_s: float = 1.0
    # heartbeat 주기(초·§9-3·SENLYT_HEARTBEAT_INTERVAL_MS 파생). 기본 10s(서버 표시 창 30s=3주기
    #   정합). 0 이하면 heartbeat 스레드 비활성(테스트가 수동 구동).
    heartbeat_interval_s: float = 10.0
    # 서버로 실어보낼 로그의 최소 심각도(2026-07-18 개편 — "하드웨어 로그 전량 전송" 요구). 기본
    #   **DEBUG** = 모든 레벨(DEBUG·INFO·WARN·ERROR)을 서버로 합류시킨다 — 실기기 진단 시 폴 단위
    #   상세(시리얼 왕복·명령 바이트·힘 선택)까지 admin 에서 본다. DEBUG 는 폴(~1s) 단위라 볼륨이
    #   크므로, 진단이 끝나 볼륨/비용이 문제되면 "INFO"(정상 흐름) 또는 "WARN"(실패만)으로 올린다.
    ship_log_min_severity: str = "DEBUG"
    # 관측 로그(pi.log.*) 배치 flush 주기(초). ERROR/WARN 은 이와 무관하게 즉시 flush 되고,
    #   INFO 등 일반 로그는 이 주기로 묶어 전송(HTTP 오버헤드 절감). 실패는 안 밀리고 맥락은 촘촘.
    trace_flush_interval_s: float = 10.0
    # 긴급정지 신호 폴 소스(§9-4·GET /api/dispenser/estop) — `() -> (active, requestedAt)`.
    #   ⚠️ **명령 폴과 무관한 전용 스레드**가 이걸 fast-poll 한다. 제조 중엔 메인 폴이 블록되므로,
    #   estop 을 큐가 아니라 이 별도 축으로 받아야 제조 중에도 즉시 전 펌프를 멈춘다. None = 비활성
    #   (watcher 미기동 — 테스트/구성 미주입 시 안전 폴백).
    estop_source: Callable[[], "tuple[bool, str | None] | None"] | None = None
    # estop fast-poll 주기(초). estop 은 반응성이 생명이라 heartbeat(10s)보다 짧게.
    estop_poll_interval_s: float = 1.0
    # 긴급정지 공유 래치(§9-4) — **어댑터·시퀀서·데몬이 같은 Event 를 봐야** '공유 _estop' 설계가
    #   실제로 하나다. bootstrap 이 이 이벤트를 만들어 Sy01bEngineAdapter(estop_event=)·시퀀서·여기에
    #   모두 주입한다. 미주입 시 데몬이 자체 생성(테스트/구성 폴백) — 이 경우 _trigger_estop 이
    #   emergency_stop_all 로 어댑터 축도 함께 구동하므로 협조적으로 동작한다.
    estop_event: "threading.Event | None" = None
    # 주기 HW 감시의 기대 주소(2026-07-19 "실시간 판단" 확정) — 부팅 자동인식(pump_map)이 **비어
    #   있어도** 이 주소들은 계속 프로브해 pumpHealth(silent=빨강)로 보고한다. 종전엔 pump_map 이
    #   빈 채 부팅하면(어댑터 미장착 등) 감시 대상 0 → admin 이 부팅 스냅샷 폴백에 갇혔다.
    #   mode 파생(flavor=[1,2]·fragrance=[1,2,3]) — senlytd 가 주입. None = pump_map 만(하위호환).
    hw_watch_addrs: "tuple[int, ...] | None" = None


class SenlytDaemon:
    """headless 디스펜서 데몬 — 상시 소비 루프(SSE→멱등→실행→역보고)."""

    def __init__(self, deps: DaemonDeps) -> None:
        self.deps = deps
        self._log = deps.logger or StructuredLogger(device_id=deps.device_id)
        self._now_iso = deps.now_iso or _now_iso_ms
        self._request_id_gen = deps.request_id_gen or (lambda: str(uuid.uuid4()))

        # stop 플래그(시그널/테스트가 set) — 루프·heartbeat 스레드 공통 종료 신호.
        self._stop = threading.Event()
        # 긴급정지 래치(§9-4) — 감시 스레드가 서버 estop 신호를 보고 set, 복구가 clear. 시퀀서에
        #   주입해 "다음 stage 미시작"을 강제하고, 어댑터는 emergency_stop_all/clear_estop 로 구동한다.
        #   shutdown(_stop)과 분리(estop 후엔 복구가 이어지지만 shutdown 은 종료).
        #   ⚠️ **공유 이벤트 우선** — bootstrap 이 어댑터에도 주입한 같은 Event 를 받으면 어댑터의
        #   in-flight 모션 폴이 이 래치를 직접 본다(설계 '단일 공유 _estop'). 미주입 시 자체 생성.
        self._estop = deps.estop_event if deps.estop_event is not None else threading.Event()
        # estop 상승엣지 판정용 마지막 처리 시각(같은 신호 재처리 방지).
        self._last_estop_at: str | None = None
        # SSE 어댑터에 종료 신호 결선(감사 P3) — 순회 중 SIGTERM 시 제너레이터 즉시 종료(우아한
        #   종료 지연 방지). bootstrap 시점엔 _stop 이 없어 여기서 결선한다(setter 지원 시).
        for src in (deps.command_source, deps.commandset_source):
            setter = getattr(src, "set_stop_event", None)
            if callable(setter):
                setter(self._stop)
        # heartbeat 에 실을 최근 오류(관측·best-effort).
        self._last_error: StatusErrorCode | None = None
        # ship_trace 배치 버퍼(heartbeat/shutdown/sender 가 flush).
        self._trace_lock = threading.Lock()
        self._trace_buffer: list[TraceSpan] = []
        # 관측 로그 즉시 flush 신호 — WARN/ERROR 도착 시 set → sender 가 곧바로 전송(실패는 안 밀림).
        self._trace_flush_now = threading.Event()
        # 버퍼 상한 초과로 드롭한 로그 스팬 수(silent 금지 — flush 시 합성 WARN 으로 보고 후 리셋).
        self._trace_dropped = 0
        # 서버 전송 최소 심각도 서열(게이트 비교값). 기본 INFO.
        self._ship_min_rank = _SEVERITY_RANK.get(str(deps.ship_log_min_severity).upper(), 1)
        self._hb_thread: threading.Thread | None = None
        # 긴급정지 감시 스레드(§9-4) — estop 신호를 fast-poll(별도 축). boot() 에서만 기동.
        self._estop_thread: threading.Thread | None = None
        # 역보고 송신 전용 워커(감사 P2 봉합·2026-07-15) — 제조 임계경로(메인 소비 스레드)에서
        # 네트워크 I/O(report_status·trace flush)를 분리한다. boot() 에서만 기동(heartbeat 결).
        # FIFO 큐(단일 소비 스레드) → 보고 순서(ACCEPTED→PROGRESS→COMPLETED) 보존.
        self._send_queue: "queue.Queue[StatusReport]" = queue.Queue()
        self._sender_thread: threading.Thread | None = None
        # 직전 poll_once 가 오류였는지(감사 P3 — 오류에만 지수 백오프·유휴는 미적용).
        self._last_poll_errored = False
        # 주기 HW 감시 실측(2026-07-19 "데몬이 항상 감시" 요구) — 하트비트 N주기마다 idle 시
        # 펌프 `?` 프로브로 갱신. admin 연결 칩의 실시간 근거(부팅 스냅샷 pumps 와 별개 축).
        self._pump_health: "dict[int, str] | None" = None
        self._hw_checked_at: str | None = None
        self._hb_count = 0
        self._shutdown_lock = threading.Lock()
        self._shutdown_done = False

        resolver = deps.resolver or RecipeResolver({})
        # Sequencer — 진행보고를 publisher 로 흘려 status_sink 로 역보고(OQ·best-effort).
        self._sequencer = PumpSequencer(
            ledger=deps.ledger,
            engine=deps.engine,
            resolver=resolver,
            request_id_gen=self._request_id_gen,
            publisher=self._publish_progress,
            now_iso=self._now_iso,
            valve=deps.valve,
            estop_event=self._estop,  # §9-4 — 감시 스레드 set 시 다음 stage 미시작(하드 중단).
            logger=self._log,  # 스텝 실패 시 raw 엔진코드·detail 을 로그로 남겨 원인 특정 가능.
        )
        # 봉투 전이 sink — status_sink 가 report_command_set_transition 을 제공하면 꽂는다.
        commandset_sink = getattr(deps.status_sink, "report_command_set_transition", None)
        self._dispatcher = Dispatcher(
            device_id=deps.device_id,
            command_source=deps.command_source,
            sequencer=self._sequencer,
            interpret=deps.interpret or _default_interpret,
            commandset_source=deps.commandset_source,
            commandset_sink=commandset_sink if callable(commandset_sink) else None,
            logger=deps.logger,  # 정비 신선도 게이트 관측(2026-07-19)
        )
        self._recovery = BootRecovery(deps.ledger)

    # ─────────────────────────────────────────────────────────────────────
    # 부팅 — 복구 → 상시 소비 루프(stop 까지) → 우아한 종료
    # ─────────────────────────────────────────────────────────────────────

    def boot(self) -> None:
        """부팅 — BootRecovery → heartbeat 스레드 기동 → 상시 소비 루프(stop 플래그까지)."""
        # 구조화 로그(WARN/ERROR) → 서버 trace sink 결선(첫 로그보다 먼저 — 부팅 자가진단·복구 로그도
        #   서버로 합류시킨다). event() 가 이후 매 WARN/ERROR 레코드를 _ship_log 로 넘긴다.
        self._log.bind_sink(self._ship_log)
        self._log.info(
            "senlytd 상시 소비 루프 시작(복구→구독→멱등→실행→역보고)",
            stage=STAGE_PI_RECEIVED,
            device_id=self.deps.device_id,
            pollIntervalS=self.deps.poll_interval_s,
            heartbeatIntervalS=self.deps.heartbeat_interval_s,
        )
        self._recover()
        # 부팅 복구 직후 밸브 강제 닫힘(§L1 선택적 SW 완화·F2 이중안전) — 직전 세션이 SIGKILL 로
        #   죽어 밸브가 열린 채 남았을 수 있다. shutdown 경로의 close_all 이 안 돈 창을 부팅에서 닫는다.
        #   valve 미주입(off)이면 skip. GpioValveAdapter 는 생성 시 이미 닫힘이라 대부분 중복이지만,
        #   물리 잔존 방어의 이중 안전(근본 해결은 HW normally-closed + 워치독).
        if self.deps.valve is not None:
            try:
                self.deps.valve.close_all()
            except Exception:  # noqa: BLE001 — 밸브 닫힘 실패가 부팅을 막지 않는다(best-effort).
                pass
        # 부팅 자가진단(fail-closed 관측·EP-08) — 매핑 펌프 0 이면 제조 보류(heartbeat 만)를 표면화.
        self._boot_self_test()
        # 부팅 시 원장 압축 — append-only 무한 성장·부팅 replay O(N) 완화(감사 P2 봉합·
        # 2026-07-15). 멱등 보증 훼손 없음(최신 상태만 보존). compact 없는 ledger 주입도 무해.
        compact = getattr(self.deps.ledger, "compact", None)
        if callable(compact):
            try:
                compact()
            except Exception:  # noqa: BLE001 — 압축 실패는 부팅을 막지 않는다(원본 유지).
                pass
        self._start_sender()
        self._start_heartbeat()
        self._start_estop_watcher()
        # 폴 오류 연속 횟수 — 오류에만 지수 백오프(감사 P3). 성공/정상유휴 시 0 리셋.
        consecutive_errors = 0
        try:
            while not self._stop.is_set():
                handled = self.poll_once()
                if handled > 0:
                    # 처리분이 있으면 즉시 다음 폴(밀린 큐 빠른 소진) + 오류 카운터 리셋.
                    consecutive_errors = 0
                    continue
                if self._last_poll_errored:
                    # 오류 유휴 — 지수 백오프(재연결 스톰 방지·상한 60s). 감사 P3 봉합.
                    consecutive_errors += 1
                    wait_s = _error_backoff_s(self.deps.poll_interval_s, consecutive_errors)
                else:
                    # ⚠️ 정상 유휴(스트림 정상 종료·도착 0)는 기존 poll 간격 그대로 —
                    # 유휴에 백오프를 걸면 주문 반응성이 나빠진다(백오프는 오류 전용).
                    consecutive_errors = 0
                    wait_s = self.deps.poll_interval_s
                # 대기 — 중단 신호에 즉시 반응(Event.wait).
                self._stop.wait(wait_s)
        finally:
            self.shutdown()

    def _boot_self_test(self) -> bool:
        """부팅 자가진단(EP-08 결·fail-closed **관측**) — 매핑된 펌프가 없으면 제조 보류.

        **실 게이트는 RecipeResolver** 다 — 미매핑 pumpAddr 스텝을 CMD_VALIDATION_FAILED 로
        drop(토출 0)하므로, pump_map 이 비면 어떤 제조도 물리 토출 없이 실패보고된다(fail-closed).
        여기서는 그 상태를 **부팅 시점에 눈에 띄게 표면화**만 한다 — heartbeat 는 계속 나가니
        운영자가 admin 에서 "online 인데 제조 보류(펌프 미인식)"를 구분할 수 있다. 소비 루프는
        멈추지 않는다(정비 명령·heartbeat 는 유효하고, 제조만 RR 이 안전측 drop). 반환 = 제조
        준비(매핑 펌프 ≥1). (펌프 자동인식·용량 결정은 bootstrap.build_resolver 가 이미 수행.)
        """
        pump_map = self._sequencer.resolver.pump_map
        pumps = sorted(pump_map)
        if pumps:
            self._log.info(
                "부팅 자가진단 통과 — 매핑 펌프 확인(제조 수용)",
                stage=STAGE_PI_RECEIVED,
                device_id=self.deps.device_id,
                pumps=pumps,
                valve=self.deps.valve is not None,
            )
            return True
        self._log.warn(
            "부팅 자가진단 — 매핑 펌프 0(제조 보류·수신 주문 CMD_VALIDATION_FAILED drop·토출0·heartbeat 지속)",
            stage=STAGE_ERROR,
            device_id=self.deps.device_id,
        )
        return False

    def poll_once(self) -> int:
        """도착분 1회 소비 — **단일 스트림**에서 봉투+Command 두 축(2026-07-19 귀머거리 봉합).

        구식(poll_commandsets→poll 순차)은 축마다 무한 스트림을 따로 열어, 한 축을 듣는 동안
        다른 축 발행분이 스트림 수명(수 분)만큼 지연됐다 — 신선도 게이트(90s) 익사의 근본.
        poll_stream 은 스트림 1개에서 snapshot 당 두 축을 함께 소비하고, 스트림 수명 상한
        (어댑터 MAX_STREAM_AGE_S=60s)이 좀비까지 자가 회복시킨다. 오류는 삼켜 루프 지속(§10-6).

        반환 = 이번 폴에서 처리(Sequencer 진입)한 건수(0 이면 유휴).
        """
        try:
            handled = self._dispatcher.poll_stream()
            self._last_poll_errored = False  # 정상 경로 — 백오프 미적용(감사 P3).
            return handled
        except Exception as e:  # noqa: BLE001 — 스트림/네트워크 오류를 삼켜 루프 지속.
            # 다음 폴에서 재시도. 역보고는 OQ 로 무손실(§4-6).
            # 오류 플래그 — boot 루프가 이 폴에만 지수 백오프를 건다(감사 P3 봉합·2026-07-15).
            self._last_poll_errored = True
            self._log.warn(
                "폴 주기 오류(삼킴·다음 폴 재시도)",
                stage=STAGE_ERROR,
                device_id=self.deps.device_id,
                error=str(e),
            )
            return 0

    def request_stop(self) -> None:
        """상시 루프 중단 요청(SIGTERM/SIGINT 핸들러·테스트). boot 의 finally 가 shutdown 수행."""
        self._stop.set()
        # PS-06 실동작화(리뷰 P2 봉합): 정지 신호와 함께 sequencer drain 도 세운다 —
        # 진행 중 job 이 **현재 stage 만 완주**하고 다음 stage 를 시작하지 않게(GRACEFUL_PARTIAL).
        # 기존엔 drain 이 shutdown(=submit 반환 후)에서만 세워져 stage 경계 조기 정지가 dead path 였다.
        self._sequencer.request_drain()

    # ─────────────────────────────────────────────────────────────────────
    # BootRecovery — RUNNING→INTERRUPTED 보고(자동 재실행 금지·과토출 0·CR-01)
    # ─────────────────────────────────────────────────────────────────────

    def _recover(self) -> None:
        decisions = self._recovery.plan()
        if not decisions:
            return
        self._log.info(
            "재기동 복구 스캔 — 잔여 결정 처리",
            stage=STAGE_PI_RECEIVED,
            device_id=self.deps.device_id,
            count=len(decisions),
        )
        for d in decisions:
            if d.action is RecoveryAction.REPORT_INTERRUPTED:
                self._report_interrupted(d.command_id)
            else:
                # RECEIVED(미시작·물리 토출 전) → ledger.clear 로 합성키를 fresh 로 되돌린다
                # (감사 P2 봉합·2026-07-15 — CLEAR_AND_FRESH 죽은 액션 실동작화). 종전엔 로그만
                # 남겨 RECEIVED 창(claim 후·모션 전) 크래시 주문이 재전달돼도 DUPLICATE drop
                # (무제조·무실패보고)됐다. RECEIVED 는 mark_running 전 = 물리 모션 0 이라
                # clear 해도 CR-01(재기동 자동재실행 금지) 비위반 — 물리 토출은 어느 경로에서도
                # 시작하지 않고, 재전달분 fresh 소비는 서버 재전달 축의 설계 의도다.
                clear = getattr(self.deps.ledger, "clear", None)
                if callable(clear):
                    try:
                        clear(d.command_id)
                    except Exception:  # noqa: BLE001 — 클리어 실패는 복구를 막지 않는다.
                        pass
                self._log.info(
                    "재기동 복구 — RECEIVED 잔여 클리어(토출 전·재전달분 fresh 소비)",
                    stage=STAGE_PI_RECEIVED,
                    device_id=self.deps.device_id,
                    command_id=d.command_id,
                )

    def _ledger_trace_id(self, command_id: str) -> str:
        """ledger 에서 claim 시 기록한 원 traceId 조회 — 재기동 복구 보고 상관용.

        멱등 원장(FileIdempotencyLedger)이 claim 시점에 원 주문 traceId 를 영속하므로,
        복구 보고(INTERRUPTED status·dispense.failed span)가 원 주문 트레이스와 상관된다.
        traceId 미보유(구엔트리·미주입 ledger)면 "" 로 안전 폴백(기존 동작).
        """
        fn = getattr(self.deps.ledger, "trace_id_of", None)
        if not callable(fn):
            return ""
        try:
            tid = fn(command_id)
        except Exception:  # noqa: BLE001 — 조회 실패는 빈값 폴백(복구를 막지 않는다).
            return ""
        return tid if isinstance(tid, str) else ""

    def _report_interrupted(self, command_id: str) -> None:
        """RUNNING 중단분 → phase=FAILED·errorCode=INTERRUPTED 보고(재토출 없음·§6-7).

        보고 후 ledger 를 FAILED 로 종결 — 다음 재기동에서 재-스캔·재보고되지 않게(멱등).
        재시도는 운영자 축(FAILED→PENDING·attempt++)으로만.
        """
        # 원장이 claim 시 영속한 원 traceId — 복구 보고를 원 주문 트레이스와 상관시킨다.
        trace_id = self._ledger_trace_id(command_id)
        report = StatusReport(
            id=command_id,
            phase=DispensePhase.FAILED.wire,
            step_k=0,
            step_n=0,
            error_code=StatusErrorCode.INTERRUPTED,
            request_id=self._request_id_gen(),
            trace_id=trace_id,  # 원장 traceId(미보유 시 "" — 기존 안전 폴백).
            updated_at=self._now_iso(),
        )
        self._log.warn(
            "재기동 복구 — INTERRUPTED 보고(자동 재실행 금지·토출0)",
            stage=STAGE_ERROR,
            device_id=self.deps.device_id,
            order_id=_order_id_of(command_id),
            command_id=command_id,
            errorCode=StatusErrorCode.INTERRUPTED.wire,
        )
        self._safe_report_status(report)
        # ── 단일 in-flight FIFO 게이트 교착 해소(2026-07-10) ──
        # CommandSet 봉투(전달·관제 관측 축)는 running 을 보고한 뒤 크래시하면 서버에 running 으로
        # 남는다. 서버 게이트는 head 가 running 이면 신규 전달 0(뒤따르는 queued 미노출) → **그
        # 기기 큐가 영구 교착**한다(운영자 재시도의 새 attempt 봉투도 더 늦은 createdAt 이라 head
        # 뒤에 갇힘). 그래서 복구 시 봉투도 FAILED(INTERRUPTED)로 종단시켜 게이트가 다음 head 를
        # 승격하게 한다. 전이 보고는 best-effort(관측 축·재토출과 무관) — 서버가 이미 종단/미존재면
        # 422/404 를 그대로 흡수(멱등·요청 dedup). 주문 status 축(위 report)과 독립.
        self._report_commandset_interrupted(command_id, trace_id)
        # 재기동 복구 span 을 트레이스에 추가하고 즉시 전송 — 크래시로 유실된 진행 span 뒤에
        # dispense.failed(errorCode=INTERRUPTED)가 서버 트레이스에 이어붙어 "제조 중 크래시 →
        # 재기동 복구" 서사가 관측된다(재토출 0 은 ledger 종결이 보장·CR-01).
        # traceId 를 원장에서 복원해 실으므로 원 주문 traceId 트레이스와 상관된다(빈값=미보유 폴백).
        self._buffer_trace(DispensePhase.FAILED, StatusErrorCode.INTERRUPTED, command_id, trace_id)
        self._flush_traces()
        self._last_error = StatusErrorCode.INTERRUPTED
        try:
            self.deps.ledger.mark_settled(command_id, success=False)
        except Exception:  # noqa: BLE001 — 종결 기록 실패는 다음 부팅에서 재보고(무해).
            pass

    def _report_commandset_interrupted(self, command_id: str, trace_id: str = "") -> None:
        """복구 시 CommandSet 봉투를 FAILED(INTERRUPTED)로 종단 — 게이트 교착 해소.

        복구 시점엔 원 봉투 객체가 없어 command_id(=commandSetId·manufacture 합성키)로 최소 봉투를
        재구성해 전이 sink 를 호출한다. sink 는 command_set_id 만 URL 로 쓰고 body 는
        {status, requestId, errorCode} 뿐 — kind/deviceId/createdAt 등은 와이어 미전송이라
        재구성값이면 충분하다. best-effort(관측 축) — 서버 종단/미존재 시 422/404 흡수.
        trace_id 는 원장에서 복원한 원 주문 traceId(상관 메타·미보유 시 "").
        """
        sink = self._dispatcher.commandset_sink
        if sink is None:
            return
        kind = "maintenance" if command_id.startswith(MAINTENANCE_COMMAND_SET_PREFIX) else "manufacture"
        cs = CommandSet(
            command_set_id=command_id,
            device_id=self.deps.device_id,
            kind=kind,
            steps=None,
            status=CommandSetStatus.RUNNING,  # 복구 전 서버가 보유한 상태(전이 근거 — 전진 FAILED).
            created_at="",  # 복구 시점 미보유(전이 PATCH 에 미전송·재정렬 무영향).
            created_by="server",
            source_order_id=_order_id_of(command_id),
            attempt=_attempt_of(command_id),
            trace_id=trace_id or None,  # 원장 복원 traceId(§7 orders.traceId 미러·미보유 시 None).
        )
        try:
            sink(cs, CommandSetStatus.FAILED, StatusErrorCode.INTERRUPTED)
        except Exception:  # noqa: BLE001 — 관측이 복구를 막지 않는다(§10-6).
            pass

    # ─────────────────────────────────────────────────────────────────────
    # 진행보고 publisher — Sequencer → status_sink.report_status(OQ·best-effort)
    # ─────────────────────────────────────────────────────────────────────

    def _publish_progress(
        self,
        phase: DispensePhase,
        step_k: int,
        step_n: int,
        error_code: StatusErrorCode | None,
        command_id: str,
        trace_id: str,
    ) -> None:
        """ProgressPublisher — Sequencer 진행보고를 StatusReport 로 조립해 역보고·trace 버퍼."""
        report = StatusReport(
            id=command_id,
            phase=phase.wire,
            step_k=step_k,
            step_n=step_n,
            error_code=error_code,
            request_id=self._request_id_gen(),
            trace_id=trace_id,
            updated_at=self._now_iso(),
        )
        if error_code is not None:
            self._last_error = error_code
        self._log.info(
            "상태 역보고 flush",
            stage=STAGE_STATUS_REPORT if phase is not DispensePhase.COMPLETED else STAGE_DISPENSE_DONE,
            trace_id=trace_id,
            order_id=_order_id_of(command_id),
            device_id=self.deps.device_id,
            phase=phase.wire,
            stepK=step_k,
            stepN=step_n,
            errorCode=error_code.wire if error_code is not None else None,
        )
        self._buffer_trace(phase, error_code, command_id, trace_id)
        # 송신 전용 워커 분리(감사 P2 봉합·2026-07-15) — sender 스레드가 살아있으면 큐에 넣고
        # 즉시 반환(제조 경로 = O(1)). 느린 링크에서 report_status(OQ flush·건당 10s 타임아웃)·
        # trace POST 가 stage 전환/다음 주문 poll 을 블록하던 것을 분리한다. FIFO 큐 + 단일
        # 소비 스레드라 보고 순서(ACCEPTED→PROGRESS→COMPLETED)는 그대로 보존된다.
        if self._sender_alive():
            self._send_queue.put(report)
            return
        # sender 미기동(boot 없이 직접 호출하는 테스트·복구 경로) — 기존 동기 경로 그대로.
        self._safe_report_status(report)
        # per-report 즉시 전송 — 진행 중 span(dispense.accepted/progress)이 배치(heartbeat 10s)를
        # 기다리지 않고 곧장 서버(POST /api/dispenser/trace)에 도달한다. pi 가 제조 중 SIGKILL 돼도
        # 그 시점까지의 span 이 서버에 남는다(§10-6 best-effort — 전송 실패는 삼킴·제조 무방해).
        self._flush_traces()

    def _safe_report_status(self, report: StatusReport) -> None:
        """status 역보고 — 예외를 삼킨다(네트워크 실패는 OQ 가 흡수·루프 지속·§10-6)."""
        try:
            self.deps.status_sink.report_status(report)
        except Exception:  # noqa: BLE001 — 관측이 제조를 막지 않는다.
            pass

    def _buffer_trace(
        self,
        phase: DispensePhase,
        error_code: StatusErrorCode | None,
        command_id: str,
        trace_id: str,
    ) -> None:
        span = TraceSpan(
            ts=self._now_iso(),
            trace_id=trace_id,
            span_id=secrets.token_hex(8),  # 16-hex
            service="pi",
            event=f"dispense.{phase.wire.lower()}",
            level="ERROR" if error_code is not None else "INFO",
            order_id=_order_id_of(command_id),
            device_id=self.deps.device_id,
            attempt=_attempt_of(command_id),
            detail={"stepPhase": phase.wire}
            if error_code is None
            else {"stepPhase": phase.wire, "errorCode": error_code.wire},
        )
        with self._trace_lock:
            self._trace_buffer.append(span)

    def _ship_log(self, record: dict[str, Any]) -> None:
        """구조화 로그 레코드 → trace 스팬으로 실어 서버 중앙집중(기존 ship_trace 재사용·신규 전송 0).

        pi 운영 로그(SSE 오류·폴 실패·부팅 자가진단·타임아웃)를 web·server 와 **같은 화면·같은
        traceId** 로 합류시킨다 — "왜 멈췄나"를 admin 에서 본다(D32 멈춤 처리의 관측성 짝).

        §5 정책(2026-07-18 개편 — "하드웨어 로그 전량 전송"):
          - severity 게이트 = **ship_log_min_severity 이상**(기본 **DEBUG** = 전 레벨 전송).
            DEBUG·INFO·WARN·ERROR 를 전부 서버로 합류 → 실기기 진단 시 폴 단위 시리얼 왕복·명령
            바이트까지 admin 에서 본다(볼륨 크면 상수만 INFO/WARN 로 올림).
          - **WARN/ERROR 는 즉시 flush**(`_trace_flush_now` set → sender 곧바로 전송), DEBUG/INFO 는
            `trace_flush_interval_s`(기본 10s) 배치. 실패는 안 밀리고 정상 흐름은 촘촘.
          - **단절 유실 0**(2026-07-19): 전송 실패 배치는 어댑터의 TraceSpill(디스크)이 보존하고
            재연결·재부팅 후 sender 주기 flush 가 전량 업로드한다 — 긴 단절 구간 DEBUG/INFO 도
            서버에서 다 보인다(journalctl 불필요). 메모리 버퍼 overflow 도 드롭 대신 스풀로 배출.
          - event = ``pi.log.{severity}`` — dispense 스팬(``dispense.*``)과 구분.
          - traceId 없는 운영 로그는 ``trace_id=""`` 로 실어 admin **로그검색(service=pi)** 축에
            합류한다(타임라인 축은 traceId 있는 것만 엮인다).
          - detail(message·stage·commandSetId·error 등)은 **서버 allowlist(trace.ts)가 2차 게이트** —
            등록 안 된 키는 서버가 폐기한다(비-PII 고정 문자열만 등록).
        """
        severity = str(record.get("severity", "INFO")).upper()
        if _SEVERITY_RANK.get(severity, 1) < self._ship_min_rank:
            return
        detail: dict[str, Any] = {
            "message": record.get("message"),
            "stage": record.get("stage"),
            "commandSetId": record.get("commandSetId"),
        }
        inner = record.get("detail")
        if isinstance(inner, dict):
            detail.update(inner)  # error 등 구조화 필드 — 서버 allowlist 가 최종 반출 결정
        span = TraceSpan(
            ts=str(record.get("ts") or self._now_iso()),
            trace_id=str(record.get("traceId") or ""),
            span_id=secrets.token_hex(8),
            service="pi",
            event=f"pi.log.{severity.lower()}",
            level=severity,
            order_id=record.get("orderId"),
            device_id=record.get("deviceId"),
            detail=detail,
        )
        overflow: "list[TraceSpan] | None" = None
        spill_fn = getattr(self.deps.status_sink, "spill_traces", None)
        with self._trace_lock:
            if len(self._trace_buffer) >= _LOG_TRACE_BUFFER_CAP:
                # 버퍼 상한 도달 — **드롭 대신 디스크 스풀로 배출**(2026-07-19 · 유실 0 원칙).
                #   오래된 것부터 잘라 자리만 비우고, 디스크 IO(spill_fn·fsync)는 **락 밖에서**
                #   한다(리뷰 P1-2 — 락 쥔 채 디스크를 타면 다른 로깅 스레드=제조 스레드가 그
                #   시간만큼 스톨). 스풀 미지원 sink(Fake 등)만 종전 RC5 심각도 인지 드롭으로
                #   폴백(WARN 이상이 저심각도를 밀어냄·건수 계수→합성 WARN).
                if callable(spill_fn):
                    overflow = self._trace_buffer[: _LOG_TRACE_BUFFER_CAP // 2]
                    del self._trace_buffer[: _LOG_TRACE_BUFFER_CAP // 2]
                elif _SEVERITY_RANK.get(severity, 1) >= 2:
                    evict_idx = next(
                        (
                            i
                            for i, s in enumerate(self._trace_buffer)
                            if _SEVERITY_RANK.get(str(s.level).upper(), 1) < 2
                        ),
                        None,
                    )
                    if evict_idx is not None:
                        del self._trace_buffer[evict_idx]  # 오래된 DEBUG/INFO 밀어냄
                    else:
                        # 버퍼 전량 WARN/ERROR — 가장 오래된 것을 드롭(최신 실패 신호가 더 가치).
                        self._trace_buffer.pop(0)
                    self._trace_dropped += 1
                else:
                    # 저심각도(DEBUG/INFO) 신착 — 조용히 버리지 않고 건수를 센다(flush 시 합성 WARN).
                    self._trace_dropped += 1
                    return
            self._trace_buffer.append(span)
        # overflow 배출 — **락 밖 디스크 IO**(fsync 수 ms·TraceSpill 이 자체 락으로 직렬화).
        if overflow is not None and callable(spill_fn):
            try:
                spill_fn(overflow)
            except Exception:  # noqa: BLE001 — 스풀 실패가 로깅(제조)을 막으면 안 된다.
                with self._trace_lock:
                    self._trace_dropped += len(overflow)
        # WARN/ERROR = 실패 신호 → sender 를 깨워 즉시 전송(INFO 는 배치 주기까지 대기).
        if _SEVERITY_RANK.get(severity, 1) >= 2:
            self._trace_flush_now.set()

    # ─────────────────────────────────────────────────────────────────────
    # 역보고 송신 전용 워커 — 제조 임계경로에서 네트워크 I/O 분리(감사 P2·2026-07-15)
    # ─────────────────────────────────────────────────────────────────────

    def _sender_alive(self) -> bool:
        """sender 스레드 기동·생존 여부 — 미기동(boot 없이 직접 호출) 시 동기 경로 폴백."""
        t = self._sender_thread
        return t is not None and t.is_alive()

    def _start_sender(self) -> None:
        """송신 워커 기동 — boot() 에서만 호출(heartbeat 스레드와 같은 패턴·daemon=True)."""
        t = threading.Thread(target=self._sender_loop, name="senlyt-sender", daemon=True)
        self._sender_thread = t
        t.start()

    def _sender_loop(self) -> None:
        """송신 루프 — FIFO 큐에서 report 를 꺼내 역보고 + trace flush(전부 best-effort).

        단일 소비 스레드 + queue.Queue = 보고 순서(ACCEPTED→PROGRESS→COMPLETED) 보존.
        큐가 비면 _stop 이벤트 wait(0.5s)로 대기. stop 이 서도 큐 잔여분은 마저 비운 뒤
        종료한다(잔여가 남아 join 이 늦으면 shutdown 이 동기 drain 으로 인계·무손실 지향).
        OQ flush 도 이 워커가 담당(heartbeat 는 전송만 — 정시성 확보·감사 P2).
        """
        # OQ flush 주기 — heartbeat 주기와 동일 결(비활성 시 30s 기본).
        oq_interval = (
            self.deps.heartbeat_interval_s if self.deps.heartbeat_interval_s > 0 else 30.0
        )
        last_oq_flush = time.monotonic()
        last_trace_flush = time.monotonic()
        while True:
            # ⚠️ **정지 시에도 큐를 순서대로 비운 뒤 종료**(리뷰 P2 #5 봉합·2026-07-15) — sender 가
            #   단일 소비자로 끝까지 남아 FIFO(ACCEPTED→PROGRESS→COMPLETED)를 보존한다. stop 은
            #   "큐가 빈 뒤"에만 종료 조건이다. shutdown 은 이 워커를 넉넉히 join(30s) 한 **뒤에만**
            #   동기 drain 을 돌리므로(아래 shutdown), 정상 경로에서 워커·메인 동시 drain 은 없다
            #   (동시 drain = 순서 역전이었던 종전 결함 제거). 링크가 병리적으로 느려 join 이
            #   30s 를 넘길 때만 동기 drain 이 인계(희귀·유실 방지 우선).
            try:
                report = self._send_queue.get_nowait()
            except queue.Empty:
                if self._stop.is_set():
                    return  # 큐 소진 + 정지 신호 → 종료(잔여 0·순서 보존).
                if time.monotonic() - last_oq_flush >= oq_interval:
                    self._flush_offline_queue()
                    # 봉투 종단(done|failed) 전이 재시도 — 5xx·단절로 유실될 뻔한 terminal 을
                    #   재전송(2026-07-19 P1 — terminal 유실 = delivered 잔류 큐 교착의 진입로).
                    self._flush_commandset_retries()
                    # 관측 로그 스풀도 같은 결로 업로드 — 단절 중 디스크에 쌓인 배치가
                    #   재연결 시 여기서 전량 서버 합류한다(부팅 직후 첫 주기 = 전 세션 잔여 업로드).
                    self._flush_trace_spill()
                    last_oq_flush = time.monotonic()
                # 관측 로그 flush — WARN/ERROR 는 _trace_flush_now 로 즉시 깨어나고,
                #   INFO 등 일반 로그는 trace_flush_interval_s(기본 10s) 배치로 묶어 보낸다.
                signaled = self._trace_flush_now.wait(0.5)
                if signaled or (
                    time.monotonic() - last_trace_flush >= self.deps.trace_flush_interval_s
                ):
                    self._flush_traces()  # 내부에서 _trace_flush_now 클리어
                    last_trace_flush = time.monotonic()
                continue
            self._safe_report_status(report)  # 예외 삼킴(_safe_* — 관측이 제조를 막지 않는다).
            self._flush_traces()
            last_trace_flush = time.monotonic()

    # ─────────────────────────────────────────────────────────────────────
    # heartbeat 10s + ship_trace 배치 flush(별도 스레드) + OQ flush
    # ─────────────────────────────────────────────────────────────────────

    def _start_heartbeat(self) -> None:
        if self.deps.heartbeat_interval_s <= 0:
            return  # 비활성(테스트가 _emit_heartbeat 수동 구동).
        t = threading.Thread(
            target=self._heartbeat_loop, name="senlyt-heartbeat", daemon=True
        )
        self._hb_thread = t
        t.start()

    # ─────────────────────────────────────────────────────────────────────
    # 긴급정지 감시(§9-4) — estop 신호 fast-poll(별도 축·제조 중에도 즉시 선점)
    # ─────────────────────────────────────────────────────────────────────

    def _start_estop_watcher(self) -> None:
        """estop 감시 스레드 기동 — estop_source 미주입 시 비활성(watcher 없음)."""
        if self.deps.estop_source is None or self.deps.estop_poll_interval_s <= 0:
            return
        t = threading.Thread(target=self._estop_watch_loop, name="senlyt-estop", daemon=True)
        self._estop_thread = t
        t.start()

    def _estop_watch_loop(self) -> None:
        """estop 신호를 짧은 주기로 폴한다 — 제조 중 메인 폴이 블록돼도 이 스레드는 계속 돈다."""
        interval = self.deps.estop_poll_interval_s
        while not self._stop.wait(interval):
            self.poll_estop_once()

    def poll_estop_once(self) -> None:
        """estop 신호 1회 폴 — active 상승엣지면 전 펌프 즉시 정지, 해제면 래치 풀기.

        네트워크 오류는 삼켜 다음 폴에서 재시도(estop 미검출은 안전측 아님이지만, 대안이 없다 —
        서버 불통이면 관제도 신호를 못 넣는다). 반환값 없음(관측 로그만).
        """
        source = self.deps.estop_source
        if source is None:
            return
        try:
            result = source()
        except Exception as e:  # noqa: BLE001 — 폴 오류 삼킴(다음 폴 재시도).
            self._log.warn(
                "estop 폴 오류(삼킴·다음 폴 재시도)",
                stage=STAGE_ERROR,
                device_id=self.deps.device_id,
                error=str(e),
            )
            return
        if result is None:
            # 불확정(폴 실패·비-2xx·malformed 응답) — **안전측: 래치를 건드리지 않는다**(fail-safe·2026-07-19
            #   안전 봉합). 서버가 estop 비활성이라고 '확인'된 게 아니라 '확인 불가'라, 여기서 clear 하면
            #   관제 estop 활성 중에 폴 1회 실패로 안전 래치가 풀리는 fail-OPEN 이 된다. fast-poll(~1s)이라
            #   다음 성공 폴에서 정상 상태(active True/False)로 수렴한다.
            return
        active, requested_at = result
        if active:
            # 상승엣지(새 requestedAt)만 처리 — 같은 신호 반복 TR 회피(TR 자체는 멱등이라 무해하나 로그 노이즈).
            if requested_at is not None and requested_at == self._last_estop_at:
                return
            self._last_estop_at = requested_at
            self._trigger_estop()
        elif self._estop.is_set():
            # 신호 해제(복구) → 래치 풀기. 실제 재홈은 초기화 명령이 별도로 수행한다.
            self._estop.clear()
            engine_clear = getattr(self.deps.engine, "clear_estop", None)
            if callable(engine_clear):
                try:
                    engine_clear()
                except Exception:  # noqa: BLE001
                    pass
            self._last_estop_at = None
            self._log.info(
                "긴급정지 해제(신호 clear) — 래치 풀림·복구 대기",
                stage=STAGE_PI_RECEIVED,
                device_id=self.deps.device_id,
            )

    def _trigger_estop(self) -> None:
        """긴급정지 발동 — `_estop` set(시퀀서 다음 stage 미시작) + 전 펌프 즉시 TR(어댑터)."""
        self._estop.set()
        addrs = sorted(self._sequencer.resolver.pump_map)
        engine_estop = getattr(self.deps.engine, "emergency_stop_all", None)
        if callable(engine_estop):
            try:
                engine_estop(addrs)
            except Exception as e:  # noqa: BLE001 — TR 실패해도 래치는 유지(제조 재개 금지).
                self._log.warn(
                    "긴급정지 TR 발송 오류(래치 유지)",
                    stage=STAGE_ERROR,
                    device_id=self.deps.device_id,
                    error=str(e),
                )
        # 기주 밸브 즉시 닫힘(2026-07-19 스위치 래치 도입 동반) — 래치 개방(ON) 중 estop 이 오면
        #   펌프 TR 만으로는 밸브가 열린 채 남는다(기주 유출). close_all = 타이머 취소 포함 멱등.
        valve = self.deps.valve
        if valve is not None:
            try:
                valve.close_all()
            except Exception as e:  # noqa: BLE001 — 밸브 닫힘 실패가 estop 래치를 막지 않는다.
                self._log.warn(
                    "긴급정지 밸브 닫힘 오류(래치 유지)",
                    stage=STAGE_ERROR,
                    device_id=self.deps.device_id,
                    error=str(e),
                )
        self._last_error = StatusErrorCode.INTERRUPTED
        self._log.warn(
            "긴급정지 발동 — 전 펌프 TR + 기주 밸브 close + 진행 제조 하드 중단(§9-4)",
            stage=STAGE_ERROR,
            device_id=self.deps.device_id,
            pumps=addrs,
        )

    def _heartbeat_loop(self) -> None:
        interval = self.deps.heartbeat_interval_s
        # Event.wait(interval): stop 시 즉시 True 반환(빠른 종료) / 타임아웃 시 False → 1회 emit.
        while not self._stop.wait(interval):
            self._emit_heartbeat()

    # 주기 HW 감시 주기 — 하트비트(10s) N회마다 1회 프로브(기본 3 = ~30s). 첫 비트에 즉시 1회.
    HW_HEALTH_EVERY_N_HEARTBEATS = 3

    def _refresh_hw_health(self) -> None:
        """idle 시 펌프 `?` 프로브로 실측 갱신 — 제조·정비 중엔 스킵(버스 무간섭 원칙).

        엔진이 `health_probe` 미제공(Fake 등)이면 no-op. 프로브 도중 제조가 시작되면 즉시 양보
        (부분 결과 폐기 — 낡은 전체 결과가 부분 신선 결과보다 일관적). 결과 의미는 어댑터
        `health_probe` 주석(ok/garbled/silent — 오늘 진단 툴과 동일 판정)."""
        probe = getattr(self.deps.engine, "health_probe", None)
        if not callable(probe) or self._sequencer.is_busy:
            return
        pumps = sorted(self._sequencer.resolver.pump_map)
        if not pumps:
            # 부팅 인식 실패(어댑터 미장착 등)여도 **기대 주소를 계속 실측**한다(실시간 판단 —
            #   2026-07-19 확정). 그래야 admin 이 "무응답(빨강)"을 정직하게 보여주고, USB 가
            #   나중에 꽂히면 ok(초록)로 살아나는 것도 보인다.
            pumps = sorted(self.deps.hw_watch_addrs or ())
        if not pumps:
            return
        health: dict[int, str] = {}
        for addr in pumps:
            if self._sequencer.is_busy or self._stop.is_set():
                return  # 제조 시작/종료 — 부분 결과 버리고 즉시 양보.
            try:
                health[addr] = str(probe(addr))
            except Exception:  # noqa: BLE001 — 프로브 예외 = 무응답 취급(감시는 best-effort).
                health[addr] = "silent"
        self._pump_health = health
        self._hw_checked_at = self._now_iso()
        # 펌프는 응답하는데 부팅 인식(pump_map)이 비어 제조가 보류 중인 상태를 표면화(WARN 즉시
        #   flush — 30s 주기 반복은 "조치 필요 지속" 신호로 의도). 자동 재발견은 백로그(resolver
        #   재구성 필요) — 현 복구 경로는 senlytd 재시작.
        if not self._sequencer.resolver.pump_map and any(v == "ok" for v in health.values()):
            self._log.warn(
                "펌프 응답 감지 — 부팅 인식 실패로 제조 보류 중(재시작 시 재개·자동 재발견 백로그)",
                stage=STAGE_PI_RECEIVED,
                device_id=self.deps.device_id,
                pumpHealth={str(a): s for a, s in health.items()},
            )

    def _emit_heartbeat(self) -> None:
        """heartbeat 전송(queueDepth 파생) + ship_trace 배치 flush + OQ flush — 전부 best-effort."""
        # 주기 HW 감시(idle 한정) — 첫 비트에 즉시 1회(부팅 ~10s 후 admin 에 실측 도달), 이후 N주기.
        self._hb_count += 1
        if self._hb_count % self.HW_HEALTH_EVERY_N_HEARTBEATS == 1:
            self._refresh_hw_health()
        hb = self._build_heartbeat()
        try:
            self.deps.status_sink.send_heartbeat(hb)
        except Exception:  # noqa: BLE001 — 실패는 다음 주기 재시도.
            pass
        # heartbeat 정시성 확보(감사 P2 봉합·2026-07-15) — sender 워커가 살아있으면 flush 들은
        # 워커가 담당하고 여기선 전송만. 종전엔 flush(느린 링크 건당 10s 타임아웃)가 30s 주기를
        # 잡아먹어 presence stale → 조기 reclaim 연쇄를 불렀다. sender 없으면 기존 3종 전부.
        if self._sender_alive():
            return
        self._flush_traces()
        self._flush_offline_queue()

    def _build_heartbeat(self) -> Heartbeat:
        # 기기 연결상태(연결상태 기능·2026-07-19) — 부팅 자동인식 결과를 admin 표시용으로 실어 보낸다.
        #   pumps = 응답한 펌프 주소(pump_map = 부팅 시리얼 probe 결과 = 실연결). valves = GPIO 라인
        #   클레임된 기주밸브 base(available_bases = 핀 사용가능·비-실행 read-only). 미지원 더블 → None.
        pumps = sorted(self._sequencer.resolver.pump_map)
        avail = getattr(self.deps.valve, "available_bases", None)
        valves = avail() if callable(avail) else None
        return self._dispatcher.build_heartbeat(
            engine=engine_wire_name(self.deps.engine),
            last_error=self._last_error,
            pumps=pumps,
            valves=valves,
            # 주기 감시 실측(idle 시 ~30s 주기 갱신) — admin 연결 칩의 실시간 근거.
            pump_health=self._pump_health,
            hw_checked_at=self._hw_checked_at,
        )

    def _flush_traces(self) -> None:
        with self._trace_lock:
            spans = self._trace_buffer
            self._trace_buffer = []
            dropped = self._trace_dropped
            self._trace_dropped = 0
            # 즉시-flush 신호 소비(전송 시작하므로 클리어).
            self._trace_flush_now.clear()
        # 상한 초과 드롭이 있었으면 합성 WARN 으로 남긴다(조용한 유실 금지 — admin 에서 보이게).
        if dropped > 0:
            spans.append(
                TraceSpan(
                    ts=self._now_iso(),
                    trace_id="",
                    span_id=secrets.token_hex(8),
                    service="pi",
                    event="pi.log.warn",
                    level="WARN",
                    device_id=self.deps.device_id,
                    detail={
                        "message": f"관측 로그 버퍼 상한({_LOG_TRACE_BUFFER_CAP}) 초과 — {dropped}건 드롭됨(네트워크 단절/폭주)",
                        "stage": "obs",
                    },
                )
            )
        if not spans:
            return
        try:
            self.deps.status_sink.ship_trace(spans)
        except Exception:  # noqa: BLE001 — trace 유실은 제조를 막지 않는다(§10-6).
            pass

    def _flush_trace_spill(self) -> None:
        """관측 로그 디스크 스풀 업로드(재연결·부팅) — 미지원 sink(Fake 등)는 no-op."""
        flush = getattr(self.deps.status_sink, "flush_trace_spill", None)
        if not callable(flush):
            return
        try:
            flush()
        except Exception:  # noqa: BLE001 — 스풀 flush 실패는 다음 주기 재시도.
            pass

    def _flush_offline_queue(self) -> None:
        flush = getattr(self.deps.status_sink, "flush_offline_queue", None)
        if not callable(flush):
            return
        try:
            flush()
        except Exception:  # noqa: BLE001 — 재연결 flush 실패는 다음 주기 재시도.
            pass

    def _flush_commandset_retries(self) -> None:
        """봉투 종단 전이 재시도 큐 재전송(2026-07-19 P1) — 미지원 sink(Fake 등)는 no-op."""
        flush = getattr(self.deps.status_sink, "flush_commandset_retries", None)
        if not callable(flush):
            return
        try:
            flush()
        except Exception:  # noqa: BLE001 — 실패는 다음 주기 재시도.
            pass

    # ─────────────────────────────────────────────────────────────────────
    # 우아한 종료 — drain → heartbeat 정지 → OQ/trace flush → 자원 정리
    # ─────────────────────────────────────────────────────────────────────

    def shutdown(self) -> None:
        """우아한 종료(멱등) — Sequencer drain → OQ/trace flush → heartbeat 정지 → ledger close."""
        with self._shutdown_lock:
            if self._shutdown_done:
                return
            self._shutdown_done = True

        # 1) 정지 신호 — 루프·heartbeat 스레드 종료 유도.
        self._stop.set()
        # 2) Sequencer drain — 진행 step 완주·다음 step 미시작(PS-06).
        try:
            self._sequencer.request_drain()
        except Exception:  # noqa: BLE001
            pass
        # 3) heartbeat + estop 감시 스레드 정지 대기(자기 자신이면 skip·_stop.wait 로 즉시 깨어남).
        t = self._hb_thread
        if t is not None and t is not threading.current_thread():
            t.join(timeout=5.0)
        et = self._estop_thread
        if et is not None and et is not threading.current_thread():
            et.join(timeout=5.0)
        # 3.5) sender 워커 정지 대기 + 큐 잔여분 동기 drain(감사 P2 + 리뷰 #5·2026-07-15) —
        #   워커는 stop 후 **큐를 순서대로 끝까지 비운 뒤** 종료한다(위 _sender_loop). join(30s)
        #   이 정상적으로 반환하면 큐는 이미 비어 아래 drain 은 no-op = **FIFO 완전 보존**.
        #   워커가 30s 를 넘겨도 살아있으면(병리적 저속 링크) 아래 동기 drain 이 잔여를 인계한다
        #   — 이 희귀 경우만 순서 역전 여지(유실 방지 우선). Queue.get 은 스레드 안전(이중 pop 0).
        st = self._sender_thread
        if st is not None and st is not threading.current_thread():
            st.join(timeout=30.0)
        while True:
            try:
                report = self._send_queue.get_nowait()
            except queue.Empty:
                break
            self._safe_report_status(report)  # 예외 삼킴 — 종료가 전송 실패에 막히지 않는다.
        # 4) 마지막 trace/OQ flush — 밀린 역보고를 최대한 전송(무손실 지향).
        self._flush_traces()
        self._flush_offline_queue()
        self._flush_commandset_retries()  # 봉투 terminal 재시도분도 종료 전 최대한 전송.
        # 5) stage 태스크 풀 정리 + 밸브 강제 닫힘(§9-1 v2 — 설계 §10 "종료 시에도 전 밸브 close").
        try:
            self._sequencer.shutdown()
        except Exception:  # noqa: BLE001
            pass
        valve = self.deps.valve
        if valve is not None:
            try:
                valve.close_all()
            except Exception:  # noqa: BLE001
                pass
        # 6) ledger close(파일 핸들 정리).
        try:
            self.deps.ledger.close()
        except Exception:  # noqa: BLE001
            pass
        self._log.info(
            "senlytd 우아한 종료 완료",
            stage=STAGE_PI_RECEIVED,
            device_id=self.deps.device_id,
        )
