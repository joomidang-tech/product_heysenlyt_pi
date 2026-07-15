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
    → heartbeat 30s 주기 send_heartbeat(queueDepth 파생) + ship_trace 배치 flush(별도 스레드).
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
from typing import Callable

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


def _now_iso_ms() -> str:
    """ISO8601 밀리초 Z — TraceSpan.ts / StatusReport.updatedAt 포맷(부록A P-3)."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


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
    # heartbeat 주기(초·§9-3). 0 이하면 heartbeat 스레드 비활성(테스트가 수동 구동).
    heartbeat_interval_s: float = 30.0


class SenlytDaemon:
    """headless 디스펜서 데몬 — 상시 소비 루프(SSE→멱등→실행→역보고)."""

    def __init__(self, deps: DaemonDeps) -> None:
        self.deps = deps
        self._log = deps.logger or StructuredLogger(device_id=deps.device_id)
        self._now_iso = deps.now_iso or _now_iso_ms
        self._request_id_gen = deps.request_id_gen or (lambda: str(uuid.uuid4()))

        # stop 플래그(시그널/테스트가 set) — 루프·heartbeat 스레드 공통 종료 신호.
        self._stop = threading.Event()
        # SSE 어댑터에 종료 신호 결선(감사 P3) — 순회 중 SIGTERM 시 제너레이터 즉시 종료(우아한
        #   종료 지연 방지). bootstrap 시점엔 _stop 이 없어 여기서 결선한다(setter 지원 시).
        for src in (deps.command_source, deps.commandset_source):
            setter = getattr(src, "set_stop_event", None)
            if callable(setter):
                setter(self._stop)
        # heartbeat 에 실을 최근 오류(관측·best-effort).
        self._last_error: StatusErrorCode | None = None
        # ship_trace 배치 버퍼(heartbeat/shutdown 이 flush).
        self._trace_lock = threading.Lock()
        self._trace_buffer: list[TraceSpan] = []
        self._hb_thread: threading.Thread | None = None
        # 역보고 송신 전용 워커(감사 P2 봉합·2026-07-15) — 제조 임계경로(메인 소비 스레드)에서
        # 네트워크 I/O(report_status·trace flush)를 분리한다. boot() 에서만 기동(heartbeat 결).
        # FIFO 큐(단일 소비 스레드) → 보고 순서(ACCEPTED→PROGRESS→COMPLETED) 보존.
        self._send_queue: "queue.Queue[StatusReport]" = queue.Queue()
        self._sender_thread: threading.Thread | None = None
        # 직전 poll_once 가 오류였는지(감사 P3 — 오류에만 지수 백오프·유휴는 미적용).
        self._last_poll_errored = False
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
        )
        self._recovery = BootRecovery(deps.ledger)

    # ─────────────────────────────────────────────────────────────────────
    # 부팅 — 복구 → 상시 소비 루프(stop 까지) → 우아한 종료
    # ─────────────────────────────────────────────────────────────────────

    def boot(self) -> None:
        """부팅 — BootRecovery → heartbeat 스레드 기동 → 상시 소비 루프(stop 플래그까지)."""
        self._log.info(
            "senlytd 상시 소비 루프 시작(복구→구독→멱등→실행→역보고)",
            stage=STAGE_PI_RECEIVED,
            device_id=self.deps.device_id,
            pollIntervalS=self.deps.poll_interval_s,
            heartbeatIntervalS=self.deps.heartbeat_interval_s,
        )
        self._recover()
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

    def poll_once(self) -> int:
        """도착분 1회 소비 — CommandSet 봉투 + Command 축. 오류는 삼켜 루프 지속(§10-6).

        반환 = 이번 폴에서 처리(Sequencer 진입)한 건수(0 이면 유휴).
        """
        try:
            handled = self._dispatcher.poll_commandsets()
            handled += self._dispatcher.poll()
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
        # per-report 즉시 전송 — 진행 중 span(dispense.accepted/progress)이 배치(heartbeat 30s)를
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
                    last_oq_flush = time.monotonic()
                self._stop.wait(0.5)
                continue
            self._safe_report_status(report)  # 예외 삼킴(_safe_* — 관측이 제조를 막지 않는다).
            self._flush_traces()

    # ─────────────────────────────────────────────────────────────────────
    # heartbeat 30s + ship_trace 배치 flush(별도 스레드) + OQ flush
    # ─────────────────────────────────────────────────────────────────────

    def _start_heartbeat(self) -> None:
        if self.deps.heartbeat_interval_s <= 0:
            return  # 비활성(테스트가 _emit_heartbeat 수동 구동).
        t = threading.Thread(
            target=self._heartbeat_loop, name="senlyt-heartbeat", daemon=True
        )
        self._hb_thread = t
        t.start()

    def _heartbeat_loop(self) -> None:
        interval = self.deps.heartbeat_interval_s
        # Event.wait(interval): stop 시 즉시 True 반환(빠른 종료) / 타임아웃 시 False → 1회 emit.
        while not self._stop.wait(interval):
            self._emit_heartbeat()

    def _emit_heartbeat(self) -> None:
        """heartbeat 전송(queueDepth 파생) + ship_trace 배치 flush + OQ flush — 전부 best-effort."""
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
        engine_name = (
            "sy01b"
            if type(self.deps.engine).__name__ == "Sy01bEngineAdapter"
            else None
        )
        return self._dispatcher.build_heartbeat(
            engine=engine_name, last_error=self._last_error
        )

    def _flush_traces(self) -> None:
        with self._trace_lock:
            spans = self._trace_buffer
            self._trace_buffer = []
        if not spans:
            return
        try:
            self.deps.status_sink.ship_trace(spans)
        except Exception:  # noqa: BLE001 — trace 유실은 제조를 막지 않는다(§10-6).
            pass

    def _flush_offline_queue(self) -> None:
        flush = getattr(self.deps.status_sink, "flush_offline_queue", None)
        if not callable(flush):
            return
        try:
            flush()
        except Exception:  # noqa: BLE001 — 재연결 flush 실패는 다음 주기 재시도.
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
        # 3) heartbeat 스레드 정지 대기(자기 자신이면 skip).
        t = self._hb_thread
        if t is not None and t is not threading.current_thread():
            t.join(timeout=5.0)
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
