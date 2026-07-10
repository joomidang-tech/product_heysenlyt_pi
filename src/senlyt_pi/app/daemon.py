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

import secrets
import threading
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


def _default_interpret(command: Command) -> list:
    """recipe==None 폴백 기본 해석기 — 명시 recipe 만 사용(recipeId/fragranceResult 해석 미주입 시).

    recipe 가 None 이면 빈 스텝 → RR 이 empty_recipe 로 CMD_VALIDATION_FAILED drop(토출 0·
    silent-success 금지). 실 폴백 해석(recipeId/expoRecipe)이 필요하면 deps.interpret 주입.
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
        # heartbeat 에 실을 최근 오류(관측·best-effort).
        self._last_error: StatusErrorCode | None = None
        # ship_trace 배치 버퍼(heartbeat/shutdown 이 flush).
        self._trace_lock = threading.Lock()
        self._trace_buffer: list[TraceSpan] = []
        self._hb_thread: threading.Thread | None = None
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
        self._start_heartbeat()
        try:
            while not self._stop.is_set():
                handled = self.poll_once()
                # 유휴(도착분 0)면 poll 간격 대기 — 중단 신호에 즉시 반응(Event.wait).
                # 처리분이 있으면 즉시 다음 폴(밀린 큐 빠른 소진).
                if handled == 0:
                    self._stop.wait(self.deps.poll_interval_s)
        finally:
            self.shutdown()

    def poll_once(self) -> int:
        """도착분 1회 소비 — CommandSet 봉투 + Command 축. 오류는 삼켜 루프 지속(§10-6).

        반환 = 이번 폴에서 처리(Sequencer 진입)한 건수(0 이면 유휴).
        """
        try:
            handled = self._dispatcher.poll_commandsets()
            handled += self._dispatcher.poll()
            return handled
        except Exception as e:  # noqa: BLE001 — 스트림/네트워크 오류를 삼켜 루프 지속.
            # 다음 폴에서 재시도. 역보고는 OQ 로 무손실(§4-6).
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
                # RECEIVED(미시작·물리 토출 전) → 서버 재전달분이 fresh 소비되도록 로그만.
                #   (물리 토출은 어느 경로에서도 시작하지 않는다 — CR-01 안전 보장.)
                self._log.info(
                    "재기동 복구 — RECEIVED 잔여(토출 전·재전달 대기)",
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
        self._safe_report_status(report)
        self._buffer_trace(phase, error_code, command_id, trace_id)
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
        # 4) 마지막 trace/OQ flush — 밀린 역보고를 최대한 전송(무손실 지향).
        self._flush_traces()
        self._flush_offline_queue()
        # 5) ledger close(파일 핸들 정리).
        try:
            self.deps.ledger.close()
        except Exception:  # noqa: BLE001
            pass
        self._log.info(
            "senlytd 우아한 종료 완료",
            stage=STAGE_PI_RECEIVED,
            device_id=self.deps.device_id,
        )
