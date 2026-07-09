"""Status Reporter — phase 단조·역행거부·멱등·errorCode 표준·PII 미포함 — SoT §9-2 / §4-5.

Dart `lib/pipeline/status_reporter.dart` 포팅.

책임:
  - phase **단조** 진행 강제(ACCEPTED → PROGRESS → COMPLETED|FAILED). 역행 보고 **거부**(SR).
  - **멱등**: 동일 (id, phase, stepK) 재보고는 호출측이 `would_be_duplicate` 로 억제 —
    pi 는 동일 진행보고를 중복 생성하지 않는다(서버 dedup(§4-6) 이중화).
  - errorCode 표준 7종(§6-7)만 방출. PII 미포함(StatusReport 는 orderId·진행도·상관메타만).
  - phase→WireStatus 매핑은 order_status.py(단조·역행 금지).

**정본 = pi(status 전진 write 주체·§4-5)**. 이 reporter 는 StatusReport 를 조립만 하고
실제 전송(PATCH·OQ flush)은 StatusSinkPort(어댑터) 책임 — 관측이 제조를 막지 않도록 분리.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

from ..core.order_status import DispensePhase, WireStatus, phase_to_wire_status
from ..core.pump_guard import StatusErrorCode
from ..core.wire_messages import StatusReport

# requestId 발급기(테스트 결정성 주입). 기본 = UUID v4 유사(랜덤).
RequestIdGen = Callable[[], str]


def _phase_rank(p: DispensePhase) -> int:
    """phase 순서 등급(단조성 판정용). ACCEPTED < PROGRESS < 종결(COMPLETED|FAILED).

    종결 두 값은 동급(둘 다 최종) — 서로 다른 종결로의 전이는 없다(한 주문 1종결).
    """
    if p is DispensePhase.ACCEPTED:
        return 0
    if p is DispensePhase.PROGRESS:
        return 1
    return 2  # COMPLETED | FAILED.


class PhaseRegressionError(Exception):
    """phase 역행 시도 예외(SR — 단조 위반)."""

    def __init__(self, last: DispensePhase, attempted: DispensePhase) -> None:
        self.last = last
        self.attempted = attempted
        super().__init__(
            f"PhaseRegressionError: {last.wire} -> {attempted.wire} (단조 위반·역행 금지)"
        )


def _default_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class StatusReporter:
    """주문(합성키 id) 단위 Status Reporter.

    인스턴스는 한 command.id(제조 1건)의 진행 상태를 추적하며, 단조성·멱등을 로컬 강제한다.
    """

    def __init__(
        self,
        *,
        command_id: str,
        trace_id: str,
        request_id_gen: RequestIdGen,
        now_iso: Callable[[], str] | None = None,
    ) -> None:
        # `{orderId}:{attempt}` — StatusReport.id.
        self.command_id = command_id
        self.trace_id = trace_id
        self._request_id_gen = request_id_gen
        self._now_iso = now_iso if now_iso is not None else _default_now_iso
        self._last_phase: DispensePhase | None = None
        self._last_step_k = -1

    @property
    def last_phase(self) -> DispensePhase | None:
        """마지막으로 보고한 phase(관찰)."""
        return self._last_phase

    def report(
        self,
        *,
        phase: DispensePhase,
        step_k: int,
        step_n: int,
        error_code: StatusErrorCode | None = None,
    ) -> StatusReport:
        """진행 보고를 조립한다. 단조 위반(역행)은 [PhaseRegressionError] raise."""
        last = self._last_phase
        if last is not None and _phase_rank(phase) < _phase_rank(last):
            raise PhaseRegressionError(last, phase)
        # 종결(COMPLETED|FAILED) 이후 추가 보고 금지(멱등·단조).
        if last is not None and (
            last is DispensePhase.COMPLETED or last is DispensePhase.FAILED
        ):
            raise PhaseRegressionError(last, phase)

        self._last_phase = phase
        self._last_step_k = step_k

        return StatusReport(
            id=self.command_id,
            phase=phase.wire,
            step_k=step_k,
            step_n=step_n,
            # errorCode 는 표준 7종(§6-7)만 — StatusErrorCode enum 이 그 집합을 강제.
            error_code=error_code,
            request_id=self._request_id_gen(),
            trace_id=self.trace_id,
            updated_at=self._now_iso(),
        )

    def would_be_duplicate(self, phase: DispensePhase, step_k: int) -> bool:
        """동일 (phase, stepK) 재보고 여부(멱등 억제 판별용)."""
        return self._last_phase is phase and self._last_step_k == step_k

    def wire_status_for(self, phase: DispensePhase) -> WireStatus:
        """phase → WireStatus(§4-5) — 서버 CAS 가 최종 게이트지만 pi 로컬 선검사에도 사용."""
        return phase_to_wire_status(phase)
