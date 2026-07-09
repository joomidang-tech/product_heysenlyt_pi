"""주문 상태 전이표 — 순수 도메인 (SoT §4). Dart `lib/core/order_status.dart` 포팅.

**정본 = heysenlyt-web `lib/server/orderStatus.ts`.** 이 파일은 그 TS 전이표를
**바이트 동일** 포팅한다 — 한 셀이라도 다르면 이중토출·완료유실이 재발한다(SoT 부록A P-1).

firebase / http 를 모른다(순수 함수만) — 단위테스트가 하드웨어·네트워크 없이 통과.

★F2 반영(SoT §4-2): PENDING→COMPLETED 직행 허용(완료 유실 방지). 전진(forward)은
  모두 허용하고, 막는 것은 비가역 역행(un-complete)뿐이다.
"""

from __future__ import annotations

import enum
from typing import Any


class WireStatus(enum.Enum):
    """와이어 상태 4종 — SoT §4-1. 정확히 이 4개 문자열 리터럴만 경계를 넘는다.

    pi 내부 도메인은 `ERROR` 를 쓸 수 있으나 **경계에서 반드시 FAILED 로 coercion**(역방향 동일).
    """

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

    @property
    def wire(self) -> str:
        """와이어 문자열(대문자 리터럴) — JSON 직렬화·전이표의 진실값."""
        return self.value

    @staticmethod
    def from_wire(v: Any) -> "WireStatus | None":
        """와이어 문자열 → enum. 4종 외 → None(라우트 입력 검증용, TS `isWireStatus` 대응)."""
        if not isinstance(v, str):
            return None
        for s in WireStatus:
            if s.wire == v:
                return s
        return None


def is_wire_status(v: Any) -> bool:
    """알려진 WireStatus 문자열인지 검사(라우트 입력 검증용) — TS `isWireStatus` 대응."""
    return WireStatus.from_wire(v) is not None


# 전이표 (ALLOWED) — SoT §4-2. TS `ALLOWED` 와 **바이트 동일**.
#
# - PENDING    → {PROCESSING, COMPLETED, FAILED}  (COMPLETED 직행 허용·F2)
# - PROCESSING → {COMPLETED, FAILED}
# - COMPLETED  → {}                               (terminal — un-complete 금지)
# - FAILED     → {PENDING}                        (운영자 재시도 유일경로)
_ALLOWED: dict[WireStatus, frozenset[WireStatus]] = {
    WireStatus.PENDING: frozenset(
        {WireStatus.PROCESSING, WireStatus.COMPLETED, WireStatus.FAILED}
    ),
    WireStatus.PROCESSING: frozenset({WireStatus.COMPLETED, WireStatus.FAILED}),
    WireStatus.COMPLETED: frozenset(),
    WireStatus.FAILED: frozenset({WireStatus.PENDING}),
}


class TransitionVerdict(enum.Enum):
    """전이 판정 결과 — TS `TransitionVerdict`."""

    NOOP = "noop"
    APPLY = "apply"
    ILLEGAL = "illegal"


def evaluate_transition(frm: WireStatus, to: WireStatus) -> TransitionVerdict:
    """from → to 전이 평가 — TS `evaluateTransition` 바이트 동일.

      - from == to           → NOOP   (멱등 재적용)
      - ALLOWED[from] ∋ to    → APPLY
      - 그 외                 → ILLEGAL
    """
    if frm is to:
        return TransitionVerdict.NOOP
    return TransitionVerdict.APPLY if to in _ALLOWED[frm] else TransitionVerdict.ILLEGAL


class StatusTransitionError(Exception):
    """updateStatus 실패 신호 — 서버 라우트가 HTTP 상태로 매핑(illegal→422, conflict→409).

    TS `StatusTransitionError` 대응. pi 는 status 전진 write 주체(§4-5)이므로 전이 판정을
    로컬에서 선검사해 불필요한 PATCH 를 줄이는 데 사용한다(서버 CAS 가 최종 게이트).
    """

    def __init__(self, kind: str, current: WireStatus, attempted: WireStatus) -> None:
        # kind: 'illegal' | 'conflict'.
        self.kind = kind
        self.current = current
        self.attempted = attempted
        super().__init__(str(self))

    def __str__(self) -> str:
        if self.kind == "illegal":
            return (
                "StatusTransitionError: illegal transition "
                f"{self.current.wire} -> {self.attempted.wire}"
            )
        return f"StatusTransitionError: expectedFrom mismatch (current={self.current.wire})"


class DispensePhase(enum.Enum):
    """pi 내부 제조 phase — SoT §9-2 status.phase 4종. 단조·역행 금지."""

    ACCEPTED = "ACCEPTED"
    PROGRESS = "PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

    @property
    def wire(self) -> str:
        return self.value

    @staticmethod
    def from_wire(v: Any) -> "DispensePhase | None":
        if not isinstance(v, str):
            return None
        for p in DispensePhase:
            if p.wire == v:
                return p
        return None


def phase_to_wire_status(phase: DispensePhase) -> WireStatus:
    """pi phase → WireStatus 매핑 — SoT §4-5 / §9-2 (단조·역행 금지).

      ACCEPTED / PROGRESS → PROCESSING
      COMPLETED           → COMPLETED
      FAILED              → FAILED
    """
    if phase is DispensePhase.ACCEPTED or phase is DispensePhase.PROGRESS:
        return WireStatus.PROCESSING
    if phase is DispensePhase.COMPLETED:
        return WireStatus.COMPLETED
    return WireStatus.FAILED
