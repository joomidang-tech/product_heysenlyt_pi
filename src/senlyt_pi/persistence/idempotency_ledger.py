"""멱등 Ledger 인터페이스 — SoT §4-6 (pi측 dedup) / 부록A P-2.

Dart `lib/persistence/idempotency_ledger.dart` 포팅. pi측 dedup 키 = 합성키
`{orderId}:{attempt}`(= command.id). 재시도 시 attempt 증가로 **fresh 판정** → 재제조 성립.
attempt 증가 없는 status-only 되돌림은 계약 위반(§4-4).
"""

from __future__ import annotations

import enum
from typing import Protocol


class LedgerVerdict(enum.Enum):
    """Ledger 판정 결과 — SoT §4-6."""

    # 처음 본 합성키 — 제조 진행.
    FRESH = "fresh"
    # 이미 처리된(또는 진행중인) 합성키 — DROP(재제조 no-op 방지 대상).
    DUPLICATE = "duplicate"


class IdempotencyLedger(Protocol):
    """멱등 Ledger — 합성키 기준 at-most-once 제조 보장.

    구현체는 crash-safe(전원 단절·재부팅 후에도 판정 유지)를 목표로 한다.
    """

    def check_and_claim(self, command_id: str, trace_id: str = "") -> LedgerVerdict:
        """합성키 `{orderId}:{attempt}` 를 처음 보는가.

        FRESH 면 예약(claim)까지 원자적으로 수행해 동시 중복 처리를 막는 것을 권장한다.
        claim 시점의 `trace_id`(원 주문 traceId)를 함께 영속하여, 재기동 복구 보고가
        원 트레이스와 상관되게 한다(빈 문자열 = 미보유·하위호환).
        """
        ...

    def mark_settled(self, command_id: str, *, success: bool) -> None:
        """제조 완료/실패 종결 기록(재부팅 후 재판정 안정화)."""
        ...

    def is_settled(self, command_id: str) -> bool:
        """진행중 여부(재부팅 복구·recover.decision 판단용)."""
        ...

    def trace_id_of(self, command_id: str) -> str:
        """claim 시 기록한 원 traceId 조회(재기동 복구 보고 상관용). 미보유 = ""(하위호환)."""
        ...


class InMemoryIdempotencyLedger:
    """인메모리 Ledger — **테스트/스켈레톤 전용**(영속 아님).

    ⚠️ 프로덕션 미사용 — 실 crash-safe 영속 어댑터(FileIdempotencyLedger)가 대체한다.
    """

    def __init__(self) -> None:
        self._claimed: set[str] = set()
        self._settled: dict[str, bool] = {}
        # commandId → claim 시점 traceId(재기동 복구 상관용).
        self._trace: dict[str, str] = {}

    def check_and_claim(self, command_id: str, trace_id: str = "") -> LedgerVerdict:
        if command_id in self._claimed:
            return LedgerVerdict.DUPLICATE
        self._claimed.add(command_id)
        if trace_id:
            self._trace[command_id] = trace_id
        return LedgerVerdict.FRESH

    def mark_settled(self, command_id: str, *, success: bool) -> None:
        self._settled[command_id] = success

    def is_settled(self, command_id: str) -> bool:
        return command_id in self._settled

    def trace_id_of(self, command_id: str) -> str:
        return self._trace.get(command_id, "")
