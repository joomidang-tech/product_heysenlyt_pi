"""Boot Recovery (on_boot) — SoT §9-1 / 질의서 Q4(CR-01·CR-02) / §6-7(INTERRUPTED).

Dart `lib/pipeline/boot_recovery.dart` 포팅.

재부팅 시 Ledger replay 로 복원된 각 합성키 상태별 결정:
  - **RUNNING → INTERRUPTED**(CR-01): 전원 단절로 중단된 제조. **자동 재실행 절대 금지**
    (dispense 0). phase=FAILED, errorCode=INTERRUPTED 로 보고만(§6-7·Q7). 재시도는 운영자 축
    (FAILED→PENDING·attempt++)으로만 — pi 가 임의로 재토출하면 이중토출(IL-02 위반).
  - **RECEIVED → 클리어 후 fresh**(CR-02): 수신했으나 제조 미시작(dispense 전) → 안전하게
    클리어하고 새 명령처럼 fresh 처리 가능(아직 물리 토출 없음).
  - **DONE → 무동작**: 이미 완료 종결. 재보고·재토출 없음.
  - **FAILED → 무동작**: 이미 실패 종결(멱등 DROP 집합). 재실행 없음(재주문은 새 attempt).

**CR-01 게이트(재기동 자동재실행 금지)**: 이 모듈은 dispense 를 **호출하지 않는다**(엔진 미주입).
  결정(RecoveryDecision)만 산출하고, INTERRUPTED 보고는 StatusReporter/Sink 가 담당.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass

from ..persistence.file_idempotency_ledger import FileIdempotencyLedger, LedgerEntryState


class RecoveryAction(enum.Enum):
    """재부팅 복구 액션."""

    # RUNNING → INTERRUPTED 보고(자동 재실행 금지·dispense0).
    REPORT_INTERRUPTED = "report_interrupted"
    # RECEIVED → 클리어 후 fresh 재처리 허용(물리 토출 전).
    CLEAR_AND_FRESH = "clear_and_fresh"
    # DONE/FAILED → 무동작.
    NONE = "none"


@dataclass(frozen=True, slots=True)
class RecoveryDecision:
    """합성키별 복구 결정."""

    command_id: str
    action: RecoveryAction
    from_state: LedgerEntryState

    def __str__(self) -> str:
        return f"RecoveryDecision({self.command_id}: {self.from_state.wire} -> {self.action})"


class BootRecovery:
    """Boot Recovery 플래너 — Ledger 상태를 스캔해 결정 목록만 산출(부작용 0).

    **엔진 미주입** — 이 클래스는 어떤 상황에서도 물리 토출을 시작하지 않는다(CR-01 구조적 보장).
    """

    def __init__(self, ledger: FileIdempotencyLedger) -> None:
        self.ledger = ledger

    def plan(self) -> list[RecoveryDecision]:
        """재부팅 복구 결정 목록.

        RUNNING(진행중 중단) → REPORT_INTERRUPTED. RECEIVED(미시작) → CLEAR_AND_FRESH.
        DONE/FAILED → none. 순서: RUNNING 우선(안전 보고), 그 다음 RECEIVED.
        """
        decisions: list[RecoveryDecision] = []

        for cid in self.ledger.running_commands():
            decisions.append(
                RecoveryDecision(
                    command_id=cid,
                    action=RecoveryAction.REPORT_INTERRUPTED,
                    from_state=LedgerEntryState.RUNNING,
                )
            )
        for cid in self.ledger.received_commands():
            decisions.append(
                RecoveryDecision(
                    command_id=cid,
                    action=RecoveryAction.CLEAR_AND_FRESH,
                    from_state=LedgerEntryState.RECEIVED,
                )
            )
        # DONE/FAILED 는 결정 목록에 넣지 않음(none = 무동작).
        return decisions
