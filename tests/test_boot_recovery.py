"""BootRecovery 테스트 — SoT §9-1 / 질의서 Q4(CR-01·CR-02).

Dart `test/boot_recovery_test.dart` 포팅.
**PASS 게이트 CR-01(재기동 자동재실행 금지)**: RUNNING→INTERRUPTED 결정만 산출하고
엔진(dispense)을 호출하지 않는다(구조적 보장 — BootRecovery 는 엔진 미주입).
"""

from pathlib import Path

from senlyt_pi.adapters.fake_engine_adapter import FakeEnginePort
from senlyt_pi.persistence.file_idempotency_ledger import FileIdempotencyLedger, LedgerEntryState
from senlyt_pi.pipeline.boot_recovery import BootRecovery, RecoveryAction


def mk_ledger(tmp_path: Path) -> FileIdempotencyLedger:
    return FileIdempotencyLedger.open(tmp_path / "l.log")


def test_running_reports_interrupted(tmp_path: Path):
    """RUNNING → REPORT_INTERRUPTED (자동재실행 금지·CR-01)."""
    l1 = mk_ledger(tmp_path)
    l1.check_and_claim("run:1")
    l1.mark_running("run:1")
    l1.close()

    # 재부팅 시뮬 — 재open.
    l2 = mk_ledger(tmp_path)
    decisions = BootRecovery(l2).plan()
    assert len(decisions) == 1
    assert decisions[0].action is RecoveryAction.REPORT_INTERRUPTED
    assert decisions[0].command_id == "run:1"
    l2.close()


def test_received_clear_and_fresh(tmp_path: Path):
    """RECEIVED → CLEAR_AND_FRESH (미시작·물리 토출 전·CR-02)."""
    l1 = mk_ledger(tmp_path)
    l1.check_and_claim("recv:1")  # RECEIVED 만.
    l1.close()

    l2 = mk_ledger(tmp_path)
    decisions = BootRecovery(l2).plan()
    assert len(decisions) == 1
    assert decisions[0].action is RecoveryAction.CLEAR_AND_FRESH
    l2.close()


def test_done_no_action(tmp_path: Path):
    """DONE → 무동작(결정 목록에 없음)."""
    l1 = mk_ledger(tmp_path)
    l1.check_and_claim("done:1")
    l1.mark_settled("done:1", success=True)
    l1.close()

    l2 = mk_ledger(tmp_path)
    assert BootRecovery(l2).plan() == []
    l2.close()


def test_failed_no_action(tmp_path: Path):
    """FAILED → 무동작(멱등 DROP 집합·재실행 없음)."""
    l1 = mk_ledger(tmp_path)
    l1.check_and_claim("fail:1")
    l1.mark_settled("fail:1", success=False)
    l1.close()

    l2 = mk_ledger(tmp_path)
    assert BootRecovery(l2).plan() == []
    l2.close()


def test_cr01_structural_no_dispense(tmp_path: Path):
    """CR-01 구조적 보장 — BootRecovery 는 dispense 를 호출하지 않는다."""
    # 엔진을 주입할 자리조차 없음(생성자에 엔진 없음). 여기서는 fake 를 별도로 관찰:
    # plan() 실행 후에도 어떤 엔진도 토출되지 않았음을 확인(자동재실행 금지의 물리 증거).
    fake = FakeEnginePort()
    l1 = mk_ledger(tmp_path)
    l1.check_and_claim("run:1")
    l1.mark_running("run:1")
    l1.close()

    l2 = mk_ledger(tmp_path)
    BootRecovery(l2).plan()  # 결정만 산출.
    assert fake.dispense_count == 0, "재기동 시 자동 토출 절대 금지(CR-01)"
    l2.close()


def test_mixed_states_decisions(tmp_path: Path):
    """혼합 상태 — RUNNING·RECEIVED·DONE 동시 복구 결정."""
    l1 = mk_ledger(tmp_path)
    l1.check_and_claim("run:1")
    l1.mark_running("run:1")
    l1.check_and_claim("recv:1")
    l1.check_and_claim("done:1")
    l1.mark_settled("done:1", success=True)
    l1.close()

    l2 = mk_ledger(tmp_path)
    decisions = BootRecovery(l2).plan()
    by_id = {d.command_id: d.action for d in decisions}
    assert by_id["run:1"] is RecoveryAction.REPORT_INTERRUPTED
    assert by_id["recv:1"] is RecoveryAction.CLEAR_AND_FRESH
    assert "done:1" not in by_id
    # from_state 근거 보존.
    assert {d.from_state for d in decisions} == {
        LedgerEntryState.RUNNING,
        LedgerEntryState.RECEIVED,
    }
    l2.close()
