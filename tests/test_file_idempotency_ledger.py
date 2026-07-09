"""FileIdempotencyLedger 테스트 — SoT §4-6 / 질의서 Q1(IL-04·CR-06) / 부록A P-2.

Dart `test/file_idempotency_ledger_test.dart` 포팅.
IL-02 게이트 근거: 합성키 4상태 전부 DROP·fsync 원자 영속·재부팅 replay 복원.
"""

from pathlib import Path

import pytest

from senlyt_pi.persistence.file_idempotency_ledger import (
    FileIdempotencyLedger,
    LedgerEntryState,
)
from senlyt_pi.persistence.idempotency_ledger import LedgerVerdict


@pytest.fixture
def ledger_path(tmp_path: Path) -> Path:
    return tmp_path / "ledger.log"


def test_fresh_then_duplicate(ledger_path: Path):
    """처음 본 합성키 = fresh, 재관찰 = duplicate."""
    ledger = FileIdempotencyLedger.open(ledger_path)
    assert ledger.check_and_claim("order-1:1") is LedgerVerdict.FRESH
    assert ledger.check_and_claim("order-1:1") is LedgerVerdict.DUPLICATE
    ledger.close()


def test_all_four_states_drop(ledger_path: Path):
    """4상태 전부 DROP — RECEIVED/RUNNING/DONE/FAILED 모두 재claim=duplicate (Q1)."""
    for settle in (None, "running", "done", "failed"):
        ledger = FileIdempotencyLedger.open(ledger_path)
        cid = "o:1"
        assert ledger.check_and_claim(cid) is LedgerVerdict.FRESH
        if settle == "running":
            ledger.mark_running(cid)
        if settle == "done":
            ledger.mark_settled(cid, success=True)
        if settle == "failed":
            ledger.mark_settled(cid, success=False)
        # 어떤 상태든 재claim 은 duplicate.
        assert ledger.check_and_claim(cid) is LedgerVerdict.DUPLICATE, (
            f"state={settle} 도 DROP 이어야"
        )
        ledger.close()
        ledger_path.unlink()


def test_attempt_increment_is_fresh(ledger_path: Path):
    """attempt 증가 = 새 합성키 = fresh (재제조 성립·§4-4)."""
    ledger = FileIdempotencyLedger.open(ledger_path)
    assert ledger.check_and_claim("order-9:1") is LedgerVerdict.FRESH
    ledger.mark_settled("order-9:1", success=False)
    # 같은 attempt 재시도는 DROP.
    assert ledger.check_and_claim("order-9:1") is LedgerVerdict.DUPLICATE
    # attempt++ = 새 합성키 = fresh.
    assert ledger.check_and_claim("order-9:2") is LedgerVerdict.FRESH
    ledger.close()


def test_fsync_persistence_across_reopen(ledger_path: Path):
    """fsync 영속 — 재open 후 duplicate 판정 유지(crash-safe)."""
    l1 = FileIdempotencyLedger.open(ledger_path)
    l1.check_and_claim("o:1")
    l1.mark_running("o:1")
    l1.close()

    # 재open(재부팅 시뮬).
    l2 = FileIdempotencyLedger.open(ledger_path)
    assert l2.check_and_claim("o:1") is LedgerVerdict.DUPLICATE
    assert l2.state_of("o:1") is LedgerEntryState.RUNNING
    l2.close()


def test_replay_running_received_scan(ledger_path: Path):
    """replay — RUNNING/RECEIVED 스캔(재부팅 복구 근거·CR-01/CR-02)."""
    l1 = FileIdempotencyLedger.open(ledger_path)
    l1.check_and_claim("run:1")
    l1.mark_running("run:1")
    l1.check_and_claim("recv:1")  # RECEIVED(미시작).
    l1.check_and_claim("done:1")
    l1.mark_settled("done:1", success=True)
    l1.close()

    l2 = FileIdempotencyLedger.open(ledger_path)
    assert l2.running_commands() == ["run:1"]
    assert l2.received_commands() == ["recv:1"]
    assert l2.is_settled("done:1") is True
    l2.close()


def test_partial_frame_ignored(ledger_path: Path):
    """부분 프레임(잘린 마지막 라인) 무시 — crash-safe."""
    l1 = FileIdempotencyLedger.open(ledger_path)
    l1.check_and_claim("o:1")
    l1.close()
    # 전원 단절로 잘린 라인 append(불완전 JSON).
    with ledger_path.open("a", encoding="utf-8") as f:
        f.write('{"commandId":"o:2","stat')

    l2 = FileIdempotencyLedger.open(ledger_path)
    assert l2.check_and_claim("o:1") is LedgerVerdict.DUPLICATE  # 온전한 레코드는 유지.
    assert l2.check_and_claim("o:2") is LedgerVerdict.FRESH  # 잘린 레코드는 무시.
    l2.close()


def test_compact_atomic_swap(ledger_path: Path):
    """compact — 최신 상태만 남기고 atomic swap 후 판정 유지."""
    l1 = FileIdempotencyLedger.open(ledger_path)
    l1.check_and_claim("o:1")
    l1.mark_running("o:1")
    l1.mark_settled("o:1", success=True)
    l1.compact()
    assert l1.check_and_claim("o:1") is LedgerVerdict.DUPLICATE
    l1.close()

    l2 = FileIdempotencyLedger.open(ledger_path)
    assert l2.state_of("o:1") is LedgerEntryState.DONE
    l2.close()
