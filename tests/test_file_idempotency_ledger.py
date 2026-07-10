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


def test_trace_id_persisted_and_replayed(ledger_path: Path):
    """claim 시 traceId 영속 → 재open(재부팅) 후에도 조회 가능(복구 상관 근거)."""
    l1 = FileIdempotencyLedger.open(ledger_path)
    assert l1.check_and_claim("o:1", "trace-abc") is LedgerVerdict.FRESH
    l1.mark_running("o:1")  # 전이 레코드(traceId 없음)가 claim traceId 를 clobber 하지 않아야.
    assert l1.trace_id_of("o:1") == "trace-abc"
    l1.close()

    # 재부팅 시뮬 — replay 로 traceId 복원.
    l2 = FileIdempotencyLedger.open(ledger_path)
    assert l2.trace_id_of("o:1") == "trace-abc"
    assert l2.state_of("o:1") is LedgerEntryState.RUNNING
    l2.close()


def test_trace_id_absent_backward_compat(ledger_path: Path):
    """traceId 미전달 claim(구엔트리 하위호환) → trace_id_of 는 빈 문자열."""
    l1 = FileIdempotencyLedger.open(ledger_path)
    l1.check_and_claim("o:1")  # traceId 미전달(기존 시그니처 호출).
    assert l1.trace_id_of("o:1") == ""
    l1.close()

    l2 = FileIdempotencyLedger.open(ledger_path)
    assert l2.trace_id_of("o:1") == ""
    assert l2.trace_id_of("never-seen") == ""  # 미기록 키도 빈 문자열.
    l2.close()


def test_legacy_record_without_traceid_field_replays(ledger_path: Path):
    """구 형식 레코드(traceId 필드 자체 없음) 하위호환 — replay 정상 + traceId 빈값."""
    # 신규 필드 없이 기록된 과거 로그 라인(traceId 키 부재)을 직접 재현.
    with ledger_path.open("w", encoding="utf-8") as f:
        f.write('{"commandId":"old:1","state":"RUNNING","ts":"2026-01-01T00:00:00Z"}\n')

    l = FileIdempotencyLedger.open(ledger_path)
    assert l.check_and_claim("old:1") is LedgerVerdict.DUPLICATE  # 상태 판정 정상.
    assert l.state_of("old:1") is LedgerEntryState.RUNNING
    assert l.trace_id_of("old:1") == ""  # traceId 없으면 빈값(폴백).
    l.close()


def test_trace_id_preserved_across_compact(ledger_path: Path):
    """compact 후에도 traceId 보존(재open 조회 유지)."""
    l1 = FileIdempotencyLedger.open(ledger_path)
    l1.check_and_claim("o:1", "trace-xyz")
    l1.mark_running("o:1")
    l1.compact()
    assert l1.trace_id_of("o:1") == "trace-xyz"
    l1.close()

    l2 = FileIdempotencyLedger.open(ledger_path)
    assert l2.trace_id_of("o:1") == "trace-xyz"
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
