"""적대적 게이트 독립검증(G≠E) — IL-02 / CR-01 / EP-03 의 카운터 급소를 직접 공격.

Dart `test/adversarial_gate_verify_test.dart` 포팅.

기존 테스트가 단일스텝·낙관경로 위주라 아래 급소를 추가 커버:
  - IL-02: **멀티스텝** 중복 dispatch → dispense 카운터 = stepN 정확히 1회분(2배 아님).
  - IL-02: **FAILED 후 동일 commandId 재제출** → DROP(추가 토출 0). attempt++ 만이 재제조 경로.
  - CR-01: **RUNNING crash → 재기동 → 동일 commandId 재제출**(전체 파이프라인) → dispense 0.
  - EP-03: **중간 스텝 empty** / **rawCode!=0 & detail==''** → silent-success 0, PARTIAL FAILED.
"""

from pathlib import Path

import pytest

from senlyt_pi.adapters.fake_engine_adapter import FakeEngineOutcome, FakeEnginePort
from senlyt_pi.core.pump_guard import StatusErrorCode, SyringeSpec
from senlyt_pi.core.wire_messages import RecipeStep
from senlyt_pi.persistence.file_idempotency_ledger import FileIdempotencyLedger, LedgerEntryState
from senlyt_pi.pipeline.boot_recovery import BootRecovery, RecoveryAction
from senlyt_pi.pipeline.pump_sequencer import JobOutcome, PumpSequencer
from senlyt_pi.pipeline.recipe_resolver import RecipeResolver

SPEC = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=1.25)


@pytest.fixture
def ledger_path(tmp_path: Path) -> Path:
    return tmp_path / "l.log"


@pytest.fixture
def fake() -> FakeEnginePort:
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    return fake


def build_seq(ledger: FileIdempotencyLedger, engine: FakeEnginePort) -> PumpSequencer:
    seq_counter = iter(range(10_000))
    return PumpSequencer(
        ledger=ledger,
        engine=engine,
        resolver=RecipeResolver({1: SPEC, 2: SPEC, 3: SPEC}),
        request_id_gen=lambda: f"req-{next(seq_counter)}",
        now_iso=lambda: "2026-07-03T00:00:00.000Z",
    )


def step(idx: int, addr: int, vol: float) -> RecipeStep:
    return RecipeStep(idx=idx, pump_addr=addr, flavor="f", volume=vol)


# ── IL-02: 멀티스텝 중복 → dispense = stepN 정확히 1회분 ──


def test_il02_multistep_duplicate_exact_one_batch(ledger_path, fake):
    """IL-02 멀티스텝 중복 dispatch — dispense 카운터 = 3(정확히 1회분·2배 아님)."""
    ledger = FileIdempotencyLedger.open(ledger_path)
    seq = build_seq(ledger, fake)
    steps = [step(0, 1, 100), step(1, 2, 100), step(2, 3, 100)]

    r1 = seq.submit(command_id="o:1", trace_id="t", steps=steps)
    assert r1.outcome is JobOutcome.COMPLETED
    assert fake.dispense_count == 3, "3스텝 1회 제조 = 3 dispense"

    # 동일 합성키 재제출(중복).
    r2 = seq.submit(command_id="o:1", trace_id="t", steps=steps)
    assert r2.outcome is JobOutcome.DUPLICATE_DROPPED
    assert fake.dispense_count == 3, "중복 → 추가 토출 0. 총 dispense 정확히 3(6 아님)"
    ledger.close()


# ── IL-02: FAILED 후 동일 commandId 재제출도 DROP ──


def test_il02_failed_then_same_command_id_drops(ledger_path, fake):
    """IL-02 FAILED 종결 후 동일 commandId 재제출 → DROP(추가 토출 0)."""
    # 첫 스텝 permanent → PARTIAL FAILED(dispense 1회 발생).
    fake.script_for(1, [FakeEngineOutcome.PERMANENT])
    ledger = FileIdempotencyLedger.open(ledger_path)
    seq = build_seq(ledger, fake)
    steps = [step(0, 1, 100), step(1, 2, 100)]

    r1 = seq.submit(command_id="o:1", trace_id="t", steps=steps)
    assert r1.outcome is JobOutcome.PARTIAL_FAILED
    after_fail = fake.dispense_count  # permanent 스텝 1회.

    # 동일 합성키 재제출 — FAILED 도 DROP 집합.
    r2 = seq.submit(command_id="o:1", trace_id="t", steps=steps)
    assert r2.outcome is JobOutcome.DUPLICATE_DROPPED
    assert fake.dispense_count == after_fail, (
        "FAILED 합성키 재제출 = 추가 토출 0(재제조는 attempt++ 만)"
    )
    ledger.close()


# ── IL-02: attempt++ 새 합성키만 fresh(재제조 성립) ──


def test_il02_attempt_increment_is_fresh(ledger_path, fake):
    """IL-02 attempt++ 새 합성키(o:2)는 fresh — 재제조 성립."""
    fake.script_for(1, [FakeEngineOutcome.PERMANENT])
    ledger = FileIdempotencyLedger.open(ledger_path)
    seq = build_seq(ledger, fake)
    steps = [step(0, 1, 100)]

    r1 = seq.submit(command_id="o:1", trace_id="t", steps=steps)
    assert r1.outcome is JobOutcome.PARTIAL_FAILED
    after_fail = fake.dispense_count

    # 새 attempt = 새 합성키 → fresh(재제조). 이번엔 ack.
    fake.script_for(1, [FakeEngineOutcome.ACK])
    r2 = seq.submit(command_id="o:2", trace_id="t", steps=steps)
    assert r2.outcome is JobOutcome.COMPLETED
    assert fake.dispense_count == after_fail + 1, "attempt++ 는 fresh → 정확히 1회 추가 토출"
    ledger.close()


# ── CR-01: RUNNING crash → 재기동 → 동일 commandId 재제출 → dispense 0(전체 파이프라인) ──


def test_cr01_running_crash_reboot_same_command_id_zero_dispense(ledger_path, fake):
    """CR-01 RUNNING crash 후 재기동 — 동일 commandId 재제출 시 dispense 증가 0."""
    # 1) claim + RUNNING 마킹(제조 시작했으나 완료 전 crash 시뮬).
    l1 = FileIdempotencyLedger.open(ledger_path)
    l1.check_and_claim("run:1")
    l1.mark_running("run:1")
    l1.close()

    # 2) 재기동 — 재open(replay 로 RUNNING 복원).
    l2 = FileIdempotencyLedger.open(ledger_path)
    # BootRecovery: RUNNING → INTERRUPTED 결정(자동재실행 금지·dispense 미호출).
    decisions = BootRecovery(l2).plan()
    assert len(decisions) == 1
    assert decisions[0].action is RecoveryAction.REPORT_INTERRUPTED
    assert decisions[0].from_state is LedgerEntryState.RUNNING

    # 3) status → INTERRUPTED 인가(RUNNING 자동재실행 아님).
    #    REPORT_INTERRUPTED = phase FAILED + errorCode INTERRUPTED(§6-7) 근거.
    assert decisions[0].action is RecoveryAction.REPORT_INTERRUPTED, (
        "RUNNING 은 INTERRUPTED 보고 대상(자동재실행 아님)"
    )

    # 4) 설령 동일 commandId 가 다시 파이프라인에 들어와도 Ledger DROP → dispense 0.
    fresh_fake = FakeEnginePort()
    fresh_fake.script_all(FakeEngineOutcome.ACK)
    seq2 = build_seq(l2, fresh_fake)
    r = seq2.submit(command_id="run:1", trace_id="t", steps=[step(0, 1, 100)])
    assert r.outcome is JobOutcome.DUPLICATE_DROPPED
    assert fresh_fake.dispense_count == 0, "CR-01: 재기동 후 동일 합성키 자동 재토출 절대 0"
    l2.close()


# ── CR-01: dispense 카운터 증가 0 — RUNNING 재기동 후 BootRecovery 만으로는 절대 토출 안 함 ──


def test_cr01_reboot_zero_dispense_structurally(ledger_path):
    """CR-01 재기동 dispense 증가 0 — BootRecovery 는 엔진 미주입(구조적)."""
    l1 = FileIdempotencyLedger.open(ledger_path)
    l1.check_and_claim("run:1")
    l1.mark_running("run:1")
    l1.close()

    probe = FakeEnginePort()
    l2 = FileIdempotencyLedger.open(ledger_path)
    BootRecovery(l2).plan()
    assert probe.dispense_count == 0
    # RUNNING 재기동 후에도 여전히 RUNNING(자동으로 DONE 승격 안 함) — 재실행 판단 근거 보존.
    assert l2.state_of("run:1") is LedgerEntryState.RUNNING
    l2.close()


# ── EP-03: 중간(2번째) 스텝 empty → silent-success 0(PARTIAL FAILED) ──


def test_ep03_mid_step_empty_partial_failed(ledger_path, fake):
    """EP-03 중간 스텝 empty — COMPLETED 오판 0, PARTIAL FAILED."""
    # step0 ack, step1(addr2) empty(무응답) → 재시도 소진 후 실패.
    fake.script_for(1, [FakeEngineOutcome.ACK])
    fake.script_for(
        2,
        [
            FakeEngineOutcome.EMPTY,
            FakeEngineOutcome.EMPTY,
            FakeEngineOutcome.EMPTY,
            FakeEngineOutcome.EMPTY,
        ],
    )
    ledger = FileIdempotencyLedger.open(ledger_path)
    seq = build_seq(ledger, fake)
    r = seq.submit(
        command_id="o:1", trace_id="t", steps=[step(0, 1, 100), step(1, 2, 100)]
    )
    assert r.outcome is not JobOutcome.COMPLETED, "empty 무응답을 성공으로 오판하면 안 됨"
    assert r.outcome is JobOutcome.PARTIAL_FAILED
    assert r.steps_done == 1, "1스텝만 성공, 2번째 empty 실패"
    assert r.error_code is StatusErrorCode.ENGINE_ERROR_TRANSIENT
    ledger.close()


# ── EP-03: 모든 스텝 empty → dispense 는 발생하나 절대 COMPLETED 아님 ──


def test_ep03_all_empty_never_success_and_settles_failed(ledger_path, fake):
    """EP-03 전 스텝 empty — 카운터는 늘지만 성공 판정 0(실패)."""
    fake.script_all(FakeEngineOutcome.EMPTY)
    ledger = FileIdempotencyLedger.open(ledger_path)
    seq = build_seq(ledger, fake)
    r = seq.submit(command_id="o:1", trace_id="t", steps=[step(0, 1, 100)])
    assert not r.is_success, "silent-success 0"
    assert r.outcome is JobOutcome.PARTIAL_FAILED
    # Ledger 도 FAILED 로 종결(DONE 아님) — 재기동 시 재실행 안 함.
    assert ledger.is_settled("o:1")
    assert ledger.state_of("o:1") is LedgerEntryState.FAILED
    ledger.close()


# ── EP-03: rawCode!=0 & detail=='' 도 실패(성공 조건은 rawCode==0 뿐) ──


def test_ep03_nonzero_raw_code_never_success(ledger_path, fake):
    """EP-03 rawCode 비0 & 빈 detail → 성공 아님(성공은 rawCode==0 유일)."""
    # busy(rawCode 1) 를 4회(첫+재시도3) → transient 소진 실패.
    fake.script_for(
        1,
        [
            FakeEngineOutcome.BUSY,
            FakeEngineOutcome.BUSY,
            FakeEngineOutcome.BUSY,
            FakeEngineOutcome.BUSY,
        ],
    )
    ledger = FileIdempotencyLedger.open(ledger_path)
    seq = build_seq(ledger, fake)
    r = seq.submit(command_id="o:1", trace_id="t", steps=[step(0, 1, 100)])
    assert not r.is_success
    assert r.outcome is JobOutcome.PARTIAL_FAILED
    # 물리 시도는 4회(첫+재시도3) 발생했으나 성공 종결 0.
    assert fake.dispense_count == 4, "R=3 → 첫+재시도3 = 4 물리 시도"
    ledger.close()
