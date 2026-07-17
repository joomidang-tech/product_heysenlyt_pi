"""PumpSequencer 테스트 — SoT §4-5 / §9-2 / 질의서 PS-*·IL-02.

Dart `test/pump_sequencer_test.dart` 포팅.
**PASS 게이트 IL-02(중복토출0)** = dispense 카운터로 객관 검증.
직렬 토출·진행보고·중간 영구오류 안전정지(PARTIAL)·동시1제조 큐잉·graceful.
"""

from pathlib import Path

import pytest

from senlyt_pi.adapters.fake_engine_adapter import FakeEngineOutcome, FakeEnginePort
from senlyt_pi.core.pump_guard import StatusErrorCode, SyringeSpec
from senlyt_pi.core.wire_messages import RecipeStep
from senlyt_pi.persistence.file_idempotency_ledger import FileIdempotencyLedger
from senlyt_pi.pipeline.pump_sequencer import JobOutcome, ProgressPublisher, PumpSequencer
from senlyt_pi.pipeline.recipe_resolver import RecipeResolver

SPEC = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=1.25)


@pytest.fixture
def ledger(tmp_path: Path):
    ledger = FileIdempotencyLedger.open(tmp_path / "l.log")
    yield ledger
    ledger.close()


@pytest.fixture
def fake() -> FakeEnginePort:
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    return fake


def make_seq(
    ledger: FileIdempotencyLedger,
    fake: FakeEnginePort,
    *,
    publisher: "ProgressPublisher | None" = None,
    max_retries: int = 3,
) -> PumpSequencer:
    seq_counter = iter(range(10_000))
    return PumpSequencer(
        ledger=ledger,
        engine=fake,
        resolver=RecipeResolver({1: SPEC, 2: SPEC, 3: SPEC}),
        request_id_gen=lambda: f"req-{next(seq_counter)}",
        publisher=publisher,
        max_retries=max_retries,
        now_iso=lambda: "2026-07-03T00:00:00.000Z",
    )


def step(idx: int, addr: int, vol: float) -> RecipeStep:
    return RecipeStep(idx=idx, pump_addr=addr, flavor="f", volume=vol)


def test_serial_dispense_completed(ledger, fake):
    """직렬 토출 성공 → COMPLETED, dispense = stepN."""
    r = make_seq(ledger, fake).submit(
        command_id="o:1",
        trace_id="t",
        steps=[step(0, 1, 100), step(1, 2, 100), step(2, 3, 100)],
    )
    assert r.outcome is JobOutcome.COMPLETED
    assert r.steps_done == 3
    assert fake.dispense_count == 3


def test_submit_persists_trace_id_in_ledger(ledger, fake):
    """submit 경로가 command 의 traceId 를 원장 claim 에 태워 영속한다(복구 상관 갭 봉합).

    재기동 복구 보고가 원 주문 traceId 로 상관되려면, 제조 시작(claim) 시점에 원장에
    traceId 가 저장돼 있어야 한다. submit → check_and_claim(command_id, trace_id) 왕복 확인.
    """
    make_seq(ledger, fake).submit(
        command_id="o:7",
        trace_id="trace-order-7",
        steps=[step(0, 1, 100)],
    )
    assert ledger.trace_id_of("o:7") == "trace-order-7"


def test_il02_duplicate_command_id_drops(ledger, fake):
    """IL-02 게이트: 중복 command.id → DROP, dispense 0 (재토출 없음)."""
    s = make_seq(ledger, fake)
    s.submit(command_id="o:1", trace_id="t", steps=[step(0, 1, 100)])
    assert fake.dispense_count == 1
    # 동일 합성키 재제출 — DROP.
    dup = s.submit(command_id="o:1", trace_id="t", steps=[step(0, 1, 100)])
    assert dup.outcome is JobOutcome.DUPLICATE_DROPPED
    assert dup.error_code is StatusErrorCode.DUPLICATE_DROPPED
    assert fake.dispense_count == 1, "중복은 추가 토출 0(IL-02)"


def test_il02_duplicate_publishes_nothing(ledger, fake):
    """중복 재제출(DUPLICATE_DROPPED)은 status/span 을 일절 발행하지 않는다(조용한 no-op).

    2026-07-10: 이전엔 여기서 FAILED·DUPLICATE_DROPPED 를 publish 해 완료 주문 트레이스에
    dispense.failed 가짜 실패 span + FAILED status(→422)를 남겼다. 이제 재토출 0 은 유지하되
    관측은 완전 무발행.
    """
    published: list[str] = []
    s = make_seq(
        ledger, fake,
        publisher=lambda phase, k, n, ec, cid, tid: published.append(phase.wire),
    )
    s.submit(command_id="o:1", trace_id="t", steps=[step(0, 1, 100)])
    # 원판 제조는 정상 phase 시퀀스 발행.
    assert published == ["ACCEPTED", "COMPLETED"]
    published.clear()

    dup = s.submit(command_id="o:1", trace_id="t", steps=[step(0, 1, 100)])
    assert dup.outcome is JobOutcome.DUPLICATE_DROPPED
    assert dup.error_code is StatusErrorCode.DUPLICATE_DROPPED  # 리포트 계약 불변.
    assert published == [], "중복은 어떤 phase 도 발행하지 않는다(FAILED span/status 0)"
    assert fake.dispense_count == 1, "재토출 0(IL-02)"


def test_il02_failed_command_id_also_drops(ledger, fake):
    """IL-02: 실패한 command.id 재제출도 DROP (Q1·FAILED 포함)."""
    fake.script_all(FakeEngineOutcome.PERMANENT)
    s = make_seq(ledger, fake)
    first = s.submit(command_id="o:9", trace_id="t", steps=[step(0, 1, 100)])
    assert first.outcome is JobOutcome.PARTIAL_FAILED
    assert fake.dispense_count == 1
    # 실패했어도 같은 합성키 재제출은 DROP(재토출 없음).
    dup = s.submit(command_id="o:9", trace_id="t", steps=[step(0, 1, 100)])
    assert dup.outcome is JobOutcome.DUPLICATE_DROPPED
    assert fake.dispense_count == 1, "FAILED 도 DROP 집합(Q1)"


def test_mid_permanent_safe_stop_partial_failed(ledger, fake):
    """중간 영구오류 안전정지 → PARTIAL FAILED(stepK/N)."""
    # step0 ok, step1 permanent → step2 미시작.
    fake.script_for(1, [FakeEngineOutcome.ACK])
    fake.script_for(2, [FakeEngineOutcome.PERMANENT])
    fake.default_outcome = FakeEngineOutcome.ACK
    r = make_seq(ledger, fake).submit(
        command_id="o:1",
        trace_id="t",
        steps=[step(0, 1, 100), step(1, 2, 100), step(2, 3, 100)],
    )
    assert r.outcome is JobOutcome.PARTIAL_FAILED
    assert r.steps_done == 1  # step0 만 완주.
    assert r.step_n == 3
    assert r.error_code is StatusErrorCode.ENGINE_ERROR_PERMANENT
    # step0(dispense 1) + step1(permanent, dispense 1) = 2. step2 미시작.
    assert fake.dispense_count_for(3) == 0, "step2 미시작(안전정지)"
    assert fake.dispense_count == 2


def test_empty_recipe_validation_failed(ledger, fake):
    """빈 레시피 → CMD_VALIDATION_FAILED, dispense 0."""
    r = make_seq(ledger, fake).submit(command_id="o:1", trace_id="t", steps=[])
    assert r.outcome is JobOutcome.VALIDATION_FAILED
    assert r.error_code is StatusErrorCode.CMD_VALIDATION_FAILED
    assert fake.dispense_count == 0


def test_empty_engine_no_silent_success(ledger, fake):
    """무응답 silent-success 0 — empty steps → PARTIAL FAILED, COMPLETED 아님."""
    fake.script_all(FakeEngineOutcome.EMPTY)
    r = make_seq(ledger, fake, max_retries=1).submit(
        command_id="o:1", trace_id="t", steps=[step(0, 1, 100)]
    )
    assert r.outcome is JobOutcome.PARTIAL_FAILED
    assert r.outcome is not JobOutcome.COMPLETED, "silent-success 금지(EP-03)"
    assert r.steps_done == 0


def test_progress_phase_sequence(ledger, fake):
    """진행보고 phase 시퀀스 — ACCEPTED, PROGRESS×(N-1), COMPLETED."""
    phases: list[str] = []
    r = make_seq(
        ledger, fake, publisher=lambda phase, k, n, ec, cid, tid: phases.append(phase.wire)
    ).submit(
        command_id="o:1",
        trace_id="t",
        steps=[step(0, 1, 100), step(1, 2, 100)],
    )
    assert r.outcome is JobOutcome.COMPLETED
    assert phases == ["ACCEPTED", "PROGRESS", "COMPLETED"]


def test_single_concurrent_job_queueing(ledger, fake):
    """동시 1제조 큐잉 — 두 job 순차 실행(직렬)."""
    s = make_seq(ledger, fake)
    r1 = s.submit(command_id="o:1", trace_id="t", steps=[step(0, 1, 100)])
    r2 = s.submit(command_id="o:2", trace_id="t", steps=[step(0, 1, 100)])
    assert r1.outcome is JobOutcome.COMPLETED
    assert r2.outcome is JobOutcome.COMPLETED
    assert fake.dispense_count == 2
    assert not s.is_busy
    assert s.queue_depth == 0


def test_graceful_finishes_current_step_only(ledger, fake):
    """graceful(SIGTERM) — 현재 step 완주·다음 미시작 → GRACEFUL_PARTIAL."""
    # publisher 안에서 첫 step 후 drain 요청 → 다음 step 미시작.
    holder: dict[str, PumpSequencer] = {}

    def publisher(phase, k, n, ec, cid, tid):
        from senlyt_pi.core.order_status import DispensePhase

        if phase is DispensePhase.PROGRESS and k == 1:
            holder["s"].request_drain()

    s = make_seq(ledger, fake, publisher=publisher)
    holder["s"] = s
    r = s.submit(
        command_id="o:1",
        trace_id="t",
        steps=[step(0, 1, 100), step(1, 2, 100), step(2, 3, 100)],
    )
    assert r.outcome is JobOutcome.GRACEFUL_PARTIAL
    assert r.steps_done == 1, "step0 완주 후 drain → step1 미시작"
    assert fake.dispense_count == 1


def test_graceful_pending_job_not_executed(ledger, fake):
    """graceful 중 대기 job 은 미실행(GRACEFUL_PARTIAL)."""
    s = make_seq(ledger, fake)
    s.request_drain()
    r = s.submit(command_id="o:1", trace_id="t", steps=[step(0, 1, 100)])
    assert r.outcome is JobOutcome.GRACEFUL_PARTIAL
    assert fake.dispense_count == 0


def test_reentrant_submit_rejected_and_never_executed(ledger, fake):
    """재진입 submit 거부(HW 안전) — RuntimeError + 거부된 job 은 이후에도 **절대 실행 안 됨**.

    회귀 케이스: 거부된 job 이 _pending 에 남으면 현재 job 종료 후 drain 루프가
    '거부된' submit 을 그대로 실행해 물리 토출을 일으킨다 — 거부 = 큐 제거여야 한다.
    """
    from senlyt_pi.core.order_status import DispensePhase

    holder: dict[str, PumpSequencer] = {}
    reentry_errors: list[RuntimeError] = []

    def publisher(phase, k, n, ec, cid, tid):
        # 제조 콜백 안에서 재진입 submit 시도(1회만).
        if cid == "o:outer" and phase is DispensePhase.ACCEPTED and not reentry_errors:
            try:
                holder["s"].submit(
                    command_id="o:inner", trace_id="t", steps=[step(0, 2, 100)]
                )
            except RuntimeError as e:
                reentry_errors.append(e)

    s = make_seq(ledger, fake, publisher=publisher)
    holder["s"] = s
    r = s.submit(command_id="o:outer", trace_id="t", steps=[step(0, 1, 100)])

    assert r.outcome is JobOutcome.COMPLETED  # 바깥 job 은 정상 완주.
    assert len(reentry_errors) == 1, "재진입 submit 은 RuntimeError 로 거부"
    assert fake.dispense_count_for(2) == 0, "거부된 job 은 물리 토출 0(큐 제거)"
    assert fake.dispense_count == 1
    assert s.queue_depth == 0, "거부된 job 이 큐에 잔류하지 않음"
    assert not s.is_busy


def test_volume_over_max_validation_failed(ledger, fake):
    """상한초과 volume → CMD_VALIDATION_FAILED, dispense 0 (Code 11 방지)."""
    r = make_seq(ledger, fake).submit(
        command_id="o:1",
        trace_id="t",
        steps=[step(0, 1, 5000)],  # maxVolumeUl=1250 초과.
    )
    assert r.outcome is JobOutcome.VALIDATION_FAILED
    assert fake.dispense_count == 0


# ── 긴급정지 선점(§9-4·2026-07-18) ────────────────────────────────────────────


def test_estop_before_submit_aborts_without_dispense(ledger, fake):
    """estop 래치가 이미 서 있으면 첫 stage 도 시작하지 않는다 → ESTOP_ABORTED·토출 0."""
    import threading

    ev = threading.Event()
    ev.set()
    seq_counter = iter(range(10_000))
    seq = PumpSequencer(
        ledger=ledger,
        engine=fake,
        resolver=RecipeResolver({1: SPEC, 2: SPEC, 3: SPEC}),
        request_id_gen=lambda: f"req-{next(seq_counter)}",
        now_iso=lambda: "2026-07-03T00:00:00.000Z",
        estop_event=ev,
    )
    r = seq.submit(
        command_id="o:1", trace_id="t",
        steps=[step(0, 1, 100), step(1, 2, 100), step(2, 3, 100)],
    )
    assert r.outcome is JobOutcome.ESTOP_ABORTED
    assert r.error_code is StatusErrorCode.INTERRUPTED
    assert fake.dispense_count == 0, "estop 래치 시 토출 0(선점)"


def test_estop_during_manufacture_preempts_in_flight(ledger):
    """제조 중 estop 발동 → 진행 중 스텝 즉시 실패 + 다음 stage 미시작(ESTOP_ABORTED).

    fake·시퀀서가 estop 이벤트를 공유한다. 배경 스레드가 제조 도중 이벤트를 세우면(감시 스레드
    모사), fake 의 _delay 가 즉시 빠져나와 스텝이 실패하고 시퀀서가 하드 중단한다.
    """
    import threading

    ev = threading.Event()
    slow = FakeEnginePort(step_delay_ms=400, estop_event=ev)
    slow.script_all(FakeEngineOutcome.ACK)
    seq_counter = iter(range(10_000))
    seq = PumpSequencer(
        ledger=ledger,
        engine=slow,
        resolver=RecipeResolver({1: SPEC, 2: SPEC, 3: SPEC}),
        request_id_gen=lambda: f"req-{next(seq_counter)}",
        now_iso=lambda: "2026-07-03T00:00:00.000Z",
        estop_event=ev,
    )
    # 제조 시작 후 곧바로 estop(감시 스레드 모사) — 첫 stage 진행 중에 발동.
    timer = threading.Timer(0.05, ev.set)
    timer.start()
    try:
        r = seq.submit(
            command_id="o:1", trace_id="t",
            steps=[step(0, 1, 100), step(1, 2, 100), step(2, 3, 100)],
        )
    finally:
        timer.cancel()
    assert r.outcome is JobOutcome.ESTOP_ABORTED
    assert r.steps_done < 3, "in-flight 선점 — 전 stage 완주 못 함"
