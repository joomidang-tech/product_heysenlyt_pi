"""CommandSet 봉투 소비 통합 테스트 — 계약 (2026-07-09) + 기존 게이트 회귀 0.

직소비(steps — 폴백 해석 우회) vs 레거시 폴백(steps=None) 분기 · 안전게이트 통과
(µL 상한·미매핑 addr — 서버 신뢰하되 검증) · maintenance 경로 · deviceId 필터 ·
전이 보고(delivered→running→done|failed) · ledger dedup(at-least-once 재전달) · 하트비트.
"""

from collections import deque
from pathlib import Path
from typing import Iterator

import pytest

from senlyt_pi.adapters.fake_engine_adapter import FakeEngineOutcome, FakeEnginePort
from senlyt_pi.app.dispatcher import Dispatcher
from senlyt_pi.core.command_set import CommandSet, CommandSetStatus
from senlyt_pi.core.pump_guard import StatusErrorCode, SyringeSpec
from senlyt_pi.core.wire_messages import RecipeStep
from senlyt_pi.persistence.file_idempotency_ledger import FileIdempotencyLedger
from senlyt_pi.pipeline.pump_sequencer import JobOutcome, PumpSequencer
from senlyt_pi.pipeline.recipe_resolver import RecipeResolver

SPEC = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=1.25)  # max 1250µL.


class FakeCommandSource:
    """기존 Command 축 fake(미사용 — 결선용)."""

    def commands(self, device_id: str) -> Iterator:
        return iter(())


class FakeCommandSetSource:
    """CommandSet 봉투 fake — push 된 도착분을 드레인(필터는 dispatcher 검증용으로 통과)."""

    def __init__(self) -> None:
        self._pending: deque[CommandSet] = deque()

    def push(self, cs: CommandSet) -> None:
        self._pending.append(cs)

    def command_sets(self, device_id: str) -> Iterator[CommandSet]:
        while self._pending:
            yield self._pending.popleft()


class SinkRecorder:
    """전이 보고 sink 기록기 — (commandSetId, status wire, errorCode wire|None)."""

    def __init__(self) -> None:
        self.events: list[tuple[str, str, "str | None"]] = []
        self.raise_on: set[str] = set()

    def __call__(self, cs: CommandSet, status: CommandSetStatus, error_code) -> None:
        if status.wire in self.raise_on:
            raise RuntimeError("sink down")
        self.events.append(
            (cs.command_set_id, status.wire, error_code.wire if error_code else None)
        )

    def of(self, command_set_id: str) -> list[tuple[str, "str | None"]]:
        return [(s, e) for (c, s, e) in self.events if c == command_set_id]


@pytest.fixture
def fake() -> FakeEnginePort:
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    return fake


@pytest.fixture
def harness(tmp_path: Path, fake: FakeEnginePort):
    ledger = FileIdempotencyLedger.open(tmp_path / "l.log")
    seq_counter = iter(range(10_000))
    sequencer = PumpSequencer(
        ledger=ledger,
        engine=fake,
        resolver=RecipeResolver({1: SPEC, 2: SPEC}),
        request_id_gen=lambda: f"req-{next(seq_counter)}",
        now_iso=lambda: "2026-07-09T00:00:00.000Z",
    )
    source = FakeCommandSetSource()
    sink = SinkRecorder()
    interpret_calls: list[str] = []

    def interpret(command):
        interpret_calls.append(command.id)
        # 레거시 폴백 해석(recipe_resolver 강등) 대역 — 유효 스텝 반환.
        return [RecipeStep(idx=0, pump_addr=2, flavor="legacy", volume=200)]

    dispatcher = Dispatcher(
        device_id="dev-A",
        command_source=FakeCommandSource(),
        sequencer=sequencer,
        interpret=interpret,
        commandset_source=source,
        commandset_sink=sink,
        # 신선도 게이트 시계 seam — fixture created_at(2026-07-09)과 같은 시각으로 고정해
        #   기존 시나리오가 전부 "신선한 봉투"로 판정되게 한다(게이트 자체는 신규 테스트가 검증).
        now_s=lambda: 1783555200.0,  # 2026-07-09T00:00:00Z epoch
    )
    yield dispatcher, source, sink, interpret_calls
    ledger.close()


def step(idx: int, addr: int, vol: float) -> RecipeStep:
    return RecipeStep(idx=idx, pump_addr=addr, flavor="f", volume=vol)


def manufacture(cid: str = "o1:1", device_id: str = "dev-A", *, steps, **over) -> CommandSet:
    order_id, attempt = cid.rsplit(":", 1)
    kw = dict(
        command_set_id=cid,
        device_id=device_id,
        kind="manufacture",
        steps=None if steps is None else tuple(steps),
        status=CommandSetStatus.QUEUED,
        created_at="2026-07-09T00:00:00.000Z",
        created_by="server",
        source_order_id=order_id,
        attempt=int(attempt),
        trace_id=f"trace-{cid}",
    )
    kw.update(over)
    return CommandSet(**kw)


def maintenance(cid: str = "mnt-1", device_id: str = "dev-A", *, steps) -> CommandSet:
    return CommandSet(
        command_set_id=cid,
        device_id=device_id,
        kind="maintenance",
        steps=None if steps is None else tuple(steps),
        status=CommandSetStatus.QUEUED,
        created_at="2026-07-09T00:00:00.000Z",
        created_by="operator:op-1",
    )


class TestEnvelopeDirectConsumption:
    def test_steps_consumed_directly_bypassing_interpreter(self, harness, fake):
        """봉투 steps 직소비 — recipe_resolver 폴백 해석(interpret) 우회 + 토출 수행."""
        dispatcher, _, sink, interpret_calls = harness
        r = dispatcher.dispatch_commandset(
            manufacture(steps=[step(0, 1, 100), step(1, 2, 200)])
        )
        assert r is not None and r.outcome is JobOutcome.COMPLETED
        assert fake.dispense_count == 2
        assert interpret_calls == []  # 우회 확인.
        assert sink.of("o1:1") == [("delivered", None), ("running", None), ("done", None)]

    def test_legacy_fallback_when_steps_none(self, harness, fake):
        """steps=None(레거시 폴백 신호) → interpret(recipe_resolver 강등) 경유."""
        dispatcher, _, sink, interpret_calls = harness
        r = dispatcher.dispatch_commandset(manufacture("o2:1", steps=None))
        assert r is not None and r.outcome is JobOutcome.COMPLETED
        assert interpret_calls == ["o2:1"]  # 합성키 그대로 전달(ledger 호환).
        assert fake.dispense_count == 1
        assert sink.of("o2:1")[-1] == ("done", None)

    def test_poll_commandsets_consumes_arrivals(self, harness, fake):
        dispatcher, source, _, _ = harness
        source.push(manufacture("o1:1", steps=[step(0, 1, 100)]))
        source.push(maintenance("mnt-1", steps=[step(0, 2, 300)]))
        assert dispatcher.poll_commandsets() == 2
        assert fake.dispense_count == 2

    def test_device_id_filter(self, harness, fake):
        """CS-08 동형 — 타 기기 봉투는 무시(토출 0·보고 0)."""
        dispatcher, source, sink, _ = harness
        source.push(manufacture("ox:1", device_id="dev-B", steps=[step(0, 1, 100)]))
        assert dispatcher.poll_commandsets() == 0
        assert fake.dispense_count == 0
        assert sink.events == []


class TestSafetyGateStillApplies:
    """서버 신뢰하되 검증 — 봉투 직소비여도 기존 RR 게이트(µL 상한 등) 그대로 통과."""

    def test_volume_over_max_dropped(self, harness, fake):
        dispatcher, _, sink, _ = harness
        r = dispatcher.dispatch_commandset(
            manufacture(steps=[step(0, 1, 1250.1)])  # max 1250µL 초과.
        )
        assert r.outcome is JobOutcome.VALIDATION_FAILED
        assert r.error_code is StatusErrorCode.CMD_VALIDATION_FAILED
        assert fake.dispense_count == 0  # 토출 0.
        assert sink.of("o1:1")[-1] == ("failed", "CMD_VALIDATION_FAILED")

    def test_unmapped_pump_addr_dropped(self, harness, fake):
        dispatcher, _, sink, _ = harness
        r = dispatcher.dispatch_commandset(manufacture(steps=[step(0, 9, 100)]))
        assert r.outcome is JobOutcome.VALIDATION_FAILED
        assert fake.dispense_count == 0

    def test_non_positive_volume_dropped(self, harness, fake):
        dispatcher, _, _, _ = harness
        r = dispatcher.dispatch_commandset(manufacture(steps=[step(0, 1, 0)]))
        assert r.outcome is JobOutcome.VALIDATION_FAILED
        assert fake.dispense_count == 0

    def test_engine_permanent_reports_failed(self, harness, fake):
        """중간 영구오류 안전정지 — failed(ENGINE_ERROR_PERMANENT) 전이 보고."""
        dispatcher, _, sink, _ = harness
        fake.script_for(2, [FakeEngineOutcome.PERMANENT])  # 2번째 스텝(addr 2)에서 영구오류.
        r = dispatcher.dispatch_commandset(
            manufacture(steps=[step(0, 1, 100), step(1, 2, 100)])
        )
        assert r.outcome is JobOutcome.PARTIAL_FAILED
        assert sink.of("o1:1")[-1] == ("failed", "ENGINE_ERROR_PERMANENT")


class TestMaintenance:
    def test_maintenance_executes_steps(self, harness, fake):
        """kind=maintenance(세척 등) — 동일 게이트+Sequencer 경로로 토출·done 보고."""
        dispatcher, _, sink, interpret_calls = harness
        r = dispatcher.dispatch_commandset(
            maintenance(steps=[step(0, 1, 500), step(1, 2, 500)])
        )
        assert r.outcome is JobOutcome.COMPLETED
        assert fake.dispense_count == 2
        assert interpret_calls == []  # maintenance 는 폴백 해석 경로 없음.
        assert sink.of("mnt-1") == [("delivered", None), ("running", None), ("done", None)]

    def test_maintenance_gate_applies(self, harness, fake):
        """maintenance 도 µL 상한 게이트 동일 적용(이중방어)."""
        dispatcher, _, _, _ = harness
        r = dispatcher.dispatch_commandset(maintenance(steps=[step(0, 1, 99999)]))
        assert r.outcome is JobOutcome.VALIDATION_FAILED
        assert fake.dispense_count == 0

    def test_maintenance_steps_none_is_contract_violation(self, harness, fake):
        """maintenance steps=None(계약 위반) → 토출 0·failed(CMD_VALIDATION_FAILED)."""
        dispatcher, _, sink, interpret_calls = harness
        r = dispatcher.dispatch_commandset(maintenance(steps=None))
        assert r.outcome is JobOutcome.VALIDATION_FAILED
        assert fake.dispense_count == 0
        assert interpret_calls == []  # 폴백 경로 진입 금지.
        assert sink.of("mnt-1") == [("delivered", None), ("failed", "CMD_VALIDATION_FAILED")]


class TestIdempotencyAcrossRedelivery:
    def test_manufacture_redelivery_is_silent_noop(self, harness, fake):
        """at-least-once 재전달(성공 주문) — terminal 봉투는 완전한 조용한 no-op.

        재토출 0(IL-02) + 재전달분은 어떤 전이 보고도(delivered/running/done/failed) 없이
        return None. 성공 트레이스를 오염(FAILED status·dispense.failed span·422)시키지 않는다.
        """
        dispatcher, _, sink, _ = harness
        first = dispatcher.dispatch_commandset(manufacture(steps=[step(0, 1, 100)]))
        assert first.outcome is JobOutcome.COMPLETED
        assert fake.dispense_count == 1
        events_after_first = list(sink.of("o1:1"))
        assert events_after_first == [
            ("delivered", None),
            ("running", None),
            ("done", None),
        ]

        # 같은 합성키(이미 DONE) 재전달 → 즉시 return None(전이 0·실행 0·span 0).
        again = dispatcher.dispatch_commandset(manufacture(steps=[step(0, 1, 100)]))
        assert again is None, "terminal 재전달은 조용한 no-op — None 반환"
        assert fake.dispense_count == 1, "재토출 0(IL-02)"
        # 재전달분이 추가한 전이 보고는 0(원판 delivered/running/done 그대로).
        assert list(sink.of("o1:1")) == events_after_first
        # 어떤 failed 전이도 남지 않는다(가짜 실패 0).
        assert [e for e in sink.of("o1:1") if e[0] == "failed"] == []

    def test_maintenance_redelivery_is_silent_noop(self, harness, fake):
        dispatcher, _, sink, _ = harness
        dispatcher.dispatch_commandset(maintenance(steps=[step(0, 1, 100)]))
        events_after_first = list(sink.of("mnt-1"))
        again = dispatcher.dispatch_commandset(maintenance(steps=[step(0, 1, 100)]))
        assert again is None
        assert fake.dispense_count == 1
        assert list(sink.of("mnt-1")) == events_after_first
        assert [e for e in sink.of("mnt-1") if e[0] == "failed"] == []

    def test_new_attempt_is_fresh(self, harness, fake):
        """재제조 = attempt++ → 새 합성키 fresh(§4-4 불변)."""
        dispatcher, _, _, _ = harness
        dispatcher.dispatch_commandset(manufacture("o1:1", steps=[step(0, 1, 100)]))
        r2 = dispatcher.dispatch_commandset(manufacture("o1:2", steps=[step(0, 1, 100)]))
        assert r2.outcome is JobOutcome.COMPLETED
        assert fake.dispense_count == 2


class TestBestEffortReporting:
    def test_sink_exception_does_not_block_manufacture(self, harness, fake):
        """관측이 제조를 막지 않는다(§10-6) — sink 예외 삼킴·토출은 정상 수행."""
        dispatcher, _, sink, _ = harness
        sink.raise_on = {"delivered", "running", "done"}
        r = dispatcher.dispatch_commandset(manufacture(steps=[step(0, 1, 100)]))
        assert r.outcome is JobOutcome.COMPLETED
        assert fake.dispense_count == 1

    def test_no_sink_configured_is_fine(self, tmp_path, fake):
        ledger = FileIdempotencyLedger.open(tmp_path / "l2.log")
        try:
            sequencer = PumpSequencer(
                ledger=ledger,
                engine=fake,
                resolver=RecipeResolver({1: SPEC}),
                request_id_gen=lambda: "req",
                now_iso=lambda: "2026-07-09T00:00:00.000Z",
            )
            dispatcher = Dispatcher(
                device_id="dev-A",
                command_source=FakeCommandSource(),
                sequencer=sequencer,
                interpret=lambda c: [],
            )
            assert dispatcher.poll_commandsets() == 0  # source 미주입 → no-op.
            r = dispatcher.dispatch_commandset(manufacture(steps=[step(0, 1, 100)]))
            assert r.outcome is JobOutcome.COMPLETED
        finally:
            ledger.close()


class TestHeartbeat:
    def test_build_heartbeat_wire(self, harness):
        """§9-3 확장 — queueDepth(유휴=0) + needsCleaning(선택·includeIfNull:false)."""
        dispatcher, _, _, _ = harness
        hb = dispatcher.build_heartbeat()
        assert hb.to_json() == {"deviceId": "dev-A", "queueDepth": 0}

        hb2 = dispatcher.build_heartbeat(
            engine="sy01b",
            last_error=StatusErrorCode.ENGINE_TIMEOUT,
            needs_cleaning=True,
        )
        assert hb2.to_json() == {
            "deviceId": "dev-A",
            "queueDepth": 0,
            "engine": "sy01b",
            "lastError": "ENGINE_TIMEOUT",
            "needsCleaning": True,
        }
