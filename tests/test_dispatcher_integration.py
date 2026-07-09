"""Dispatcher 통합 테스트 — CS(Fake)→IL→RR→PS→EP→SR 봉합 — SoT §1-1 / §9 / 질의서 §0.

Dart `test/dispatcher_integration_test.dart` 포팅.
deviceId 필터(CS-08)·recipe==None 폴백 해석·fragrance mL→µL 정규화·end-to-end 봉합을
dispense 카운터로 검증. 3개 PASS 게이트(IL-02·CR-01·EP-03)가 통합 경로에서도 성립함을 확인.
"""

from collections import deque
from pathlib import Path
from typing import Iterator

import pytest

from senlyt_pi.adapters.fake_engine_adapter import FakeEngineOutcome, FakeEnginePort
from senlyt_pi.app.dispatcher import Dispatcher, RecipeInterpreter, fragrance_notes_to_steps
from senlyt_pi.core.pump_guard import SyringeSpec
from senlyt_pi.core.wire_messages import Command, RecipeStep
from senlyt_pi.persistence.file_idempotency_ledger import FileIdempotencyLedger
from senlyt_pi.pipeline.pump_sequencer import JobOutcome, PumpSequencer
from senlyt_pi.pipeline.recipe_resolver import RecipeResolver

SPEC = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=1.25)
FRAG_SPEC = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=0.5)


class FakeCommandSource:
    """Fake CommandSource — server-mediated 단일 어댑터(질의서 §0·Q5 종결) 하네스.

    Dart `test/support/fake_command_source.dart` 포팅. 실 SSE 대신 테스트가 command 를 push
    하여 dispatch 봉합을 검증한다. deviceId 필터는 dispatcher 가 하므로 이 fake 는 필터 없이
    그대로 흘린다(필터 검증을 위해). commands() 는 push 된 도착분을 드레인하는 iterator.
    """

    def __init__(self) -> None:
        self._pending: deque[Command] = deque()

    def push(self, c: Command) -> None:
        self._pending.append(c)

    def commands(self, device_id: str) -> Iterator[Command]:
        while self._pending:
            yield self._pending.popleft()


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


@pytest.fixture
def source() -> FakeCommandSource:
    return FakeCommandSource()


def build(
    ledger: FileIdempotencyLedger,
    fake: FakeEnginePort,
    source: FakeCommandSource,
    *,
    interpret: "RecipeInterpreter | None" = None,
) -> Dispatcher:
    seq_counter = iter(range(10_000))
    sequencer = PumpSequencer(
        ledger=ledger,
        engine=fake,
        resolver=RecipeResolver({1: SPEC, 2: SPEC, 5: FRAG_SPEC}),
        request_id_gen=lambda: f"req-{next(seq_counter)}",
        now_iso=lambda: "2026-07-03T00:00:00.000Z",
    )
    return Dispatcher(
        device_id="dev-A",
        command_source=source,
        sequencer=sequencer,
        interpret=interpret if interpret is not None else (lambda c: c.recipe or []),
    )


def cmd(cid: str, device_id: str, *, recipe: "list[RecipeStep] | None" = None) -> Command:
    order_id, attempt = cid.rsplit(":", 1)
    return Command(
        id=cid,
        order_id=order_id,
        attempt=int(attempt),
        device_id=device_id,
        recipe=None if recipe is None else tuple(recipe),
        trace_id=f"trace-{cid}",
        created_at="2026-07-03T00:00:00.000Z",
    )


def step(idx: int, addr: int, vol: float) -> RecipeStep:
    return RecipeStep(idx=idx, pump_addr=addr, flavor="f", volume=vol)


def test_end_to_end_dispatch_once(ledger, fake, source):
    """end-to-end 봉합 — 명령 → COMPLETED (dispatch_once)."""
    dispatcher = build(ledger, fake, source)
    r = dispatcher.dispatch_once(
        cmd("o:1", "dev-A", recipe=[step(0, 1, 100), step(1, 2, 100)])
    )
    assert r.outcome is JobOutcome.COMPLETED
    assert fake.dispense_count == 2


def test_cs08_device_id_filter(ledger, fake, source):
    """CS-08 deviceId 필터 — 타 매장 명령 무시(dispense 0)."""
    dispatcher = build(ledger, fake, source)

    source.push(cmd("o:1", "dev-B", recipe=[step(0, 1, 100)]))  # 타 매장.
    dispatcher.poll()
    assert fake.dispense_count == 0, "deviceId 불일치 → 미소비"

    source.push(cmd("o:2", "dev-A", recipe=[step(0, 1, 100)]))  # 내 매장.
    dispatcher.poll()
    assert fake.dispense_count == 1
    assert len(dispatcher.reports) == 1


def test_recipe_none_fallback_interpretation(ledger, fake, source):
    """recipe==None 폴백 해석 — recipeId/fragranceResult → steps."""

    # fragrance notes → mL→µL 정규화 해석기 주입.
    def interpret(c: Command):
        # amountMl 0.3 → 300µL.
        return fragrance_notes_to_steps(
            [{"name": "rose", "amountMl": 0.3}],
            pump_addr_of=lambda _name: 5,  # fragrance 펌프 addr.
        )

    dispatcher = build(ledger, fake, source, interpret=interpret)
    r = dispatcher.dispatch_once(cmd("o:1", "dev-A", recipe=None))
    assert r.outcome is JobOutcome.COMPLETED
    # 300µL / 0.5mL(FRAG_SPEC) → 12000 × 300 ÷ 500 = 7200 steps. dispense 1회.
    assert fake.dispense_count == 1
    assert fake.dispense_calls[0].volume_ul == 300  # mL→µL 정규화 확인(§6-6).
    assert fake.dispense_calls[0].steps == 7200


def test_integration_il02_stream_duplicate(ledger, fake, source):
    """통합 IL-02 — 스트림 중복 command → DROP(추가 토출 0)."""
    dispatcher = build(ledger, fake, source)
    source.push(cmd("o:1", "dev-A", recipe=[step(0, 1, 100)]))
    dispatcher.poll()
    source.push(cmd("o:1", "dev-A", recipe=[step(0, 1, 100)]))  # 중복.
    dispatcher.poll()
    assert fake.dispense_count == 1, "중복 command.id → 추가 토출 0(IL-02)"


def test_integration_ep03_empty_not_completed(ledger, fake, source):
    """통합 EP-03 — empty 명령 → COMPLETED 아님(silent-success 0)."""
    fake.script_all(FakeEngineOutcome.EMPTY)
    dispatcher = build(ledger, fake, source)
    r = dispatcher.dispatch_once(cmd("o:1", "dev-A", recipe=[step(0, 1, 100)]))
    assert r.outcome is not JobOutcome.COMPLETED
    assert r.outcome is JobOutcome.PARTIAL_FAILED
