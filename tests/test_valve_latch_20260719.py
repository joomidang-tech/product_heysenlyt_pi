"""기주 밸브 ON/OFF 스위치(래치 개방) — 2026-07-19 사용자 요청.

관제 "밸브 제어" 스위치: ON = op:"open"(openSec 상한 뒤 어댑터 타이머 자동 닫힘·비블로킹) /
OFF = op:"close"(즉시 강제 닫힘·멱등). 무기한 개방 금지가 핵심 안전 불변식 — 래치도
auto_close_sec(관제 10s ≤ 어댑터 max_open_sec 15s) 안에서만 열린다.

층별 검증: 어댑터(타이머·상호배타) → wire(op 왕복) → RR(게이트) → 시퀀서(디스패치) → 데몬(estop 닫힘).
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from senlyt_pi.adapters.fake_engine_adapter import FakeEngineOutcome, FakeEnginePort
from senlyt_pi.adapters.valve_adapter import FakeValveAdapter
from senlyt_pi.core.pump_guard import SyringeSpec
from senlyt_pi.core.wire_messages import RecipeStep
from senlyt_pi.persistence.file_idempotency_ledger import FileIdempotencyLedger
from senlyt_pi.pipeline.pump_sequencer import JobOutcome, PumpSequencer
from senlyt_pi.pipeline.recipe_resolver import (
    RecipeResolver,
    RecipeValidationError,
    ResolvedValveStep,
)

SPEC = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=1.25)


@pytest.fixture
def ledger(tmp_path: Path):
    ledger = FileIdempotencyLedger.open(tmp_path / "l.log")
    yield ledger
    ledger.close()


def make_seq(ledger: FileIdempotencyLedger, valve: FakeValveAdapter) -> PumpSequencer:
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    seq_counter = iter(range(10_000))
    return PumpSequencer(
        ledger=ledger,
        engine=fake,
        resolver=RecipeResolver({1: SPEC, 2: SPEC}),
        request_id_gen=lambda: f"req-{next(seq_counter)}",
        now_iso=lambda: "2026-07-19T00:00:00.000Z",
        valve=valve,
    )


def latch_step(base: str = "sour", *, op: str | None = "open", open_sec: float | None = 10.0):
    return RecipeStep(
        idx=0,
        pump_addr=-2,
        flavor=f"base:{base}",
        volume=0.0,
        kind="valve",
        stage=0,
        base=base,
        volume_ml=0.0,
        open_sec=open_sec,
        op=op,
    )


# ── A. 어댑터 — 비블로킹 래치 + 타이머 자동 닫힘 + 상호배타. ─────────────────────────


class TestFakeAdapterLatch:
    def test_open_latch_is_nonblocking_and_auto_closes(self):
        v = FakeValveAdapter(max_open_sec=15.0)
        t0 = time.monotonic()
        r = v.open_latch("sour", 0.05)
        assert r.ok and r.open_sec == 0.05
        assert time.monotonic() - t0 < 0.04, "open_latch 는 즉시 반환(비블로킹)"
        assert v.latched == "sour"
        time.sleep(0.2)  # 타이머 만료 대기.
        assert v.latched is None, "auto_close_sec 뒤 타이머가 자동으로 닫는다(무기한 금지)"
        assert v.close_all_calls >= 1

    def test_close_all_cancels_latch_immediately(self):
        v = FakeValveAdapter(max_open_sec=15.0)
        assert v.open_latch("normal", 10.0).ok
        assert v.latched == "normal"
        v.close_all()  # 스위치 OFF — 10초를 기다리지 않는다.
        assert v.latched is None

    def test_latch_mutual_exclusion(self):
        # 상호배타 — 신기주 ON 중 베이스 ON 이면 신기주는 닫히고 교체된다(한 번에 밸브 1개).
        v = FakeValveAdapter(max_open_sec=15.0)
        assert v.open_latch("sour", 10.0).ok
        assert v.open_latch("normal", 10.0).ok
        assert v.latched == "normal"

    def test_latch_validation_fail_closed(self):
        v = FakeValveAdapter(max_open_sec=15.0)
        assert not v.open_latch("sour", 0.0).ok  # 0 이하 거부.
        assert not v.open_latch("sour", 16.0).ok  # 상한 초과 거부(무기한 개방 금지).
        assert not v.open_latch("vodka", 5.0).ok  # 미지의 base 거부.
        assert v.latched is None

    def test_timed_dispense_during_latch_ends_latch(self):
        # 래치 중 시간축 토출(제조/점검) — 상호배타로 래치 종료 + stale 타이머 close 방지.
        v = FakeValveAdapter(max_open_sec=15.0)
        assert v.open_latch("sour", 10.0).ok
        assert v.dispense_volume("normal", 20.0).ok
        assert v.latched is None


# ── B. wire — valve op 왕복(from_json/to_json 대칭·openSec 유실 봉합). ─────────────


class TestWireValveOp:
    def test_from_json_parses_op(self):
        step = RecipeStep.from_json(
            {"kind": "valve", "idx": 0, "base": "sour", "op": "open", "openSec": 10}
        )
        assert step.op == "open" and step.open_sec == 10.0
        closed = RecipeStep.from_json({"kind": "valve", "idx": 0, "base": "normal", "op": "close"})
        assert closed.op == "close" and closed.open_sec is None

    def test_to_json_round_trips_op_and_open_sec(self):
        step = latch_step("sour", op="open", open_sec=10.0)
        j = step.to_json()
        assert j["op"] == "open" and j["openSec"] == 10.0
        again = RecipeStep.from_json(j)
        assert again.op == "open" and again.open_sec == 10.0
        # op/openSec 미보유 스텝은 키 자체를 방출하지 않는다(구계약 바이트 보존).
        legacy = latch_step("sour", op=None, open_sec=None)
        legacy_j = legacy.to_json()
        assert "op" not in legacy_j and "openSec" not in legacy_j


# ── C. RR 게이트 — open 은 openSec 필수·close 는 인자 불요·모르는 op 거부. ──────────


class TestResolverValveOpGate:
    def resolve_one(self, step: RecipeStep):
        return RecipeResolver({1: SPEC}).resolve([step]).steps[0]

    def test_open_requires_open_sec(self):
        with pytest.raises(RecipeValidationError, match="valve_open_requires_open_sec"):
            self.resolve_one(latch_step("sour", op="open", open_sec=None))

    def test_close_needs_no_args(self):
        resolved = self.resolve_one(latch_step("normal", op="close", open_sec=None))
        assert isinstance(resolved, ResolvedValveStep)
        assert resolved.valve_op == "close" and resolved.open_sec is None

    def test_unknown_op_rejected(self):
        with pytest.raises(RecipeValidationError, match="unknown_valve_op"):
            self.resolve_one(latch_step("sour", op="toggle", open_sec=10.0))

    def test_open_with_open_sec_resolves(self):
        resolved = self.resolve_one(latch_step("sour", op="open", open_sec=10.0))
        assert isinstance(resolved, ResolvedValveStep)
        assert resolved.valve_op == "open" and resolved.open_sec == 10.0


# ── D. 시퀀서 — open/close 디스패치(래치는 비블로킹·close 는 즉시 성공). ───────────


class TestSequencerValveOpDispatch:
    def test_open_dispatches_to_latch_nonblocking(self, ledger):
        valve = FakeValveAdapter(max_open_sec=15.0)
        seq = make_seq(ledger, valve)
        t0 = time.monotonic()
        report = seq.submit(
            command_id="mnt-latch:1", trace_id="t", steps=[latch_step("sour", open_sec=10.0)]
        )
        elapsed = time.monotonic() - t0
        assert report.outcome is JobOutcome.COMPLETED
        assert valve.latched == "sour"
        assert elapsed < 1.0, "래치 개방은 10초를 기다리지 않는다(비블로킹)"
        valve.close_all()  # 테스트 뒷정리.

    def test_close_dispatches_to_close_all(self, ledger):
        valve = FakeValveAdapter(max_open_sec=15.0)
        valve.open_latch("normal", 10.0)
        seq = make_seq(ledger, valve)
        report = seq.submit(
            command_id="mnt-latch:2",
            trace_id="t",
            steps=[latch_step("normal", op="close", open_sec=None)],
        )
        assert report.outcome is JobOutcome.COMPLETED
        assert valve.latched is None

    def test_open_on_adapter_without_latch_fails_closed(self, ledger):
        class LegacyValve:
            """구 어댑터 더블 — open_latch 미보유(dispense/close_all 만)."""

            def dispense_volume(self, base, volume_ml, open_sec=None):  # pragma: no cover
                raise AssertionError("래치 스텝이 시간축 토출로 새면 안 된다")

            def close_all(self):
                pass

            def available_bases(self):
                return ["sour", "normal"]

        seq = make_seq(ledger, LegacyValve())  # type: ignore[arg-type]
        report = seq.submit(
            command_id="mnt-latch:3", trace_id="t", steps=[latch_step("sour", open_sec=10.0)]
        )
        # 스텝 실행 단계 실패라 PARTIAL_FAILED(RR raise 아님) — 핵심은 "조용한 성공 금지".
        assert report.outcome is not JobOutcome.COMPLETED


# ── E. 데몬 estop — 래치 개방 중 긴급정지 = 밸브 즉시 닫힘(기주 유출 차단). ─────────


class TestEstopClosesLatchedValve:
    def test_trigger_estop_closes_valves(self):
        from senlyt_pi.app.daemon import DaemonDeps, SenlytDaemon
        from senlyt_pi.persistence.idempotency_ledger import InMemoryIdempotencyLedger

        class _Sink:
            def report_status(self, r):
                pass

            def heartbeat(self, hb):
                pass

        valve = FakeValveAdapter(max_open_sec=15.0)
        d = SenlytDaemon(
            DaemonDeps(
                device_id="dev-V",
                command_source=type("S", (), {"commands": lambda s, i: iter(())})(),
                status_sink=_Sink(),
                engine=FakeEnginePort(),
                ledger=InMemoryIdempotencyLedger(),  # type: ignore[arg-type]
                heartbeat_interval_s=0,
                valve=valve,
            )
        )
        assert valve.open_latch("sour", 10.0).ok
        d._trigger_estop()
        assert valve.latched is None, "estop 은 펌프 TR 뿐 아니라 기주 밸브도 즉시 닫는다"
