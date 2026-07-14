"""stage 병렬 실행 게이트 — §9-1 v2 (2026-07-14 병렬토출 설계 §7·§8·§10).

객관 판정:
  - PAR-01: 같은 stage 3펌프 동시 실행 → 총 시간 ≈ max(스텝) — sum 아님(설계 §3 타임라인).
  - PAR-02: 구계약(stage 부재) → stage=idx → 기존 완전 직렬(총 시간 ≈ sum·하위호환).
  - PAR-03: stage 게이트(pumpAddr 중복·valve 2개·stage 결번) = CMD_VALIDATION_FAILED drop(토출 0).
  - PAR-04: valve 스텝 실행(FakeValve 기록 — 계약은 임의 stage 배치 허용·정본 조립은
            옵션 B=stage 0·PAR-08) · 밸브 미결선 + valve 스텝 = fail-closed drop(토출 0).
  - PAR-05: stage 내 partial 실패 — 실패 태스크 외 나머지는 완주(취소 없음) → PARTIAL_FAILED.
  - PAR-06: RecipeStep v2 파싱/방출 — valve from_json/to_json·구계약 바이트 보존.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from senlyt_pi.adapters.fake_engine_adapter import FakeEngineOutcome, FakeEnginePort
from senlyt_pi.adapters.valve_adapter import FakeValveAdapter
from senlyt_pi.core.pump_guard import StatusErrorCode, SyringeSpec
from senlyt_pi.core.wire_messages import RecipeStep
from senlyt_pi.persistence.file_idempotency_ledger import FileIdempotencyLedger
from senlyt_pi.pipeline.pump_sequencer import JobOutcome, PumpSequencer
from senlyt_pi.pipeline.recipe_resolver import RecipeResolver

SPEC = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=1.25)

# FakeEngine 스텝 지연(ms) — 병렬/직렬 벽시계 대비가 명확하도록 100ms.
STEP_DELAY_MS = 100


@pytest.fixture
def ledger(tmp_path: Path):
    ledger = FileIdempotencyLedger.open(tmp_path / "l.log")
    yield ledger
    ledger.close()


def make_seq(
    ledger: FileIdempotencyLedger,
    fake: FakeEnginePort,
    *,
    valve: FakeValveAdapter | None = None,
) -> PumpSequencer:
    seq_counter = iter(range(10_000))
    return PumpSequencer(
        ledger=ledger,
        engine=fake,
        resolver=RecipeResolver({1: SPEC, 2: SPEC, 3: SPEC}),
        request_id_gen=lambda: f"req-{next(seq_counter)}",
        now_iso=lambda: "2026-07-14T00:00:00.000Z",
        valve=valve,
    )


def syr(idx: int, addr: int, stage: int | None = None, vol: float = 100.0) -> RecipeStep:
    return RecipeStep(idx=idx, pump_addr=addr, flavor=f"f{idx}", volume=vol, stage=stage)


def valve_step(idx: int, stage: int | None, base: str = "normal") -> RecipeStep:
    return RecipeStep(
        idx=idx,
        pump_addr=-2,
        flavor=f"base:{base}",
        volume=0.0,
        kind="valve",
        stage=stage,
        base=base,
        volume_ml=20.0,
    )


# ── PAR-01: 같은 stage = 동시 실행 → 총 시간 ≈ max (설계 §3·§7 향장향 stage 0). ──


def test_same_stage_runs_parallel_wall_clock_max(ledger):
    fake = FakeEnginePort(step_delay_ms=STEP_DELAY_MS)
    fake.script_all(FakeEngineOutcome.ACK)
    seq = make_seq(ledger, fake)

    t0 = time.monotonic()
    report = seq.submit(
        command_id="o-par:1",
        trace_id="t",
        steps=[syr(0, 1, stage=0), syr(1, 2, stage=0), syr(2, 3, stage=0)],
    )
    elapsed = time.monotonic() - t0

    assert report.outcome is JobOutcome.COMPLETED
    assert report.steps_done == 3
    assert fake.dispense_count == 3
    # 직렬이면 ≥ 3×delay(0.3s) — 병렬은 ≈ 1×delay. 여유 마진 2×delay 미만으로 판정.
    assert elapsed < (STEP_DELAY_MS / 1000) * 2, f"parallel expected, took {elapsed:.3f}s"


# ── PAR-02: 구계약(stage 부재) = stage=idx = 기존 완전 직렬(하위호환). ──


def test_legacy_steps_without_stage_stay_serial(ledger):
    fake = FakeEnginePort(step_delay_ms=STEP_DELAY_MS)
    fake.script_all(FakeEngineOutcome.ACK)
    seq = make_seq(ledger, fake)

    t0 = time.monotonic()
    report = seq.submit(
        command_id="o-ser:1",
        trace_id="t",
        steps=[syr(0, 1), syr(1, 2), syr(2, 3)],  # stage 없음 → idx 가 stage.
    )
    elapsed = time.monotonic() - t0

    assert report.outcome is JobOutcome.COMPLETED
    # 직렬 = 3 스텝 순차 ≈ 3×delay 이상(배리어가 실제로 작동함을 벽시계로 판정).
    assert elapsed >= (STEP_DELAY_MS / 1000) * 3 * 0.9, f"serial expected, took {elapsed:.3f}s"


# ── PAR-03: stage 게이트 — 검증 실패는 전부 drop(토출 0). ──


def test_duplicate_pump_in_stage_drops(ledger):
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    seq = make_seq(ledger, fake)
    report = seq.submit(
        command_id="o-dup:1",
        trace_id="t",
        steps=[syr(0, 1, stage=0), syr(1, 1, stage=0)],  # 같은 stage 같은 펌프.
    )
    assert report.outcome is JobOutcome.VALIDATION_FAILED
    assert fake.dispense_count == 0  # 토출 0.


def test_multiple_valves_drop(ledger):
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    valve = FakeValveAdapter()
    seq = make_seq(ledger, fake, valve=valve)
    report = seq.submit(
        command_id="o-2v:1",
        trace_id="t",
        steps=[syr(0, 1, stage=0), valve_step(1, 1, "normal"), valve_step(2, 1, "sour")],
    )
    assert report.outcome is JobOutcome.VALIDATION_FAILED
    assert fake.dispense_count == 0
    assert valve.dispensed == []


def test_stage_gap_drops(ledger):
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    seq = make_seq(ledger, fake)
    report = seq.submit(
        command_id="o-gap:1",
        trace_id="t",
        steps=[syr(0, 1, stage=0), syr(1, 2, stage=2)],  # stage 1 결번.
    )
    assert report.outcome is JobOutcome.VALIDATION_FAILED
    assert fake.dispense_count == 0


# ── PAR-04: valve 실행(옵션 A 배리어) + 미결선 fail-closed. ──


def test_valve_step_dispenses_after_syringes(ledger):
    """밸브를 뒤 stage 에 둔 배치도 계약상 유효(배리어 검증) — FakeValve 가 기주 개방을 기록한다.

    ⚠️ 정본 조립은 옵션 B(valve=stage 0·3병렬·사용자 확정 2026-07-14 — PAR-08이 검증).
    이 테스트는 stage 배리어(시린지 stage 완주 후 밸브 stage 진입) 의미론 자체를 고정한다."""
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    valve = FakeValveAdapter(flow_ml_per_sec=10.0)
    seq = make_seq(ledger, fake, valve=valve)

    report = seq.submit(
        command_id="o-valve:1",
        trace_id="t",
        # stage 0 = 향료(펌프1)∥당(펌프2) → stage 1 = 기주 밸브(배리어 검증용 배치).
        steps=[syr(0, 1, stage=0), syr(1, 2, stage=0), valve_step(2, 1, "sour")],
    )
    assert report.outcome is JobOutcome.COMPLETED
    assert report.steps_done == 3
    assert fake.dispense_count == 2
    # 기주 20mL ÷ 10mL/s = 2s 개방 기록(클램프 내).
    assert valve.dispensed == [("sour", 20.0, 2.0)]


def test_valve_step_without_valve_port_fail_closed(ledger):
    """밸브 미결선 + valve 스텝 = pre-flight drop — 시린지 포함 어떤 토출도 0(설계 §8)."""
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    seq = make_seq(ledger, fake, valve=None)
    report = seq.submit(
        command_id="o-novalve:1",
        trace_id="t",
        steps=[syr(0, 1, stage=0), valve_step(1, 1, "normal")],
    )
    assert report.outcome is JobOutcome.VALIDATION_FAILED
    assert report.error_code is StatusErrorCode.CMD_VALIDATION_FAILED
    assert fake.dispense_count == 0  # 시린지도 시작 전 — fail-closed.


def test_valve_failure_is_permanent(ledger):
    """밸브 실패 = permanent(시간축 재시도 금지 — 과토출 위험·설계 §8)."""
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    valve = FakeValveAdapter()
    valve.fail_next = True
    seq = make_seq(ledger, fake, valve=valve)
    report = seq.submit(
        command_id="o-vfail:1",
        trace_id="t",
        steps=[syr(0, 1, stage=0), valve_step(1, 1, "normal")],
    )
    assert report.outcome is JobOutcome.PARTIAL_FAILED
    assert report.error_code is StatusErrorCode.ENGINE_ERROR_PERMANENT
    assert report.steps_done == 1  # 시린지는 완주·밸브만 실패.


# ── PAR-05: stage 내 partial 실패 — 나머지 태스크 완주(취소 없음·설계 §10). ──


def test_stage_partial_failure_others_complete(ledger):
    fake = FakeEnginePort(step_delay_ms=20)
    fake.script_all(FakeEngineOutcome.ACK)
    fake.script_for(2, [FakeEngineOutcome.PERMANENT])  # 펌프 2만 즉시 permanent.
    seq = make_seq(ledger, fake)

    report = seq.submit(
        command_id="o-pfail:1",
        trace_id="t",
        steps=[syr(0, 1, stage=0), syr(1, 2, stage=0), syr(2, 3, stage=0)],
    )
    assert report.outcome is JobOutcome.PARTIAL_FAILED
    assert report.error_code is StatusErrorCode.ENGINE_ERROR_PERMANENT
    # 실패 1 · 나머지 2 는 in-flight 완주(강제 취소 없음).
    assert report.steps_done == 2
    assert fake.dispense_count == 3  # 3태스크 모두 dispense 시도됨.


# ── PAR-06: RecipeStep v2 파싱/방출. ──


def test_recipe_step_v2_json_roundtrip():
    v = RecipeStep.from_json(
        {"idx": 4, "stage": 2, "kind": "valve", "base": "sour", "volumeMl": 20}
    )
    assert v.is_valve and v.base == "sour" and v.volume_ml == 20.0 and v.effective_stage == 2
    assert v.to_json() == {
        "idx": 4,
        "stage": 2,
        "kind": "valve",
        "base": "sour",
        "volumeMl": 20.0,
        "flavor": "base:sour",
        "pumpAddr": -2,  # 구데몬 호환 sentinel — 구 pi 는 미매핑 drop(크래시 아님).
        "volume": 0,
    }

    s = RecipeStep.from_json({"idx": 1, "stage": 0, "kind": "syringe", "pumpAddr": 2,
                              "flavor": "grape", "volume": 750})
    assert not s.is_valve and s.effective_stage == 0

    # 구계약(4필드) — 파싱 시 stage=idx 해석·방출 시 구형 4필드 바이트 보존.
    legacy = RecipeStep.from_json({"idx": 3, "pumpAddr": 1, "flavor": "f", "volume": 100})
    assert legacy.effective_stage == 3 and legacy.kind == "syringe"
    assert legacy.to_json() == {"idx": 3, "pumpAddr": 1, "flavor": "f", "volume": 100}


# ── PAR-07: 태스크 예외 흡수(리뷰 P1-1) — 실엔진 raise 에도 형제 완주·settle·L4 보존. ──


class RaisingEnginePort(FakeEnginePort):
    """실 RS485 어댑터의 예외(pyserial SerialException 등)를 근사 — 특정 펌프만 raise."""

    def __init__(self, *, raise_for_addr: int, **kw):
        super().__init__(**kw)
        self._raise_for_addr = raise_for_addr

    def dispense(self, cmd):
        if cmd.pump_addr == self._raise_for_addr:
            raise RuntimeError("serial bus exploded (simulated)")
        return super().dispense(cmd)


def test_task_exception_absorbed_siblings_complete_and_settled(ledger):
    fake = RaisingEnginePort(raise_for_addr=1, step_delay_ms=20)
    fake.script_all(FakeEngineOutcome.ACK)
    seq = make_seq(ledger, fake)

    report = seq.submit(
        command_id="o-raise:1",
        trace_id="t",
        steps=[syr(0, 1, stage=0), syr(1, 2, stage=0), syr(2, 3, stage=0)],
    )
    # 예외가 submit 밖으로 새지 않고 permanent 실패로 흡수된다.
    assert report.outcome is JobOutcome.PARTIAL_FAILED
    assert report.error_code is StatusErrorCode.ENGINE_ERROR_PERMANENT
    assert report.steps_done == 2  # 형제 2펌프는 완주(미대기 이탈 없음).
    # L4(동시 1제조) 보존 — busy 해제 + ledger settle(재제출 = 멱등 DROP).
    assert seq.is_busy is False
    dup = seq.submit(
        command_id="o-raise:1",
        trace_id="t",
        steps=[syr(0, 1, stage=0)],
    )
    assert dup.outcome is JobOutcome.DUPLICATE_DROPPED
    assert fake.dispense_count == 2  # 재제출이 재토출을 만들지 않음(raise 펌프 제외 2회 그대로).

# ── PAR-08: 식향 옵션 B(사용자 확정 2026-07-14) — 향료∥당∥기주 밸브 3병렬. ──


def test_flavor_option_b_three_way_parallel(ledger):
    """stage 0 = 향료 시린지(펌프1) ∥ 당 시린지(펌프2) ∥ 기주 밸브 — 총 시간 ≈ max(3)."""
    fake = FakeEnginePort(step_delay_ms=STEP_DELAY_MS)
    fake.script_all(FakeEngineOutcome.ACK)
    # 밸브 개방도 시린지와 같은 100ms 로 시뮬 — 3개가 겹치면 총 ≈ 1×delay.
    valve = FakeValveAdapter(flow_ml_per_sec=10.0, delay_s=STEP_DELAY_MS / 1000)
    seq = make_seq(ledger, fake, valve=valve)

    t0 = time.monotonic()
    report = seq.submit(
        command_id="o-3par:1",
        trace_id="t",
        steps=[
            syr(0, 1, stage=0),          # 향료 시린지.
            syr(1, 2, stage=0),          # 당 시린지.
            valve_step(2, 0, "normal"),  # 기주 밸브 — stage 0(옵션 B).
        ],
    )
    elapsed = time.monotonic() - t0

    assert report.outcome is JobOutcome.COMPLETED
    assert report.steps_done == 3
    assert fake.dispense_count == 2
    assert valve.dispensed == [("normal", 20.0, 2.0)]
    # 직렬이면 ≥ 3×delay — 3병렬은 ≈ 1×delay(마진 2×delay 미만).
    assert elapsed < (STEP_DELAY_MS / 1000) * 2, f"3-way parallel expected, took {elapsed:.3f}s"

# ── PAR-09: 클램프 발동 = under-dispense — 개방 전 fail-closed 거부(조용한 성공 금지). ──


def test_valve_clamp_underdispense_fail_closed(ledger):
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    # 저유량 1mL/s → 20mL 는 20s 필요 > max 15s → 개방 없이 거부.
    valve = FakeValveAdapter(flow_ml_per_sec=1.0, max_open_sec=15.0)
    seq = make_seq(ledger, fake, valve=valve)
    report = seq.submit(
        command_id="o-clamp:1",
        trace_id="t",
        steps=[syr(0, 1, stage=0), valve_step(1, 0, "normal")],
    )
    assert report.outcome is JobOutcome.PARTIAL_FAILED
    assert report.error_code is StatusErrorCode.ENGINE_ERROR_PERMANENT
    assert valve.dispensed == []  # 개방 0 — 부분 토출 낭비 없음.
