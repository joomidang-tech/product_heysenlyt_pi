"""식향 무결성 통합 테스트 (2026-07-17) — 버텀업 아키텍처 구현 체크 후속.

pi daemon 식향 경로의 무결성을 단위·통합으로 고정한다(Docker/E2E 아님 — FakeEngine/FakeValve).
조사(버텀업 매핑)가 짚은 "식향 미커버 갭"을 대상으로:
  1) 식향 정본 조립 = 시린지(stage0 병렬) → 기주 밸브(stage1) 배리어 순서·완주
  2) 중복 재전달 재토출 0(IL-02) · attempt 증가 fresh
  3) 크래시 재기동 좀비 회수(CR-01) — RUNNING=INTERRUPTED·재보고 없음, RECEIVED=clear→fresh
  4) 펌프가드 식향 용량 경계(F9) — 0.5mL 실장에서 상한 초과 drop / 1.25 오설정이면 통과(발산 고정)

dispense_count = P0 게이트의 진실(물리 토출 시도 횟수).
"""

from pathlib import Path

from senlyt_pi.adapters.fake_engine_adapter import FakeEngineOutcome, FakeEnginePort
from senlyt_pi.adapters.valve_adapter import FakeValveAdapter
from senlyt_pi.core.pump_guard import SyringeSpec
from senlyt_pi.core.wire_messages import RecipeStep
from senlyt_pi.persistence.file_idempotency_ledger import FileIdempotencyLedger
from senlyt_pi.pipeline.boot_recovery import BootRecovery, RecoveryAction
from senlyt_pi.pipeline.pump_sequencer import JobOutcome, PumpSequencer
from senlyt_pi.pipeline.recipe_resolver import RecipeResolver

# 식향 시린지 스펙 — 실장·코드 기본값 모두 0.5mL(2026-07-17 확정). 1.25 는 이제 폴백이 아니라
# **오설정**(admin 에서 잘못 고른 값)을 재현하기 위한 스펙이다. 둘 다 명시적으로 쓴다.
FLAVOR_05 = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=0.5)  # maxVol 500µL
FLAVOR_125 = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=1.25)  # maxVol 1250µL(오설정)


def _ledger(tmp_path: Path, name: str = "l.log") -> FileIdempotencyLedger:
    return FileIdempotencyLedger.open(tmp_path / name)


def _seq(ledger, engine, *, valve=None, pump_map=None) -> PumpSequencer:
    counter = iter(range(100_000))
    return PumpSequencer(
        ledger=ledger,
        engine=engine,
        resolver=RecipeResolver(pump_map or {0: FLAVOR_05, 1: FLAVOR_05, 7: FLAVOR_05}),
        request_id_gen=lambda: f"req-{next(counter)}",
        now_iso=lambda: "2026-07-17T00:00:00.000Z",
        valve=valve,
    )


def _syr(idx: int, addr: int, stage: int, vol: float = 100.0) -> RecipeStep:
    return RecipeStep(idx=idx, pump_addr=addr, flavor=f"f{idx}", volume=vol, stage=stage)


def _valve(idx: int, stage: int, base: str = "normal", vol_ml: float = 20.0) -> RecipeStep:
    return RecipeStep(
        idx=idx, pump_addr=-2, flavor=f"base:{base}", volume=0.0,
        kind="valve", stage=stage, base=base, volume_ml=vol_ml,
    )


# ── 1) 식향 정본 조립: 시린지(stage0 병렬) → 기주 밸브(stage1) 배리어 ──────────────

def test_flavor_syringes_then_base_valve_barrier(tmp_path):
    led = _ledger(tmp_path)
    eng = FakeEnginePort()  # 기본 ACK
    valve = FakeValveAdapter()
    seq = _seq(led, eng, valve=valve)
    rep = seq.submit(
        command_id="ord-1:1", trace_id="t1",
        steps=[_syr(0, 0, 0), _syr(1, 1, 0), _valve(2, 1, "sour")],
    )
    assert rep.outcome is JobOutcome.COMPLETED
    assert rep.steps_done == 3 and rep.step_n == 3
    assert eng.dispense_count == 2                       # 시린지 2개만 엔진 토출
    assert len(valve.dispensed) == 1                     # 기주 밸브 1회
    assert valve.dispensed[0][0] == "sour"              # base 정확
    led.close()


def test_flavor_valve_step_without_valve_is_fail_closed(tmp_path):
    """밸브 스텝인데 밸브 미결선 → 어떤 토출도 시작 전 drop(fail-closed·토출 0)."""
    led = _ledger(tmp_path)
    eng = FakeEnginePort()
    seq = _seq(led, eng, valve=None)                     # 밸브 미주입
    rep = seq.submit(command_id="ord-2:1", trace_id="t2",
                     steps=[_syr(0, 0, 0), _valve(1, 1, "normal")])
    assert rep.outcome is JobOutcome.VALIDATION_FAILED
    assert eng.dispense_count == 0                       # 시린지도 안 나감(pre-flight drop)
    led.close()


# ── 2) 중복 재전달 재토출 0(IL-02) · attempt 증가 fresh ─────────────────────────

def test_duplicate_redelivery_no_redispense(tmp_path):
    led = _ledger(tmp_path)
    eng = FakeEnginePort()
    seq = _seq(led, eng)
    steps = [_syr(0, 0, 0), _syr(1, 1, 0)]
    r1 = seq.submit(command_id="ord-3:1", trace_id="t3", steps=steps)
    r2 = seq.submit(command_id="ord-3:1", trace_id="t3", steps=steps)  # 동일 합성키 재전달
    assert r1.outcome is JobOutcome.COMPLETED
    assert r2.outcome is JobOutcome.DUPLICATE_DROPPED
    assert eng.dispense_count == 2                       # 재토출 0 — 1회 제조분만
    led.close()


def test_new_attempt_is_fresh_and_redispenses(tmp_path):
    led = _ledger(tmp_path)
    eng = FakeEnginePort()
    seq = _seq(led, eng)
    steps = [_syr(0, 0, 0)]
    seq.submit(command_id="ord-4:1", trace_id="t4", steps=steps)      # attempt 1
    r2 = seq.submit(command_id="ord-4:2", trace_id="t4", steps=steps)  # attempt 2 = fresh
    assert r2.outcome is JobOutcome.COMPLETED
    assert eng.dispense_count == 2                       # 두 attempt 각 1회
    led.close()


# ── 3) 크래시 재기동 좀비 회수(CR-01) — ledger 파일 경유 ───────────────────────

def test_crash_running_zombie_reports_interrupted_once_no_redispense(tmp_path):
    p = tmp_path / "l.log"
    # 제조 중 크래시 시뮬 — RUNNING 마킹 후 settle 없이 프로세스 종료.
    led = FileIdempotencyLedger.open(p)
    led.check_and_claim("ord-5:1", "t5")
    led.mark_running("ord-5:1")
    led.close()

    # 재기동 — BootRecovery 는 엔진을 건드리지 않는다(구조적 재토출 0·CR-01).
    led2 = FileIdempotencyLedger.open(p)
    decisions = BootRecovery(led2).plan()
    assert len(decisions) == 1
    assert decisions[0].command_id == "ord-5:1"
    assert decisions[0].action is RecoveryAction.REPORT_INTERRUPTED
    # 복구 보고 = FAILED 종결 → 다음 재기동엔 재스캔·재보고 없음(멱등).
    led2.mark_settled("ord-5:1", success=False)
    led2.close()

    led3 = FileIdempotencyLedger.open(p)
    assert BootRecovery(led3).plan() == []              # 재보고 없음
    led3.close()


def test_crash_received_zombie_clears_to_fresh(tmp_path):
    """RECEIVED(claim 후·물리 모션 전) 크래시 → CLEAR_AND_FRESH(재전달분 fresh 소비·토출 전이라 CR-01 비위반)."""
    p = tmp_path / "l.log"
    led = FileIdempotencyLedger.open(p)
    led.check_and_claim("ord-6:1", "t6")                 # RECEIVED (mark_running 전)
    led.close()

    led2 = FileIdempotencyLedger.open(p)
    decisions = BootRecovery(led2).plan()
    assert len(decisions) == 1
    assert decisions[0].action is RecoveryAction.CLEAR_AND_FRESH
    led2.close()


# ── 4) 펌프가드 식향 용량 경계(F9) — 0.5mL 실장 vs 1.25 오설정 발산 고정 ────────────

def test_flavor_over_capacity_dropped_on_05ml(tmp_path):
    """0.5mL 실장(maxVol 500µL): 상한 초과 부피는 CMD_VALIDATION_FAILED drop(토출 0·과흡입 차단)."""
    led = _ledger(tmp_path)
    eng = FakeEnginePort()
    seq = _seq(led, eng, pump_map={0: FLAVOR_05})
    rep = seq.submit(command_id="ord-7:1", trace_id="t7",
                     steps=[_syr(0, 0, 0, vol=501.0)])   # 500µL 상한 +1
    assert rep.outcome is JobOutcome.VALIDATION_FAILED
    assert eng.dispense_count == 0
    led.close()


def test_flavor_at_capacity_boundary_passes_on_05ml(tmp_path):
    led = _ledger(tmp_path)
    eng = FakeEnginePort()
    seq = _seq(led, eng, pump_map={0: FLAVOR_05})
    rep = seq.submit(command_id="ord-8:1", trace_id="t8",
                     steps=[_syr(0, 0, 0, vol=500.0)])   # 정확히 상한
    assert rep.outcome is JobOutcome.COMPLETED
    assert eng.dispense_count == 1
    led.close()


def test_f9_capacity_divergence_is_locked(tmp_path):
    """F9 발산 고정: 501µL 는 0.5mL 스펙이면 drop, 1.25 스펙이면 통과 — 용량 **오설정**이
    과흡입(Code 11 계열)을 통과시킨다는 위험을 회귀로 고정한다.

    2026-07-17 확정으로 코드 기본값이 0.5 가 되어 '무설정'은 이제 안전측으로 떨어진다
    (구 동작: flavor 폴백 1.25 = 위험측). 남은 노출면은 admin 이 1.25 를 **명시**한 경우뿐.
    """
    # 0.5mL → drop
    led_a = _ledger(tmp_path, "a.log")
    eng_a = FakeEnginePort()
    _seq(led_a, eng_a, pump_map={0: FLAVOR_05}).submit(
        command_id="o:1", trace_id="t", steps=[_syr(0, 0, 0, vol=501.0)])
    assert eng_a.dispense_count == 0
    led_a.close()
    # 1.25mL 오설정 → 통과(발산!)
    led_b = _ledger(tmp_path, "b.log")
    eng_b = FakeEnginePort()
    _seq(led_b, eng_b, pump_map={0: FLAVOR_125}).submit(
        command_id="o:1", trace_id="t", steps=[_syr(0, 0, 0, vol=501.0)])
    assert eng_b.dispense_count == 1
    led_b.close()
