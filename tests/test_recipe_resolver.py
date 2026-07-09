"""RecipeResolver 테스트 — SoT §6-4 / §9-1 / 질의서 Q2(RR-05)·Q3(RR-07).

Dart `test/recipe_resolver_test.dart` 포팅 + v1.2.0 expoRecipe/flavor_recipes 폴백 헬퍼.
정렬(idx 오름차순)·검증 게이트(음수·0·상한초과·미매핑·빈레시피 → CMD_VALIDATION_FAILED)·
steps 파생(하드코딩 금지·§6-4 검산).
"""

import pytest

from senlyt_pi.core.pump_guard import StatusErrorCode, SyringeSpec
from senlyt_pi.core.wire_messages import RecipeStep
from senlyt_pi.pipeline.recipe_resolver import (
    UNMAPPED_PUMP_ADDR,
    RecipeResolver,
    RecipeValidationError,
    expo_recipe_to_steps,
    expo_sour_ml,
    flavor_recipe_source_to_steps,
)

# flavor 1.25mL(fullStroke 12000) → maxVolumeUl=1250, stepsPerMl=9600.
FLAVOR_SPEC = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=1.25)
# fragrance 0.5mL → maxVolumeUl=500, stepsPerMl=24000.
FRAG_SPEC = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=0.5)

RESOLVER = RecipeResolver({1: FLAVOR_SPEC, 2: FLAVOR_SPEC, 5: FRAG_SPEC})


def step(idx: int, addr: int, vol: float) -> RecipeStep:
    return RecipeStep(idx=idx, pump_addr=addr, flavor=f"f{addr}", volume=vol)


def test_sorts_by_idx_ascending():
    """idx 오름차순 직렬 정렬(§9-1)."""
    r = RESOLVER.resolve([step(2, 1, 100), step(0, 1, 100), step(1, 2, 100)])
    assert [s.idx for s in r.steps] == [0, 1, 2]


def test_steps_derivation():
    """steps 파생 검산 — 100µL/1.25mL = 960 steps(§6-4)."""
    r = RESOLVER.resolve([step(0, 1, 100)])
    assert r.steps[0].steps == 960  # 12000 × 100 ÷ 1250 = 960.


def test_empty_recipe_drops():
    """RR-07(Q3): 빈 레시피 → CMD_VALIDATION_FAILED (0step COMPLETED 금지)."""
    with pytest.raises(RecipeValidationError) as e:
        RESOLVER.resolve([])
    assert e.value.reason == "empty_recipe"
    assert e.value.error_code is StatusErrorCode.CMD_VALIDATION_FAILED


def test_zero_or_negative_volume_drops():
    """RR-05(Q2): 0/음수 volume → drop."""
    with pytest.raises(RecipeValidationError) as e0:
        RESOLVER.resolve([step(0, 1, 0)])
    assert e0.value.reason == "non_positive_volume"
    with pytest.raises(RecipeValidationError) as e1:
        RESOLVER.resolve([step(0, 1, -5)])
    assert e1.value.reason == "non_positive_volume"


def test_volume_over_max_drops():
    """RR-05(Q2): 상한초과 volume → drop (maxVolumeUl=1250)."""
    with pytest.raises(RecipeValidationError) as e:
        RESOLVER.resolve([step(0, 1, 1251)])
    assert e.value.reason == "volume_over_max"
    # 경계값 1250 은 통과(≤).
    assert RESOLVER.resolve([step(0, 1, 1250)]).steps[0].steps == 12000


def test_unmapped_pump_addr_drops():
    """미매핑 pumpAddr → drop."""
    with pytest.raises(RecipeValidationError) as e:
        RESOLVER.resolve([step(0, 99, 100)])
    assert e.value.reason == "unmapped_pump_addr"


def test_fragrance_half_ml_pump_boundary():
    """fragrance 0.5mL 펌프 — maxVolumeUl=500, 500µL 경계 통과."""
    r = RESOLVER.resolve([step(0, 5, 500)])
    assert r.steps[0].steps == 12000  # 12000 × 500 ÷ 500 = 12000.
    with pytest.raises(RecipeValidationError) as e:
        RESOLVER.resolve([step(0, 5, 501)])
    assert e.value.reason == "volume_over_max"


def test_multiple_steps_step_n():
    """여러 스텝 stepN 반영."""
    r = RESOLVER.resolve([step(0, 1, 100), step(1, 2, 200)])
    assert r.step_n == 2
    assert r.steps[1].steps == 1920  # 12000 × 200 ÷ 1250.


# ── v1.2.0 expoRecipe 소비 (ExpoRecipePayload → RecipeStep) ──────────────────


def test_expo_recipe_items_ml_to_ul():
    """expoRecipe items[] 시린지 도징 — amount_ml → µL ×1000 정규화(§6-6)."""
    payload = {
        "items": [
            {"channel_id": "grape", "amount_ml": 0.6, "role": "main"},
            {"channel_id": "citrus", "amount_ml": 0.4, "role": "sub"},
        ],
        "sweetMl": 0,
        "sourMl": 0.1,
        "baseMl": 20.0,
    }
    steps = expo_recipe_to_steps(payload, pump_addr_of={"grape": 1, "citrus": 2}.get)
    assert [(s.idx, s.pump_addr, s.volume) for s in steps] == [(0, 1, 600.0), (1, 2, 400.0)]


def test_expo_recipe_sweet_ml_becomes_step_only_when_pump_configured():
    """sweetMl(당) — sweet_pump_addr 구성 시에만 당 스텝 추가(mL→µL)."""
    payload = {"items": [{"channel_id": "grape", "amount_ml": 1.0, "role": "main"}], "sweetMl": 1.2}
    without = expo_recipe_to_steps(payload, pump_addr_of={"grape": 1}.get)
    assert len(without) == 1  # 당 채널 미구성 → 생략.
    with_sweet = expo_recipe_to_steps(payload, pump_addr_of={"grape": 1}.get, sweet_pump_addr=9)
    assert len(with_sweet) == 2
    assert with_sweet[1].pump_addr == 9
    assert with_sweet[1].volume == 1200.0


def test_expo_recipe_sour_ml_is_not_dosed():
    """sourMl(산) — 시린지 스텝 아님(기주 밸브 threshold 판단 자리·오케스트레이션 몫)."""
    payload = {
        "items": [{"channel_id": "grape", "amount_ml": 1.0, "role": "main"}],
        "sweetMl": 0,
        "sourMl": 0.2,
    }
    steps = expo_recipe_to_steps(payload, pump_addr_of={"grape": 1}.get, sweet_pump_addr=9)
    assert len(steps) == 1  # sour 스텝 없음.
    assert expo_sour_ml(payload) == 0.2  # 판단값만 노출.


def test_expo_recipe_unmapped_channel_falls_to_rr_gate():
    """미매핑 channel_id → UNMAPPED sentinel → RR unmapped_pump_addr 게이트로 drop(silent 금지)."""
    payload = {"items": [{"channel_id": "unknown", "amount_ml": 0.5, "role": "main"}]}
    steps = expo_recipe_to_steps(payload, pump_addr_of=lambda _cid: None)
    assert steps[0].pump_addr == UNMAPPED_PUMP_ADDR
    with pytest.raises(RecipeValidationError) as e:
        RESOLVER.resolve(steps)
    assert e.value.reason == "unmapped_pump_addr"


# ── flavor_recipes 폴백 (FlavorRecipeSource → RecipeStep) ────────────────────


def test_flavor_recipe_source_to_steps():
    """flavor_recipes flavors[]{name, volume(µL), port} → RecipeStep — volume 은 이미 µL."""
    source = {
        "flavors": [
            {"name": "럭셔리", "volume": 300, "port": 1},
            {"name": "퓨어", "volumeUl": 200},  # port 없음 → 이름 매핑.
        ],
        "sweetenerVolume": 150,
    }
    steps = flavor_recipe_source_to_steps(
        source, pump_addr_of={"퓨어": 2}.get, sweet_pump_addr=9
    )
    assert [(s.pump_addr, s.volume) for s in steps] == [(1, 300.0), (2, 200.0), (9, 150.0)]
    assert [s.idx for s in steps] == [0, 1, 2]
    # RR 게이트 통과(µL 그대로 — 이중 정규화 없음).
    resolved = RESOLVER.resolve(steps[:2])
    assert resolved.steps[0].steps == 2880  # 12000 × 300 ÷ 1250.


def test_flavor_recipe_source_volume_wins_over_volume_ul():
    """두 키 공존 시 volume 우선 — 정본 wire.ts:163 `typeof f.volume === "number" ? f.volume : (f.volumeUl ?? 0)`.

    서버 조립 경로(wire.ts)와 pi 폴백 경로가 **같은 부피**를 택해야 한다 —
    우선순위 역전 시 게이트를 통과하는 '유효하지만 틀린' 부피가 토출된다.
    """
    source = {
        "flavors": [
            # 공존 — volume 이 이겨야 함.
            {"name": "럭셔리", "volume": 300, "volumeUl": 999, "port": 1},
            # volume 이 숫자가 아니면 volumeUl 폴백 (typeof number 미충족).
            {"name": "퓨어", "volume": "300", "volumeUl": 200, "port": 2},
            # volume=bool 은 숫자 아님(typeof boolean) → volumeUl 폴백.
            {"name": "포멜로", "volume": True, "volumeUl": 100, "port": 3},
        ],
    }
    steps = flavor_recipe_source_to_steps(source, pump_addr_of=lambda _n: None)
    assert [(s.pump_addr, s.volume) for s in steps] == [(1, 300.0), (2, 200.0), (3, 100.0)]
