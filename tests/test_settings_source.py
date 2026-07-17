"""settings read-only 소비 테스트 — clamp 된 settings → pumpAddr→SyringeSpec(O-18).

방어적 재clamp(clamp_pump_preset)·용량 allowlist 폴백(O-15)·불량 항목 스킵(silent 매핑 금지).
"""

from senlyt_pi.adapters.settings_source import pump_map_from_settings
from senlyt_pi.pipeline.recipe_resolver import RecipeResolver


def test_pump_map_from_settings_builtin_preset():
    """빌트인 sy01b — fullStroke 12000 강제 + 명시 용량은 기본값(0.5)을 덮어쓴다.

    addr 1 은 1.25mL 를 **명시**(allowlist 안 → 스냅 없이 통과) → maxVolumeUl 1250.
    """
    settings = {
        "pumps": [
            {"pumpAddr": 1, "mode": "flavor", "syringeCapacityMl": 1.25, "pumpPresetId": "sy01b"},
            {"pumpAddr": 5, "mode": "fragrance", "syringeCapacityMl": 0.5, "pumpPresetId": "sy01b"},
        ]
    }
    m = pump_map_from_settings(settings)
    assert set(m.keys()) == {1, 5}
    assert m[1].pump_full_stroke == 12000
    assert m[1].max_volume_ul == 1250
    assert m[5].max_volume_ul == 500
    # RecipeResolver pump_map 으로 그대로 소비 가능.
    RecipeResolver(m)


def test_capacity_allowlist_fallback_to_mode_default():
    """9종 allowlist 밖 용량 → 기본값 폴백 — 양 모드 공통 0.5mL(O-15·2026-07-17 확정)."""
    settings = {
        "pumps": [
            {"pumpAddr": 1, "mode": "flavor", "syringeCapacityMl": 3.3, "pumpPresetId": "sy01b"},
            {"pumpAddr": 5, "mode": "fragrance", "syringeCapacityMl": -1, "pumpPresetId": "sy01b"},
        ]
    }
    m = pump_map_from_settings(settings)
    assert m[1].syringe_capacity_ml == 0.5
    assert m[5].syringe_capacity_ml == 0.5


def test_invalid_entries_skipped():
    """pumpAddr 누락/비정수·비매핑 항목은 스킵(미매핑 addr 는 RR 게이트 몫) + None/불량 settings."""
    settings = {
        "pumps": [
            {"mode": "flavor"},  # pumpAddr 없음.
            {"pumpAddr": "x"},  # 비정수.
            {"pumpAddr": True},  # bool 은 정수 취급 안 함(방어).
            "garbage",
            {"pumpAddr": 2, "pumpPresetId": "sy01b"},
        ]
    }
    m = pump_map_from_settings(settings)
    assert set(m.keys()) == {2}
    assert pump_map_from_settings(None) == {}
    assert pump_map_from_settings({"pumps": "not-a-list"}) == {}
