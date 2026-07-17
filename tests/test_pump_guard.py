"""PumpGuard 회귀 — SoT §6 (byte-parity 안전 급소·부록A P-7/P-8).

Dart `test/pump_guard_test.dart` 포팅 + **서버 TS(pumpGuard.ts) 정본 대조 정정 2건**:
  ① cavro_xlp6000/xcalibur typeCode 는 200 (Dart 이관본 0 = 구값 버그)
  ② 유효 시린지 용량 = 9종 allowlist (Dart 이관본 4종 = 구값 버그)
"""

from senlyt_pi.core.pump_guard import (
    VALID_SYRINGE_CAPACITIES_ML,
    EngineErrorClass,
    StatusErrorCode,
    SyringeSpec,
    clamp_pump_preset,
    classify_engine_error_code,
    fragrance_ml_to_ul,
    is_volume_within_gate,
    resolve_syringe_capacity_ml,
)


class TestClampPumpPresetBuiltin:
    """clampPumpPreset — builtin 정식 수치 강제(§6-2)."""

    def test_sy01b_forces_table_values(self):
        """sy01b — 입력 수치 무시하고 표 그대로."""
        p = clamp_pump_preset({
            "pumpPresetId": "sy01b",
            # 아래 입력은 전부 무시되어야 한다(builtin 강제).
            "pumpFullStroke": 99999,
            "pumpMaxStartSpeedHz": 99999,
        })
        assert p.pump_full_stroke == 12000
        assert p.pump_max_start_speed_hz == 1000
        assert p.pump_max_top_speed_hz == 6000
        assert p.pump_max_cutoff_speed_hz == 5400
        assert p.pump_max_slope == 20
        assert p.pump_syringe_type_code == 200

    def test_cavro_xlp6000_table(self):
        """cavro_xlp6000 — 표 그대로 (typeCode 200 = 서버 pumpGuard.ts 정본)."""
        p = clamp_pump_preset({"pumpPresetId": "cavro_xlp6000"})
        assert p.pump_full_stroke == 6000
        assert p.pump_max_start_speed_hz == 8000
        assert p.pump_max_top_speed_hz == 48000
        assert p.pump_max_cutoff_speed_hz == 21600
        assert p.pump_syringe_type_code == 200  # 서버 TS U200 (Dart 구값 0 은 버그)

    def test_cavro_xcalibur_conservative(self):
        """cavro_xcalibur — v/V/c 미확정은 SY-01B 하한(§6-2 * / O-12)."""
        p = clamp_pump_preset({"pumpPresetId": "cavro_xcalibur"})
        assert p.pump_full_stroke == 3000
        assert p.pump_max_start_speed_hz == 1000  # SY-01B 하한
        assert p.pump_max_top_speed_hz == 6000
        assert p.pump_max_cutoff_speed_hz == 5400
        assert p.pump_syringe_type_code == 200  # 서버 TS U200 (Dart 구값 0 은 버그)

    def test_unknown_id_falls_back_to_sy01b(self):
        """unknown id → sy01b 폴백(§6-3.3)."""
        p = clamp_pump_preset({"pumpPresetId": "nonsense_pump"})
        assert p.pump_preset_id == "sy01b"
        assert p.pump_full_stroke == 12000

    def test_none_falls_back_to_sy01b(self):
        """None/누락 문서 → sy01b(§6-3.4)."""
        p = clamp_pump_preset(None)
        assert p.pump_preset_id == "sy01b"


class TestClampPumpPresetCustom:
    """clampPumpPreset — custom 절대상한 + 단조성(§6-3.2·부록A P-7)."""

    def test_absolute_limits(self):
        """절대상한 clamp."""
        p = clamp_pump_preset({
            "pumpPresetId": "custom",
            "pumpFullStroke": 999999,  # → 96000
            "pumpMaxStartSpeedHz": 0,  # → 1
            "pumpMaxTopSpeedHz": 999999,  # → 48000
            "pumpMaxCutoffSpeedHz": 999999,  # → 48000
            "pumpMaxSlope": 999,  # → 40
            "pumpSyringeTypeCode": 9999,  # → 999
        })
        assert p.pump_full_stroke == 96000
        assert p.pump_max_top_speed_hz == 48000
        assert p.pump_max_slope == 40
        assert p.pump_syringe_type_code == 999
        # 단조성: v ≤ c ≤ V. v 입력 1 → clamp 후 v≤c 유지.
        assert p.pump_max_start_speed_hz <= p.pump_max_cutoff_speed_hz
        assert p.pump_max_cutoff_speed_hz <= p.pump_max_top_speed_hz

    def test_monotonicity_two_line_order(self):
        """단조성 2줄 순서 — c=min(max(c,v),V); v=min(v,c) 경계 결과(부록A P-7).

        v=5000, V=1000, c=100 (역전 입력) → c=min(max(100,5000),1000)=1000; v=min(5000,1000)=1000.
        """
        p = clamp_pump_preset({
            "pumpPresetId": "custom",
            "pumpFullStroke": 12000,
            "pumpMaxStartSpeedHz": 5000,
            "pumpMaxTopSpeedHz": 1000,
            "pumpMaxCutoffSpeedHz": 100,
            "pumpMaxSlope": 20,
            "pumpSyringeTypeCode": 200,
        })
        assert p.pump_max_cutoff_speed_hz == 1000, "c = min(max(c,v),V)"
        assert p.pump_max_start_speed_hz == 1000, "v = min(v,c)"
        assert p.pump_max_top_speed_hz == 1000


class TestSyringeSpec:
    """SyringeSpec 파생 — 검산(§6-4)."""

    def test_960_steps(self):
        """12000 × 100 ÷ 1250 = 960 steps."""
        spec = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=1.25)
        assert spec.steps_for_volume_ul(100) == 960

    def test_steps_per_ml_9600_at_1_25ml(self):
        """1.25mL 명시 선택 시 stepsPerMl = 9600 (모드 기본값 아님 — 2026-07-17 확정)."""
        spec = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=1.25)
        assert spec.steps_per_ml == 9600
        assert spec.max_volume_ul == 1250

    def test_default_steps_per_ml_24000_at_05ml(self):
        """양 모드 공통 기본 0.5mL stepsPerMl = 24000 (Code 11 방지 검산값)."""
        spec = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=0.5)
        assert spec.steps_per_ml == 24000
        assert spec.max_volume_ul == 500

    def test_round_half_up(self):
        """round half-up 고정(부록A P-8)."""
        spec = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=1.25)
        # 62.5µL → 12000*62.5/1250 = 600.0 정수. 대신 소수 경계 확인:
        # 6.510416...µL → steps 이론상 62.5 → half-up 63.
        v = 62.5 * 1250 / 12000
        assert spec.steps_for_volume_ul(v) == 63


class TestSyringeInitDerivation:
    """초기화 파라미터 파생 — Manual V1.2 §1.2(스톨전류)·§4.4.1(초기화힘).

    v1.1.0 `syringe_spec.dart` 와 **같은 표**여야 한다(자매 파리티). 이 표가 틀리면 물리
    피해가 난다 — 작은 시린지에 Full force 를 걸면 씰이 상한다(v1.1.0 실기기 리포트 사고 경로).
    """

    def spec(self, capacity_ml: float) -> SyringeSpec:
        return SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=capacity_ml)

    def test_init_force_half_at_05ml_v120_default(self):
        """★ v1.2.0 기본 0.5mL → Z1R(Half). 양 모드 공통이므로 **식향도 Z1R** 이다.

        구 v1.1.0 식향은 1.25mL 라 ZR(Full) 이었다 — 용량이 0.5 로 바뀌었는데 ZR 를 그대로
        들고 오면 500µL 시린지에 Full force 가 걸린다(매뉴얼 위반). 이 테스트가 그 회귀를 막는다.
        """
        assert self.spec(0.5).init_command == "Z1R"
        assert self.spec(0.5).stall_current == 5

    def test_init_force_full_at_and_above_1ml(self):
        """≥1.0mL → ZR(Full). 경계 1.0 포함."""
        assert self.spec(1.0).init_command == "ZR"
        assert self.spec(1.25).init_command == "ZR"
        assert self.spec(2.5).init_command == "ZR"
        assert self.spec(5.0).init_command == "ZR"

    def test_init_force_half_at_250ul_boundary(self):
        """250·500µL → Z1R(Half). 250 은 Half 하한 경계(포함)."""
        assert self.spec(0.25).init_command == "Z1R"
        assert self.spec(0.5).init_command == "Z1R"

    def test_init_force_third_below_250ul(self):
        """50·100µL → Z2R(Third). 25µL 이하도 Third."""
        assert self.spec(0.05).init_command == "Z2R"
        assert self.spec(0.1).init_command == "Z2R"
        assert self.spec(0.025).init_command == "Z2R"

    def test_stall_current_table(self):
        """≤25µL → 4 · 50µL~1.25mL → 5 · 2.5~5mL → 6 (§1.2 Table). 경계 포함 확인."""
        assert self.spec(0.025).stall_current == 4  # 25µL — 4 상한 경계
        assert self.spec(0.05).stall_current == 5  # 50µL — 5 하한 경계
        assert self.spec(1.25).stall_current == 5  # 1250µL — 5 상한 경계
        assert self.spec(2.5).stall_current == 6  # 2500µL — 6 하한 경계
        assert self.spec(5.0).stall_current == 6

    def test_derivation_is_capacity_not_mode(self):
        """파생 축은 **용량 하나** — 같은 용량이면 어느 모드에서 왔든 같은 명령이 나온다."""
        flavor_spec = self.spec(resolve_syringe_capacity_ml(None, is_flavor=True))
        fragrance_spec = self.spec(resolve_syringe_capacity_ml(None, is_flavor=False))
        assert flavor_spec.syringe_capacity_ml == fragrance_spec.syringe_capacity_ml == 0.5
        assert flavor_spec.init_command == fragrance_spec.init_command == "Z1R"


class TestFragranceNormalization:
    """fragrance 단위 정규화(§6-6)."""

    def test_ml_times_1000(self):
        """amountMl × 1000 = volumeUl."""
        assert fragrance_ml_to_ul(0.1) == 100
        assert fragrance_ml_to_ul(0.5) == 500


class TestResolveSyringeCapacityMl:
    """resolveSyringeCapacityMl — 이산값 폴백(§6-1/O-15)."""

    def test_fallback_outside_valid_set(self):
        """유효집합 밖 → 기본값(양 모드 공통 0.5mL)."""
        # 유효집합 안 값은 그대로 통과(스냅 아님).
        assert resolve_syringe_capacity_ml(1.25, is_flavor=True) == 1.25
        assert resolve_syringe_capacity_ml(0.5, is_flavor=False) == 0.5
        # 유효집합 밖 → 폴백.
        assert resolve_syringe_capacity_ml(0.99, is_flavor=True) == 0.5
        assert resolve_syringe_capacity_ml(0.99, is_flavor=False) == 0.5
        assert resolve_syringe_capacity_ml(None, is_flavor=True) == 0.5

    def test_default_capacity_is_05ml_for_both_modes(self):
        """기본 용량 = 양 모드 공통 0.5mL — 2026-07-17 확정(식향 2펌프·향장향 3펌프).

        회귀 고정: 식향 기본이 1.25mL 로 되돌아가면 maxVolumeUl 이 1250 으로 벌어져
        과흡입(Code 11 계열)이 게이트를 통과한다(F9 발산).
        """
        for bad in (None, 0.99, 3.3, -1, "x", True):
            assert resolve_syringe_capacity_ml(bad, is_flavor=True) == 0.5
            assert resolve_syringe_capacity_ml(bad, is_flavor=False) == 0.5

    def test_v110_allowlist_9_values(self):
        """v1.1.0 확정 9종 allowlist — 서버 pumpGuard.ts VALID_SYRINGE_ML 정본 (Dart 구값 4종은 버그)."""
        assert VALID_SYRINGE_CAPACITIES_ML == frozenset(
            {0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 1.25, 2.5, 5.0}
        )
        # 9종 전부 스냅 없이 통과(구 4종 집합이라면 0.025/1.0 등이 폴백돼 버림).
        for ml in sorted(VALID_SYRINGE_CAPACITIES_ML):
            assert resolve_syringe_capacity_ml(ml, is_flavor=True) == ml
            assert resolve_syringe_capacity_ml(ml, is_flavor=False) == ml


class TestVolumeGate:
    """안전 게이트(§6-4)."""

    def test_gate_bounds(self):
        """0 < volumeUl ≤ maxVolumeUl."""
        spec = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=1.25)
        assert is_volume_within_gate(100, spec) is True
        assert is_volume_within_gate(1250, spec) is True
        assert is_volume_within_gate(0, spec) is False
        assert is_volume_within_gate(-1, spec) is False
        assert is_volume_within_gate(1251, spec) is False  # Code 11 방지


class TestClassifyEngineErrorCode:
    """classifyEngineErrorCode(§6-7)."""

    def test_classification(self):
        """0=normal · 1/7/11/15=transient · 2/3/9/10=permanent."""
        assert classify_engine_error_code(0) is EngineErrorClass.NORMAL
        for c in (1, 7, 11, 15):
            assert classify_engine_error_code(c) is EngineErrorClass.TRANSIENT
        for c in (2, 3, 9, 10):
            assert classify_engine_error_code(c) is EngineErrorClass.PERMANENT


class TestStatusErrorCode:
    def test_wire_roundtrip(self):
        """status.errorCode 7종 wire 문자열 왕복(§6-7/§9-2)."""
        assert len(StatusErrorCode) == 7
        for e in StatusErrorCode:
            assert StatusErrorCode.from_wire(e.wire) is e
        assert StatusErrorCode.from_wire("NOPE") is None
