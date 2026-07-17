"""펌프 프리셋 clamp + 부피→스텝 파생 — SoT §6 (byte-parity 안전 급소).

**바이트 동일 축 = 서버 `pumpGuard.ts`/`settingsClamp.ts`(TS) ↔ 이 파일(Python).**
P0 HW 안전 — 식향 Code 11(플런저 오버로드·과다흡입) 재발 방지. 근본원인은
"하드코딩 24000 vs 파생 9600"의 2.5배 불일치였다(SoT 서두).

Dart `lib/core/pump_guard.dart` 포팅이되, **수치 정본 = 서버 TS**:
  - Dart 이관본의 구값 2건은 버그로 판정, 서버 TS(v1.1.0 확정값)로 정정:
    ① validSyringeCapacitiesMl 4종 [1.25,0.5,2.5,5] → **9종**
       [0.025,0.05,0.1,0.25,0.5,1.0,1.25,2.5,5.0] (pumpGuard.ts VALID_SYRINGE_ML)
    ② cavro_xlp6000·cavro_xcalibur pumpSyringeTypeCode 0 → **200**
       (pumpGuard.ts PUMP_PRESETS — 빌트인 3종 전부 U200)
  - 그 외(프리셋 수치·custom 절대상한·기본값·단조성 2줄 순서)는 서버 TS와 대조 완료(동일).

라운딩(부록A P-8): round = half-up(양수 도메인 JS `Math.round` = `floor(x+0.5)`).
Python 내장 round() 는 banker's rounding 이라 **사용 금지** — `_round_half_up` 고정.
"""

from __future__ import annotations

import enum
import math
from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class PumpPreset:
    """PumpPreset 7필드 — SoT §6-1 (고정 필드명·타입·순서)."""

    pump_preset_id: str  # ∈ {sy01b, cavro_xlp6000, cavro_xcalibur, custom}
    pump_full_stroke: int  # 풀스트로크
    pump_max_start_speed_hz: int  # v 상한(start speed)
    pump_max_top_speed_hz: int  # V 상한(top speed)
    pump_max_cutoff_speed_hz: int  # c 상한(cutoff speed)
    pump_max_slope: int  # L 상한(slope)
    pump_syringe_type_code: int  # 스톨 서브코드 U<code>


# 빌트인 프리셋 정식 수치표 — SoT §6-2 (입력 무시·강제 · 바이트 동일 SoT = pumpGuard.ts).
#
# XCalibur 의 v/V/c 는 미확정(§6-2 `*`, O-12) → **보수적 SY-01B 하한**(1000/6000/5400) 채택.
# 속도 clamp 는 낮을수록 안전. typeCode 는 빌트인 3종 전부 **200**(서버 TS 정본 —
# Dart 이관본의 cavro 0 은 구값 버그였다).
PUMP_PRESETS: dict[str, PumpPreset] = {
    "sy01b": PumpPreset(
        pump_preset_id="sy01b",
        pump_full_stroke=12000,
        pump_max_start_speed_hz=1000,
        pump_max_top_speed_hz=6000,
        pump_max_cutoff_speed_hz=5400,
        pump_max_slope=20,
        pump_syringe_type_code=200,
    ),
    "cavro_xlp6000": PumpPreset(
        pump_preset_id="cavro_xlp6000",
        pump_full_stroke=6000,
        pump_max_start_speed_hz=8000,
        pump_max_top_speed_hz=48000,
        pump_max_cutoff_speed_hz=21600,
        pump_max_slope=20,
        pump_syringe_type_code=200,  # 서버 pumpGuard.ts U200 (v1.1.0 확정)
    ),
    "cavro_xcalibur": PumpPreset(
        pump_preset_id="cavro_xcalibur",
        pump_full_stroke=3000,
        pump_max_start_speed_hz=1000,  # * SY-01B 하한(미확정·보수적)
        pump_max_top_speed_hz=6000,  # * SY-01B 하한
        pump_max_cutoff_speed_hz=5400,  # * SY-01B 하한
        pump_max_slope=20,
        pump_syringe_type_code=200,  # 서버 pumpGuard.ts U200 (v1.1.0 확정)
    ),
}

# custom 절대상한 — SoT §6-3 (pumpGuard.ts CUSTOM_LIMITS 와 바이트 동일).
_CUSTOM_STROKE_MIN = 100
_CUSTOM_STROKE_MAX = 96000
_CUSTOM_SPEED_MIN = 1
_CUSTOM_SPEED_MAX = 48000
_CUSTOM_SLOPE_MIN = 1
_CUSTOM_SLOPE_MAX = 40
_CUSTOM_TYPE_MIN = 0
_CUSTOM_TYPE_MAX = 999

# 유효 syringe 용량 이산값(mL) — v1.1.0 allowlist **9종**(서버 pumpGuard.ts VALID_SYRINGE_ML 정본).
VALID_SYRINGE_CAPACITIES_ML: frozenset[float] = frozenset(
    {0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 1.25, 2.5, 5.0}
)


def _round_half_up(x: float) -> int:
    """round = half-up(.5 올림 · 양수 도메인 JS Math.round 등가) — 부록A P-8. 내장 round() 금지."""
    return math.floor(x + 0.5)


def _clamp_int(v: Any, lo: int, hi: int, fallback: int) -> int:
    """정수 clamp(round 후 [min,max]) — TS `clampInt`. NaN/누락 → fallback.

    Dart `_clampInt` 등가: num 이 아니면 문자열 파싱 시도. bool 은 수치로 취급하지 않는다.
    """
    n: float | None
    if isinstance(v, bool):  # Python bool 은 int 서브클래스 — 수치 아님(방어).
        n = None
    elif isinstance(v, (int, float)):
        n = float(v)
    else:
        try:
            n = float(str(v))
        except (TypeError, ValueError):
            n = None
    if n is None or not math.isfinite(n):
        return fallback
    r = _round_half_up(n)
    return lo if r < lo else (hi if r > hi else r)


def clamp_pump_preset(cfg: Mapping[str, Any] | None) -> PumpPreset:
    """clampPumpPreset(cfg) — SoT §6-3 (서버 ↔ pi 동일 알고리즘).

    1) builtin(sy01b|cavro_xlp6000|cavro_xcalibur) → 표의 정식 수치 그대로(입력 전부 무시).
    2) custom → 절대상한 clamp + 속도 단조성 강제(⚠️ 2줄 순서 고정·부록A P-7).
    3) unknown id → sy01b 폴백.
    4) 미설정/누락 → sy01b 프리셋(호출 측이 None 전달).
    """
    raw_id = cfg.get("pumpPresetId") if cfg is not None else None
    preset_id = raw_id if isinstance(raw_id, str) else "sy01b"

    # 1) builtin — 정식 수치 강제(입력 수치 전부 무시).
    builtin = PUMP_PRESETS.get(preset_id)
    if builtin is not None and preset_id != "custom":
        return builtin

    # 3) unknown id → sy01b 폴백.
    if preset_id != "custom":
        return PUMP_PRESETS["sy01b"]

    # 2) custom → 절대상한 clamp (기본값 = 서버 TS 와 동일: 12000/1000/6000/5400/20/200).
    get = cfg.get if cfg is not None else (lambda _k: None)
    stroke = _clamp_int(get("pumpFullStroke"), _CUSTOM_STROKE_MIN, _CUSTOM_STROKE_MAX, 12000)
    v = _clamp_int(get("pumpMaxStartSpeedHz"), _CUSTOM_SPEED_MIN, _CUSTOM_SPEED_MAX, 1000)
    big_v = _clamp_int(get("pumpMaxTopSpeedHz"), _CUSTOM_SPEED_MIN, _CUSTOM_SPEED_MAX, 6000)
    c = _clamp_int(get("pumpMaxCutoffSpeedHz"), _CUSTOM_SPEED_MIN, _CUSTOM_SPEED_MAX, 5400)
    slope = _clamp_int(get("pumpMaxSlope"), _CUSTOM_SLOPE_MIN, _CUSTOM_SLOPE_MAX, 20)
    type_code = _clamp_int(get("pumpSyringeTypeCode"), _CUSTOM_TYPE_MIN, _CUSTOM_TYPE_MAX, 200)

    # 속도 단조성 강제 — ⚠️ 순서 고정(부록A P-7: 2줄 순서가 바이트 동일이어야 경계 입력 결과 일치).
    #   (SY-01B 제약 v ≤ c ≤ V)
    c = min(max(c, v), big_v)
    v = min(v, c)

    return PumpPreset(
        pump_preset_id="custom",
        pump_full_stroke=stroke,
        pump_max_start_speed_hz=v,
        pump_max_top_speed_hz=big_v,
        pump_max_cutoff_speed_hz=c,
        pump_max_slope=slope,
        pump_syringe_type_code=type_code,
    )


def resolve_syringe_capacity_ml(raw: Any, *, is_flavor: bool) -> float:
    """syringeCapacityMl 이산값 검증 — SoT §6-1 / O-15 (TS coerceSyringeCapacityMl 등가).

    유효집합(9종) 밖이면 **모드 기본값 폴백**(스냅 아님). 기본 용량 = 양 모드 공통 0.5mL
    (2026-07-17 확정: flavor 2펌프·fragrance 3펌프 모두 시린지 0.5mL). is_flavor 는
    시그니처 호환을 위해 유지하되 현재 폴백값은 동일하다.
    """
    fallback = 0.5
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return fallback
    d = float(raw)
    return d if d in VALID_SYRINGE_CAPACITIES_ML else fallback


@dataclass(frozen=True, slots=True)
class SyringeSpec:
    """부피→스텝 파생(SyringeSpec) — SoT §6-4 (하드코딩 금지·파생이 SoT).

      steps         = round( pumpFullStroke × volumeUl ÷ (syringeCapacityMl × 1000) )
      stepsPerMl    = pumpFullStroke ÷ syringeCapacityMl
      maxVolumeUl   = syringeCapacityMl × 1000   (per-pump 안전 게이트 상한)

    검산(§6-4): 12000 × 100 ÷ 500 = 2400 steps / 시린지 0.5mL stepsPerMl=24000, maxVol 500µL
               (양 모드 공통 0.5mL — 2026-07-17 확정: 식향 2펌프·향장향 3펌프). 참고: 1.25mL
               선택 시 12000 × 100 ÷ 1250 = 960 steps / stepsPerMl=9600.
    """

    pump_full_stroke: int
    syringe_capacity_ml: float

    @property
    def max_volume_ul(self) -> float:
        """per-pump 안전 게이트 상한(µL)."""
        return self.syringe_capacity_ml * 1000

    @property
    def steps_per_ml(self) -> float:
        """mL 당 스텝수."""
        return self.pump_full_stroke / self.syringe_capacity_ml

    def steps_for_volume_ul(self, volume_ul: float) -> int:
        """부피(µL) → 스텝수. round = half-up(양수 도메인·부록A P-8)."""
        return _round_half_up(
            self.pump_full_stroke * volume_ul / (self.syringe_capacity_ml * 1000)
        )

    # ── 초기화 파라미터 (용량 파생) — v1.1.0 `syringe_spec.dart` 포팅 ──────────────
    #
    # ⚠️ **모드가 아니라 용량이 결정한다.** v1.1.0 실기기 리포트에서 드러난 사고 경로가
    #    "설정 용량을 무시하고 모드 기본으로 초기화힘을 유도 → Z1R(Half)이어야 할 500µL
    #    시린지에 ZR(Full)이 나가는 매뉴얼 위반"이었다. 그래서 파생을 이 한 곳에 응집한다.
    #    v1.2.0 은 양 모드 공통 0.5mL 이므로 **둘 다 Z1R**(구 식향 1.25mL = ZR 이었다).

    @property
    def stall_current(self) -> int:
        """스톨 전류 단계 n (`U<code>,<n>R`) — 용량 파생 (Manual V1.2 §1.2 Table).

        ≤25µL → 4 · 50µL~1.25mL → 5 · 2.5~5mL → 6.
        """
        ul = self.max_volume_ul
        if ul <= 25:
            return 4
        if ul <= 1250:
            return 5
        return 6

    @property
    def init_command(self) -> str:
        """초기화 실행 명령 — 용량 파생 (Manual V1.2 §4.4.1). `Z<n1>R`: n1=0 Full·1 Half·2 Third.

        ≥1.0mL → `ZR`(Full) · 250·500µL → `Z1R`(Half) · 50·100µL → `Z2R`(Third).
        시린지 씰 보호가 목적이라 **작은 시린지에 Full 을 걸면 안 된다**.
        """
        if self.syringe_capacity_ml >= 1.0:
            return "ZR"
        if self.max_volume_ul >= 250:
            return "Z1R"
        return "Z2R"


def fragrance_ml_to_ul(amount_ml: float) -> float:
    """fragrance 단위 정규화 — SoT §6-6. amountMl → volumeUl(µL). 미스매치 = Code 11.

    (flavor volume 은 이미 µL 이므로 정규화 불필요.)
    """
    return amount_ml * 1000


def is_volume_within_gate(volume_ul: float, spec: SyringeSpec) -> bool:
    """recipe 스텝 검증 게이트 — SoT §6-4 / §9-1.
    0 < volumeUl ≤ maxVolumeUl. 위반 → CMD_VALIDATION_FAILED(drop).
    """
    return 0 < volume_ul <= spec.max_volume_ul


class EngineErrorClass(enum.Enum):
    """EnginePort 에러코드 분류 — SoT §6-7."""

    NORMAL = "normal"
    TRANSIENT = "transient"
    PERMANENT = "permanent"


def classify_engine_error_code(code: int) -> EngineErrorClass:
    """엔진 raw errorCode(정수) → 분류 — SoT §6-7.
    0 = 정상 / 1·7·11·15·timeout = transient(R=3 재시도) / 2·3·9·10 = permanent(즉시중단 FAILED).
    """
    if code == 0:
        return EngineErrorClass.NORMAL
    if code in (1, 7, 11, 15):
        return EngineErrorClass.TRANSIENT
    if code in (2, 3, 9, 10):
        return EngineErrorClass.PERMANENT
    # 미분류 코드는 보수적으로 permanent(안전측·즉시중단).
    return EngineErrorClass.PERMANENT


class StatusErrorCode(enum.Enum):
    """status.errorCode 7종 — SoT §6-7 / §9-2."""

    CMD_VALIDATION_FAILED = "CMD_VALIDATION_FAILED"
    DUPLICATE_DROPPED = "DUPLICATE_DROPPED"
    ENGINE_TIMEOUT = "ENGINE_TIMEOUT"
    ENGINE_ERROR_TRANSIENT = "ENGINE_ERROR_TRANSIENT"
    ENGINE_ERROR_PERMANENT = "ENGINE_ERROR_PERMANENT"
    PARTIAL_DISPENSE = "PARTIAL_DISPENSE"
    INTERRUPTED = "INTERRUPTED"

    @property
    def wire(self) -> str:
        return self.value

    @staticmethod
    def from_wire(v: Any) -> "StatusErrorCode | None":
        if not isinstance(v, str):
            return None
        for e in StatusErrorCode:
            if e.wire == v:
                return e
        return None
