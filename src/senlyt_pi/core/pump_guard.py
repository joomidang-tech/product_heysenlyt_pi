"""펌프 프리셋 clamp + 부피→스텝 파생 — SoT §6 (byte-parity 안전 급소).

**바이트 동일 축 = 서버 `pumpGuard.ts`/`settingsClamp.ts`(TS) ↔ 이 파일(Python).**
P0 HW 안전 — 식향 Code 11(플런저 오버로드·과다흡입) 재발 방지. 근본원인은
"하드코딩 24000 vs 파생 9600"의 2.5배 불일치였다(SoT 서두).

Dart `lib/core/pump_guard.dart` 포팅이되, **수치 정본 = 서버 TS**:
  - Dart 이관본의 validSyringeCapacitiesMl 4종 [1.25,0.5,2.5,5] → **9종**
    [0.025,0.05,0.1,0.25,0.5,1.0,1.25,2.5,5.0] (pumpGuard.ts VALID_SYRINGE_ML)으로 정정.
  - Tecan(Cavro XLP6000·XCalibur) 프리셋은 제거됨(2026-07-18 · 미도입·기기 미입고 — 서버 TS와 동일).
    현재 빌트인 = SY-01B 1종(+ custom). Dart 파일엔 남아 있으나 그건 동결된 포팅 오라클.
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

    pump_preset_id: str  # 항상 "sy01b" (사용자 지정·Tecan/Cavro 제거·2026-07-18)
    pump_full_stroke: int  # 풀스트로크
    pump_max_start_speed_hz: int  # v 상한(start speed)
    pump_max_top_speed_hz: int  # V 상한(top speed)
    pump_max_cutoff_speed_hz: int  # c 상한(cutoff speed)
    pump_max_slope: int  # L 상한(slope)
    pump_syringe_type_code: int  # 스톨 서브코드 U<code>


# 빌트인 프리셋 정식 수치표 — SoT §6-2 (입력 무시·강제 · 바이트 동일 SoT = pumpGuard.ts).
#
# 현재 빌트인 = SY-01B 1종. 사용자 지정(custom)·Tecan(Cavro)은 제거됨(2026-07-18 · 서버 pumpGuard.ts
# 와 동일) — clamp 는 어떤 입력이든 SY-01B 로 정규화한다. 도입 시 이 표에 1항목만 되살리면 된다.
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
}

# 유효 syringe 용량 이산값(mL) — v1.1.0 allowlist **9종**(서버 pumpGuard.ts VALID_SYRINGE_ML 정본).
VALID_SYRINGE_CAPACITIES_ML: frozenset[float] = frozenset(
    {0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 1.25, 2.5, 5.0}
)


def _round_half_up(x: float) -> int:
    """round = half-up(.5 올림 · 양수 도메인 JS Math.round 등가) — 부록A P-8. 내장 round() 금지."""
    return math.floor(x + 0.5)


def clamp_pump_preset(cfg: Mapping[str, Any] | None) -> PumpPreset:
    """clampPumpPreset(cfg) — SoT §6-3 (서버 ↔ pi 동일 알고리즘).

    **항상 SY-01B 로 정규화**한다(사용자 지정 제거·2026-07-18). 입력 pumpPresetId·수치
    (custom·unknown·레거시 포함)는 전부 무시하고 SY-01B 정식 수치를 강제 → 손 튜닝값이
    물리로 가는 경로 차단(과다흡입 안전). syringeCapacityMl 은 호출 측이 별도 주입한다.
    """
    return PUMP_PRESETS["sy01b"]


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
