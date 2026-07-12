"""Recipe Resolver — 주문(command) → 정렬·검증된 실행 스텝 — SoT §6-4 / §9-1 / 질의서 Q2·Q3.

Dart `lib/pipeline/recipe_resolver.dart` 포팅.

책임:
  1) command.recipe(steps) → idx **오름차순 직렬** 정렬(§9-1: 0부터 오름차순 직렬).
  2) 검증 게이트(§6-4·§9-1): 음수·0·상한초과·미매핑 pumpAddr·빈 레시피 → CMD_VALIDATION_FAILED.
     - RR-05(Q2): `0 < volumeUl ≤ maxVolumeUl`(per-pump).
     - RR-07(Q3): 빈 레시피(steps=0) = drop(0step COMPLETED 금지).
  3) 각 스텝을 SyringeSpec 로 steps(수) 파생(§6-4·하드코딩 금지).

recipe == None(§9-1 폴백 신호) 은 이 resolver 의 책임 밖 — 상위(dispatcher)가
recipeId/fragranceResult/**flavorRecipe** 로 해석해 RecipeStep 리스트를 만든 뒤 이
resolver 로 넘긴다. 폴백 해석 헬퍼(flavor_recipe_to_steps·flavor_recipe_source_to_steps)는
이 모듈 하단(순수 함수·검증은 여전히 RR 게이트).

순수 함수(firebase/http/시리얼 무의존) — 단위테스트가 하드웨어 없이 통과.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from ..core.pump_guard import StatusErrorCode, SyringeSpec
from ..core.wire_messages import RecipeStep


@dataclass(frozen=True, slots=True)
class ResolvedStep:
    """해석된 실행 스텝(정렬·검증·파생 완료)."""

    idx: int
    pump_addr: int
    flavor: str
    # per-pump 정규화된 µL(fragrance/flavor 는 상위에서 mL→µL 정규화 후 전달).
    volume_ul: float
    # SyringeSpec 파생 스텝수(§6-4).
    steps: int
    spec: SyringeSpec


class RecipeValidationError(Exception):
    """Resolver 실패 사유 — SoT §6-7 status.errorCode 로 매핑."""

    def __init__(
        self,
        reason: str,
        *,
        idx: int | None = None,
        pump_addr: int | None = None,
        volume_ul: float | None = None,
    ) -> None:
        self.reason = reason
        self.idx = idx
        self.pump_addr = pump_addr
        self.volume_ul = volume_ul
        super().__init__(str(self))

    @property
    def error_code(self) -> StatusErrorCode:
        """검증 실패는 전부 CMD_VALIDATION_FAILED 로 drop(§6-4)."""
        return StatusErrorCode.CMD_VALIDATION_FAILED

    def __str__(self) -> str:
        parts = [self.reason]
        if self.idx is not None:
            parts.append(f"idx={self.idx}")
        if self.pump_addr is not None:
            parts.append(f"pumpAddr={self.pump_addr}")
        if self.volume_ul is not None:
            parts.append(f"volumeUl={self.volume_ul}")
        return f"RecipeValidationError({' '.join(parts)})"


@dataclass(frozen=True, slots=True)
class ResolvedRecipe:
    """성공 결과 — idx 오름차순 직렬 정렬된 실행 스텝."""

    steps: tuple[ResolvedStep, ...]

    @property
    def step_n(self) -> int:
        return len(self.steps)


class RecipeResolver:
    """Recipe Resolver.

    `pump_map` = pumpAddr → SyringeSpec(펌프별 프리셋·용량). 미매핑 addr → CMD_VALIDATION_FAILED.
    `pump_map` 은 pi settings(GET-SSE) 로 수신된 clamp 된 프리셋에서 구성된다(O-18) —
    adapters.settings_source 참조(read-only 소비).
    """

    def __init__(self, pump_map: Mapping[int, SyringeSpec]) -> None:
        # pumpAddr → SyringeSpec. PUMP_MAP(§9-1) 검증에 사용.
        self.pump_map = dict(pump_map)

    def resolve(self, steps: Sequence[RecipeStep]) -> ResolvedRecipe:
        """steps 를 정렬·검증·파생한다. 위반 시 [RecipeValidationError] raise(→ drop).

        `steps` 는 이미 µL 정규화 완료(fragrance/flavor mL→µL 는 상위·§6-6)를 전제한다.
        """
        # RR-07(Q3): 빈 레시피 → drop(0step COMPLETED 금지).
        if not steps:
            raise RecipeValidationError("empty_recipe")

        # idx 오름차순 직렬 정렬(§9-1). 안정 정렬 — 동일 idx 는 입력 순서 보존.
        sorted_steps = sorted(steps, key=lambda s: s.idx)

        resolved: list[ResolvedStep] = []
        for s in sorted_steps:
            volume_ul = float(s.volume)

            # 미매핑 pumpAddr(§9-1 PUMP_MAP) → drop.
            spec = self.pump_map.get(s.pump_addr)
            if spec is None:
                raise RecipeValidationError(
                    "unmapped_pump_addr", idx=s.idx, pump_addr=s.pump_addr
                )

            # RR-05(Q2): 음수·0·상한초과 → drop. 게이트 = `0 < volumeUl ≤ maxVolumeUl`.
            if not math.isfinite(volume_ul) or volume_ul <= 0:
                raise RecipeValidationError(
                    "non_positive_volume", idx=s.idx, pump_addr=s.pump_addr, volume_ul=volume_ul
                )
            if volume_ul > spec.max_volume_ul:
                raise RecipeValidationError(
                    "volume_over_max", idx=s.idx, pump_addr=s.pump_addr, volume_ul=volume_ul
                )

            # steps 파생(§6-4·하드코딩 금지).
            step_count = spec.steps_for_volume_ul(volume_ul)
            # steps ≥ 1(§6-4) — 파생 결과가 0 이면(극소 부피) 게이트 위반.
            if step_count < 1:
                raise RecipeValidationError(
                    "derived_zero_steps", idx=s.idx, pump_addr=s.pump_addr, volume_ul=volume_ul
                )

            resolved.append(
                ResolvedStep(
                    idx=s.idx,
                    pump_addr=s.pump_addr,
                    flavor=s.flavor,
                    volume_ul=volume_ul,
                    steps=step_count,
                    spec=spec,
                )
            )

        return ResolvedRecipe(tuple(resolved))


# ─────────────────────────────────────────────────────────────────────────────
# recipe==None 폴백 해석 헬퍼 (v1.2.0 flavorRecipe / flavor_recipes)
#
# 검증은 여전히 RR 게이트 — 여기서는 단위 정규화(mL→µL)와 스텝 조립만 한다.
# ─────────────────────────────────────────────────────────────────────────────

# 향(채널 id / 향 이름) → pumpAddr 매핑. 미매핑이면 None 반환 → 해당 스텝 pumpAddr 에
# -1 을 실어 RR 의 unmapped_pump_addr 게이트로 떨어뜨린다(silent-drop 금지).
PumpAddrOf = Callable[[str], "int | None"]

# RR 에서 절대 매칭되지 않는 미매핑 sentinel(음수 addr) — RR unmapped 게이트로 유도.
UNMAPPED_PUMP_ADDR = -1


def flavor_recipe_to_steps(
    payload: Mapping[str, Any],
    *,
    pump_addr_of: PumpAddrOf,
    sweet_pump_addr: int | None = None,
) -> list[RecipeStep]:
    """flavorRecipe(ExpoRecipePayload) → RecipeStep 리스트 — v1.2.0 식향 생성형 레시피 소비.

    정본 계약 = heysenlyt-web `lib/expo/types.ts` ExpoRecipePayload
    (= ai-developer `_expo/contract.py` RecipeResult.recipe).

      - items[] (시린지 도징): {channel_id, amount_ml, role} → RecipeStep.
        amount_ml 은 mL → **µL ×1000 정규화**(§6-6·Code 11 방지). 합 1.0mL 고정(2026-07-06).
      - sweetMl (당, 0~2mL): sweet_pump_addr 가 주어지면 당 스텝 1개 추가(µL 정규화).
        None 이면 당 도징 채널 미구성 → 스텝 생략(informational).
      - sourMl (산): **시린지 도징 아님** — 기주 밸브 온오프 threshold 판단 자리.
        threshold 비교·기주 택1 은 오케스트레이션 몫(kernel.ts 주석·2026-07-06 확정).
        여기서는 스텝을 만들지 않는다 → `flavor_sour_ml(payload)` 로 판단값만 노출(TODO: 밸브 웨이브).
      - baseMl (기주): 시린지 아님(밸브·앞단 20mL) — 스텝 생략.

    반환 리스트는 아직 검증 전(RR 이 게이트). idx 는 0부터 오름차순 직렬.
    """
    steps: list[RecipeStep] = []
    idx = 0

    raw_items = payload.get("items")
    if isinstance(raw_items, Sequence):
        for item in raw_items:
            if not isinstance(item, Mapping):
                continue
            channel_id = str(item.get("channel_id") or "")
            amount_ml = item.get("amount_ml")
            amount = float(amount_ml) if isinstance(amount_ml, (int, float)) else 0.0
            addr = pump_addr_of(channel_id)
            steps.append(
                RecipeStep(
                    idx=idx,
                    pump_addr=addr if addr is not None else UNMAPPED_PUMP_ADDR,
                    flavor=channel_id,
                    volume=amount * 1000,  # mL→µL(§6-6).
                )
            )
            idx += 1

    # 당(sweetMl) — 도징 채널이 구성된 경우에만 스텝화.
    raw_sweet = payload.get("sweetMl")
    sweet_ml = float(raw_sweet) if isinstance(raw_sweet, (int, float)) else 0.0
    if sweet_pump_addr is not None and sweet_ml > 0:
        steps.append(
            RecipeStep(
                idx=idx,
                pump_addr=sweet_pump_addr,
                flavor="sweet",
                volume=sweet_ml * 1000,  # mL→µL.
            )
        )
        idx += 1

    # sourMl 은 스텝화하지 않는다(기주 밸브 판단 자리) — flavor_sour_ml 로 노출.
    return steps


def flavor_sour_ml(payload: Mapping[str, Any]) -> float:
    """flavorRecipe.sourMl — 기주 밸브 온오프 threshold 판단용 값(mL).

    TODO(밸브 웨이브): threshold 비교 → 기주(산미/일반) 택1 밸브 제어는 오케스트레이션
    (kernel.ts: "threshold 초과 여부에 따른 기주 밸브 온오프는 오케스트레이션앱이 판단").
    지금은 판단값만 노출(절대값 threshold 는 관능 테스트 확정 대기).
    """
    raw = payload.get("sourMl")
    return float(raw) if isinstance(raw, (int, float)) else 0.0


def flavor_recipe_source_to_steps(
    source: Mapping[str, Any],
    *,
    pump_addr_of: PumpAddrOf,
    sweet_pump_addr: int | None = None,
) -> list[RecipeStep]:
    """flavor_recipes 문서(recipeId 매칭) → RecipeStep 리스트 — 폴백 경로(§9-1).

    정본 계약 = heysenlyt-web `lib/server/wire.ts` FlavorRecipeSource:
      flavors[]{name, volume|volumeUl, port} — volume 은 **이미 µL**(정규화 불필요).
      두 키 공존 시 **volume 우선**(wire.ts buildCommandRecipe 와 동일 우선순위).
      port 가 있으면 pumpAddr 로 우선 사용, 없으면 pump_addr_of(name) 매핑.
      sweetenerVolume(µL) — sweet_pump_addr 구성 시 당 스텝 추가.

    반환 리스트는 아직 검증 전(RR 이 게이트). idx 는 0부터 오름차순 직렬.
    """
    steps: list[RecipeStep] = []
    idx = 0

    raw_flavors = source.get("flavors")
    if isinstance(raw_flavors, Sequence):
        for f in raw_flavors:
            if not isinstance(f, Mapping):
                continue
            name = str(f.get("name") or "")
            # 정본 wire.ts:163 — `typeof f.volume === "number" ? f.volume : (f.volumeUl ?? 0)`
            # → **volume 우선**, 숫자가 아닐 때만 volumeUl 폴백. 두 키 공존 시 서버 조립
            # 경로와 동일한 부피를 택해야 한다(우선순위 역전 = 유효하지만 틀린 토출).
            raw_vol = f.get("volume")
            if not (isinstance(raw_vol, (int, float)) and not isinstance(raw_vol, bool)):
                raw_vol = f.get("volumeUl")
            volume_ul = (
                float(raw_vol)
                if isinstance(raw_vol, (int, float)) and not isinstance(raw_vol, bool)
                else 0.0
            )
            port = f.get("port")
            if isinstance(port, int) and not isinstance(port, bool):
                addr: int | None = port
            else:
                addr = pump_addr_of(name)
            steps.append(
                RecipeStep(
                    idx=idx,
                    pump_addr=addr if addr is not None else UNMAPPED_PUMP_ADDR,
                    flavor=name,
                    volume=volume_ul,  # 이미 µL.
                )
            )
            idx += 1

    raw_sweet = source.get("sweetenerVolume")
    sweet_ul = float(raw_sweet) if isinstance(raw_sweet, (int, float)) else 0.0
    if sweet_pump_addr is not None and sweet_ul > 0:
        steps.append(
            RecipeStep(idx=idx, pump_addr=sweet_pump_addr, flavor="sweetener", volume=sweet_ul)
        )
        idx += 1

    return steps
