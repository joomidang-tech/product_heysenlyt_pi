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
from ..ports.valve_port import VALVE_BASES


# 회전 밸브 헤드의 물리 구멍 범위 — SY-01B 12포트(서버 portLayout.MIN_PORT/MAX_PORT 와 동일).
MIN_PORT = 1
MAX_PORT = 12


# wire `op`(camelCase·서버 계약) → pi op(snake_case). 여기 없는 op 는 거부(fail-closed).
#   새 정비 동작은 **양쪽 계약에 명시적으로** 추가해야 한다 — 조용히 통과시키면 미지의 물리
#   동작이 실기기에서 실행된다.
WIRE_OP_TO_PI: dict[str, str] = {
    "estop": "estop",
    "initialize": "initialize",
    "plungerFull": "plunger_full",
    "plungerHome": "plunger_home",
}


def _is_port_valid(port: int | None) -> bool:
    """구멍 번호가 물리적으로 실재하는가(1~12). 서버 `isPortValid` 와 동일 규칙."""
    return isinstance(port, int) and not isinstance(port, bool) and MIN_PORT <= port <= MAX_PORT


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
    # 동시 실행 그룹(§9-1 v2) — 같은 stage 병렬·오름차순 배리어.
    stage: int = 0
    # ── 회전 밸브 구멍 + 속도 — **서버가 배치·정책을 해석해 실어 보낸 값**(pi 는 배치를 모른다). ──
    #   None = 구계약 스텝(포트·속도 미보유) → 어댑터가 안전 기본으로 폴백(하위호환).
    in_port: int | None = None
    out_port: int | None = None
    aspirate_speed_hz: int | None = None
    dispense_speed_hz: int | None = None
    slope: int | None = None


@dataclass(frozen=True, slots=True)
class ResolvedOpStep:
    """해석된 엔진 조작 스텝 — 토출 아님(정비 버튼).

    부피가 없으므로 부피 게이트를 타지 않는다. 대신 **op 화이트리스트**가 게이트다 —
    모르는 op 는 거부(fail-closed). 문법 번역은 어댑터 몫이라 여기선 의도만 나른다.
    """

    idx: int
    pump_addr: int
    op: str
    flavor: str
    spec: SyringeSpec
    stage: int = 0
    # 플런저 이동 **전** 회전할 밸브 포트(v1.1.0 시퀀스 복원·2026-07-19) — 흡입=air/배출=output.
    #   서버(포트 배치 SoT)가 해석해 실어 보낸다. None(구 서버) = 어댑터가 회전 생략(하위호환).
    valve_port: int | None = None


@dataclass(frozen=True, slots=True)
class ResolvedValveStep:
    """해석된 기주 밸브 스텝(§9-1 v2) — GPIO 시간축(시린지 버스와 독립·뮤텍스 L3).

    openSec 파생(volume_ml ÷ flowRate)·클램프는 ValveAdapter(설정값) — 여기는 검증만.
    """

    idx: int
    base: str  # "normal" | "sour"
    volume_ml: float  # 기주 고정 20mL
    flavor: str  # 관측 라벨 `base:{base}`
    stage: int = 0
    # 개방 시간 직접 지정(점검 "N초 열기"·2026-07-19) — None = volume_ml→flowRate 파생(제조).
    open_sec: float | None = None
    # 밸브 스위치(2026-07-19) — "open"=래치 개방(open_sec 상한 뒤 어댑터 자동 닫힘·비블로킹) ·
    #   "close"=즉시 강제 닫힘(멱등·인자 불요). None = 기존 시간축 토출/점검.
    valve_op: str | None = None


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
    """성공 결과 — stage-major(stage asc·idx asc) 정렬된 실행 스텝(§9-1 v2).

    `stages` = stage 별 동시 실행 그룹(배리어 단위). 구계약(stage 부재) 스텝은
    stage=idx 로 해석되어 그룹당 1스텝 = **기존 완전 직렬과 동일 동작**.
    """

    steps: tuple["ResolvedStep | ResolvedValveStep | ResolvedOpStep", ...]
    stages: tuple[tuple["ResolvedStep | ResolvedValveStep | ResolvedOpStep", ...], ...] = ()

    @property
    def step_n(self) -> int:
        return len(self.steps)

    @property
    def has_valve(self) -> bool:
        return any(isinstance(s, ResolvedValveStep) for s in self.steps)


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

        §9-1 v2: stage-major(stage asc·idx asc) 정렬 + stage 게이트(서버와 **동일 규칙·
        동일 순서** — pump_guard 바이트 동일 원칙 확장): ① stage 내 syringe pumpAddr 유일
        ② 잔당 valve ≤ 1 ③ stage 0..N 연속. 구계약(stage 부재)은 stage=idx = 기존 직렬.
        """
        # RR-07(Q3): 빈 레시피 → drop(0step COMPLETED 금지).
        if not steps:
            raise RecipeValidationError("empty_recipe")

        # stage-major 정렬(§9-1 v2) — tie 는 idx(전역 유일)·구계약은 stage=idx 라 기존과 동일.
        sorted_steps = sorted(steps, key=lambda s: (s.effective_stage, s.idx))

        resolved: list[ResolvedStep | ResolvedValveStep | ResolvedOpStep] = []
        stage_pumps: dict[int, set[int]] = {}
        valve_count = 0
        for s in sorted_steps:
            stage = s.effective_stage
            if stage < 0:
                raise RecipeValidationError("negative_stage", idx=s.idx)

            if s.is_engine_op:
                # ── 엔진 조작 게이트 — op 화이트리스트 + 실주소. ───────────────────
                pi_op = WIRE_OP_TO_PI.get(s.op or "")
                if pi_op is None:
                    raise RecipeValidationError("unknown_engine_op", idx=s.idx)
                op_spec = self.pump_map.get(s.pump_addr)
                if op_spec is None:
                    # ⚠️ **미매핑 addr 은 그 스텝만 건너뛴다(배치 전체를 죽이지 않는다·리뷰 P1·2026-07-18).**
                    #   dispense 는 미매핑=재료 누락=엉뚱 제품이라 fail-closed(아래 syringe 분기 raise)지만,
                    #   engineOp(특히 긴급정지)은 **닿는 펌프엔 반드시 실행돼야** 안전하다. 예: 2펌프 기기에
                    #   관제가 addr 1,2,3 estop 을 쏘면(mode 파생 초과분) 옛 코드는 addr 3 하나 때문에 **전
                    #   펌프 정지가 통째로 drop**됐다(정지가 가장 필요할 때 아무 것도 안 멈춤). 건너뛰면
                    #   1,2 는 정지한다. 전부 미매핑이면 아래 empty 가드가 실패로 잡는다(silent COMPLETE 금지).
                    continue
                resolved.append(
                    ResolvedOpStep(
                        idx=s.idx,
                        pump_addr=s.pump_addr,
                        op=pi_op,
                        flavor=s.flavor,
                        spec=op_spec,
                        stage=stage,
                        # 이동 전 밸브 회전 대상(1~12 밖·비정수는 안전측 무시 → 회전 생략).
                        valve_port=(
                            s.in_port if s.in_port is not None and 1 <= s.in_port <= 12 else None
                        ),
                    )
                )
                pumps = stage_pumps.setdefault(stage, set())
                if s.pump_addr in pumps:
                    raise RecipeValidationError(
                        "duplicate_pump_in_stage", idx=s.idx, pump_addr=s.pump_addr
                    )
                pumps.add(s.pump_addr)
                continue

            if s.is_valve:
                # ── valve 게이트(§9-1 v2): base 2종 · volumeMl > 0 · 잔당 ≤ 1(상호배타 L3). ──
                valve_count += 1
                if valve_count > 1:
                    raise RecipeValidationError("multiple_valve_steps", idx=s.idx)
                if s.base not in VALVE_BASES:
                    raise RecipeValidationError("unknown_valve_base", idx=s.idx)
                # 밸브 스위치(2026-07-19) — op 화이트리스트(모르는 op 는 fail-closed drop).
                valve_op = s.op
                if valve_op is not None and valve_op not in ("open", "close"):
                    raise RecipeValidationError("unknown_valve_op", idx=s.idx)
                volume_ml = float(s.volume_ml) if s.volume_ml is not None else 0.0
                open_sec = getattr(s, "open_sec", None)
                if valve_op == "close":
                    # 닫기 = 인자 불요(openSec·volumeMl 무시) — 멱등 강제 닫힘이라 게이트 없음.
                    resolved.append(
                        ResolvedValveStep(
                            idx=s.idx,
                            base=s.base,
                            volume_ml=0.0,
                            flavor=s.flavor,
                            stage=stage,
                            open_sec=None,
                            valve_op="close",
                        )
                    )
                    continue
                if valve_op == "open" and open_sec is None:
                    # 래치 개방은 자동 닫힘 상한(openSec)이 **필수** — 무기한 개방 금지(fail-closed).
                    raise RecipeValidationError("valve_open_requires_open_sec", idx=s.idx)
                if open_sec is not None:
                    # 점검 "N초 열기"/래치 개방(2026-07-19) — 개방시간 직접 지정. 유한·양수만(어댑터가
                    #   max_open_sec 로 상한 클램프·거부). volume_ml 은 관측 라벨용(0 허용).
                    if not math.isfinite(float(open_sec)) or float(open_sec) <= 0:
                        raise RecipeValidationError("non_positive_valve_open_sec", idx=s.idx)
                elif not math.isfinite(volume_ml) or volume_ml <= 0:
                    raise RecipeValidationError("non_positive_valve_volume", idx=s.idx)
                resolved.append(
                    ResolvedValveStep(
                        idx=s.idx,
                        base=s.base,
                        volume_ml=volume_ml,
                        flavor=s.flavor,
                        stage=stage,
                        open_sec=(float(open_sec) if open_sec is not None else None),
                        valve_op=valve_op,
                    )
                )
                continue

            # ── stage 내 syringe pumpAddr 유일(L2 — 한 펌프 in-flight 모션 1). ──
            pumps = stage_pumps.setdefault(stage, set())
            if s.pump_addr in pumps:
                raise RecipeValidationError(
                    "duplicate_pump_in_stage", idx=s.idx, pump_addr=s.pump_addr
                )
            pumps.add(s.pump_addr)

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

            # ── 회전 밸브 구멍 게이트(2차 자물쇠 — 서버가 1차). ──────────────────────
            #   pi 는 **배치를 모른다**("3번 구멍에 진짜 유자가 있나"는 알 수 없다) — 그건 설정
            #   정확성 문제이고 서버 몫이다. pi 가 지킬 수 있는 건 **물리 불변식**뿐:
            #     ① 구멍이 실재하는 범위(1~12)인가  ② 흡입≠배출(같으면 밸브를 안 돌리고 빨았다
            #     그대로 뱉는 꼴 = 조립 버그).
            #   부피 게이트(위)는 **물리 안전**(과흡입 → Code 11 펌프 파손)이라 pi 가 끝까지 쥔다.
            if s.in_port is not None or s.out_port is not None:
                if not _is_port_valid(s.in_port):
                    raise RecipeValidationError(
                        "in_port_out_of_range", idx=s.idx, pump_addr=s.pump_addr
                    )
                if not _is_port_valid(s.out_port):
                    raise RecipeValidationError(
                        "out_port_out_of_range", idx=s.idx, pump_addr=s.pump_addr
                    )
                if s.in_port == s.out_port:
                    raise RecipeValidationError(
                        "in_port_equals_out_port", idx=s.idx, pump_addr=s.pump_addr
                    )

            resolved.append(
                ResolvedStep(
                    idx=s.idx,
                    pump_addr=s.pump_addr,
                    flavor=s.flavor,
                    volume_ul=volume_ul,
                    steps=step_count,
                    spec=spec,
                    stage=stage,
                    in_port=s.in_port,
                    out_port=s.out_port,
                    aspirate_speed_hz=s.aspirate_speed_hz,
                    dispense_speed_hz=s.dispense_speed_hz,
                    slope=s.slope,
                )
            )

        # 전부 skip(예: 모든 engineOp addr 미매핑) → resolved 빈 채로 max() 가 터진다. 명시 실패로 잡아
        #   silent COMPLETE(0스텝 성공) 를 막는다 — 정지를 눌렀는데 아무 펌프도 안 멈춘 걸 성공으로 보고하면
        #   안 된다(리뷰 P1). 입력 자체가 빈 경우는 위(:180)에서 이미 empty_recipe 로 잡힌다.
        if not resolved:
            raise RecipeValidationError("empty_recipe")

        # ── stage 연속성(0..N 결번 금지 — 결번 = 조립 버그·배리어 의미 모호). ──
        seen_stages = {r.stage for r in resolved}
        max_stage = max(seen_stages)
        for i in range(max_stage + 1):
            if i not in seen_stages:
                raise RecipeValidationError(f"stage_gap_missing_{i}")

        # stage 그룹핑(배리어 단위) — resolved 는 이미 stage-major 정렬.
        stages: list[tuple[ResolvedStep | ResolvedValveStep | ResolvedOpStep, ...]] = []
        for i in range(max_stage + 1):
            stages.append(tuple(r for r in resolved if r.stage == i))

        return ResolvedRecipe(tuple(resolved), tuple(stages))


# ─────────────────────────────────────────────────────────────────────────────
# recipe==None 폴백 해석 헬퍼 (v1.2.0 flavorRecipe / flavor_recipes)
#
# 검증은 여전히 RR 게이트 — 여기서는 단위 정규화(mL→µL)와 스텝 조립만 한다.
#
# ⚠️ **미배선(legacy·테스트 전용)**: 이 헬퍼들(flavor_recipe_to_steps·flavor_sour_ml·
#   flavor_recipe_source_to_steps)은 **데몬 실행 경로에 결선돼 있지 않다**(비-테스트 호출 0).
#   현 기본 결선 dispatcher interpret = daemon._default_interpret(명시 recipe 만·recipe==None →
#   빈 스텝 → RR empty drop = fail-closed). 특히 생성형 flavorRecipe 는 **기주 valve 스텝을
#   재구성할 수 없어**(서버 조립 축) 이 폴백을 실행 경로에 주입하면 안 된다(위 flavor_recipe_to_steps
#   docstring 리뷰 봉합). 서버 계약(§9-1 v2)이 valve 를 실물화하므로 pi 는 서버 조립 스텝만
#   소비한다 — 이 헬퍼는 계약 파싱 단위테스트 + 미래 recipeId 폴백 승격용으로 남긴다.
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
        threshold 비교·기주 택1 은 **서버** 몫(D18 — threshold 는 서버 설정 지식·pi 재현 불가).
        ⚠️ §9-1 v2: 기주는 서버 조립 valve 스텝으로 실물화된다 — **이 폴백은 valve 를 재구성할
        수 없으므로**, 생성형(flavorRecipe) 주문의 실행 경로로 이 헬퍼를 주입 결선하지 말 것
        (기본 결선 _default_interpret = 빈 스텝 → empty drop = fail-closed 유지·리뷰 봉합).
        여기서는 스텝을 만들지 않는다 → `flavor_sour_ml(payload)` 로 판단값만 노출.
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
