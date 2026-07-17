"""pi settings read-only 소비 — clamp 된 프리셋 → pumpAddr→SyringeSpec 매핑(O-18).

서버가 GET-SSE 로 내려주는 settings(이미 settingsClamp.ts 로 clamp 완료)를 **읽기 전용**으로
소비해 RecipeResolver 의 pump_map 을 구성한다. pi 는 settings 를 절대 수정/역보고하지 않는다
(정본 = 서버·단방향). 방어적 이중 clamp: 수신값도 core.pump_guard.clamp_pump_preset 로 한 번 더
통과시킨다(서버 ↔ pi 바이트-parity 이므로 정상 입력에선 no-op — §11 O-17 이중방어와 같은 결).

⚠️ **런타임 미배선(감사 P2·후속 웨이브)**: 이 모듈의 헬퍼는 아직 daemon 결선에 쓰이지 않는다.
  실 진입점(senlytd._run)은 bootstrap.build_resolver 가 부팅 시점 `PUMP_ADDRESSES` env 로 만든
  pump_map 을 고정 사용하고, RecipeResolver.pump_map 은 런타임 변이 경로가 없다. 즉 운영자의
  admin 설정(syringeCapacityMl 등) 변경은 **가동 중 pi 에 반영되지 않으며 재기동이 필요**하다.
  실배선(SSE settings 구독 → 진행 중 제조와 무경합 스왑)은 별도 웨이브(밸브 flowRate SoT 승격과 함께).

기대 settings 형태(관련 부분만·나머지 키는 무시):
  {
    "pumps": [
      {"pumpAddr": 1, "mode": "flavor",            # "flavor" | "fragrance"
       "syringeCapacityMl": 0.5,                    # 9종 allowlist 밖 → 기본 0.5mL 폴백(양 모드 공통)
       "pumpPresetId": "sy01b", ...프리셋 필드},     # clamp_pump_preset 입력 그대로
      ...
    ]
  }
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from ..core.pump_guard import SyringeSpec, clamp_pump_preset, resolve_syringe_capacity_ml


def syringe_spec_from_pump_settings(pump: Mapping[str, Any]) -> SyringeSpec:
    """펌프 1개 settings → SyringeSpec (read-only 파생·방어적 재clamp)."""
    preset = clamp_pump_preset(pump)
    mode = pump.get("mode")
    is_flavor = mode != "fragrance"  # 미지정/오타는 flavor 취급(폴백 용량은 양 모드 공통 0.5mL — O-15)
    capacity = resolve_syringe_capacity_ml(pump.get("syringeCapacityMl"), is_flavor=is_flavor)
    return SyringeSpec(pump_full_stroke=preset.pump_full_stroke, syringe_capacity_ml=capacity)


def pump_map_from_settings(settings: Mapping[str, Any] | None) -> dict[int, SyringeSpec]:
    """settings → pumpAddr→SyringeSpec 매핑(RecipeResolver.pump_map 입력).

    pumpAddr 누락/비정수 항목은 건너뛴다(미매핑 addr 는 RR 게이트가 drop — silent 매핑 금지).
    """
    if settings is None:
        return {}
    raw_pumps = settings.get("pumps")
    if not isinstance(raw_pumps, Sequence):
        return {}

    pump_map: dict[int, SyringeSpec] = {}
    for pump in raw_pumps:
        if not isinstance(pump, Mapping):
            continue
        addr = pump.get("pumpAddr")
        if isinstance(addr, bool) or not isinstance(addr, int):
            continue
        pump_map[addr] = syringe_spec_from_pump_settings(pump)
    return pump_map
