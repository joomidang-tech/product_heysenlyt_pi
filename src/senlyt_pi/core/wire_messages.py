"""와이어 메시지 (command / status / heartbeat) — SoT §9. 세 와이어 모두 PII 미포함.

Dart `lib/core/wire_messages.dart` 포팅. **양 언어 바이트 동일**(TS interface ↔ Python
dataclass). includeIfNull:false 규칙(부록A P-4)을 `put_if_present` 로 지킨다.

합성 멱등키 규약(부록A P-2): `command.id` = `status.id` = `{orderId}:{attempt}`
  — 콜론 구분·attempt 십진(zero-pad 금지). order.id 와 다르다.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .pump_guard import StatusErrorCode
from .wire_json import put_if_present


def build_command_id(order_id: str, attempt: int) -> str:
    """합성 멱등키 조립 — SoT §5-6 / 부록A P-2. `{orderId}:{attempt}` (콜론·zero-pad 없음)."""
    return f"{order_id}:{attempt}"


# ─────────────────────────────────────────────────────────────────────────────
# §9-1  command (서버 → pi · SSE snapshot 파생)
# ─────────────────────────────────────────────────────────────────────────────


# RecipeStep.kind 2종(§9-1 v2 — 2026-07-14 병렬 stage 계약).
SYRINGE_STEP_KIND = "syringe"
VALVE_STEP_KIND = "valve"

# valve 스텝의 pump_addr sentinel — 시린지 버스(RS485) 주소가 아니다(GPIO). RR pump_map
# 검증을 우회하는 게 아니라 valve 분기에서 아예 pump_addr 를 보지 않는다.
VALVE_PUMP_ADDR = -2


@dataclass(frozen=True, slots=True)
class RecipeStep:
    """recipe 스텝 — SoT §9-1 v2(병렬 stage 계약 · 99_daily/2026-07-14-pi데몬-병렬토출-설계).

    - kind="syringe"(기본): pump_addr·volume(µL) 사용.
    - kind="valve"(식향 기주 택1): base("normal"|"sour")·volume_ml(고정 20) 사용 —
      openSec 계산·핀 매핑(신기주 핀11/BCM17·베이스 핀13/BCM27)·클램프는 ValveAdapter(설정값).
    - stage: 동시 실행 그룹 — 같은 stage 병렬·오름차순 배리어. **부재(None) = idx 로 해석**
      (하위호환: 전 스텝이 서로 다른 stage = 기존 완전 직렬과 동일 동작).
    """

    idx: int
    pump_addr: int
    flavor: str
    volume: float  # µL (int|float 그대로 보존) — valve 스텝은 0.0(미사용)
    kind: str = SYRINGE_STEP_KIND
    stage: int | None = None  # None = idx (하위호환·§9-1 v2)
    base: str | None = None  # valve 전용 — "normal" | "sour"
    volume_ml: float | None = None  # valve 전용 — 기주 부피(고정 20mL)

    @property
    def effective_stage(self) -> int:
        """stage 부재(구계약) 스텝은 idx 가 곧 stage — 기존 완전 직렬 보존."""
        return self.stage if self.stage is not None else self.idx

    @property
    def is_valve(self) -> bool:
        return self.kind == VALVE_STEP_KIND

    @staticmethod
    def from_json(j: Mapping[str, Any]) -> "RecipeStep":
        kind = str(j.get("kind", SYRINGE_STEP_KIND))
        raw_stage = j.get("stage")
        stage = int(raw_stage) if raw_stage is not None else None
        if kind == VALVE_STEP_KIND:
            base = str(j["base"])
            return RecipeStep(
                idx=int(j["idx"]),
                pump_addr=VALVE_PUMP_ADDR,
                flavor=str(j.get("flavor", f"base:{base}")),
                volume=0.0,
                kind=VALVE_STEP_KIND,
                stage=stage,
                base=base,
                volume_ml=float(j["volumeMl"]),
            )
        return RecipeStep(
            idx=int(j["idx"]),
            pump_addr=int(j["pumpAddr"]),
            flavor=j["flavor"],
            volume=j["volume"],
            kind=kind,
            stage=stage,
        )

    def to_json(self) -> dict[str, Any]:
        if self.kind == VALVE_STEP_KIND:
            return {
                "idx": self.idx,
                "stage": self.effective_stage,
                "kind": VALVE_STEP_KIND,
                "base": self.base,
                "volumeMl": self.volume_ml,
                "flavor": self.flavor,
                # 구데몬 호환 sentinel(리뷰 P2-4) — kind 를 모르는 구 pi 가 이 스텝을 시린지로
                # 읽어도 pumpAddr=-2(미매핑)·volume=0 이라 RR 게이트가 CMD_VALIDATION_FAILED 로
                # **우아하게 drop**(KeyError 크래시 아님·토출 0).
                "pumpAddr": VALVE_PUMP_ADDR,
                "volume": 0,
            }
        m: dict[str, Any] = {
            "idx": self.idx,
            "pumpAddr": self.pump_addr,
            "flavor": self.flavor,
            "volume": self.volume,
        }
        # 하위호환 바이트 보존 — 구계약 스텝(stage 미지정)은 구형 4필드 그대로 방출.
        if self.stage is not None:
            m["stage"] = self.stage
            m["kind"] = self.kind
        return m


@dataclass(frozen=True, slots=True)
class Command:
    """command — SoT §9-1.

    `recipe is None` 이면 pi 가 recipeId(flavor)/fragranceResult(fragrance)로 해석(§9-1).
    """

    id: str  # `{orderId}:{attempt}` — 합성 멱등키(order.id 아님·부록A P-2)
    order_id: str
    attempt: int  # int·최초 1·재시도마다 +1
    device_id: str  # 라우팅·pi 자기것만 소비(CS-08)
    recipe: tuple[RecipeStep, ...] | None  # recipe steps | None
    trace_id: str
    created_at: str  # ISO8601 (resync 기준·재포맷 금지·부록A P-3)

    @staticmethod
    def from_json(j: Mapping[str, Any]) -> "Command":
        raw_recipe = j.get("recipe")
        return Command(
            id=j["id"],
            order_id=j["orderId"],
            attempt=int(j["attempt"]),
            device_id=j["deviceId"],
            recipe=None
            if raw_recipe is None
            else tuple(RecipeStep.from_json(s) for s in raw_recipe),
            trace_id=j["traceId"],
            created_at=j["createdAt"],
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "orderId": self.order_id,
            "attempt": self.attempt,
            "deviceId": self.device_id,
            # recipe 는 null 도 의미가 있으므로(§9-1 폴백 신호) 명시적으로 방출.
            "recipe": None if self.recipe is None else [s.to_json() for s in self.recipe],
            "traceId": self.trace_id,
            "createdAt": self.created_at,
        }


# ─────────────────────────────────────────────────────────────────────────────
# §9-2  status (pi → 서버 · PATCH /api/dispenser/orders/[id] body)
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class StatusReport:
    """status report — SoT §9-2. phase→WireStatus 는 order_status.phase_to_wire_status."""

    id: str  # `{orderId}:{attempt}` (= command.id)
    phase: str  # "ACCEPTED" | "PROGRESS" | "COMPLETED" | "FAILED" — 단조·역행 금지
    step_k: int
    step_n: int
    error_code: StatusErrorCode | None  # 7종 enum | None
    request_id: str  # uuid — 서버 dedup(OQ flush at-least-once)
    trace_id: str
    updated_at: str  # ISO8601 (재포맷 금지·부록A P-3)

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "phase": self.phase,
            "stepK": self.step_k,
            "stepN": self.step_n,
            # errorCode 는 null 도 의미(정상)이므로 명시 방출 — 서버 계약이 `ErrorCode | null`.
            "errorCode": self.error_code.wire if self.error_code else None,
            "requestId": self.request_id,
            "traceId": self.trace_id,
            "updatedAt": self.updated_at,
        }


# ─────────────────────────────────────────────────────────────────────────────
# §9-3  heartbeat (pi → 서버 · PATCH /api/dispenser/heartbeat)
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class Heartbeat:
    """heartbeat request — SoT §9-3. ⚠️ traceId 없음(주문 무관·deviceId 상관).

    주기 30s(±jitter). online 판정 = 최근 3주기(90s) 내(서버 판정·pi 시계 미신뢰).
    """

    device_id: str
    queue_depth: int  # int·유휴=0
    engine: str | None = None  # "sy01b" | None
    last_error: StatusErrorCode | None = None  # 7종 | None
    # (선택·세척 계약 기존 설계 유지) — HeartbeatRequest.needsCleaning (2026-07-09 레지스트리 연동 확장).
    needs_cleaning: bool | None = None

    def to_json(self) -> dict[str, Any]:
        """includeIfNull:false — engine/lastError/needsCleaning 은 부재 시 키 방출 안 함(부록A P-4)."""
        m: dict[str, Any] = {
            "deviceId": self.device_id,
            "queueDepth": self.queue_depth,
        }
        put_if_present(m, "engine", self.engine)
        put_if_present(m, "lastError", self.last_error.wire if self.last_error else None)
        put_if_present(m, "needsCleaning", self.needs_cleaning)
        return m
