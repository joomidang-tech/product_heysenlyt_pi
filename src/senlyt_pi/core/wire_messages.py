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
# 엔진 조작(정비 버튼) — 토출 아님. 서버가 **의도**(op)만 싣고 문법 번역은 어댑터가 한다.
ENGINE_OP_STEP_KIND = "engineOp"

# valve 스텝의 pump_addr sentinel — 시린지 버스(RS485) 주소가 아니다(GPIO). RR pump_map
# 검증을 우회하는 게 아니라 valve 분기에서 아예 pump_addr 를 보지 않는다.
VALVE_PUMP_ADDR = -2


@dataclass(frozen=True, slots=True)
class RecipeStep:
    """recipe 스텝 — SoT §9-1 v2(병렬 stage 계약 · 99_daily/2026-07-14-pi데몬-병렬토출-설계).

    - kind="syringe"(기본): pump_addr·volume(µL) 사용.
    - kind="valve"(식향 기주 택1): base("normal"|"sour")·volume_ml(고정 20) 사용 —
      openSec 계산·핀 매핑(신기주 BCM17/물리핀11·베이스 BCM27/물리핀13)·클램프는 ValveAdapter(설정값).
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
    # valve 전용(2026-07-19 점검 기능) — **개방 시간 직접 지정**(초). 있으면 volume_ml→flowRate
    #   파생 대신 이 값으로 개방(어댑터 max_open_sec 클램프는 동일 적용). 관제 "기주밸브 N초
    #   열기" 점검용 — 구 pi 는 이 키를 몰라 volumeMl(0) 파생 0초 개방 = 무해 no-op(하위호환).
    open_sec: float | None = None
    # engineOp 전용 — "estop" | "initialize" | "plungerFull" | "plungerHome"(wire camelCase 그대로).
    op: str | None = None
    # ── 회전 밸브 구멍 + 속도 (2026-07-17 · 서버가 배치·정책을 해석해 실어 보낸다) ──────
    #
    # `in_port` = 이 액체가 꽂힌 구멍(`I{n}`) · `out_port` = 배출 구멍(`O{n}`). **pi 는 배치를
    # 모른다** — 어느 펌프 몇 번 구멍에 뭐가 꽂혔는지는 기기설정(서버 `pumpPorts`)이 정본이고,
    # pi 는 받은 번호로 밸브를 돌릴 뿐이다. pump_addr 만으로는 한 펌프 다포트 헤드의 여러 액체를
    # 구분할 수 없어(향료 16종 vs 펌프 2대) 이 필드 없이는 실토출 자체가 불가능하다.
    #
    # 속도도 서버가 전역설정 × 포트 오버라이드를 해석해 확정한 값이다(더 느린 쪽 = 고점도 보호).
    # pi 는 프리셋 상한으로 클램프만 하고 그대로 쓴다 — 속도 *정책*을 알 필요가 없다.
    #
    # None = 구계약(포트·속도 미보유) — 어댑터가 안전 기본으로 폴백한다(하위호환).
    in_port: int | None = None
    out_port: int | None = None
    aspirate_speed_hz: int | None = None
    dispense_speed_hz: int | None = None
    slope: int | None = None

    @property
    def effective_stage(self) -> int:
        """stage 부재(구계약) 스텝은 idx 가 곧 stage — 기존 완전 직렬 보존."""
        return self.stage if self.stage is not None else self.idx

    @property
    def is_valve(self) -> bool:
        return self.kind == VALVE_STEP_KIND

    @property
    def is_engine_op(self) -> bool:
        return self.kind == ENGINE_OP_STEP_KIND

    @staticmethod
    def from_json(j: Mapping[str, Any]) -> "RecipeStep":
        kind = str(j.get("kind", SYRINGE_STEP_KIND))
        raw_stage = j.get("stage")
        stage = int(raw_stage) if raw_stage is not None else None
        if kind == ENGINE_OP_STEP_KIND:
            # `valvePort`(2026-07-19 · v1.1.0 시퀀스 복원) = 플런저 이동 **전** 회전할 밸브 구멍.
            #   흡입(plungerFull)=air / 배출(plungerHome)=output — 포트 배치 SoT=서버가 해석해
            #   실어 보낸다. 의미가 `I{n}` 회전 대상이라 기존 in_port 필드에 싣는다(새 필드 불요).
            #   부재(구 서버) = None → 어댑터가 회전 생략(하위호환).
            vp = j.get("valvePort")
            return RecipeStep(
                idx=int(j["idx"]),
                pump_addr=int(j["pumpAddr"]),
                flavor=str(j.get("flavor", f"op:{j.get('op')}")),
                volume=0.0,
                kind=ENGINE_OP_STEP_KIND,
                stage=stage,
                op=str(j["op"]),
                in_port=(
                    int(vp) if isinstance(vp, (int, float)) and not isinstance(vp, bool) else None
                ),
            )
        if kind == VALVE_STEP_KIND:
            base = str(j["base"])
            raw_open = j.get("openSec")
            return RecipeStep(
                idx=int(j["idx"]),
                pump_addr=VALVE_PUMP_ADDR,
                flavor=str(j.get("flavor", f"base:{base}")),
                volume=0.0,
                kind=VALVE_STEP_KIND,
                stage=stage,
                base=base,
                volume_ml=float(j.get("volumeMl", 0.0)),
                open_sec=(
                    float(raw_open)
                    if isinstance(raw_open, (int, float)) and not isinstance(raw_open, bool)
                    else None
                ),
            )
        def _opt_int(key: str) -> int | None:
            v = j.get(key)
            return int(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None

        return RecipeStep(
            idx=int(j["idx"]),
            pump_addr=int(j["pumpAddr"]),
            flavor=j["flavor"],
            volume=j["volume"],
            kind=kind,
            stage=stage,
            in_port=_opt_int("inPort"),
            out_port=_opt_int("outPort"),
            aspirate_speed_hz=_opt_int("aspirateSpeedHz"),
            dispense_speed_hz=_opt_int("dispenseSpeedHz"),
            slope=_opt_int("slope"),
        )

    def to_json(self) -> dict[str, Any]:
        if self.kind == ENGINE_OP_STEP_KIND:
            out: dict[str, Any] = {
                "idx": self.idx,
                "stage": self.effective_stage,
                "kind": ENGINE_OP_STEP_KIND,
                "pumpAddr": self.pump_addr,
                "op": self.op,
                "flavor": self.flavor,
                "volume": 0,
            }
            if self.in_port is not None:
                out["valvePort"] = self.in_port  # 이동 전 회전 밸브(왕복 보존·from_json 대칭)
            return out
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
        # 포트·속도는 **보유할 때만** 방출 — 구계약 스텝의 바이트를 늘리지 않는다(왕복 보존).
        for key, val in (
            ("inPort", self.in_port),
            ("outPort", self.out_port),
            ("aspirateSpeedHz", self.aspirate_speed_hz),
            ("dispenseSpeedHz", self.dispense_speed_hz),
            ("slope", self.slope),
        ):
            if val is not None:
                m[key] = val
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

    주기 10s(±jitter·SENLYT_HEARTBEAT_INTERVAL_MS). online 표시 판정 = 최근 3주기(30s)·안전 판정(reclaim 등)은 90s 별도 창(서버 판정·pi 시계 미신뢰).
    """

    device_id: str
    queue_depth: int  # int·유휴=0
    # "sy01b"(실 RS485) | "fake"(자동감지 폴백·비-Pi·어댑터 미장착) | 미지 어댑터 클래스명 | None.
    #   ⚠️ 데몬은 fake 도 **항상** 실어 보낸다(daemon.engine_wire_name) — 키 부재 = 보고 누락이지
    #   "엔진 없음"이 아니다. admin 이 online 인데 "엔진 —"으로 뜨던 원인(2026-07-17 봉합).
    engine: str | None = None
    last_error: StatusErrorCode | None = None  # 7종 | None
    # (선택·세척 계약 기존 설계 유지) — HeartbeatRequest.needsCleaning (2026-07-09 레지스트리 연동 확장).
    needs_cleaning: bool | None = None
    # 기기 연결 상태(2026-07-19·연결상태 기능) — 부팅 자동인식 결과를 admin 표시용으로 보고.
    #   pumps = 응답한 펌프 주소(시리얼 probe = 실연결 판정). valves = GPIO 라인이 클레임된 기주밸브 base
    #   (= "핀 사용가능"·비-실행 read-only 판정 — on/off 안 함). ⚠️ 밸브는 '핀 살아있음'이지 '실제 밸브
    #   장착'이 아니다(GPIO 출력이라 응답 없음) — admin 라벨에서 구분.
    pumps: "list[int] | None" = None
    valves: "list[str] | None" = None
    # 주기 HW 감시 실측(2026-07-19 "데몬이 항상 감시해야 서버에서 상황 파악" 요구) —
    #   pumpHealth = {addr: "ok"|"garbled"|"silent"} (idle 시 ~30s 주기 `?` 프로브 실측).
    #   pumps(부팅 스냅샷)와 달리 **지금 상태** — admin 연결 칩이 이걸로 초록/노랑/빨강 표시.
    #   hwCheckedAt = 마지막 프로브 시각(ISO) — "N초 전 확인" 표시용.
    pump_health: "dict[int, str] | None" = None
    hw_checked_at: str | None = None

    def to_json(self) -> dict[str, Any]:
        """includeIfNull:false — 선택 필드는 부재 시 키 방출 안 함(부록A P-4)."""
        m: dict[str, Any] = {
            "deviceId": self.device_id,
            "queueDepth": self.queue_depth,
        }
        put_if_present(m, "engine", self.engine)
        put_if_present(m, "lastError", self.last_error.wire if self.last_error else None)
        put_if_present(m, "needsCleaning", self.needs_cleaning)
        put_if_present(m, "pumps", self.pumps)
        put_if_present(m, "valves", self.valves)
        put_if_present(
            m,
            "pumpHealth",
            {str(a): s for a, s in self.pump_health.items()} if self.pump_health else None,
        )
        put_if_present(m, "hwCheckedAt", self.hw_checked_at)
        return m
