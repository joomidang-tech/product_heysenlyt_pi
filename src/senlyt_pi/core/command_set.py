"""CommandSet 봉투 와이어 모델 — 계약 `device-registration-commandset-wire` (2026-07-09).

정본: 01_tech-spec §6.5 · 04_erd §16 · 05_api §8.

서버=두뇌(wire.ts buildCommandRecipe 가 steps 를 완성), pi=손(안전게이트+실행+상태보고).
  - PumpStep = 기존 RecipeStep(§9-1) 와이어 동형 — `RecipeStep.from_json` 재사용.
  - kind=manufacture → commandSetId = 기존 합성 멱등키 `{orderId}:{attempt}`
    (pi ledger 키 동일값 — ledger 무변경). sourceOrderId·attempt 필수.
  - kind=maintenance → commandSetId = `mnt-{uuid}` (admin 발행 — 세척·퍼지·프라임).
  - steps=None = 레거시 폴백 신호(manufacture 만) — pi recipe_resolver(강등·삭제 아님)가
    recipeId/fragranceResult/expoRecipe 로 자체 해석.
  - 전달 = 기존 SSE snapshot 에 commandSets 필드 추가(queued|delivered 만 push ·
    자기 deviceId 필터 CS-08 동형). 기존 orders/commands 소비자 무파괴.

CommandSet.status 는 큐/전달 관측용 **별도 축** — 주문 status 소유권(pi 단독 전진)·
D15 baseLiquorUsed 동봉(PATCH /api/dispenser/orders/[id])은 불변.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .pump_guard import StatusErrorCode
from .wire_json import put_if_present
from .wire_messages import RecipeStep

# maintenance commandSetId prefix — `mnt-{uuid}` (계약 CommandSet.commandSetId).
MAINTENANCE_COMMAND_SET_PREFIX = "mnt-"

# kind 2종 — 계약 CommandSet.kind.
COMMAND_SET_KINDS = ("manufacture", "maintenance")


class CommandSetStatus(enum.Enum):
    """CommandSet 상태 5종 — 계약 CommandSetStatus.

    단조 전이: queued(0)→delivered(1)→running(2)→done|failed(3, 동급 terminal).
    전진 skip 허용(at-least-once 레이스 무해) · 역행 illegal(서버 422) · 동일값 noop.
    write 주체: queued=서버(생성) / delivered·running·done·failed=pi(PATCH).
    """

    QUEUED = "queued"
    DELIVERED = "delivered"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"

    @property
    def wire(self) -> str:
        return self.value

    @property
    def rank(self) -> int:
        """단조성 판정 등급 — done|failed 는 동급 terminal(3)."""
        if self is CommandSetStatus.QUEUED:
            return 0
        if self is CommandSetStatus.DELIVERED:
            return 1
        if self is CommandSetStatus.RUNNING:
            return 2
        return 3  # DONE | FAILED — terminal.

    @property
    def is_terminal(self) -> bool:
        return self is CommandSetStatus.DONE or self is CommandSetStatus.FAILED

    @staticmethod
    def from_wire(v: Any) -> "CommandSetStatus | None":
        if not isinstance(v, str):
            return None
        for s in CommandSetStatus:
            if s.wire == v:
                return s
        return None


def can_transition(frm: CommandSetStatus, to: CommandSetStatus) -> bool:
    """전이 허용 판정 — 계약 x-transitions 표와 동형.

    전진(skip 포함)만 True. 동일값(noop)·역행·terminal 이탈은 False —
    동일값 noop(applied:false) 처리와 역행 422 구분은 호출측(서버 게이트) 몫.
    """
    if frm.is_terminal:
        return False  # terminal 은 exit 없음(done→failed 도 금지 — 동급 terminal).
    return to.rank > frm.rank


@dataclass(frozen=True, slots=True)
class CommandSet:
    """CommandSet 봉투 — 1회 전달(HTTP 다회 왕복 금지)."""

    command_set_id: str  # PK. manufacture=`{orderId}:{attempt}` / maintenance=`mnt-{uuid}`
    device_id: str  # 라우팅 대상 — pi 는 자기 것만 소비(CS-08 동형)
    kind: str  # "manufacture" | "maintenance"
    steps: tuple[RecipeStep, ...] | None  # None=레거시 폴백 신호(manufacture 만)
    status: CommandSetStatus
    created_at: str  # ISO8601 — 큐 정렬·resync 기준(재포맷 금지·부록A P-3)
    created_by: str  # 'server' | 'operator:{operatorId}'
    source_order_id: str | None = None  # manufacture 필수
    attempt: int | None = None  # manufacture 필수 — 합성키 구성·D1
    trace_id: str | None = None  # (선택) orders.traceId 미러(§7)
    error_code: StatusErrorCode | None = None  # (선택) failed 시
    updated_at: str | None = None  # (선택) 마지막 전이 시각

    @staticmethod
    def from_json(j: Mapping[str, Any]) -> "CommandSet":
        """와이어 파싱 — 계약 required 위반은 ValueError(방어)."""
        kind = j["kind"]
        if kind not in COMMAND_SET_KINDS:
            raise ValueError(f"CommandSet.kind 미지값: {kind!r}")

        status = CommandSetStatus.from_wire(j["status"])
        if status is None:
            raise ValueError(f"CommandSet.status 미지값: {j['status']!r}")

        raw_steps = j["steps"]  # 키 자체는 required(null 허용 — 폴백 신호).
        steps = (
            None
            if raw_steps is None
            else tuple(RecipeStep.from_json(s) for s in raw_steps)
        )
        if steps is not None and len(steps) < 1:
            raise ValueError("CommandSet.steps 는 minItems 1(빈 배열 금지 — null 이 폴백 신호)")

        source_order_id = j.get("sourceOrderId")
        raw_attempt = j.get("attempt")
        attempt = int(raw_attempt) if raw_attempt is not None else None

        if kind == "manufacture":
            if not isinstance(source_order_id, str) or source_order_id == "":
                raise ValueError("manufacture CommandSet 은 sourceOrderId 필수")
            if attempt is None or attempt < 1:
                raise ValueError("manufacture CommandSet 은 attempt(≥1) 필수")
        else:
            # maintenance 는 steps 필수(폴백 신호 없음 — 계약 oneOf 는 manufacture 전용).
            if steps is None:
                raise ValueError("maintenance CommandSet 은 steps 필수(null 폴백 없음)")

        return CommandSet(
            command_set_id=j["commandSetId"],
            device_id=j["deviceId"],
            kind=kind,
            steps=steps,
            status=status,
            created_at=j["createdAt"],
            created_by=j["createdBy"],
            source_order_id=source_order_id if isinstance(source_order_id, str) else None,
            attempt=attempt,
            trace_id=j.get("traceId"),
            error_code=StatusErrorCode.from_wire(j.get("errorCode")),
            updated_at=j.get("updatedAt"),
        )

    def to_json(self) -> dict[str, Any]:
        m: dict[str, Any] = {
            "commandSetId": self.command_set_id,
            "deviceId": self.device_id,
            "kind": self.kind,
            # steps 는 null 도 의미(폴백 신호)이므로 명시 방출 — Command.recipe 와 동형.
            "steps": None if self.steps is None else [s.to_json() for s in self.steps],
            "status": self.status.wire,
            "createdAt": self.created_at,
            "createdBy": self.created_by,
        }
        put_if_present(m, "sourceOrderId", self.source_order_id)
        put_if_present(m, "attempt", self.attempt)
        put_if_present(m, "traceId", self.trace_id)
        put_if_present(m, "errorCode", self.error_code.wire if self.error_code else None)
        put_if_present(m, "updatedAt", self.updated_at)
        return m


def command_sets_from_snapshot(
    snapshot: Mapping[str, Any], device_id: str
) -> list[CommandSet]:
    """SSE snapshot data → 소비 대상 CommandSet 목록 — 계약 DispenserSnapshotEvent.

    - `commandSets` 신규 필드(부재 시 빈 목록 — 구서버 상위 호환).
    - 자기 deviceId 만(CS-08 동형) + queued|delivered 만(서버 push 범위와 동일 —
      running/terminal 은 소비 대상 아님).
    - createdAt 오름차순 정렬(큐 정렬 기준 — ISO8601 문자열 비교·부록A P-3).
    - 항목 단위 방어 파싱: 깨진 항목은 건너뛴다(전체 snapshot 을 죽이지 않음).
    """
    raw = snapshot.get("commandSets")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return []

    out: list[CommandSet] = []
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        try:
            cs = CommandSet.from_json(item)
        except (KeyError, TypeError, ValueError):
            continue  # 항목 단위 방어 — 깨진 봉투는 skip(나머지 소비 계속).
        if cs.device_id != device_id:
            continue
        if cs.status not in (CommandSetStatus.QUEUED, CommandSetStatus.DELIVERED):
            continue
        out.append(cs)

    out.sort(key=lambda c: c.created_at)
    return out
