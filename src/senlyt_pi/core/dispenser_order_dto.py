"""DispenserOrder DTO (수신측 미러) — SoT §5. Dart `lib/core/dispenser_order_dto.dart` 포팅.

**정본 = 서버 `dispenserOrder.ts` `toDispenserOrderDTO`.** 서버가 이 형상으로 투영해
SSE snapshot 으로 보내면 pi 가 이 클래스로 파싱한다. 양 언어 바이트 동일.

pi 는 **소비자**(투영을 만들지 않는다) — PII 봉인은 서버가 담당하고(§5-2), pi 는 구조적으로
uid/userName/연락처/IP/sessionId 를 볼 수 없다. 이 클래스에는 그 필드가 존재하지 않는다.

includeIfNull:false(부록A P-4): 옵셔널 부재 = None 으로 수용, 직렬화 시 키 재부재로 재현.
createdAt(부록A P-3): 서버가 준 ISO8601 string 을 **재포맷 없이 보존**.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .order_status import WireStatus
from .wire_json import put_if_present

# net-new 3필드 마이그레이션 폴백 — SoT §5-4.
#
# v1.1.0 계승 주문(deviceId/attempt/traceId 부재)의 non-null 파싱 보호.
# 실제 값은 서버 투영이 채우는 것이 정상 경로이며, 이 상수는 방어선일 뿐이다.
DEFAULT_DEVICE_ID = "default"


def _as_map(v: Any) -> dict[str, Any] | None:
    return dict(v) if isinstance(v, Mapping) else None


@dataclass(frozen=True, slots=True)
class FlavorSub:
    """flavor 서브객체 — SoT §5-1. content 는 형상 보존만(pi 는 평탄미러 우선 읽기·O-8)."""

    recipe_id: str  # 없으면 ""
    flavor_content: dict[str, Any] | None = None  # LocalizedContent<FlavorContent>

    @staticmethod
    def from_json(j: Mapping[str, Any]) -> "FlavorSub":
        recipe_id = j.get("recipeId")
        return FlavorSub(
            recipe_id=recipe_id if isinstance(recipe_id, str) else "",
            flavor_content=_as_map(j.get("flavorContent")),
        )

    def to_json(self) -> dict[str, Any]:
        m: dict[str, Any] = {"recipeId": self.recipe_id}
        put_if_present(m, "flavorContent", self.flavor_content)
        return m


@dataclass(frozen=True, slots=True)
class FragranceSub:
    """fragrance 서브객체 — SoT §5-1."""

    fragrance_content: dict[str, Any] | None = None
    fragrance_result: dict[str, Any] | None = None

    @staticmethod
    def from_json(j: Mapping[str, Any]) -> "FragranceSub":
        return FragranceSub(
            fragrance_content=_as_map(j.get("fragranceContent")),
            fragrance_result=_as_map(j.get("fragranceResult")),
        )

    def to_json(self) -> dict[str, Any]:
        m: dict[str, Any] = {}
        put_if_present(m, "fragranceContent", self.fragrance_content)
        put_if_present(m, "fragranceResult", self.fragrance_result)
        return m


@dataclass(frozen=True, slots=True)
class DispenserOrderDto:
    """DispenserOrderDTO — SoT §5-5 타입 시그니처 바이트 동일."""

    id: str
    mode: str  # "flavor" | "fragrance"
    status: WireStatus
    order_number: int
    language: str  # "ko" | "en" | "ja" | "vi"
    created_at: str  # ISO8601, 항상 (재포맷 금지)
    is_deleted: bool
    is_demo: bool

    # net-new (필수·§5-1·O-5) — 마이그레이션 폴백(§5-4)으로 non-null 보장.
    device_id: str
    attempt: int
    trace_id: str

    # 비식별(옵셔널) — 값 있을 때만 키 존재.
    user_age: int | None = None
    user_gender: str | None = None  # "male" | "female"

    flavor: FlavorSub | None = None  # mode == "flavor" 일 때만
    fragrance: FragranceSub | None = None  # mode == "fragrance" 일 때만

    @staticmethod
    def from_json(j: Mapping[str, Any]) -> "DispenserOrderDto":
        """서버 투영 JSON 파싱 — 마이그레이션 폴백(§5-4) 적용(구버전 문서 non-null 보호)."""
        mode = j["mode"]
        # status 는 와이어 문자열 → enum. 4종 외면 명시적으로 예외를 던져 오염을 조기에 드러낸다
        # (알 수 없으면 pending 으로 두지 않는다 — 서버가 항상 유효값을 보낸다는 계약).
        status = WireStatus.from_wire(j.get("status"))
        if status is None:
            raise ValueError(f"unknown status: {j.get('status')}")

        device_id = j.get("deviceId")
        attempt = j.get("attempt")
        trace_id = j.get("traceId")
        user_age = j.get("userAge")
        user_gender = j.get("userGender")
        flavor_raw = j.get("flavor")
        fragrance_raw = j.get("fragrance")

        return DispenserOrderDto(
            id=j["id"],
            mode=mode,
            status=status,
            order_number=int(j["orderNumber"]),
            language=j["language"],
            created_at=j["createdAt"],
            is_deleted=j.get("isDeleted") is True,
            is_demo=j.get("isDemo") is True,
            # §5-4 마이그레이션 폴백: 구버전 문서에 net-new 3필드 부재 시 기본값.
            device_id=device_id if isinstance(device_id, str) else DEFAULT_DEVICE_ID,
            attempt=int(attempt) if isinstance(attempt, (int, float)) and not isinstance(attempt, bool) else 1,
            trace_id=trace_id if isinstance(trace_id, str) else "",
            user_age=int(user_age) if isinstance(user_age, (int, float)) and not isinstance(user_age, bool) else None,
            user_gender=user_gender if isinstance(user_gender, str) else None,
            flavor=FlavorSub.from_json(flavor_raw)
            if mode == "flavor" and isinstance(flavor_raw, Mapping)
            else None,
            fragrance=FragranceSub.from_json(fragrance_raw)
            if mode == "fragrance" and isinstance(fragrance_raw, Mapping)
            else None,
        )

    def to_json(self) -> dict[str, Any]:
        """직렬화 — 서버 DTO 형상 재현(§5-3): 옵셔널 부재 시 키 재부재(includeIfNull:false)."""
        m: dict[str, Any] = {
            "id": self.id,
            "mode": self.mode,
            "status": self.status.wire,
            "orderNumber": self.order_number,
            "language": self.language,
            "createdAt": self.created_at,
            "isDeleted": self.is_deleted,
            "isDemo": self.is_demo,
            "deviceId": self.device_id,
            "attempt": self.attempt,
            "traceId": self.trace_id,
        }
        put_if_present(m, "userAge", self.user_age)
        put_if_present(m, "userGender", self.user_gender)
        put_if_present(m, "flavor", self.flavor.to_json() if self.flavor else None)
        put_if_present(m, "fragrance", self.fragrance.to_json() if self.fragrance else None)
        return m

    @property
    def command_id(self) -> str:
        """command 파생 키 — SoT §5-6. `{id}:{attempt}`."""
        return f"{self.id}:{self.attempt}"
