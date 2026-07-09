"""DispenserOrderDTO 미러 회귀 — SoT §5 (부록A P-2/P-3/P-4).

Dart `test/dispenser_order_dto_test.dart` 포팅.
includeIfNull:false · createdAt 재포맷 금지 · 마이그레이션 폴백 · command 파생 키.
"""

from senlyt_pi.core.dispenser_order_dto import DEFAULT_DEVICE_ID, DispenserOrderDto
from senlyt_pi.core.order_status import WireStatus


def _min_dto(created_at: str = "2026-07-03T00:00:00.000Z") -> DispenserOrderDto:
    return DispenserOrderDto.from_json({
        "id": "ord",
        "mode": "flavor",
        "status": "PENDING",
        "orderNumber": 1,
        "language": "ko",
        "createdAt": created_at,
        "isDeleted": False,
        "isDemo": False,
        "deviceId": "d",
        "attempt": 1,
        "traceId": "t",
        "flavor": {"recipeId": "r"},
    })


class TestRoundtripFlavor:
    """fromJson/toJson roundtrip — flavor."""

    def test_required_plus_net_new_fields(self):
        """필수 + net-new 3필드 파싱."""
        dto = DispenserOrderDto.from_json({
            "id": "ord123",
            "mode": "flavor",
            "status": "PENDING",
            "orderNumber": 42,
            "language": "ko",
            "createdAt": "2026-07-03T12:34:56.789Z",
            "isDeleted": False,
            "isDemo": False,
            "deviceId": "store-A",
            "attempt": 2,
            "traceId": "trace-uuid",
            "flavor": {"recipeId": "럭퓨-01"},
        })
        assert dto.status is WireStatus.PENDING
        assert dto.device_id == "store-A"
        assert dto.attempt == 2
        assert dto.flavor is not None and dto.flavor.recipe_id == "럭퓨-01"
        assert dto.fragrance is None

    def test_created_at_no_reformat(self):
        """createdAt 재포맷 금지(부록A P-3) — 밀리초·Z 그대로 보존."""
        iso = "2026-07-03T12:34:56.789Z"
        dto = _min_dto(created_at=iso)
        assert dto.created_at == iso
        assert dto.to_json()["createdAt"] == iso


class TestIncludeIfNullFalse:
    """includeIfNull:false(부록A P-4)."""

    def test_absent_optionals_emit_no_keys(self):
        """옵셔널 부재 시 키 자체 미방출."""
        j = _min_dto().to_json()
        assert "userAge" not in j
        assert "userGender" not in j
        # 필수 net-new 는 항상 존재.
        assert "deviceId" in j
        assert "attempt" in j
        assert "traceId" in j

    def test_present_optionals_emit_keys(self):
        """옵셔널 존재 시 키 방출."""
        j = DispenserOrderDto.from_json({
            "id": "o",
            "mode": "flavor",
            "status": "PENDING",
            "orderNumber": 1,
            "language": "ko",
            "createdAt": "2026-07-03T00:00:00.000Z",
            "isDeleted": False,
            "isDemo": False,
            "deviceId": "d",
            "attempt": 1,
            "traceId": "t",
            "userAge": 30,
            "userGender": "male",
            "flavor": {"recipeId": "r"},
        }).to_json()
        assert j["userAge"] == 30
        assert j["userGender"] == "male"


class TestMigrationFallback:
    """마이그레이션 폴백(§5-4)."""

    def test_legacy_doc_non_null_protection(self):
        """net-new 3필드 부재 구버전 문서 — non-null 보호."""
        dto = DispenserOrderDto.from_json({
            "id": "legacy",
            "mode": "fragrance",
            "status": "COMPLETED",
            "orderNumber": 7,
            "language": "en",
            "createdAt": "2026-01-01T00:00:00.000Z",
            "isDeleted": False,
            "isDemo": False,
            # deviceId/attempt/traceId 부재.
            "fragrance": {"name": "Rose"},
        })
        assert dto.device_id == DEFAULT_DEVICE_ID
        assert dto.attempt == 1
        assert dto.trace_id == ""
        assert dto.fragrance is not None and dto.fragrance.name == "Rose"


class TestStrictBoolCoercion:
    """isDeleted/isDemo === true 강제(§5-3.5)."""

    def test_non_truthy_becomes_false(self):
        """truthy 아닌 값 → False."""
        dto = DispenserOrderDto.from_json({
            "id": "o",
            "mode": "flavor",
            "status": "PENDING",
            "orderNumber": 1,
            "language": "ko",
            "createdAt": "2026-07-03T00:00:00.000Z",
            # isDeleted/isDemo 부재 → False.
            "deviceId": "d",
            "attempt": 1,
            "traceId": "t",
            "flavor": {"recipeId": "r"},
        })
        assert dto.is_deleted is False
        assert dto.is_demo is False


class TestCommandDerivedKey:
    """command 파생 키(§5-6·부록A P-2)."""

    def test_command_id_shape(self):
        """commandId = {id}:{attempt} — 콜론·zero-pad 없음."""
        assert _min_dto().command_id == "ord:1"
        dto2 = DispenserOrderDto.from_json({
            "id": "ord",
            "mode": "flavor",
            "status": "PENDING",
            "orderNumber": 1,
            "language": "ko",
            "createdAt": "2026-07-03T00:00:00.000Z",
            "isDeleted": False,
            "isDemo": False,
            "deviceId": "d",
            "attempt": 12,
            "traceId": "t",
            "flavor": {"recipeId": "r"},
        })
        assert dto2.command_id == "ord:12"


class TestPiiSealing:
    """PII 봉인(§5-2) — 구조적 부재."""

    def test_uid_username_not_absorbed(self):
        """uid/userName 키가 있어도 파싱·직렬화에 흡수되지 않음."""
        dto = DispenserOrderDto.from_json({
            "id": "o",
            "mode": "flavor",
            "status": "PENDING",
            "orderNumber": 1,
            "language": "ko",
            "createdAt": "2026-07-03T00:00:00.000Z",
            "isDeleted": False,
            "isDemo": False,
            "deviceId": "d",
            "attempt": 1,
            "traceId": "t",
            "uid": "SECRET-UID",
            "userName": "홍길동",
            "flavor": {"recipeId": "r"},
        })
        j = dto.to_json()
        assert "uid" not in j
        assert "userName" not in j
