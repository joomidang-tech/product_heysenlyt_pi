"""와이어 메시지 회귀 — SoT §9 (부록A P-2/P-4). Dart `test/wire_messages_test.dart` 포팅.

합성 멱등키 조립 · command recipe null 폴백 신호 · heartbeat includeIfNull:false.
"""

from senlyt_pi.core.pump_guard import StatusErrorCode
from senlyt_pi.core.wire_messages import Command, Heartbeat, StatusReport, build_command_id


class TestBuildCommandId:
    def test_shape(self):
        """{orderId}:{attempt} — 콜론·zero-pad 없음 (부록A P-2)."""
        assert build_command_id("ord", 1) == "ord:1"
        assert build_command_id("ord", 12) == "ord:12"
        # orderId 에 콜론 있어도 lastIndexOf 규칙은 서버측.
        assert build_command_id("a:b", 3) == "a:b:3"


class TestCommandRoundtrip:
    """Command roundtrip(§9-1)."""

    def test_recipe_steps_parse_serialize(self):
        """recipe steps 파싱·직렬화."""
        c = Command.from_json({
            "id": "ord:1",
            "orderId": "ord",
            "attempt": 1,
            "deviceId": "store-A",
            "recipe": [
                {"idx": 0, "pumpAddr": 1, "flavor": "rose", "volume": 100},
                {"idx": 1, "pumpAddr": 2, "flavor": "musk", "volume": 50},
            ],
            "traceId": "t",
            "createdAt": "2026-07-03T00:00:00.000Z",
        })
        assert c.recipe is not None
        assert len(c.recipe) == 2
        assert c.recipe[0].idx == 0
        assert c.recipe[1].volume == 50
        j = c.to_json()
        assert len(j["recipe"]) == 2

    def test_recipe_null_fallback_signal(self):
        """recipe = null 폴백 신호 보존(§9-1)."""
        c = Command.from_json({
            "id": "ord:1",
            "orderId": "ord",
            "attempt": 1,
            "deviceId": "d",
            "recipe": None,
            "traceId": "t",
            "createdAt": "2026-07-03T00:00:00.000Z",
        })
        assert c.recipe is None
        # recipe null 은 의미가 있으므로 키가 남아야 한다(pi 가 recipeId/fragranceResult 로 해석).
        assert "recipe" in c.to_json()
        assert c.to_json()["recipe"] is None


class TestHeartbeatIncludeIfNullFalse:
    """Heartbeat includeIfNull:false(§9-3·부록A P-4)."""

    def test_absent_keys_not_emitted(self):
        """engine/lastError 부재 시 키 미방출."""
        hb = Heartbeat(device_id="d", queue_depth=0).to_json()
        assert hb["deviceId"] == "d"
        assert hb["queueDepth"] == 0
        assert "engine" not in hb
        assert "lastError" not in hb

    def test_present_keys_emitted(self):
        """engine/lastError 존재 시 키 방출."""
        hb = Heartbeat(
            device_id="d",
            queue_depth=2,
            engine="sy01b",
            last_error=StatusErrorCode.ENGINE_TIMEOUT,
        ).to_json()
        assert hb["engine"] == "sy01b"
        assert hb["lastError"] == "ENGINE_TIMEOUT"


class TestStatusReport:
    """StatusReport(§9-2)."""

    def test_error_code_null_explicit(self):
        """errorCode null 도 명시 방출(계약 ErrorCode|null)."""
        r = StatusReport(
            id="ord:1",
            phase="PROGRESS",
            step_k=3,
            step_n=10,
            error_code=None,
            request_id="req",
            trace_id="t",
            updated_at="2026-07-03T00:00:00.000Z",
        ).to_json()
        assert "errorCode" in r
        assert r["errorCode"] is None
        assert r["phase"] == "PROGRESS"

    def test_error_code_wire_string(self):
        """errorCode 7종 wire 문자열."""
        r = StatusReport(
            id="ord:1",
            phase="FAILED",
            step_k=3,
            step_n=10,
            error_code=StatusErrorCode.PARTIAL_DISPENSE,
            request_id="req",
            trace_id="t",
            updated_at="2026-07-03T00:00:00.000Z",
        ).to_json()
        assert r["errorCode"] == "PARTIAL_DISPENSE"
