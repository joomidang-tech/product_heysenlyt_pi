"""CommandSet 봉투 와이어 계약 테스트 — device-registration-commandset-wire (2026-07-09).

파싱(required·manufacture 조건부 필수·steps null 폴백 신호)·직렬화(includeIfNull:false)·
상태 전이표(단조·전진 skip 허용·역행 금지·terminal 폐쇄)·snapshot 필터(deviceId·status·정렬)를
계약 그대로 고정한다.
"""

import pytest

from senlyt_pi.core.command_set import (
    CommandSet,
    CommandSetStatus,
    can_transition,
    command_sets_from_snapshot,
)
from senlyt_pi.core.pump_guard import StatusErrorCode


def manufacture_json(**over):
    j = {
        "commandSetId": "order-1:1",
        "deviceId": "dev-A",
        "kind": "manufacture",
        "steps": [{"idx": 0, "pumpAddr": 1, "flavor": "yuzu", "volume": 100}],
        "sourceOrderId": "order-1",
        "attempt": 1,
        "traceId": "trace-1",
        "status": "queued",
        "createdAt": "2026-07-09T00:00:00.000Z",
        "createdBy": "server",
    }
    j.update(over)
    return j


def maintenance_json(**over):
    j = {
        "commandSetId": "mnt-abc",
        "deviceId": "dev-A",
        "kind": "maintenance",
        "steps": [{"idx": 0, "pumpAddr": 1, "flavor": "flush", "volume": 500}],
        "status": "queued",
        "createdAt": "2026-07-09T00:00:01.000Z",
        "createdBy": "operator:op-1",
    }
    j.update(over)
    return j


class TestCommandSetWire:
    def test_manufacture_roundtrip(self):
        """manufacture 봉투 — 파싱·재직렬화 왕복(합성 멱등키 `{orderId}:{attempt}` 보존)."""
        cs = CommandSet.from_json(manufacture_json())
        assert cs.command_set_id == "order-1:1"
        assert cs.kind == "manufacture"
        assert cs.source_order_id == "order-1"
        assert cs.attempt == 1
        assert cs.steps is not None and len(cs.steps) == 1
        assert cs.steps[0].pump_addr == 1 and cs.steps[0].volume == 100
        assert cs.status is CommandSetStatus.QUEUED

        j = cs.to_json()
        assert j == manufacture_json()  # 왕복 무손실.

    def test_maintenance_roundtrip(self):
        """maintenance 봉투 — sourceOrderId/attempt 없이 유효, mnt- 프리픽스."""
        cs = CommandSet.from_json(maintenance_json())
        assert cs.command_set_id.startswith("mnt-")
        assert cs.source_order_id is None and cs.attempt is None
        assert cs.to_json() == maintenance_json()

    def test_steps_null_is_legacy_fallback_signal(self):
        """steps=null(manufacture) = 레거시 폴백 신호 — None 으로 파싱·null 명시 재방출."""
        cs = CommandSet.from_json(manufacture_json(steps=None))
        assert cs.steps is None
        assert cs.to_json()["steps"] is None  # null 도 의미(폴백 신호) — 키 유지.

    def test_manufacture_requires_source_order_and_attempt(self):
        """계약 if/then — manufacture 는 sourceOrderId·attempt(≥1) 필수."""
        base = manufacture_json()
        del base["sourceOrderId"]
        with pytest.raises(ValueError):
            CommandSet.from_json(base)

        base2 = manufacture_json()
        del base2["attempt"]
        with pytest.raises(ValueError):
            CommandSet.from_json(base2)

        with pytest.raises(ValueError):
            CommandSet.from_json(manufacture_json(attempt=0))

    def test_maintenance_steps_null_rejected(self):
        """maintenance 는 steps 필수(oneOf null 은 manufacture 전용 폴백)."""
        with pytest.raises(ValueError):
            CommandSet.from_json(maintenance_json(steps=None))

    def test_empty_steps_rejected(self):
        """steps minItems 1 — 빈 배열은 null(폴백 신호)과 다른 계약 위반."""
        with pytest.raises(ValueError):
            CommandSet.from_json(manufacture_json(steps=[]))

    def test_unknown_kind_and_status_rejected(self):
        with pytest.raises(ValueError):
            CommandSet.from_json(manufacture_json(kind="cleaning"))
        with pytest.raises(ValueError):
            CommandSet.from_json(manufacture_json(status="pending"))

    def test_optional_fields_include_if_null_false(self):
        """traceId/errorCode/updatedAt 부재 시 키 미방출(부록A P-4 결)."""
        j = maintenance_json()
        cs = CommandSet.from_json(j)
        out = cs.to_json()
        assert "traceId" not in out
        assert "errorCode" not in out
        assert "updatedAt" not in out
        assert "sourceOrderId" not in out and "attempt" not in out

    def test_error_code_parsed_from_wire(self):
        cs = CommandSet.from_json(
            manufacture_json(status="failed", errorCode="ENGINE_ERROR_PERMANENT")
        )
        assert cs.error_code is StatusErrorCode.ENGINE_ERROR_PERMANENT
        assert cs.to_json()["errorCode"] == "ENGINE_ERROR_PERMANENT"


class TestCommandSetStatusTransitions:
    """계약 x-transitions 매트릭스 — queued(0)→delivered(1)→running(2)→done|failed(3)."""

    ALLOWED = {
        ("queued", "delivered"), ("queued", "running"), ("queued", "done"), ("queued", "failed"),
        ("delivered", "running"), ("delivered", "done"), ("delivered", "failed"),
        ("running", "done"), ("running", "failed"),
    }

    def test_full_matrix(self):
        """5×5 전체 매트릭스 — 허용 전이는 계약 표와 정확히 일치(전진 skip 포함)."""
        for frm in CommandSetStatus:
            for to in CommandSetStatus:
                expected = (frm.wire, to.wire) in self.ALLOWED
                assert can_transition(frm, to) is expected, f"{frm.wire}->{to.wire}"

    def test_terminal_closed(self):
        """done|failed 는 동급 terminal — 상호 전이 포함 exit 0."""
        for term in (CommandSetStatus.DONE, CommandSetStatus.FAILED):
            assert term.is_terminal
            for to in CommandSetStatus:
                assert not can_transition(term, to)

    def test_same_value_not_a_transition(self):
        """동일값은 전이 아님(noop applied:false 는 서버 게이트 몫)."""
        for s in CommandSetStatus:
            assert not can_transition(s, s)


class TestSnapshotConsumption:
    """DispenserSnapshotEvent.commandSets — 자기 deviceId·queued|delivered 만·createdAt 정렬."""

    def test_missing_field_is_backward_compatible(self):
        """commandSets 부재(구서버) → 빈 목록(기존 orders/commands 소비자 무파괴)."""
        assert command_sets_from_snapshot({"orders": [], "commands": []}, "dev-A") == []

    def test_filters_device_and_status_and_sorts(self):
        snapshot = {
            "orders": [],
            "commands": [],
            "commandSets": [
                manufacture_json(commandSetId="o2:1", createdAt="2026-07-09T00:00:02.000Z"),
                maintenance_json(),  # 00:00:01 — 앞으로 정렬돼야 함.
                manufacture_json(commandSetId="ox:1", deviceId="dev-B"),  # 타 기기.
                manufacture_json(commandSetId="o3:1", status="running"),  # 소비 대상 아님.
                manufacture_json(commandSetId="o4:1", status="done"),  # terminal.
                manufacture_json(
                    commandSetId="o5:1", status="delivered",
                    createdAt="2026-07-09T00:00:03.000Z",
                ),  # delivered 는 재전달 소비 대상.
            ],
        }
        got = command_sets_from_snapshot(snapshot, "dev-A")
        assert [c.command_set_id for c in got] == ["mnt-abc", "o2:1", "o5:1"]

    def test_malformed_items_are_skipped(self):
        """깨진 항목은 항목 단위 skip — 나머지 소비 지속(전체 snapshot 불사)."""
        snapshot = {
            "commandSets": [
                {"garbage": True},
                "not-a-mapping",
                manufacture_json(),
                manufacture_json(commandSetId="bad", kind="manufacture", sourceOrderId=""),
            ]
        }
        got = command_sets_from_snapshot(snapshot, "dev-A")
        assert [c.command_set_id for c in got] == ["order-1:1"]

    def test_non_list_commandsets_ignored(self):
        assert command_sets_from_snapshot({"commandSets": "oops"}, "dev-A") == []
        assert command_sets_from_snapshot({"commandSets": {"a": 1}}, "dev-A") == []
