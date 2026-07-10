"""SSE command/commandSet source 실어댑터 테스트 — 실 SSE 구독 + CS-08 필터(스텁 제거).

로컬 fake SSE 서버로 snapshot 파싱·Command/CommandSet 파생·자기 deviceId 필터·스트림 URL
shaping(mode/view/deviceId·Bearer)을 실 소켓으로 검증한다.
"""

from __future__ import annotations

import json

from senlyt_pi.adapters.sse_command_source_adapter import (
    SseCommandSourceAdapter,
    commands_from_snapshot,
)
from senlyt_pi.config.server_target import SENLYT_ENV_KEY, ServerConfig
from support_http import FakeHttpServer

MINE = "dev-A"
OTHER = "dev-B"


def _cmd(order_id: str, device_id: str, attempt: int = 1) -> dict:
    return {
        "id": f"{order_id}:{attempt}",
        "orderId": order_id,
        "attempt": attempt,
        "deviceId": device_id,
        "recipe": None,
        "traceId": f"trace-{order_id}",
        "createdAt": "2026-07-10T00:00:00.000Z",
    }


def _cs(order_id: str, device_id: str, attempt: int = 1, status: str = "queued") -> dict:
    return {
        "commandSetId": f"{order_id}:{attempt}",
        "deviceId": device_id,
        "kind": "manufacture",
        "steps": [{"idx": 0, "pumpAddr": 1, "flavor": "cola", "volume": 100}],
        "status": status,
        "createdAt": "2026-07-10T00:00:00.000Z",
        "createdBy": "server",
        "sourceOrderId": order_id,
        "attempt": attempt,
        "traceId": f"trace-{order_id}",
    }


class TestCommandsFromSnapshot:
    def test_cs08_filters_foreign_device(self) -> None:
        snap = {"commands": [_cmd("o1", MINE), _cmd("o2", OTHER)]}
        got = commands_from_snapshot(snap, MINE)
        assert [c.order_id for c in got] == ["o1"]

    def test_broken_item_skipped(self) -> None:
        snap = {"commands": [{"broken": True}, _cmd("o1", MINE)]}
        got = commands_from_snapshot(snap, MINE)
        assert [c.order_id for c in got] == ["o1"]

    def test_missing_commands_field_is_empty(self) -> None:
        assert commands_from_snapshot({}, MINE) == []


class TestRealSseSubscription:
    def test_commands_streamed_and_filtered(self) -> None:
        snapshot = {
            "orders": [],
            "commands": [_cmd("o1", MINE), _cmd("o2", OTHER), _cmd("o3", MINE)],
            "commandSets": [],
        }
        with FakeHttpServer() as srv:
            srv.set_handler(
                lambda req: {"sse": [("snapshot", json.dumps(snapshot))]}
            )
            adapter = SseCommandSourceAdapter(
                base_url=srv.base_url, bearer_token="tok-d", timeout=5.0
            )
            got = list(adapter.commands(MINE))
            assert [c.order_id for c in got] == ["o1", "o3"]  # CS-08.
            # 스트림 URL shaping — mode/view/deviceId 쿼리 + Bearer.
            rec = srv.requests[-1]
            assert rec.path == "/api/dispenser/orders/stream"
            assert "deviceId=dev-A" in rec.query
            assert "mode=flavor" in rec.query
            assert rec.header("Authorization") == "Bearer tok-d"

    def test_command_sets_streamed_and_filtered(self) -> None:
        snapshot = {
            "orders": [],
            "commands": [],
            "commandSets": [
                _cs("o1", MINE),
                _cs("o2", OTHER),
                _cs("o3", MINE, status="running"),  # running 은 소비 대상 아님.
            ],
        }
        with FakeHttpServer() as srv:
            srv.set_handler(
                lambda req: {"sse": [("snapshot", json.dumps(snapshot))]}
            )
            adapter = SseCommandSourceAdapter(base_url=srv.base_url, timeout=5.0)
            got = list(adapter.command_sets(MINE))
            # queued(자기것)만 — OTHER·running 제외.
            assert [c.command_set_id for c in got] == ["o1:1"]

    def test_multiple_snapshots_yield_across_frames(self) -> None:
        s1 = {"commands": [_cmd("o1", MINE)]}
        s2 = {"commands": [_cmd("o2", MINE)]}
        with FakeHttpServer() as srv:
            srv.set_handler(
                lambda req: {
                    "sse": [
                        ("snapshot", json.dumps(s1)),
                        ("snapshot", json.dumps(s2)),
                    ]
                }
            )
            adapter = SseCommandSourceAdapter(base_url=srv.base_url, timeout=5.0)
            got = [c.order_id for c in adapter.commands(MINE)]
            assert got == ["o1", "o2"]

    def test_uses_server_config_base(self) -> None:
        cfg = ServerConfig.from_environ({SENLYT_ENV_KEY: "v1_1_0"})
        adapter = SseCommandSourceAdapter(server_config=cfg)
        assert adapter._stream_url(MINE).startswith(
            "https://v1-1-0.env.senlyt.com/api/dispenser/orders/stream"
        )
