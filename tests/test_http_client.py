"""http_client 테스트 — 표준 urllib JSON 왕복 + SSE 스트리밍 파싱 + 전송오류 분류.

로컬 fake HTTP 서버(실 소켓)로 왕복을 검증한다(seam mock 아님).
"""

from __future__ import annotations

import socket

import pytest

from senlyt_pi.adapters.http_client import (
    HttpTransportError,
    bearer_headers,
    open_sse,
    request_json,
)
from support_http import FakeHttpServer


def _closed_port_url() -> str:
    """열려있지 않은(연결 거부) 포트 URL — 전송오류 유발용."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return f"http://127.0.0.1:{port}/x"


class TestRequestJson:
    def test_post_shapes_json_and_headers(self) -> None:
        with FakeHttpServer() as srv:
            srv.set_handler(lambda req: {"status": 200, "json": {"ok": True}})
            status, body = request_json(
                "POST",
                f"{srv.base_url}/echo",
                body={"a": 1, "b": "값"},
                headers=bearer_headers("tok-1"),
            )
            assert status == 200
            assert body == {"ok": True}
            rec = srv.requests[-1]
            assert rec.method == "POST"
            assert rec.header("Content-Type") == "application/json"
            assert rec.header("Authorization") == "Bearer tok-1"
            assert rec.json() == {"a": 1, "b": "값"}

    def test_http_error_status_returned_not_raised(self) -> None:
        """4xx/5xx 는 예외가 아니라 (status, body) 로 반환(어댑터가 분류)."""
        with FakeHttpServer() as srv:
            srv.set_handler(
                lambda req: {"status": 422, "json": {"code": "illegal"}}
            )
            status, body = request_json("PATCH", f"{srv.base_url}/x", body={"s": 1})
            assert status == 422
            assert body == {"code": "illegal"}

    def test_network_failure_raises_transport_error(self) -> None:
        with pytest.raises(HttpTransportError):
            request_json("POST", _closed_port_url(), body={"a": 1}, timeout=1.0)

    def test_empty_body_parses_to_none(self) -> None:
        with FakeHttpServer() as srv:
            srv.set_handler(lambda req: {"status": 204, "json": None})
            status, body = request_json("DELETE", f"{srv.base_url}/x")
            assert status == 204
            assert body is None


class TestSse:
    def test_sse_frames_parsed(self) -> None:
        with FakeHttpServer() as srv:
            srv.set_handler(
                lambda req: {
                    "sse": [
                        ("snapshot", '{"n": 1}'),
                        ("snapshot", '{"n": 2}'),
                    ]
                }
            )
            with open_sse(f"{srv.base_url}/stream", timeout=5.0) as stream:
                events = list(stream.events())
            assert events == [("snapshot", '{"n": 1}'), ("snapshot", '{"n": 2}')]

    def test_sse_start_failure_raises(self) -> None:
        with pytest.raises(HttpTransportError):
            open_sse(_closed_port_url(), timeout=1.0)
