"""테스트 지원 — 로컬 fake HTTP 서버(실 어댑터를 실제 소켓 왕복/SSE 로 검증).

실 어댑터(urllib)를 **진짜 로컬 서버**에 붙여 요청 shaping·SSE 파싱·CS-08·재시도·OQ flush 를
검증한다(seam mock 이 아니라 실 소켓). pytest fixture 없이 컨텍스트 매니저로 제공.

사용:
    with FakeHttpServer() as srv:
        srv.set_handler(lambda req: {"status": 200, "json": {"ok": True}})
        # adapter(base_url=srv.base_url) ... 왕복
        assert srv.requests[-1].json()["status"] == "COMPLETED"

SSE 응답: {"sse": [("snapshot", '{"commands": [...]}'), ...]} → 프레임 방출 후 연결 종료.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import urlparse


@dataclass
class RecordedRequest:
    method: str
    path: str
    query: str
    headers: dict[str, str]
    body: bytes

    def json(self) -> Any:
        return json.loads(self.body.decode("utf-8")) if self.body else None

    def header(self, name: str) -> str | None:
        # HTTP 헤더는 대소문자 무시.
        for k, v in self.headers.items():
            if k.lower() == name.lower():
                return v
        return None


# handler: RecordedRequest → 응답 스펙 dict.
#   {"status": int, "json": dict|None, "headers": {...}}  또는  {"sse": [(event, data), ...]}
HandlerFn = Callable[[RecordedRequest], dict[str, Any]]


@dataclass
class FakeHttpServer:
    requests: list[RecordedRequest] = field(default_factory=list)
    _handler: HandlerFn | None = None
    _server: ThreadingHTTPServer | None = None
    _thread: threading.Thread | None = None

    def set_handler(self, fn: HandlerFn) -> None:
        self._handler = fn

    @property
    def base_url(self) -> str:
        assert self._server is not None
        host, port = self._server.server_address[:2]
        return f"http://127.0.0.1:{port}"

    def __enter__(self) -> "FakeHttpServer":
        server = self

        class _Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args: Any) -> None:  # 조용히(테스트 출력 오염 방지).
                pass

            def _dispatch(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                parsed = urlparse(self.path)
                rec = RecordedRequest(
                    method=self.command,
                    path=parsed.path,
                    query=parsed.query,
                    headers={k: v for k, v in self.headers.items()},
                    body=body,
                )
                server.requests.append(rec)
                spec = (
                    server._handler(rec)
                    if server._handler is not None
                    else {"status": 200, "json": {}}
                )
                if "sse" in spec:
                    self._write_sse(spec["sse"])
                    return
                self._write_json(
                    spec.get("status", 200),
                    spec.get("json"),
                    spec.get("headers"),
                )

            def _write_json(
                self,
                status: int,
                payload: Any,
                extra_headers: dict[str, str] | None,
            ) -> None:
                data = json.dumps(payload).encode("utf-8") if payload is not None else b""
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                for k, v in (extra_headers or {}).items():
                    self.send_header(k, v)
                self.end_headers()
                if data:
                    self.wfile.write(data)

            def _write_sse(self, frames: list[tuple[str, str]]) -> None:
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                # 스트림 종료를 EOF 로 알리기 위해 Connection: close.
                self.send_header("Connection", "close")
                self.end_headers()
                for event, data in frames:
                    chunk = f"event: {event}\ndata: {data}\n\n".encode("utf-8")
                    self.wfile.write(chunk)
                self.wfile.flush()
                # 반환 시 연결 종료 → 어댑터 SseStream 이 EOF 로 순회 종료.

            do_GET = _dispatch
            do_POST = _dispatch
            do_PATCH = _dispatch
            do_DELETE = _dispatch

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
