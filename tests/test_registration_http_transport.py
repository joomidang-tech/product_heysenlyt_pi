"""실 HTTP RegisterTransport 테스트 — make_http_register_transport (스텁 제거 검증 · TOFU).

로컬 fake 서버로 POST /api/dispensers/register 요청 shaping(**인증 헤더 없음**·body)·
상태 분류(200 승인·202 pending·4xx/5xx)·네트워크 전송오류를 검증하고, RegistrationClient 와
실제 결합해 재시도(R=3)·pending(None)까지 실 소켓으로 확인한다.
공유키(프로비저닝 키) 제거(2026-07-17) — 보안은 서버측 pending + 운영자 승인(TOFU)으로 이동.
"""

from __future__ import annotations

import pytest

from senlyt_pi.adapters.http_client import HttpTransportError
from senlyt_pi.adapters.registration_client import (
    RegistrationClient,
    RegistrationError,
    make_http_register_transport,
)
from support_http import FakeHttpServer

# deviceId 는 제시 시리얼 echo(pi 는 무시) — dispenserToken·exp 만 서버 발급.
OK_BODY = {"deviceId": "server-echo-ignored", "dispenserToken": "tok-1", "exp": 2_000_000_000}


def test_transport_shapes_request_no_auth_header() -> None:
    """TOFU: body(deviceId·name)만 전송 · Authorization 헤더 없음."""
    with FakeHttpServer() as srv:
        srv.set_handler(lambda req: {"status": 200, "json": OK_BODY})
        transport = make_http_register_transport(f"{srv.base_url}/api/dispensers/register")
        status, body = transport({"deviceId": "10000000abcd1234", "name": "매장1"})
        assert status == 200
        assert body == OK_BODY
        rec = srv.requests[-1]
        assert rec.method == "POST"
        assert rec.path == "/api/dispensers/register"
        assert rec.header("Authorization") is None  # 공유키 제거 — 인증 헤더 없음.
        assert rec.json() == {"deviceId": "10000000abcd1234", "name": "매장1"}


def test_transport_returns_202_pending_not_raise() -> None:
    """202 pending(승인 대기)은 예외 아님 — (status, body) 그대로 반환(RegistrationClient 가 분류)."""
    with FakeHttpServer() as srv:
        srv.set_handler(lambda req: {"status": 202, "json": {"deviceId": "hw-1", "status": "pending"}})
        transport = make_http_register_transport(f"{srv.base_url}/api/dispensers/register")
        status, body = transport({"deviceId": "hw-1"})
        assert status == 202
        assert body == {"deviceId": "hw-1", "status": "pending"}


def test_transport_network_failure_raises_transport_error() -> None:
    def raising_request(*_a, **_k):
        raise HttpTransportError("연결 거부")

    transport = make_http_register_transport(
        "http://web:3000/api/dispensers/register", request=raising_request
    )
    with pytest.raises(HttpTransportError):
        transport({"deviceId": "hw-1"})


def test_client_with_real_transport_registers() -> None:
    """RegistrationClient + 실 HTTP transport — 성공 경로 실 소켓 왕복."""
    with FakeHttpServer() as srv:
        srv.set_handler(lambda req: {"status": 200, "json": OK_BODY})
        transport = make_http_register_transport(f"{srv.base_url}/api/dispensers/register")
        identity = RegistrationClient(
            transport, device_id="hw-serial", name=None, sleep=lambda _s: None
        ).register()
        assert identity.device_id == "hw-serial"  # 자기 시리얼 = deviceId(echo 무시).
        assert identity.dispenser_token == "tok-1"
        # 등록 요청이 deviceId(시리얼)를 제시했는지 실 소켓 바디로 확인.
        assert srv.requests[-1].json() == {"deviceId": "hw-serial"}


def test_client_retries_5xx_then_succeeds_over_socket() -> None:
    """5xx 2회 후 200 — 실 서버 상태를 바꿔가며 R=3 재시도 확인."""
    calls = {"n": 0}

    def handler(req):
        calls["n"] += 1
        if calls["n"] <= 2:
            return {"status": 500, "json": {"code": "register_failed"}}
        return {"status": 200, "json": OK_BODY}

    with FakeHttpServer() as srv:
        srv.set_handler(handler)
        transport = make_http_register_transport(f"{srv.base_url}/api/dispensers/register")
        identity = RegistrationClient(
            transport, device_id="hw", sleep=lambda _s: None
        ).register()
        assert identity.device_id == "hw"
        assert calls["n"] == 3


def test_client_pending_202_returns_none_over_socket() -> None:
    """TOFU: 202 pending 응답 → register() 는 None(승인 대기·오류 아님) · 재시도 없이 1회."""
    with FakeHttpServer() as srv:
        srv.set_handler(lambda req: {"status": 202, "json": {"status": "pending"}})
        transport = make_http_register_transport(f"{srv.base_url}/api/dispensers/register")
        result = RegistrationClient(transport, device_id="hw", sleep=lambda _s: None).register()
        assert result is None
        assert len(srv.requests) == 1  # pending 은 register() 내부 재시도 없음.


def test_client_permanent_400_no_retry_over_socket() -> None:
    """400 invalid_request — 구성 오류는 재시도 없이 즉시중단(1회)."""
    with FakeHttpServer() as srv:
        srv.set_handler(lambda req: {"status": 400, "json": {"code": "invalid_request"}})
        transport = make_http_register_transport(f"{srv.base_url}/api/dispensers/register")
        with pytest.raises(RegistrationError) as ei:
            RegistrationClient(transport, device_id="hw", sleep=lambda _s: None).register()
        assert ei.value.retryable is False
        assert ei.value.code == "invalid_request"
        assert len(srv.requests) == 1  # 재시도 없음.
