"""실 HTTP RegisterTransport 테스트 — make_http_register_transport (스텁 제거 검증).

로컬 fake 서버로 POST /api/dispensers/register 요청 shaping(Bearer 프로비저닝 키·body)·
상태 분류(2xx/4xx/5xx)·네트워크 전송오류를 검증하고, RegistrationClient 와 실제 결합해
재시도(R=3)까지 실 소켓으로 확인한다.
"""

from __future__ import annotations

import pytest

from senlyt_pi.adapters.http_client import HttpTransportError
from senlyt_pi.adapters.registration_client import (
    RegistrationClient,
    RegistrationError,
    make_http_register_transport,
    read_provision_key,
)
from support_http import FakeHttpServer

OK_BODY = {"deviceId": "dev-1", "dispenserToken": "tok-1", "exp": 2_000_000_000}


def test_transport_shapes_request_and_returns_ok() -> None:
    with FakeHttpServer() as srv:
        srv.set_handler(lambda req: {"status": 200, "json": OK_BODY})
        transport = make_http_register_transport(
            f"{srv.base_url}/api/dispensers/register", "prov-key-xyz"
        )
        status, body = transport({"hardwareId": "hw-1", "name": "매장1"})
        assert status == 200
        assert body == OK_BODY
        rec = srv.requests[-1]
        assert rec.method == "POST"
        assert rec.path == "/api/dispensers/register"
        assert rec.header("Authorization") == "Bearer prov-key-xyz"
        assert rec.json() == {"hardwareId": "hw-1", "name": "매장1"}


def test_transport_returns_4xx_status_not_raise() -> None:
    with FakeHttpServer() as srv:
        srv.set_handler(
            lambda req: {"status": 401, "json": {"code": "invalid_provision_key"}}
        )
        transport = make_http_register_transport(
            f"{srv.base_url}/api/dispensers/register", "bad"
        )
        status, body = transport({"hardwareId": "hw-1"})
        assert status == 401
        assert body == {"code": "invalid_provision_key"}


def test_transport_network_failure_raises_transport_error() -> None:
    def raising_request(*_a, **_k):
        raise HttpTransportError("연결 거부")

    transport = make_http_register_transport(
        "http://web:3000/api/dispensers/register", "k", request=raising_request
    )
    with pytest.raises(HttpTransportError):
        transport({"hardwareId": "hw-1"})


def test_client_with_real_transport_registers() -> None:
    """RegistrationClient + 실 HTTP transport — 성공 경로 실 소켓 왕복."""
    with FakeHttpServer() as srv:
        srv.set_handler(lambda req: {"status": 200, "json": OK_BODY})
        transport = make_http_register_transport(
            f"{srv.base_url}/api/dispensers/register", "k"
        )
        identity = RegistrationClient(
            transport, hardware_id="hw-serial", name=None, sleep=lambda _s: None
        ).register()
        assert identity.device_id == "dev-1"
        assert identity.dispenser_token == "tok-1"
        assert identity.hardware_id == "hw-serial"


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
        transport = make_http_register_transport(
            f"{srv.base_url}/api/dispensers/register", "k"
        )
        identity = RegistrationClient(
            transport, hardware_id="hw", sleep=lambda _s: None
        ).register()
        assert identity.device_id == "dev-1"
        assert calls["n"] == 3


def test_client_permanent_401_no_retry_over_socket() -> None:
    with FakeHttpServer() as srv:
        srv.set_handler(
            lambda req: {"status": 401, "json": {"code": "invalid_provision_key"}}
        )
        transport = make_http_register_transport(
            f"{srv.base_url}/api/dispensers/register", "bad"
        )
        with pytest.raises(RegistrationError) as ei:
            RegistrationClient(transport, hardware_id="hw", sleep=lambda _s: None).register()
        assert ei.value.retryable is False
        assert ei.value.code == "invalid_provision_key"
        assert len(srv.requests) == 1  # 재시도 없음.


class TestReadProvisionKey:
    def test_reads_and_trims(self) -> None:
        assert read_provision_key({"DISPENSER_PROVISION_KEY": " k123 "}) == "k123"

    def test_absent_is_empty(self) -> None:
        assert read_provision_key({}) == ""
