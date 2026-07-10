"""bootstrap 테스트 — 실 어댑터 조립(ServerConfig 결정·엔진 분기·정체성)·fail-fast.

등록(실 HTTP)은 register=False + identity_store 선주입으로 네트워크 없이 조립을 검증하고,
등록 경로는 별도로 로컬 fake register 서버로 확인한다.
"""

from __future__ import annotations

import pytest

from senlyt_pi.adapters.device_identity_store import DeviceIdentity, DeviceIdentityStore
from senlyt_pi.adapters.fake_engine_adapter import FakeEnginePort
from senlyt_pi.app.bootstrap import BootstrapError, build_components, build_engine
from senlyt_pi.config.server_target import SENLYT_ENV_KEY, ServerTargetError
from support_http import FakeHttpServer

IDENTITY = DeviceIdentity(device_id="dev-A", dispenser_token="tok-1", exp=9_999_999_999)


def _store(tmp_path) -> DeviceIdentityStore:
    store = DeviceIdentityStore(tmp_path / "identity.json")
    store.save(IDENTITY)
    return store


def test_assembles_real_adapters_from_env(tmp_path) -> None:
    env = {SENLYT_ENV_KEY: "v1_1_0"}
    comp = build_components(env, identity_store=_store(tmp_path), register=False)
    assert comp.device_id == "dev-A"
    assert comp.server_config.base_url == "https://v1-1-0.env.senlyt.com"
    # 어댑터가 동일 base·동일 토큰을 소비.
    assert comp.command_source.base_url == "https://v1-1-0.env.senlyt.com"
    assert comp.command_source.bearer_token == "tok-1"
    assert comp.status_sink.base_url == "https://v1-1-0.env.senlyt.com"
    assert comp.status_sink.bearer_token == "tok-1"
    # 엔진 기본 = Fake(유일 mock).
    assert isinstance(comp.engine, FakeEnginePort)
    # logger 에 deviceId 바인딩.
    assert comp.logger.device_id == "dev-A"


def test_fail_fast_when_server_target_unset(tmp_path) -> None:
    with pytest.raises(ServerTargetError):
        build_components({}, identity_store=_store(tmp_path), register=False)


def test_missing_identity_without_register_raises(tmp_path) -> None:
    empty = DeviceIdentityStore(tmp_path / "none.json")
    with pytest.raises(BootstrapError):
        build_components(
            {SENLYT_ENV_KEY: "dev"}, identity_store=empty, register=False
        )


def test_explicit_base_url_escape_hatch(tmp_path) -> None:
    env = {"SENLYT_SERVER_BASE_URL": "http://web:3000"}
    comp = build_components(env, identity_store=_store(tmp_path), register=False)
    assert comp.server_config.base_url == "http://web:3000"


def test_engine_default_is_fake() -> None:
    assert isinstance(build_engine({}), FakeEnginePort)


def test_engine_injection_wins() -> None:
    injected = FakeEnginePort()
    assert build_engine({"SENLYT_ENGINE": "sy01b"}, engine=injected) is injected


def test_register_path_over_socket(tmp_path) -> None:
    """실 등록 경로 — 로컬 fake register 서버로 build_components(register=True) 왕복.

    [D-A] deviceId = SENLYT_HARDWARE_ID(수집 시리얼)로 확정 — 서버 echo(다른 값)는 무시.
    """
    ok_body = {"deviceId": "server-echo-ignored", "dispenserToken": "tok-X", "exp": 9_999_999_999}
    with FakeHttpServer() as srv:
        srv.set_handler(lambda req: {"status": 200, "json": ok_body})
        env = {
            "SENLYT_SERVER_BASE_URL": srv.base_url,
            "SENLYT_HARDWARE_ID": "hw-e2e",
            "DISPENSER_PROVISION_KEY": "prov",
        }
        store = DeviceIdentityStore(tmp_path / "id.json")
        comp = build_components(env, identity_store=store, register=True)
        assert comp.device_id == "hw-e2e"  # 시리얼 = deviceId(서버 echo 아님).
        # 등록 요청이 올바른 경로·Bearer·deviceId(시리얼)로 갔는지.
        reg = srv.requests[-1]
        assert reg.path == "/api/dispensers/register"
        assert reg.header("Authorization") == "Bearer prov"
        assert reg.json()["deviceId"] == "hw-e2e"
        # 정체성 파일 영속.
        assert store.load().device_id == "hw-e2e"
