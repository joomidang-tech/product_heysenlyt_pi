"""bootstrap 테스트 — 실 어댑터 조립(ServerConfig 결정·엔진 분기·정체성)·fail-fast.

등록(실 HTTP)은 register=False + identity_store 선주입으로 네트워크 없이 조립을 검증하고,
등록 경로는 별도로 로컬 fake register 서버로 확인한다.
"""

from __future__ import annotations

import pytest

from senlyt_pi.adapters.device_identity_store import DeviceIdentity, DeviceIdentityStore
from senlyt_pi.adapters.fake_engine_adapter import FakeEnginePort
from senlyt_pi.adapters.serial_port_discovery import SerialPortInfo
from senlyt_pi.adapters.sy01b_engine_adapter import Sy01bEngineAdapter
from senlyt_pi.adapters.valve_adapter import FakeValveAdapter
from senlyt_pi.app.bootstrap import (
    BootstrapError,
    build_components,
    build_engine,
    build_valve,
    pump_map_from_addresses_env,
)
from senlyt_pi.config.server_target import SENLYT_ENV_KEY, ServerTargetError
from support_http import FakeHttpServer

_CH340 = [SerialPortInfo(device="/dev/ttyUSB0", vid=0x1A86, pid=0x7523)]

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


# ── 자동감지("URL만" — SENLYT_ENGINE/VALVE 없이) ─────────────────────────────
def test_engine_autodetect_pi_with_serial_is_sy01b() -> None:
    """실 Pi + 시리얼 어댑터 존재 → sy01b(설치 시 SENLYT_ENGINE 불요)."""
    eng = build_engine({}, on_pi=lambda: True, port_lister=lambda: list(_CH340))
    assert isinstance(eng, Sy01bEngineAdapter)


def test_engine_autodetect_pi_without_serial_is_fake() -> None:
    assert isinstance(build_engine({}, on_pi=lambda: True, port_lister=list), FakeEnginePort)


def test_engine_autodetect_non_pi_is_fake_even_with_serial() -> None:
    """비-Pi 는 시리얼이 있어도 fake(자동감지 게이트 = Pi 여부·CI 결정성)."""
    eng = build_engine({}, on_pi=lambda: False, port_lister=lambda: list(_CH340))
    assert isinstance(eng, FakeEnginePort)


def test_engine_explicit_env_overrides_autodetect() -> None:
    """명시 SENLYT_ENGINE=fake 는 자동감지보다 우선(E2E 고정)."""
    eng = build_engine({"SENLYT_ENGINE": "fake"}, on_pi=lambda: True, port_lister=lambda: list(_CH340))
    assert isinstance(eng, FakeEnginePort)


def test_valve_autodetect_non_pi_is_fake() -> None:
    assert isinstance(build_valve({}, on_pi=lambda: False), FakeValveAdapter)


def test_valve_autodetect_pi_returns_valve_no_crash() -> None:
    """실 Pi 자동감지 — gpio 시도(gpiozero 부재 시 graceful fake). 어느 쪽이든 부팅 중단 없이 valve 반환."""
    assert build_valve({}, on_pi=lambda: True) is not None


def test_valve_off_is_none() -> None:
    assert build_valve({"SENLYT_VALVE": "off"}) is None


def test_register_path_over_socket(tmp_path) -> None:
    """실 등록 경로 — 로컬 fake register 서버로 build_components(register=True) 왕복.

    [D-A] deviceId = SENLYT_HARDWARE_ID(수집 시리얼)로 확정 — 서버 echo(다른 값)는 무시.
    """
    ok_body = {"deviceId": "server-echo-ignored", "dispenserToken": "tok-X", "exp": 9_999_999_999}
    with FakeHttpServer() as srv:
        srv.set_handler(lambda req: {"status": 200, "json": ok_body})
        # TOFU(2026-07-17): 공유키 env 없음 — deviceId 만 제시. (서버가 200 승인 응답을 즉시 준 경우.)
        env = {
            "SENLYT_SERVER_BASE_URL": srv.base_url,
            "SENLYT_HARDWARE_ID": "hw-e2e",
        }
        store = DeviceIdentityStore(tmp_path / "id.json")
        comp = build_components(env, identity_store=store, register=True)
        assert comp.device_id == "hw-e2e"  # 시리얼 = deviceId(서버 echo 아님).
        # 등록 요청이 올바른 경로·인증헤더 없음·deviceId(시리얼)로 갔는지(TOFU).
        reg = srv.requests[-1]
        assert reg.path == "/api/dispensers/register"
        assert reg.header("Authorization") is None  # 공유키 제거 — 인증 헤더 없음.
        assert reg.json()["deviceId"] == "hw-e2e"
        # 정체성 파일 영속.
        assert store.load().device_id == "hw-e2e"


# ── PUMP_ADDRESSES 부트스트랩 pump_map (install.sh 각인값과 계약) ──────────────
#
# install.sh 가 device.env 에 각인하는 값이 이 파서로 들어온다. 비면 pump_map 이 비어
# 모든 레시피 스텝이 CMD_VALIDATION_FAILED 로 drop(토출 0) → 주문 실패. 그 계약을 고정한다.

_INSTALL_SH_PUMP_ADDRESSES = "flavor:1,2;fragrance:1,2,3;aroma:1,2,3"


def test_pump_map_from_install_sh_value() -> None:
    """install.sh 각인값 그대로 → 유효 addr 전부 매핑(빈 pump_map = 토출 0 회귀 방지).

    식향 2펌프(addr 1,2)·향장향 3펌프(addr 1,2,3) — 2026-07-17 확정.
    """
    m = pump_map_from_addresses_env(_INSTALL_SH_PUMP_ADDRESSES)
    assert set(m.keys()) == {1, 2, 3}, "서버가 쓰는 pumpAddr ⊆ pi pump_map 이어야 drop 없음"
    for addr, spec in m.items():
        assert spec.pump_full_stroke == 12000, "sy01b 프리셋 스트로크"
        assert spec.syringe_capacity_ml == 0.5, "양 모드 공통 기본 용량(2026-07-17 확정)"
        assert spec.max_volume_ul == 500


def test_pump_map_never_maps_broadcast_addr_0() -> None:
    """⚠️ addr 0 = RS485 브로드캐스트 — install.sh 각인값이 0 을 기기주소로 쓰지 않는다."""
    assert 0 not in pump_map_from_addresses_env(_INSTALL_SH_PUMP_ADDRESSES)


def test_pump_map_flavor_default_capacity_is_05ml() -> None:
    """식향 기본 용량 회귀 고정 — 1.25mL 로 되돌아가면 과흡입이 게이트를 통과한다(F9)."""
    m = pump_map_from_addresses_env("flavor:1,2")
    assert set(m.keys()) == {1, 2}
    assert all(s.syringe_capacity_ml == 0.5 for s in m.values())


def test_pump_map_empty_env_is_empty_map() -> None:
    """미설정 → 빈 매핑(= 전 스텝 drop). install.sh 가 이 값을 반드시 각인해야 하는 이유."""
    assert pump_map_from_addresses_env(None) == {}
    assert pump_map_from_addresses_env("") == {}
