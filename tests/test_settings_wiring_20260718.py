"""settings 부팅 배선 회귀 테스트(2026-07-18·감사 P1·P2 최소 봉합).

검증 대상:
  1) build_resolver(server_settings=…) — 서버 프리셋 시린지 용량/스트로크가 pump_map 에 얹힌다
     (env 고정 경로 + 물리 프로브 자동인식 경로 모두). 용량 오류 = Code 11 이라 이게 안전 급소.
  2) build_components(fetch_settings=…, settings_fetcher=…) — 부팅 1회 settings fetch seam 이
     네트워크 없이 주입되고, 실패해도 부팅을 막지 않는다(best-effort None 폴백).
  3) SenlytDaemon._boot_self_test — 매핑 펌프 0 이면 제조 보류(fail-closed) 관측, ≥1 이면 통과.
"""

from __future__ import annotations

from senlyt_pi.adapters.device_identity_store import DeviceIdentity, DeviceIdentityStore
from senlyt_pi.adapters.fake_engine_adapter import FakeEnginePort
from senlyt_pi.app.bootstrap import build_components, build_resolver
from senlyt_pi.config.server_target import SENLYT_ENV_KEY
from senlyt_pi.core.pump_guard import SyringeSpec
from senlyt_pi.pipeline.recipe_resolver import RecipeResolver

# 1.25mL 시린지 프리셋(9종 allowlist 안) — 기본 0.5mL 와 다른 용량이어야 threading 이 관측된다.
_SETTINGS_125 = {"pumpPreset": {"pumpPresetId": "sy01b", "syringeCapacityMl": 1.25}}


# ── 1) build_resolver 용량 threading ────────────────────────────────────────


class _Bus:
    """주소 1·2 에만 펌프가 응답하는 버스(probe seam)."""

    def probe(self, addr: int) -> bool:
        return addr in (1, 2)


def test_env_path_uses_server_capacity():
    """PUMP_ADDRESSES 고정 경로 — 서버 프리셋 용량(1.25mL)이 기본 0.5mL 를 덮는다."""
    r = build_resolver(
        {"PUMP_ADDRESSES": "flavor:1,2", "SENLYT_MODE": "flavor"},
        server_settings=_SETTINGS_125,
    )
    assert sorted(r.pump_map) == [1, 2]
    assert r.pump_map[1].syringe_capacity_ml == 1.25
    assert r.pump_map[1].max_volume_ul == 1250  # 0.5mL(500) 였으면 과다흡입 게이트 오작동.


def test_probe_path_uses_server_capacity():
    """물리 프로브 자동인식 경로 — 발견 주소에 서버 용량을 얹는다."""
    r = build_resolver(
        {"SENLYT_MODE": "flavor"}, engine=_Bus(), server_settings=_SETTINGS_125
    )
    assert sorted(r.pump_map) == [1, 2]
    assert r.pump_map[1].max_volume_ul == 1250


def test_absent_settings_falls_back_to_mode_default():
    """server_settings=None → 모드 기본 0.5mL(기존 동작 보존)."""
    r = build_resolver({"PUMP_ADDRESSES": "flavor:1,2"}, server_settings=None)
    assert r.pump_map[1].syringe_capacity_ml == 0.5
    assert r.pump_map[1].max_volume_ul == 500


class _Bus123:
    """주소 1·2·3 모두 응답하는 버스 — 프로브 범위(expected)가 결과를 가른다."""

    def probe(self, addr: int) -> bool:
        return addr in (1, 2, 3)


def test_mode_param_overrides_env_for_probe_range():
    """#3(2026-07-18) — mode 인자(서버배정)가 env 보다 우선. 'URL만' 설치(env 없음) 식향 기기가
    예상주소 [1,2]만 프로브해 부재 addr 3 낭비를 없앤다."""
    # env 없음 + mode='flavor' → [1,2]만 (버스는 3도 응답하지만 프로브 범위 밖).
    r = build_resolver({}, engine=_Bus123(), mode="flavor")
    assert sorted(r.pump_map) == [1, 2]
    # mode 미주입 + env 없음 → 향장향 기본 [1,2,3].
    r2 = build_resolver({}, engine=_Bus123())
    assert sorted(r2.pump_map) == [1, 2, 3]
    # mode 가 env 를 이긴다(env=fragrance 인데 mode=flavor → [1,2]).
    r3 = build_resolver({"SENLYT_MODE": "fragrance"}, engine=_Bus123(), mode="flavor")
    assert sorted(r3.pump_map) == [1, 2]


def test_build_engine_injects_shared_estop_event():
    """#4(2026-07-18) — build_engine(estop_event=ev) 이 어댑터에 **같은 Event** 를 주입한다
    (설계 '단일 공유 _estop'). fake 경로도 동일."""
    import threading

    from senlyt_pi.app.bootstrap import build_engine

    ev = threading.Event()
    fake = build_engine({"SENLYT_ENGINE": "fake"}, estop_event=ev)
    assert fake._estop is ev  # 공유 이벤트가 그대로 주입됨


# ── 2) build_components fetch seam ──────────────────────────────────────────


def _store(tmp_path) -> DeviceIdentityStore:
    store = DeviceIdentityStore(tmp_path / "identity.json")
    store.save(DeviceIdentity(device_id="dev-A", dispenser_token="tok-1", exp=9_999_999_999))
    return store


def test_build_components_injects_settings_fetcher(tmp_path):
    """fetch_settings=True + seam 주입 → server_settings 채워지고 fetcher 는 (cfg,token,mode) 수신."""
    seen: dict = {}

    def fake_fetch(cfg, token, mode):
        seen["base"] = cfg.base_url
        seen["token"] = token
        seen["mode"] = mode
        return _SETTINGS_125

    comp = build_components(
        {SENLYT_ENV_KEY: "v1_2_0"},
        identity_store=_store(tmp_path),
        register=False,
        fetch_settings=True,
        settings_fetcher=fake_fetch,
    )
    assert comp.server_settings == _SETTINGS_125
    assert comp.mode == "flavor"  # identity.mode 없음 → SENLYT_MODE 없음 → flavor 기본.
    assert seen["base"] == "https://v1-2-0.env.senlyt.com"
    assert seen["token"] == "tok-1"
    assert seen["mode"] == "flavor"


def test_build_components_fetch_disabled_by_default(tmp_path):
    """기본(fetch_settings 미지정) → server_settings=None(조립 테스트는 네트워크 없이)."""
    comp = build_components(
        {SENLYT_ENV_KEY: "v1_2_0"}, identity_store=_store(tmp_path), register=False
    )
    assert comp.server_settings is None


def test_build_components_settings_fetch_failure_is_best_effort(tmp_path):
    """fetcher 예외 → 부팅 유지·server_settings=None(폴백)."""

    def boom(cfg, token, mode):
        raise RuntimeError("network down")

    comp = build_components(
        {SENLYT_ENV_KEY: "v1_2_0"},
        identity_store=_store(tmp_path),
        register=False,
        fetch_settings=True,
        settings_fetcher=boom,
    )
    assert comp.server_settings is None


# ── 3) daemon 부팅 자가진단 ──────────────────────────────────────────────────


def _daemon_with_resolver(resolver):
    from senlyt_pi.app.daemon import DaemonDeps, SenlytDaemon

    class _Ledger:
        def close(self):
            pass

    deps = DaemonDeps(
        device_id="dev-A",
        command_source=None,
        status_sink=None,
        engine=FakeEnginePort(),
        ledger=_Ledger(),
        resolver=resolver,
        heartbeat_interval_s=0,  # heartbeat 스레드 비활성(자가진단만 검증).
    )
    return SenlytDaemon(deps)


def test_boot_self_test_holds_when_no_pumps():
    """매핑 펌프 0 → 제조 보류(False) — fail-closed 관측."""
    d = _daemon_with_resolver(RecipeResolver({}))
    assert d._boot_self_test() is False


def test_boot_self_test_ready_with_pumps():
    """매핑 펌프 ≥1 → 제조 수용(True)."""
    spec = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=0.5)
    d = _daemon_with_resolver(RecipeResolver({1: spec, 2: spec}))
    assert d._boot_self_test() is True
