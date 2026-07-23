"""settings read-only 소비 테스트 — 서버 MachineSettings → 시린지 용량/스트로크 파생(O-18).

계약 = heysenlyt-web lib/server/settingsClamp.ts MachineSettings(단일 pumpPreset + pumpPorts).
방어적 재clamp(clamp_pump_preset)·용량 allowlist 폴백(O-15)·부팅 1회 SSE fetch(seam) 검증.
"""

import json

from senlyt_pi.adapters.settings_source import (
    fetch_settings_once,
    full_stroke_from_settings,
    pump_addrs_from_settings,
    pump_map_from_settings,
    syringe_capacity_from_settings,
)
from senlyt_pi.config.server_target import ServerConfig
from senlyt_pi.pipeline.recipe_resolver import RecipeResolver


def test_syringe_capacity_from_preset():
    """pumpPreset.syringeCapacityMl(9종 allowlist 안) 그대로 검증 통과."""
    s = {"pumpPreset": {"pumpPresetId": "sy01b", "syringeCapacityMl": 1.25}}
    assert syringe_capacity_from_settings(s) == 1.25
    # fullStroke 는 빌트인 sy01b 강제(12000).
    assert full_stroke_from_settings(s) == 12000


def test_capacity_allowlist_fallback_to_default():
    """9종 밖 용량 → 기본 0.5mL 폴백(양 모드 공통·O-15·2026-07-17 확정)."""
    assert syringe_capacity_from_settings({"pumpPreset": {"syringeCapacityMl": 3.3}}) == 0.5
    assert syringe_capacity_from_settings({"pumpPreset": {"syringeCapacityMl": -1}}) == 0.5


def test_capacity_none_when_absent_or_invalid():
    """프리셋/용량 부재·불량 → None(서버 미제공 신호 — 호출측이 모드 기본 폴백)."""
    assert syringe_capacity_from_settings(None) is None
    assert syringe_capacity_from_settings({"pumpPreset": {}}) is None
    assert syringe_capacity_from_settings({"pumpPreset": {"syringeCapacityMl": True}}) is None
    assert syringe_capacity_from_settings({"pumpPreset": "x"}) is None
    assert full_stroke_from_settings({}) is None


def test_pump_addrs_from_ports():
    """pumpPorts 키("1".."12") → 정수 주소(≥1) 오름차순. 0/비정수/부재 방어."""
    s = {"pumpPorts": {"2": {}, "1": {}, "0": {}, "x": {}}}
    assert pump_addrs_from_settings(s) == [1, 2]  # 0=브로드캐스트 배제·"x" 스킵.
    assert pump_addrs_from_settings({}) == []
    assert pump_addrs_from_settings({"pumpPorts": "no"}) == []


def test_pump_map_from_settings_real_contract():
    """MachineSettings → pumpAddr→SyringeSpec — 주소=pumpPorts 키, 용량=프리셋, 스트로크 12000."""
    settings = {
        "pumpPreset": {"pumpPresetId": "sy01b", "syringeCapacityMl": 1.25},
        "pumpPorts": {"1": {}, "2": {}},
    }
    m = pump_map_from_settings(settings)
    assert set(m.keys()) == {1, 2}
    assert m[1].pump_full_stroke == 12000
    assert m[1].max_volume_ul == 1250  # 1.25mL → 게이트 상한.
    RecipeResolver(m)  # RR pump_map 으로 그대로 소비 가능.


def test_pump_map_addrs_override_and_default_capacity():
    """addrs 인자 우선(실배선은 물리 프로브 결과 주입) + 프리셋 부재 시 모드 기본 용량."""
    m = pump_map_from_settings({}, addrs=[1, 2], mode_is_flavor=True)
    assert set(m.keys()) == {1, 2}
    assert m[1].syringe_capacity_ml == 0.5  # 프리셋 없음 → 모드 기본.
    assert m[1].pump_full_stroke == 12000  # 스트로크 없음 → sy01b.


# ── 부팅 1회 SSE fetch(seam·네트워크 없이) ──────────────────────────────────────


class _FakeStream:
    def __init__(self, frames):
        self._frames = frames
        self.closed = False

    def events(self):
        yield from self._frames

    def close(self):
        self.closed = True


_CFG = ServerConfig(base_url="https://v1-2-0.env.senlyt.com")


def test_fetch_settings_once_reads_first_settings_event():
    ms = {"pumpPreset": {"pumpPresetId": "sy01b", "syringeCapacityMl": 0.5}, "pumpPorts": {"1": {}}}
    captured = {}
    stream = _FakeStream([("settings", json.dumps({"settings": ms}))])

    def fake_open(url, *, headers, timeout, connect_timeout):
        captured["url"] = url
        captured["headers"] = headers
        return stream

    out = fetch_settings_once(_CFG, "tok", "flavor", open_stream=fake_open)
    assert out == ms
    assert captured["url"].endswith("/api/dispenser/settings?mode=flavor")
    assert captured["headers"]["Authorization"] == "Bearer tok"
    assert stream.closed  # 첫 프레임 읽고 스트림 정리.


def test_fetch_settings_once_skips_non_settings_then_reads():
    ms = {"pumpPreset": {"syringeCapacityMl": 2.5}}
    frames = [("message", "ignored"), ("settings", json.dumps({"settings": ms}))]
    out = fetch_settings_once(_CFG, "t", "fragrance", open_stream=lambda *a, **k: _FakeStream(frames))
    assert out == ms


def test_fetch_settings_once_best_effort_none_on_failures():
    # 연결 실패 → None.
    def boom(*a, **k):
        raise RuntimeError("connect refused")

    assert fetch_settings_once(_CFG, "t", "flavor", open_stream=boom) is None
    # settings 이벤트 없이 스트림 종료 → None.
    assert (
        fetch_settings_once(_CFG, "t", "flavor", open_stream=lambda *a, **k: _FakeStream([]))
        is None
    )
    # 깨진 JSON → None.
    bad = [("settings", "{not-json")]
    assert (
        fetch_settings_once(_CFG, "t", "flavor", open_stream=lambda *a, **k: _FakeStream(bad))
        is None
    )
