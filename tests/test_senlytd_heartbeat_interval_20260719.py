"""heartbeat 주기 env 노브 회귀 테스트(2026-07-19·30s→10s 개편).

검증 대상: senlytd._resolve_heartbeat_interval_s —
  SENLYT_HEARTBEAT_INTERVAL_MS(기본 10000ms=10s) → 초 환산. 파싱 실패·비양수는 10.0s 로
  안전 폴백(기존 _resolve_poll_interval_s 와 동일 패턴). 기본 10s 는 서버 online 표시 창
  (30s=3주기)과 정합 — 값을 바꾸면 서버 창도 함께 조정해야 한다.
  파싱 성공한 양수는 [1.0s, 30.0s] 클램프(상한 30s = 표시 창 붕괴 방지 · 하한 1s = 쓰기 폭주 방지).
"""

from __future__ import annotations

from senlyt_pi.app.senlytd import (
    SENLYT_HEARTBEAT_INTERVAL_MS_ENV,
    _resolve_heartbeat_interval_s,
)


def test_default_is_10s():
    """env 미설정 — 기본 10.0s(서버 표시 창 30s=3주기 정합)."""
    assert _resolve_heartbeat_interval_s({}) == 10.0


def test_valid_ms_converts_to_seconds():
    """"5000" → 5.0s 환산."""
    assert _resolve_heartbeat_interval_s({SENLYT_HEARTBEAT_INTERVAL_MS_ENV: "5000"}) == 5.0


def test_invalid_value_falls_back_to_10s():
    """숫자 아님 — 10.0s 안전 폴백."""
    assert _resolve_heartbeat_interval_s({SENLYT_HEARTBEAT_INTERVAL_MS_ENV: "abc"}) == 10.0


def test_non_positive_falls_back_to_10s():
    """비양수(음수·0) — 10.0s 안전 폴백."""
    assert _resolve_heartbeat_interval_s({SENLYT_HEARTBEAT_INTERVAL_MS_ENV: "-1000"}) == 10.0
    assert _resolve_heartbeat_interval_s({SENLYT_HEARTBEAT_INTERVAL_MS_ENV: "0"}) == 10.0


def test_blank_value_falls_back_to_10s():
    """공백 문자열 — 미설정과 동일하게 10.0s 폴백."""
    assert _resolve_heartbeat_interval_s({SENLYT_HEARTBEAT_INTERVAL_MS_ENV: "  "}) == 10.0


def test_clamp_upper_bound_30s():
    """상한 클램프 — "60000"(60s) → 30.0s(서버 표시 창 30s=3주기 붕괴 방지)."""
    assert _resolve_heartbeat_interval_s({SENLYT_HEARTBEAT_INTERVAL_MS_ENV: "60000"}) == 30.0


def test_clamp_lower_bound_1s():
    """하한 클램프 — "500"(0.5s) → 1.0s(서버/Firestore 쓰기 폭주 방지)."""
    assert _resolve_heartbeat_interval_s({SENLYT_HEARTBEAT_INTERVAL_MS_ENV: "500"}) == 1.0


def test_clamp_bounds_pass_through():
    """경계값은 그대로 — "30000" → 30.0s · "1000" → 1.0s."""
    assert _resolve_heartbeat_interval_s({SENLYT_HEARTBEAT_INTERVAL_MS_ENV: "30000"}) == 30.0
    assert _resolve_heartbeat_interval_s({SENLYT_HEARTBEAT_INTERVAL_MS_ENV: "1000"}) == 1.0
