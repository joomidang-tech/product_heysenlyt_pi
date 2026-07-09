"""서버 타겟 결정(config.server_target) 테스트 — 브랜치=환경 → base URL.

계약(2026-07-09 사용자 확정):
  - 4환경(prod|dev|v1_2_0|v1_1_0) 각각 올바른 base URL 로 매핑.
  - 명시 SENLYT_SERVER_BASE_URL 은 SENLYT_ENV 보다 우선(탈출구).
  - 미설정(둘 다 없음) → fail-fast(ServerTargetError·prod 조용한 접속 방지).
  - 미지원 env·잘못된 URL → ServerTargetError.
"""

from __future__ import annotations

import pytest

from senlyt_pi.config.server_target import (
    ENV_TO_BASE_URL,
    SENLYT_ENV_KEY,
    SENLYT_SERVER_BASE_URL_KEY,
    ServerConfig,
    ServerTargetError,
    join_url,
    resolve_from_environ,
    resolve_server_base_url,
)


# ── 1. ENV → base URL 매핑(4환경 각각 올바른 URL) ──


@pytest.mark.parametrize(
    "env,expected",
    [
        ("prod", "https://senlyt.com"),
        ("dev", "https://dev.senlyt.com"),
        ("v1_2_0", "https://v1-2-0.env.senlyt.com"),
        ("v1_1_0", "https://v1-1-0.env.senlyt.com"),
    ],
)
def test_resolve_maps_each_env_to_correct_url(env: str, expected: str) -> None:
    assert resolve_server_base_url(env) == expected


def test_mapping_table_covers_exactly_four_environments() -> None:
    assert set(ENV_TO_BASE_URL) == {"prod", "dev", "v1_2_0", "v1_1_0"}


def test_env_is_case_insensitive_and_trimmed() -> None:
    assert resolve_server_base_url("  PROD ") == "https://senlyt.com"
    assert resolve_server_base_url("V1_2_0") == "https://v1-2-0.env.senlyt.com"


# ── 2. 명시 URL 우선(탈출구) ──


def test_explicit_url_takes_precedence_over_env() -> None:
    # env=prod 이어도 명시 URL 이 있으면 그것을 쓴다.
    got = resolve_server_base_url("prod", "https://staging.example.com")
    assert got == "https://staging.example.com"


def test_explicit_url_used_when_env_none() -> None:
    assert resolve_server_base_url(None, "https://local.test:8080") == "https://local.test:8080"


def test_explicit_url_trailing_slash_normalized() -> None:
    assert resolve_server_base_url(None, "https://x.test/") == "https://x.test"
    assert resolve_server_base_url(None, "https://x.test///") == "https://x.test"


def test_explicit_url_whitespace_treated_as_unset_falls_back_to_env() -> None:
    # 빈/공백 명시값은 미설정으로 취급 → env 폴백.
    assert resolve_server_base_url("dev", "   ") == "https://dev.senlyt.com"
    assert resolve_server_base_url("dev", "") == "https://dev.senlyt.com"


def test_explicit_http_scheme_allowed() -> None:
    assert resolve_server_base_url(None, "http://192.168.0.10:3000") == "http://192.168.0.10:3000"


# ── 3. fail-fast(미설정·미지원·잘못된 URL) ──


def test_unset_both_raises_fail_fast() -> None:
    with pytest.raises(ServerTargetError):
        resolve_server_base_url(None, None)


def test_empty_env_and_no_explicit_raises() -> None:
    with pytest.raises(ServerTargetError):
        resolve_server_base_url("   ", None)


def test_unknown_env_raises() -> None:
    with pytest.raises(ServerTargetError):
        resolve_server_base_url("staging", None)


def test_unknown_env_still_rejected_even_if_similar() -> None:
    # 오타·유사값도 조용히 폴백하지 않는다.
    with pytest.raises(ServerTargetError):
        resolve_server_base_url("v1.2.0", None)  # 점 표기는 미지원(언더스코어만)


@pytest.mark.parametrize(
    "bad_url",
    [
        "ftp://x.test",
        "senlyt.com",  # 스킴 없음
        "://nohost",
        "https://",  # 호스트 없음
    ],
)
def test_malformed_explicit_url_raises(bad_url: str) -> None:
    with pytest.raises(ServerTargetError):
        resolve_server_base_url(None, bad_url)


# ── 4. resolve_from_environ 진입점 ──


def test_resolve_from_environ_reads_env_key() -> None:
    assert resolve_from_environ({SENLYT_ENV_KEY: "prod"}) == "https://senlyt.com"


def test_resolve_from_environ_explicit_wins() -> None:
    env = {SENLYT_ENV_KEY: "prod", SENLYT_SERVER_BASE_URL_KEY: "https://override.test"}
    assert resolve_from_environ(env) == "https://override.test"


def test_resolve_from_environ_empty_mapping_fails_fast() -> None:
    with pytest.raises(ServerTargetError):
        resolve_from_environ({})


# ── 5. join_url 결합(단일 슬래시) ──


@pytest.mark.parametrize(
    "base,path,expected",
    [
        ("https://x.test", "/api/dispenser/login", "https://x.test/api/dispenser/login"),
        ("https://x.test/", "/api/dispenser/login", "https://x.test/api/dispenser/login"),
        ("https://x.test", "api/dispenser/login", "https://x.test/api/dispenser/login"),
        ("https://x.test/", "api/dispenser/login", "https://x.test/api/dispenser/login"),
    ],
)
def test_join_url_single_slash(base: str, path: str, expected: str) -> None:
    assert join_url(base, path) == expected


# ── 6. ServerConfig — 엔드포인트 조립의 단일 소비 지점 ──


def test_server_config_from_environ_builds_endpoints() -> None:
    cfg = ServerConfig.from_environ({SENLYT_ENV_KEY: "v1_2_0"})
    assert cfg.base_url == "https://v1-2-0.env.senlyt.com"
    assert cfg.register_url == "https://v1-2-0.env.senlyt.com/api/dispensers/register"
    assert cfg.login_url == "https://v1-2-0.env.senlyt.com/api/dispenser/login"
    assert cfg.heartbeat_url == "https://v1-2-0.env.senlyt.com/api/dispenser/heartbeat"
    assert cfg.orders_stream_url == "https://v1-2-0.env.senlyt.com/api/dispenser/orders/stream"
    assert cfg.settings_stream_url == "https://v1-2-0.env.senlyt.com/api/dispenser/settings"
    assert cfg.commandsets_url == "https://v1-2-0.env.senlyt.com/api/dispenser/commandsets"


def test_server_config_from_environ_fails_fast_when_unset() -> None:
    with pytest.raises(ServerTargetError):
        ServerConfig.from_environ({})


def test_server_config_prod_endpoints_target_prod_host() -> None:
    # prod 환경은 prod 호스트만 — 프리뷰/버전 호스트가 섞이지 않음(오접속 방지 회귀 가드).
    cfg = ServerConfig.from_environ({SENLYT_ENV_KEY: "prod"})
    assert cfg.register_url.startswith("https://senlyt.com/")
    assert "env.senlyt.com" not in cfg.register_url


def test_server_config_url_arbitrary_path() -> None:
    cfg = ServerConfig(base_url="https://x.test")
    assert cfg.url("/api/dispenser/orders/abc") == "https://x.test/api/dispenser/orders/abc"
