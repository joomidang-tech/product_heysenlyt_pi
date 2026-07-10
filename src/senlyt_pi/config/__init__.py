"""pi 실행 환경 설정 — 서버 타겟(base URL) 결정의 단일 지점.

`server_target` = 브랜치=환경에 따라 pi 가 붙는 서버 base URL 을 결정하는 순수 로직.
register/SSE/heartbeat/status 어댑터는 이 모듈이 조립한 엔드포인트만 소비한다
(하드코딩 URL 금지 — 프리뷰 기계가 prod 를 조용히 보는 사고를 구조적으로 차단).
"""

from __future__ import annotations

from .server_target import (
    ENV_TO_BASE_URL,
    PATH_COMMANDSETS,
    PATH_HEARTBEAT,
    PATH_ORDERS,
    PATH_ORDERS_STREAM,
    PATH_REGISTER,
    PATH_TRACE,
    SENLYT_ENV_KEY,
    SENLYT_SERVER_BASE_URL_KEY,
    ServerConfig,
    ServerTargetError,
    join_url,
    resolve_from_environ,
    resolve_server_base_url,
)

__all__ = [
    "ENV_TO_BASE_URL",
    "PATH_COMMANDSETS",
    "PATH_HEARTBEAT",
    "PATH_ORDERS",
    "PATH_ORDERS_STREAM",
    "PATH_REGISTER",
    "PATH_TRACE",
    "SENLYT_ENV_KEY",
    "SENLYT_SERVER_BASE_URL_KEY",
    "ServerConfig",
    "ServerTargetError",
    "join_url",
    "resolve_from_environ",
    "resolve_server_base_url",
]
