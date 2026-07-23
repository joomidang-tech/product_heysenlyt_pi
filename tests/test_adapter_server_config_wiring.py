"""어댑터 ↔ ServerConfig 결선 테스트 — register/SSE/status 가 단일 base 를 소비한다.

하드코딩 URL 금지 계약: SSE·status 어댑터는 ServerConfig 가 결정한 base 를 그대로 쓴다.
base_url 직접 주입은 하위호환(테스트) 경로로 계속 동작.
"""

from __future__ import annotations

from senlyt_pi.adapters.http_status_sink_adapter import HttpStatusSinkAdapter
from senlyt_pi.adapters.sse_command_source_adapter import SseCommandSourceAdapter
from senlyt_pi.config.server_target import SENLYT_ENV_KEY, ServerConfig


def test_sse_adapter_consumes_server_config_base() -> None:
    cfg = ServerConfig.from_environ({SENLYT_ENV_KEY: "v1_1_0"})
    adapter = SseCommandSourceAdapter(server_config=cfg, bearer_token="t")
    assert adapter.base_url == "https://v1-1-0.env.senlyt.com"
    assert adapter.server_config is cfg


def test_status_adapter_consumes_server_config_base() -> None:
    cfg = ServerConfig.from_environ({SENLYT_ENV_KEY: "prod"})
    adapter = HttpStatusSinkAdapter(server_config=cfg, bearer_token="t")
    assert adapter.base_url == "https://senlyt.com"
    assert adapter.server_config is cfg


def test_adapters_backward_compatible_base_url_arg() -> None:
    # server_config 없이 base_url 직접 주입(기존 경로) 계속 동작.
    sse = SseCommandSourceAdapter(base_url="https://x.test")
    status = HttpStatusSinkAdapter(base_url="https://x.test")
    assert sse.base_url == "https://x.test"
    assert sse.server_config is None
    assert status.base_url == "https://x.test"
    assert status.server_config is None
