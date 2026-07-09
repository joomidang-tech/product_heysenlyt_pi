"""서버 SSE CommandSourcePort 실어댑터 — ⛔ TODO 스텁(이후 웨이브).

Dart `lib/adapters/sse_command_source_adapter.dart` 포팅(스텁 그대로).

실 SSE 클라이언트(http·재연결·resync fetchSince·deviceId 필터)는 이후 웨이브(실 서버 연결).
지금은 포트 계약만. 서버 base URL 은 하드코딩하지 않고 `ServerConfig`(config.server_target)가
환경별로 결정한 단일 base 를 소비한다(프리뷰가 prod 를 보는 사고 구조적 차단).
"""

from __future__ import annotations

from typing import Iterator

from ..config.server_target import ServerConfig
from ..core.wire_messages import Command


class SseCommandSourceAdapter:
    """서버 SSE command 구독 어댑터 — 미구현 스텁."""

    def __init__(
        self,
        *,
        server_config: ServerConfig | None = None,
        base_url: str = "",
        bearer_token: str = "",
    ) -> None:
        # 서버 base 는 ServerConfig(환경별 결정) 우선 — 하드코딩 URL 금지.
        # base_url 인자는 하위호환(테스트·직접 주입)용. server_config 가 있으면 그 base 를 쓴다.
        self.server_config = server_config
        self.base_url = server_config.base_url if server_config is not None else base_url
        self.bearer_token = bearer_token

    def commands(self, device_id: str) -> Iterator[Command]:
        # TODO(wave-next): GET SSE snapshot 구독 → DTO→Command 파생 → deviceId 필터(CS-08).
        raise NotImplementedError("SSE command source — 이후 웨이브(실 서버 연결 유보)")
