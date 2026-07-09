"""서버 SSE CommandSourcePort 실어댑터 — ⛔ TODO 스텁(이후 웨이브).

Dart `lib/adapters/sse_command_source_adapter.dart` 포팅(스텁 그대로).

실 SSE 클라이언트(http·재연결·resync fetchSince·deviceId 필터)는 이후 웨이브(실 서버 연결).
지금은 포트 계약만.
"""

from __future__ import annotations

from typing import Iterator

from ..core.wire_messages import Command


class SseCommandSourceAdapter:
    """서버 SSE command 구독 어댑터 — 미구현 스텁."""

    def __init__(self, *, base_url: str = "", bearer_token: str = "") -> None:
        # 실구현(이후 웨이브) 시그니처 예약 — GET SSE snapshot + Bearer dispenser.
        self.base_url = base_url
        self.bearer_token = bearer_token

    def commands(self, device_id: str) -> Iterator[Command]:
        # TODO(wave-next): GET SSE snapshot 구독 → DTO→Command 파생 → deviceId 필터(CS-08).
        raise NotImplementedError("SSE command source — 이후 웨이브(실 서버 연결 유보)")
