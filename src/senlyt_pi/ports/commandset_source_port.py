"""CommandSetSourcePort — CommandSet 봉투 구독(인터페이스만) — 계약 (2026-07-09).

기존 CommandSourcePort(§9-1 command)와 병행하는 신규 축 — 기존 소비자 무파괴.
전달 채널 = 기존 SSE snapshot `commandSets` 필드(queued|delivered 만·자기 deviceId 필터) +
보조 폴링/resync GET /api/dispenser/commandsets?status=queued. 실 SSE 클라이언트는
이후 웨이브 — 이번 웨이브는 포트 계약 + Fake 소비 경로까지.
"""

from __future__ import annotations

from typing import Iterator, Protocol

from ..core.command_set import CommandSet


class CommandSetSourcePort(Protocol):
    """서버 → pi CommandSet 봉투 스트림 소스."""

    def command_sets(self, device_id: str) -> Iterator[CommandSet]:
        """deviceId 로 필터된 CommandSet 스트림(queued|delivered).
        재연결·resync(createdAt cursor)는 구현체 책임(이후 웨이브)."""
        ...
