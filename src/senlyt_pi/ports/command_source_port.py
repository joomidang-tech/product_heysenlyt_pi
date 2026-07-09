"""CommandSourcePort — 서버 SSE 명령 구독(인터페이스만) — SoT §1-1 / §8-3 / §9-1.

Dart `lib/ports/command_source_port.dart` 포팅. pi 는 Firestore 직결 0 — 서버 SSE 로
command snapshot(DTO 파생)을 구독하고, 자기 deviceId 명령만 필터한다(CS-08).
실 SSE 클라이언트(http)는 이후 웨이브(TODO) — 이번 웨이브는 Fake/스텁만.
"""

from __future__ import annotations

from typing import Iterator, Protocol

from ..core.wire_messages import Command


class CommandSourcePort(Protocol):
    """서버 → pi command 스트림 소스."""

    def commands(self, device_id: str) -> Iterator[Command]:
        """deviceId 로 필터된 command 스트림. 재연결·resync 는 구현체 책임(이후 웨이브)."""
        ...
