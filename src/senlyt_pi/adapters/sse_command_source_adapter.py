"""서버 SSE CommandSource/CommandSetSource 실어댑터 — 실 HTTP 구독(스텁 제거).

정본 계약: 05_api §8 · GET /api/dispenser/orders/stream (Bearer dispenser).

스텁 제거(사용자 원칙 2026-07-10): 이전 웨이브의 `raise NotImplementedError` 를 걷어내고
**실 SSE 클라이언트**(표준 urllib·http_client.open_sse)로 서버 큐를 구독한다.
  - 서버가 event:snapshot{orders, commands, commandSets} 를 push → 이 어댑터가 파싱.
  - `commands(device_id)` = snapshot.commands → Command 파생 → **CS-08 자기 deviceId 필터** → yield.
  - `command_sets(device_id)` = snapshot.commandSets → CommandSet 파생(queued|delivered·CS-08) → yield.
    (CommandSourcePort + CommandSetSourcePort 두 축을 한 어댑터가 제공 — 동일 snapshot 소비.)
  - deviceId 는 스트림 쿼리(`?deviceId=`)로도 서버가 1차 필터하고, 어댑터가 2차 필터(이중방어).

서버 base URL 은 하드코딩하지 않고 `ServerConfig`(config.server_target)가 환경별로 결정한
단일 base 를 소비한다(프리뷰가 prod 를 보는 사고 구조적 차단). SSE 는 스트리밍이라
`commands()`/`command_sets()` 는 연결이 살아있는 동안 도착분을 순차 방출하는 **무한 제너레이터**
(스트림 종료 시 순회 종료). 실 소비 루프(재연결·resync)는 daemon 이 조립한다.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Iterator, Mapping

from ..config.server_target import ServerConfig
from ..core.command_set import CommandSet, command_sets_from_snapshot
from ..core.wire_messages import Command
from ..obs.log import STAGE_PI_RECEIVED, StructuredLogger
from .http_client import DEFAULT_TIMEOUT_SECONDS, SseStream, bearer_headers, open_sse

# SSE 구독 소켓 타임아웃(초) — 스트리밍이므로 짧은 왕복 타임아웃보다 길게(무응답 감지용).
# None 이면 무한 대기. 기본은 서버 15s heartbeat 의 여유 배수.
DEFAULT_SSE_TIMEOUT_SECONDS = 60.0

# open_sse seam — (url, headers, timeout) → SseStream. 테스트가 fake 스트림을 주입.
OpenStream = Callable[..., SseStream]


def commands_from_snapshot(
    snapshot: Mapping[str, Any], device_id: str
) -> list[Command]:
    """SSE snapshot data → 소비 대상 Command 목록 — CS-08 자기 deviceId 필터.

    - `commands` 필드(부재 시 빈 목록). 항목 단위 방어 파싱(깨진 항목 skip).
    - 자기 deviceId 만(다매장 라우팅·CS-08). 서버 1차 필터의 2차 방어.
    """
    raw = snapshot.get("commands")
    if not isinstance(raw, list):
        return []
    out: list[Command] = []
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        try:
            cmd = Command.from_json(item)
        except (KeyError, TypeError, ValueError):
            continue  # 깨진 command 는 skip(전체 snapshot 을 죽이지 않음).
        if cmd.device_id != device_id:
            continue
        out.append(cmd)
    return out


class SseCommandSourceAdapter:
    """서버 SSE command/commandSet 구독 실어댑터."""

    def __init__(
        self,
        *,
        server_config: ServerConfig | None = None,
        base_url: str = "",
        bearer_token: str = "",
        mode: str = "flavor",
        view: str = "pending",
        timeout: float | None = DEFAULT_SSE_TIMEOUT_SECONDS,
        open_stream: OpenStream = open_sse,
        logger: StructuredLogger | None = None,
    ) -> None:
        # 서버 base 는 ServerConfig(환경별 결정) 우선 — 하드코딩 URL 금지.
        # base_url 인자는 하위호환(테스트·직접 주입)용. server_config 가 있으면 그 base 를 쓴다.
        self.server_config = server_config
        self.base_url = server_config.base_url if server_config is not None else base_url
        self.bearer_token = bearer_token
        self.mode = mode
        self.view = view
        self.timeout = timeout
        self._open_stream = open_stream
        self._log = logger

    def _config(self) -> ServerConfig:
        return self.server_config or ServerConfig(base_url=self.base_url)

    def _stream_url(self, device_id: str) -> str:
        return self._config().orders_stream_query_url(
            mode=self.mode, view=self.view, device_id=device_id
        )

    def _open(self, device_id: str) -> SseStream:
        return self._open_stream(
            self._stream_url(device_id),
            headers=bearer_headers(self.bearer_token),
            timeout=self.timeout,
        )

    def _snapshots(self, stream: SseStream) -> Iterator[dict[str, Any]]:
        """SseStream → snapshot data(dict) 순회 — event:snapshot 만·본문 JSON 파싱."""
        for event, data in stream.events():
            if event != "snapshot":
                continue  # error/기타 이벤트는 이 축에서 무시(재연결은 daemon 책임).
            try:
                parsed = json.loads(data)
            except ValueError:
                continue
            if isinstance(parsed, dict):
                yield parsed

    def commands(self, device_id: str) -> Iterator[Command]:
        """자기 deviceId Command 스트림(CS-08). snapshot 도착분을 순차 방출."""
        with self._open(device_id) as stream:
            for snapshot in self._snapshots(stream):
                for cmd in commands_from_snapshot(snapshot, device_id):
                    if self._log is not None:
                        self._log.info(
                            "SSE snapshot 에서 command 수신",
                            stage=STAGE_PI_RECEIVED,
                            trace_id=cmd.trace_id,
                            order_id=cmd.order_id,
                            device_id=device_id,
                            command_id=cmd.id,
                            attempt=cmd.attempt,
                        )
                    yield cmd

    def command_sets(self, device_id: str) -> Iterator[CommandSet]:
        """자기 deviceId CommandSet 봉투 스트림(queued|delivered·CS-08). snapshot 순차 방출."""
        with self._open(device_id) as stream:
            for snapshot in self._snapshots(stream):
                for cs in command_sets_from_snapshot(snapshot, device_id):
                    if self._log is not None:
                        self._log.info(
                            "SSE snapshot 에서 CommandSet 봉투 수신",
                            stage=STAGE_PI_RECEIVED,
                            trace_id=cs.trace_id,
                            order_id=cs.source_order_id,
                            device_id=device_id,
                            command_set_id=cs.command_set_id,
                            kind=cs.kind,
                            status=cs.status.wire,
                        )
                    yield cs
