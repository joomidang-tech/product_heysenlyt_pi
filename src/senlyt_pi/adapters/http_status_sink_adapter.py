"""HTTP StatusSinkPort 실어댑터 — ⛔ TODO 스텁(이후 웨이브).

Dart `lib/adapters/http_status_sink_adapter.dart` 포팅(스텁 그대로).

실 http 클라이언트(PATCH orders/heartbeat·POST trace·Bearer·오프라인 큐 flush)는
이후 웨이브(실 서버 연결). 지금은 포트 계약만. 서버 base URL 은 하드코딩하지 않고
`ServerConfig`(config.server_target)가 환경별로 결정한 단일 base 를 소비한다.
"""

from __future__ import annotations

from typing import Sequence

from ..config.server_target import ServerConfig
from ..core.wire_messages import Heartbeat, StatusReport
from ..ports.status_sink_port import TraceSpan


class HttpStatusSinkAdapter:
    """서버 경유 status/heartbeat/trace 역보고 어댑터 — 미구현 스텁."""

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

    def report_status(self, report: StatusReport) -> None:
        # TODO(wave-next): PATCH /api/dispenser/orders/[id] (Bearer dispenser) + OQ flush.
        raise NotImplementedError("HTTP status sink — 이후 웨이브(실 서버 연결 유보)")

    def send_heartbeat(self, hb: Heartbeat) -> None:
        # TODO(wave-next): PATCH /api/dispenser/heartbeat 30s 주기.
        raise NotImplementedError("HTTP heartbeat — 이후 웨이브(실 서버 연결 유보)")

    def ship_trace(self, spans: Sequence[TraceSpan]) -> None:
        # TODO(wave-next): POST /api/dispenser/trace best-effort 배치(최대 100 span).
        raise NotImplementedError("HTTP trace ship — 이후 웨이브(실 서버 연결 유보)")
