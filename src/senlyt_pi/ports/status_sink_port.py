"""StatusSinkPort — status 역보고 + heartbeat + trace 전송(인터페이스만) — SoT §9 / §10.

Dart `lib/ports/status_sink_port.dart` 포팅. pi 는 status 전진 write 주체(§4-5·pi 단독)이나
**직결 0** — 유일 경로 = PATCH /api/dispenser/orders/[id] (Bearer dispenser).
heartbeat·trace 도 서버 경유. 실 http 클라이언트·오프라인 큐(OQ) flush 는 이후 웨이브(TODO).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, Sequence

from ..core.wire_messages import Heartbeat, StatusReport


@dataclass(frozen=True, slots=True)
class TraceSpan:
    """trace span 배치 항목 — SoT §10-4 (detail allowlist 는 서버가 2차 sanitize)."""

    ts: str  # ISO8601·밀리초 Z
    trace_id: str
    span_id: str  # 16-hex
    service: str  # pi 전송분은 서버가 'pi' 강제(§10-4)
    event: str  # 점표기(§10-2)
    level: str  # DEBUG|INFO|WARN|ERROR
    parent_span_id: str | None = None  # 16-hex | None
    order_id: str | None = None
    device_id: str | None = None
    attempt: int | None = None
    detail: dict[str, Any] | None = None  # 비식별만(§10-3)


class StatusSinkPort(Protocol):
    """status/heartbeat/trace 역보고 싱크(서버 경유)."""

    def report_status(self, report: StatusReport) -> None:
        """PATCH /api/dispenser/orders/[id] — status 전진 역보고(§9-2).
        OQ flush at-least-once → requestId 로 서버 dedup(§4-6)."""
        ...

    def send_heartbeat(self, hb: Heartbeat) -> None:
        """PATCH /api/dispenser/heartbeat — 30s 주기(§9-3)."""
        ...

    def ship_trace(self, spans: Sequence[TraceSpan]) -> None:
        """POST /api/dispenser/trace — best-effort 배치(최대 100 span·§10-4/O-19)."""
        ...
