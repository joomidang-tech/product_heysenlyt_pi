"""HTTP StatusSinkPort 실어댑터 — 실 HTTP 역보고(스텁 제거).

정본 계약: 05_api §8 / SoT §9 · §10.

스텁 제거(사용자 원칙 2026-07-10): 이전 웨이브의 `raise NotImplementedError` 를 걷어내고
**실 HTTP 클라이언트**(표준 urllib·http_client.request_json)로 서버에 역보고한다.
  - report_status  → PATCH /api/dispenser/orders/{orderId}?mode=  (주문 status 전진·§9-2).
                     phase→WireStatus 매핑(§4-5) 후 {status, requestId, traceId} 전송.
                     **OQ(오프라인 큐) 경유** — 단절 중 적재·재연결 FIFO flush(멱등·§4-6).
  - send_heartbeat → PATCH /api/dispenser/heartbeat  (30s 주기·§9-3·traceId 없음).
  - ship_trace     → POST /api/dispenser/trace  (best-effort 배치 ≤100 span·§10-4).
  - report_command_set_transition → PATCH /api/dispenser/commandsets/{id}  (봉투 전이·05_api §8).
    (Dispatcher.commandset_sink 로 꽂아 delivered→running→done|failed 전이를 서버에 보고.)

서버 base URL 은 하드코딩하지 않고 `ServerConfig` 가 환경별로 결정한 단일 base 를 소비한다.
전송 seam(`request`)은 테스트 주입 — 기본 = http_client.request_json.
"""

from __future__ import annotations

import uuid
from typing import Any, Callable, Mapping, Sequence

from ..config.server_target import ServerConfig
from ..core.command_set import CommandSet, CommandSetStatus
from ..core.order_status import DispensePhase, phase_to_wire_status
from ..core.pump_guard import StatusErrorCode
from ..core.wire_messages import Heartbeat, StatusReport
from ..obs.log import (
    STAGE_ERROR,
    STAGE_STATUS_REPORT,
    STAGE_TRANSITION_DONE,
    StructuredLogger,
)
from ..pipeline.offline_queue import OfflineQueue
from ..ports.status_sink_port import TraceSpan
from .http_client import (
    DEFAULT_TIMEOUT_SECONDS,
    HttpTransportError,
    bearer_headers,
    request_json,
)

# trace 배치 상한 — 서버 계약 TRACE_BATCH_MAX(§10-4/O-19).
TRACE_BATCH_MAX = 100

# 전송 seam 타입 — (method, url, body, headers, timeout) → (status, body|None).
RequestFn = Callable[..., "tuple[int, Mapping[str, Any] | None]"]


def _order_id_of(command_id: str) -> str:
    """합성키 `{orderId}:{attempt}` → orderId(마지막 콜론 앞). 콜론 없으면 그대로."""
    return command_id.rsplit(":", 1)[0]


def _span_to_json(s: TraceSpan) -> dict[str, Any]:
    """TraceSpan → trace 라우트 와이어(§10-4). 부재 옵셔널은 키 생략."""
    m: dict[str, Any] = {
        "ts": s.ts,
        "traceId": s.trace_id,
        "spanId": s.span_id,
        "service": s.service,  # 서버가 'pi' 강제하지만 계약상 전송.
        "event": s.event,
        "level": s.level,
    }
    if s.parent_span_id is not None:
        m["parentSpanId"] = s.parent_span_id
    if s.order_id is not None:
        m["orderId"] = s.order_id
    if s.device_id is not None:
        m["deviceId"] = s.device_id
    if s.attempt is not None:
        m["attempt"] = s.attempt
    if s.detail is not None:
        m["detail"] = s.detail
    return m


class HttpStatusSinkAdapter:
    """서버 경유 status/heartbeat/trace/봉투전이 역보고 실어댑터."""

    def __init__(
        self,
        *,
        server_config: ServerConfig | None = None,
        base_url: str = "",
        bearer_token: str = "",
        mode: str = "flavor",
        offline_queue: OfflineQueue | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        request: RequestFn = request_json,
        request_id_gen: Callable[[], str] | None = None,
        logger: StructuredLogger | None = None,
    ) -> None:
        # 서버 base 는 ServerConfig(환경별 결정) 우선 — 하드코딩 URL 금지.
        self.server_config = server_config
        self.base_url = server_config.base_url if server_config is not None else base_url
        self.bearer_token = bearer_token
        self.mode = mode
        self.timeout = timeout
        self._request = request
        self._new_request_id = request_id_gen or (lambda: str(uuid.uuid4()))
        self._log = logger
        # OQ — report_status 는 항상 이 큐를 경유(단절 중 적재·재연결 flush·멱등).
        self._oq = offline_queue if offline_queue is not None else OfflineQueue()

    def _config(self) -> ServerConfig:
        return self.server_config or ServerConfig(base_url=self.base_url)

    # ─────────────────────────────────────────────────────────────────────
    # §9-2  status 역보고 — PATCH orders/[id] (OQ 경유·멱등)
    # ─────────────────────────────────────────────────────────────────────

    def report_status(self, report: StatusReport) -> None:
        """주문 status 전진 역보고 — OQ 적재 후 즉시 flush(단절이면 큐에 남아 재연결 시 전송)."""
        self._oq.enqueue(report)
        self._oq.reconnect()  # 온라인 가정 후 flush 시도 — 네트워크 실패면 send 가 False→큐 유지.
        sent = self._oq.flush(self._send_status_once)
        if self._log is not None and sent == 0 and not self._oq.is_empty:
            self._log.warn(
                "status 전송 보류(단절·OQ 적재) — 재연결 시 FIFO flush",
                stage=STAGE_STATUS_REPORT,
                trace_id=report.trace_id,
                order_id=_order_id_of(report.id),
                depth=self._oq.depth,
            )

    def flush_offline_queue(self) -> int:
        """재연결 시 OQ FIFO flush(멱등·at-least-once) — 성공 전송 건수 반환."""
        self._oq.reconnect()
        return self._oq.flush(self._send_status_once)

    def _send_status_once(self, report: StatusReport) -> bool:
        """단건 status PATCH — 성공/영구실패 True(큐 제거), 재시도대상 False(큐 유지).

        분류:
          - 2xx           → True (적용/멱등 noop).
          - 4xx(≠5xx)     → True (409 conflict·422 illegal 등 서버 확정 판정 — 재시도 무의미·제거).
          - 5xx·네트워크   → False (재시도 대상 — 큐에 남겨 다음 flush).
        """
        order_id = _order_id_of(report.id)
        phase = DispensePhase.from_wire(report.phase)
        wire_status = phase_to_wire_status(phase).wire if phase is not None else report.phase
        body: dict[str, Any] = {
            "status": wire_status,
            "requestId": report.request_id,
            "traceId": report.trace_id,
        }
        url = self._config().order_url(order_id, self.mode)
        headers = bearer_headers(self.bearer_token, {"x-trace-id": report.trace_id})
        try:
            status, _ = self._request(
                "PATCH", url, body=body, headers=headers, timeout=self.timeout
            )
        except HttpTransportError:
            return False  # 네트워크 — 큐 유지(재연결 flush).
        if 200 <= status < 300:
            if self._log is not None:
                self._log.info(
                    "status 역보고 성공",
                    stage=STAGE_STATUS_REPORT,
                    trace_id=report.trace_id,
                    order_id=order_id,
                    device_id=self._oq_device_hint(),
                    phase=report.phase,
                    wireStatus=wire_status,
                    httpStatus=status,
                )
            return True
        if 400 <= status < 500:
            # 서버 확정 거부(409/422/404 등) — 재시도로 해소 안 됨. 로컬 제거(로그로 표면화).
            if self._log is not None:
                self._log.warn(
                    "status 역보고 서버 거부(재시도 무의미·큐 제거)",
                    stage=STAGE_STATUS_REPORT,
                    trace_id=report.trace_id,
                    order_id=order_id,
                    phase=report.phase,
                    httpStatus=status,
                )
            return True
        # 5xx — 서버 일시 오류·재시도 대상.
        return False

    def _oq_device_hint(self) -> str | None:
        return self._log.device_id if self._log is not None else None

    # ─────────────────────────────────────────────────────────────────────
    # §9-3  heartbeat — PATCH heartbeat (30s·traceId 없음)
    # ─────────────────────────────────────────────────────────────────────

    def send_heartbeat(self, hb: Heartbeat) -> None:
        """하트비트 전송 — best-effort(실패는 다음 주기 재시도·제조를 막지 않음)."""
        url = self._config().heartbeat_url
        headers = bearer_headers(self.bearer_token)
        try:
            status, _ = self._request(
                "PATCH", url, body=hb.to_json(), headers=headers, timeout=self.timeout
            )
        except HttpTransportError:
            if self._log is not None:
                self._log.warn(
                    "heartbeat 전송 실패(단절·다음 주기 재시도)",
                    stage=STAGE_ERROR,
                    device_id=hb.device_id,
                    queueDepth=hb.queue_depth,
                )
            return
        if self._log is not None and not (200 <= status < 300):
            self._log.warn(
                "heartbeat 서버 거부",
                stage=STAGE_ERROR,
                device_id=hb.device_id,
                httpStatus=status,
            )

    # ─────────────────────────────────────────────────────────────────────
    # §9-4  긴급정지 신호 fast-poll — GET estop
    # ─────────────────────────────────────────────────────────────────────

    def poll_estop(self, device_id: str) -> "tuple[bool, str | None]":
        """긴급정지 신호 조회 — `(active, requestedAt)`. 실패/거부는 `(False, None)` 안전 폴백.

        ⚠️ **fast-poll**(데몬 estop 감시 스레드가 ~1s 주기 호출). best-effort — 네트워크 오류/비-2xx
           는 (False, None)로 흡수해 다음 폴에서 재시도한다(서버 불통이면 관제도 신호를 못 넣는다).
           서버 200 응답 body = { active: bool, requestedAt: string|null }.
        """
        url = self._config().estop_url(device_id)
        headers = bearer_headers(self.bearer_token)
        try:
            status, data = self._request("GET", url, headers=headers, timeout=self.timeout)
        except HttpTransportError:
            return (False, None)
        if not (200 <= status < 300) or not isinstance(data, dict):
            return (False, None)
        active = bool(data.get("active"))
        requested_at = data.get("requestedAt")
        return (active, requested_at if isinstance(requested_at, str) else None)

    # ─────────────────────────────────────────────────────────────────────
    # §10-4  trace ship — POST trace (best-effort 배치 ≤100)
    # ─────────────────────────────────────────────────────────────────────

    def ship_trace(self, spans: Sequence[TraceSpan]) -> None:
        """trace span 배치 전송 — best-effort. 100개 초과는 청크로 분할. 실패는 삼킨다(§10-6)."""
        if not spans:
            return
        url = self._config().trace_url
        headers = bearer_headers(self.bearer_token)
        for start in range(0, len(spans), TRACE_BATCH_MAX):
            chunk = spans[start : start + TRACE_BATCH_MAX]
            body = {"logs": [_span_to_json(s) for s in chunk]}
            try:
                self._request(
                    "POST", url, body=body, headers=headers, timeout=self.timeout
                )
            except HttpTransportError:
                # best-effort — trace 유실은 제조를 막지 않는다(§10-6 (1)). 조용히 포기.
                return

    # ─────────────────────────────────────────────────────────────────────
    # 05_api §8  CommandSet 봉투 전이 보고 — PATCH commandsets/[id]
    # ─────────────────────────────────────────────────────────────────────

    def report_command_set_transition(
        self,
        cs: CommandSet,
        status: CommandSetStatus,
        error_code: StatusErrorCode | None,
    ) -> None:
        """봉투 전이 보고(delivered→running→done|failed) — Dispatcher.commandset_sink 시그니처.

        best-effort: 예외는 삼킨다(관측이 제조를 막지 않는다·§10-6). requestId dedup(at-least-once).
        서버 게이트가 역행 422·동일값 noop 를 흡수하므로 늦은 재보고도 무해.
        """
        url = self._config().commandset_url(cs.command_set_id)
        body: dict[str, Any] = {
            "status": status.wire,
            "requestId": self._new_request_id(),
        }
        if error_code is not None:
            body["errorCode"] = error_code.wire
        headers = bearer_headers(self.bearer_token)
        if cs.trace_id:
            headers["x-trace-id"] = cs.trace_id
        try:
            http_status, _ = self._request(
                "PATCH", url, body=body, headers=headers, timeout=self.timeout
            )
        except HttpTransportError:
            if self._log is not None:
                self._log.warn(
                    "봉투 전이 보고 실패(단절·무해 재보고 가능)",
                    stage=STAGE_ERROR,
                    trace_id=cs.trace_id,
                    order_id=cs.source_order_id,
                    command_set_id=cs.command_set_id,
                    to=status.wire,
                )
            return
        if self._log is not None:
            self._log.info(
                "봉투 전이 보고",
                stage=STAGE_TRANSITION_DONE,
                trace_id=cs.trace_id,
                order_id=cs.source_order_id,
                device_id=cs.device_id,
                command_set_id=cs.command_set_id,
                to=status.wire,
                httpStatus=http_status,
            )
