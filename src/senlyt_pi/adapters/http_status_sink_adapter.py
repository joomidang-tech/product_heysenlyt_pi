"""HTTP StatusSinkPort 실어댑터 — 실 HTTP 역보고(스텁 제거).

정본 계약: 05_api §8 / SoT §9 · §10.

스텁 제거(사용자 원칙 2026-07-10): 이전 웨이브의 `raise NotImplementedError` 를 걷어내고
**실 HTTP 클라이언트**(표준 urllib·http_client.request_json)로 서버에 역보고한다.
  - report_status  → PATCH /api/dispenser/orders/{orderId}?mode=  (주문 status 전진·§9-2).
                     phase→WireStatus 매핑(§4-5) 후 {status, requestId, traceId} 전송.
                     **OQ(오프라인 큐) 경유** — 단절 중 적재·재연결 FIFO flush(멱등·§4-6).
  - send_heartbeat → PATCH /api/dispenser/heartbeat  (10s 주기·§9-3·traceId 없음).
  - ship_trace     → POST /api/dispenser/trace  (배치 ≤100 span·§10-4).
                     **TraceSpill(디스크 스풀) 경유** — 전송 실패 배치를 버리지 않고 스풀에
                     보존, 재연결 시 FIFO 업로드(2026-07-19 · "서버가 전부" 원칙: 긴 단절
                     구간의 DEBUG/INFO 도 서버에서 전량 보인다 — journalctl 갈 일 없게).
  - report_command_set_transition → PATCH /api/dispenser/commandsets/{id}  (봉투 전이·05_api §8).
    (Dispatcher.commandset_sink 로 꽂아 delivered→running→done|failed 전이를 서버에 보고.)

서버 base URL 은 하드코딩하지 않고 `ServerConfig` 가 환경별로 결정한 단일 base 를 소비한다.
전송 seam(`request`)은 테스트 주입 — 기본 = http_client.request_json.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
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
from ..pipeline.trace_spill import TraceSpill
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


# 정비 봉투 PK 접두(web `commandSet.ts`: maintenance = `mnt-{uuid}` / manufacture = `{orderId}:{attempt}`).
#   정비는 주문이 없어 주문 status 창구로 보내면 404 → 이 접두로 판별해 주문 역보고 축에서 제외한다.
_MAINTENANCE_ID_PREFIX = "mnt-"


def _order_id_of(command_id: str) -> str:
    """합성키 `{orderId}:{attempt}` → orderId(마지막 콜론 앞). 콜론 없으면 그대로."""
    return command_id.rsplit(":", 1)[0]


def _utc_now_iso() -> str:
    """스풀 합성 WARN 용 UTC ISO8601(ms) — 데몬 now_iso 와 같은 포맷."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


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
        trace_spill: TraceSpill | None = None,
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
        # 관측 로그 디스크 스풀(단절 유실 0) — None 이면 구 best-effort 동작(테스트·하위호환).
        self._spill = trace_spill

    def _config(self) -> ServerConfig:
        return self.server_config or ServerConfig(base_url=self.base_url)

    # ─────────────────────────────────────────────────────────────────────
    # §9-2  status 역보고 — PATCH orders/[id] (OQ 경유·멱등)
    # ─────────────────────────────────────────────────────────────────────

    def report_status(self, report: StatusReport) -> None:
        """주문 status 전진 역보고 — OQ 적재 후 즉시 flush(단절이면 큐에 남아 재연결 시 전송).

        ⚠️ 정비(maintenance·`mnt-`) 봉투는 주문이 없다 → 주문 status 창구(PATCH /orders/{id})로 보내면
          매번 404("서버 거부"). 정비 상태는 commandSet 전이 보고(report_command_set_transition·PATCH
          /commandsets/{id})가 담당하므로 **주문 역보고 축에서 제외**한다(2026-07-18 · 404 노이즈 봉합).
          진행 관측(trace 스팬)은 상위(_publish_progress)가 별도로 남기므로 무영향.
        """
        if report.id.startswith(_MAINTENANCE_ID_PREFIX):
            return
        self._oq.enqueue(report)
        self._oq.reconnect()  # 온라인 가정 후 flush 시도 — 네트워크 실패면 send 가 False→큐 유지.
        sent = self._oq.flush(self._send_status_once)
        if self._log is not None and sent == 0 and not self._oq.is_empty:
            self._log.warn(
                "status 전송 보류(단절·OQ 적재) — 재연결 시 FIFO flush",
                stage=STAGE_STATUS_REPORT,
                trace_id=report.trace_id,
                order_id=_order_id_of(report.id),
                queueDepth=self._oq.depth,  # 서버 allowlist 키명과 일치(구 depth 는 폐기되던 오탈자)
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
    # §9-3  heartbeat — PATCH heartbeat (10s·traceId 없음)
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

    def poll_estop(self, device_id: str) -> "tuple[bool, str | None] | None":
        """긴급정지 신호 조회 — 성공 시 `(active, requestedAt)`, **실패/거부/불확정 시 `None`**.

        ⚠️ **fail-SAFE**(2026-07-19 안전 봉합) — 네트워크 오류·비-2xx(401 토큰만료·403·500
           estop_query_failed)·비-dict 응답을 **(False,None)로 흡수하지 않는다**. 그건 "서버가 estop
           비활성이라고 확인"과 구분 불가라, 폴 1회 실패만으로 데몬이 안전 래치를 풀어버리는 **fail-OPEN**
           이었다(관제가 서버에서 여전히 estop 활성인데 언래치). 대신 **`None`(불확정)** 을 반환해 데몬이
           **래치를 유지**하게 한다(확인불가 = 정지측). 오직 2xx 로 확인된 값만 (active, requestedAt).
           fast-poll(~1s)이라 다음 성공 폴에서 정상 상태로 수렴한다. body = { active, requestedAt }.
        """
        url = self._config().estop_url(device_id)
        headers = bearer_headers(self.bearer_token)
        try:
            status, data = self._request("GET", url, headers=headers, timeout=self.timeout)
        except HttpTransportError:
            return None  # 불확정 — 데몬이 래치 유지(fail-safe·확인불가=정지측).
        if not (200 <= status < 300) or not isinstance(data, dict):
            return None  # 불확정(비-2xx·malformed) — 래치 유지.
        active = bool(data.get("active"))
        requested_at = data.get("requestedAt")
        return (active, requested_at if isinstance(requested_at, str) else None)

    # ─────────────────────────────────────────────────────────────────────
    # §10-4  trace ship — POST trace (best-effort 배치 ≤100)
    # ─────────────────────────────────────────────────────────────────────

    def ship_trace(self, spans: Sequence[TraceSpan]) -> None:
        """trace span 배치 전송 — 100개 초과는 청크로 분할. 예외는 밖으로 안 던진다(§10-6).

        **스풀 유무로 유실 정책이 갈린다** (2026-07-19 · "서버가 전부" 원칙):
          - 스풀 있음(실기기 기본): 전송 실패 배치를 **TraceSpill(디스크)에 보존** 후 반환.
            스풀에 잔여가 있으면 새 스팬보다 **스풀을 먼저 배출**(FIFO·시간순 보존)하고, 아직
            단절이면 새 스팬도 스풀 뒤에 붙인다. 재연결 시 sender 의 주기 flush 가 전량 업로드
            → 긴 단절 구간의 DEBUG/INFO 도 서버에서 다 보인다(유실 0·journalctl 불필요).
          - 스풀 없음(구 어댑터·일부 테스트): 종전 best-effort — 실패 청크만 스킵하고 계속
            (RC2: 한 청크 실패가 나머지 전량을 포기시키던 결함 봉합 유지).
        """
        if not spans:
            return
        # 스풀 잔여 우선 배출 — 순서 보존. 여기선 소량(≤5배치)만 시도해 sender 사이클이 대형
        #   스풀에 통째로 잡히지 않게 한다(잔량은 주기 flush 몫). 여전히 단절이면 새 스팬도
        #   스풀 뒤에 이어붙이고 끝(시간 역전 방지).
        if self._spill is not None and self._spill.depth > 0:
            self.flush_trace_spill(max_batches=5)
            if self._spill.depth > 0:
                self._spill.append([_span_to_json(s) for s in spans])
                return
        chunks = [
            [_span_to_json(s) for s in spans[start : start + TRACE_BATCH_MAX]]
            for start in range(0, len(spans), TRACE_BATCH_MAX)
        ]
        for i, chunk in enumerate(chunks):
            if self._send_trace_batch(chunk):
                continue
            if self._spill is not None:
                # 실패 청크부터 끝까지 스풀(순서 보존) — 재연결 flush 가 이어서 전송.
                self._spill.append([d for c in chunks[i:] for d in c])
                return
            # 스풀 없음 — 실패 청크만 스킵(best-effort·RC2).

    def _send_trace_batch(self, batch: "list[dict[str, Any]]") -> bool:
        """직렬화된 span 배치 1회 POST — 성공 True / 전송오류 False (스풀 drain 의 send seam).

        ⛔ 여기서 StructuredLogger 로 로그를 찍지 말 것 — drain 의 send 콜백으로 불리며,
        로거 sink 는 daemon `_trace_lock` 을 잡는다(락 역전 데드락 재료·TraceSpill docstring).
        """
        try:
            self._request(
                "POST",
                self._config().trace_url,
                body={"logs": batch},
                headers=bearer_headers(self.bearer_token),
                timeout=self.timeout,
            )
            return True
        except HttpTransportError:
            return False

    def spill_traces(self, spans: Sequence[TraceSpan]) -> None:
        """전송 시도 없이 곧장 스풀에 적재 — 데몬 메모리 버퍼 overflow 의 배출구(드롭 대체)."""
        if self._spill is None or not spans:
            return
        self._spill.append([_span_to_json(s) for s in spans])

    def flush_trace_spill(self, *, max_batches: int | None = 50) -> int:
        """스풀 FIFO 업로드(재연결·부팅 시) — 성공 전송 스팬 수 반환.

        `max_batches`(기본 50 = 5k 스팬)로 한 사이클의 배출량을 상한 — 대형 스풀도 sender
        주기 몇 번에 나눠 비운다(사이클 시간 유계·리뷰 P2-5). trim 으로 잘려나간 건수가
        있으면 **합성 WARN 스팬**으로 서버에 표면화한다(조용한 유실 금지 — 상한 20k 를
        넘길 만큼 긴 단절이었다는 사실 자체가 신호).
        """
        if self._spill is None:
            return 0
        sent = self._spill.drain(
            self._send_trace_batch, batch_max=TRACE_BATCH_MAX, max_batches=max_batches
        )
        dropped = self._spill.pop_dropped()
        if dropped > 0:
            ok = self._send_trace_batch(
                [
                    {
                        "ts": _utc_now_iso(),
                        "traceId": "",
                        "spanId": uuid.uuid4().hex[:16],
                        "service": "pi",
                        "event": "pi.log.warn",
                        "level": "WARN",
                        "detail": {
                            "message": f"관측 로그 스풀 상한 초과 — 오래된 {dropped}건 잘림(초장기 단절/디스크 실패)",
                            "stage": "obs",
                        },
                    }
                ]
            )
            if not ok:
                # 합성 WARN 도 전송 실패(단절 지속) — 카운트를 되살려 다음 flush 가 재시도
                #   (리뷰 P2-4: "N건 잘림" 신호 자체가 조용히 소실되면 '조용한 유실 금지' 모순).
                self._spill.restore_dropped(dropped)
        return sent

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
