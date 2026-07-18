"""HTTP status sink 실어댑터 테스트 — 실 HTTP 역보고(스텁 제거).

로컬 fake 서버로 PATCH orders/[id](phase→WireStatus·requestId·traceId)·OQ flush(단절→재연결)·
heartbeat·POST trace 배치(≤100·best-effort)·PATCH commandsets/[id] 전이 보고를 실 소켓 검증.
"""

from __future__ import annotations

from senlyt_pi.adapters.http_client import HttpTransportError, request_json
from senlyt_pi.adapters.http_status_sink_adapter import (
    TRACE_BATCH_MAX,
    HttpStatusSinkAdapter,
)
from senlyt_pi.core.command_set import CommandSet, CommandSetStatus
from senlyt_pi.core.pump_guard import StatusErrorCode
from senlyt_pi.core.wire_messages import Heartbeat, RecipeStep, StatusReport
from senlyt_pi.pipeline.offline_queue import OfflineQueue
from senlyt_pi.ports.status_sink_port import TraceSpan
from support_http import FakeHttpServer


def _raising_request(*_a, **_k):
    """전송오류(네트워크 단절) seam — 실 소켓 타임아웃 없이 결정적으로 유발."""
    raise HttpTransportError("연결 거부")


def _report(phase: str, *, order_id: str = "o1", attempt: int = 1, step_k: int = 0) -> StatusReport:
    return StatusReport(
        id=f"{order_id}:{attempt}",
        phase=phase,
        step_k=step_k,
        step_n=2,
        error_code=None,
        request_id=f"req-{phase}-{step_k}",
        trace_id="trace-o1",
        updated_at="2026-07-10T00:00:00.000Z",
    )


class TestReportStatus:
    def test_patch_orders_shape_and_phase_mapping(self) -> None:
        with FakeHttpServer() as srv:
            srv.set_handler(lambda req: {"status": 200, "json": {"applied": True}})
            sink = HttpStatusSinkAdapter(
                base_url=srv.base_url, bearer_token="tok-d", mode="flavor"
            )
            sink.report_status(_report("PROGRESS", step_k=1))
            rec = srv.requests[-1]
            assert rec.method == "PATCH"
            assert rec.path == "/api/dispenser/orders/o1"  # 합성키에서 orderId 추출.
            assert "mode=flavor" in rec.query
            assert rec.header("Authorization") == "Bearer tok-d"
            assert rec.header("x-trace-id") == "trace-o1"
            body = rec.json()
            assert body["status"] == "PROCESSING"  # PROGRESS → PROCESSING(§4-5).
            assert body["requestId"] == "req-PROGRESS-1"
            assert body["traceId"] == "trace-o1"

    def test_completed_maps_to_completed(self) -> None:
        with FakeHttpServer() as srv:
            srv.set_handler(lambda req: {"status": 200, "json": {"applied": True}})
            sink = HttpStatusSinkAdapter(base_url=srv.base_url, bearer_token="t")
            sink.report_status(_report("COMPLETED", step_k=2))
            assert srv.requests[-1].json()["status"] == "COMPLETED"

    def test_maintenance_id_skips_order_status_no_404_noise(self) -> None:
        """정비 봉투(mnt-)는 주문이 없어 주문 status 창구로 안 보낸다 — PATCH 자체가 안 나감(404 노이즈 봉합).

        정비 상태는 commandSet 전이 보고가 담당. 여기선 주문 역보고 축에서 제외돼 죽은 PATCH·404 WARN 이 사라진다.
        """
        with FakeHttpServer() as srv:
            srv.set_handler(lambda req: {"status": 404, "json": {"code": "order_not_found"}})
            sink = HttpStatusSinkAdapter(base_url=srv.base_url, bearer_token="t", mode="flavor")
            mnt = StatusReport(
                id="mnt-abc123",
                phase="PROGRESS",
                step_k=1,
                step_n=2,
                error_code=None,
                request_id="req-mnt",
                trace_id="trace-mnt",
                updated_at="2026-07-18T00:00:00.000Z",
            )
            sink.report_status(mnt)
            assert srv.requests == [], "정비 id 는 주문 status PATCH 를 만들지 않아야(404 노이즈 없음)"

    def test_offline_queue_flush_on_reconnect(self) -> None:
        """단절(전송오류) 중 적재 → 재연결 시 실 서버로 FIFO flush(멱등)."""
        oq = OfflineQueue()
        online = {"up": False}

        def flaky_request(method, url, **kw):
            if not online["up"]:
                raise HttpTransportError("단절")
            return request_json(method, url, **kw)

        with FakeHttpServer() as srv:
            srv.set_handler(lambda req: {"status": 200, "json": {"applied": True}})
            sink = HttpStatusSinkAdapter(
                base_url=srv.base_url,
                bearer_token="t",
                offline_queue=oq,
                request=flaky_request,
            )
            # 단절 중 — 전송 실패 → OQ 에 적재.
            sink.report_status(_report("ACCEPTED", step_k=0))
            assert oq.depth == 1
            assert srv.requests == []  # 실제 왕복 없음(seam 이 raise).

            # 재연결 → 실 서버로 FIFO flush.
            online["up"] = True
            sent = sink.flush_offline_queue()
            assert sent == 1
            assert oq.is_empty
            assert srv.requests[-1].json()["status"] == "PROCESSING"  # ACCEPTED→PROCESSING.

    def test_server_reject_4xx_drops_from_queue(self) -> None:
        """409/422 등 서버 확정 거부 → 재시도 무의미·큐에서 제거(무한 재시도 방지)."""
        oq = OfflineQueue()
        with FakeHttpServer() as srv:
            srv.set_handler(lambda req: {"status": 422, "json": {"code": "illegal"}})
            sink = HttpStatusSinkAdapter(
                base_url=srv.base_url, bearer_token="t", offline_queue=oq
            )
            sink.report_status(_report("PROGRESS"))
            assert oq.is_empty  # 4xx → 제거.

    def test_5xx_keeps_in_queue(self) -> None:
        oq = OfflineQueue()
        with FakeHttpServer() as srv:
            srv.set_handler(lambda req: {"status": 500, "json": {"code": "update_failed"}})
            sink = HttpStatusSinkAdapter(
                base_url=srv.base_url, bearer_token="t", offline_queue=oq
            )
            sink.report_status(_report("PROGRESS"))
            assert oq.depth == 1  # 5xx → 재시도 대상·유지.


class TestHeartbeat:
    def test_patch_heartbeat(self) -> None:
        with FakeHttpServer() as srv:
            srv.set_handler(lambda req: {"status": 200, "json": {"ok": True}})
            sink = HttpStatusSinkAdapter(base_url=srv.base_url, bearer_token="tok-d")
            sink.send_heartbeat(Heartbeat(device_id="dev-A", queue_depth=0, engine="sy01b"))
            rec = srv.requests[-1]
            assert rec.method == "PATCH"
            assert rec.path == "/api/dispenser/heartbeat"
            body = rec.json()
            assert body == {"deviceId": "dev-A", "queueDepth": 0, "engine": "sy01b"}

    def test_heartbeat_network_failure_is_swallowed(self) -> None:
        sink = HttpStatusSinkAdapter(base_url="http://web:3000", request=_raising_request)
        # 예외 없이 반환(best-effort).
        sink.send_heartbeat(Heartbeat(device_id="dev-A", queue_depth=0))


class TestShipTrace:
    def _span(self, i: int) -> TraceSpan:
        return TraceSpan(
            ts="2026-07-10T00:00:00.000Z",
            trace_id="trace-o1",
            span_id=f"{i:016x}",
            service="pi",
            event="pi.step",
            level="INFO",
            order_id="o1",
            device_id="dev-A",
            attempt=1,
        )

    def test_post_trace_batch_shape(self) -> None:
        with FakeHttpServer() as srv:
            srv.set_handler(lambda req: {"status": 200, "json": {"accepted": 2, "deduped": 0}})
            sink = HttpStatusSinkAdapter(base_url=srv.base_url, bearer_token="t")
            sink.ship_trace([self._span(0), self._span(1)])
            rec = srv.requests[-1]
            assert rec.path == "/api/dispenser/trace"
            body = rec.json()
            assert len(body["logs"]) == 2
            assert body["logs"][0]["traceId"] == "trace-o1"
            assert body["logs"][0]["service"] == "pi"
            assert body["logs"][0]["orderId"] == "o1"

    def test_batch_split_over_100(self) -> None:
        with FakeHttpServer() as srv:
            srv.set_handler(lambda req: {"status": 200, "json": {"accepted": 1}})
            sink = HttpStatusSinkAdapter(base_url=srv.base_url, bearer_token="t")
            spans = [self._span(i) for i in range(TRACE_BATCH_MAX + 5)]
            sink.ship_trace(spans)
            # 2 왕복(100 + 5).
            assert len(srv.requests) == 2
            assert len(srv.requests[0].json()["logs"]) == TRACE_BATCH_MAX
            assert len(srv.requests[1].json()["logs"]) == 5

    def test_empty_is_noop(self) -> None:
        calls = []
        sink = HttpStatusSinkAdapter(
            base_url="http://web:3000", request=lambda *a, **k: calls.append(1) or (200, None)
        )
        sink.ship_trace([])  # 왕복 없음·예외 없음.
        assert calls == []

    def test_network_failure_swallowed(self) -> None:
        sink = HttpStatusSinkAdapter(base_url="http://web:3000", request=_raising_request)
        sink.ship_trace([self._span(0)])  # best-effort — 예외 없음.


class TestCommandSetTransition:
    def _cs(self) -> CommandSet:
        return CommandSet(
            command_set_id="o1:1",
            device_id="dev-A",
            kind="manufacture",
            steps=(RecipeStep(idx=0, pump_addr=1, flavor="cola", volume=100),),
            status=CommandSetStatus.QUEUED,
            created_at="2026-07-10T00:00:00.000Z",
            created_by="server",
            source_order_id="o1",
            attempt=1,
            trace_id="trace-o1",
        )

    def test_patch_commandset_transition(self) -> None:
        with FakeHttpServer() as srv:
            srv.set_handler(lambda req: {"status": 200, "json": {"applied": True}})
            sink = HttpStatusSinkAdapter(
                base_url=srv.base_url,
                bearer_token="tok-d",
                request_id_gen=lambda: "req-fixed",
            )
            sink.report_command_set_transition(self._cs(), CommandSetStatus.RUNNING, None)
            rec = srv.requests[-1]
            assert rec.method == "PATCH"
            assert rec.path == "/api/dispenser/commandsets/o1%3A1"  # 콜론 인코딩.
            assert rec.header("x-trace-id") == "trace-o1"
            body = rec.json()
            assert body == {"status": "running", "requestId": "req-fixed"}

    def test_failed_includes_error_code(self) -> None:
        with FakeHttpServer() as srv:
            srv.set_handler(lambda req: {"status": 200, "json": {"applied": True}})
            sink = HttpStatusSinkAdapter(base_url=srv.base_url, request_id_gen=lambda: "r")
            sink.report_command_set_transition(
                self._cs(), CommandSetStatus.FAILED, StatusErrorCode.PARTIAL_DISPENSE
            )
            assert srv.requests[-1].json()["errorCode"] == "PARTIAL_DISPENSE"

    def test_transition_network_failure_swallowed(self) -> None:
        sink = HttpStatusSinkAdapter(base_url="http://web:3000", request=_raising_request)
        # best-effort — 관측이 제조를 막지 않는다.
        sink.report_command_set_transition(self._cs(), CommandSetStatus.DELIVERED, None)


class TestPollEstop:
    """긴급정지 신호 fast-poll(§9-4) — GET estop → (active, requestedAt)."""

    def test_active_signal(self) -> None:
        with FakeHttpServer() as srv:
            srv.set_handler(
                lambda req: {
                    "status": 200,
                    "json": {"active": True, "requestedAt": "2026-07-18T00:00:00.000Z"},
                }
            )
            sink = HttpStatusSinkAdapter(base_url=srv.base_url, bearer_token="t")
            active, at = sink.poll_estop("dev-A")
            assert active is True
            assert at == "2026-07-18T00:00:00.000Z"
            rec = srv.requests[-1]
            assert rec.method == "GET"
            assert rec.path == "/api/dispenser/estop"
            assert "deviceId=dev-A" in rec.query

    def test_inactive_signal(self) -> None:
        with FakeHttpServer() as srv:
            srv.set_handler(lambda req: {"status": 200, "json": {"active": False, "requestedAt": None}})
            sink = HttpStatusSinkAdapter(base_url=srv.base_url, bearer_token="t")
            assert sink.poll_estop("dev-A") == (False, None)

    def test_server_error_safe_fallback(self) -> None:
        with FakeHttpServer() as srv:
            srv.set_handler(lambda req: {"status": 500, "json": {"code": "estop_query_failed"}})
            sink = HttpStatusSinkAdapter(base_url=srv.base_url, bearer_token="t")
            assert sink.poll_estop("dev-A") == (False, None)

    def test_network_failure_safe_fallback(self) -> None:
        sink = HttpStatusSinkAdapter(base_url="http://web:3000", request=_raising_request)
        assert sink.poll_estop("dev-A") == (False, None)
