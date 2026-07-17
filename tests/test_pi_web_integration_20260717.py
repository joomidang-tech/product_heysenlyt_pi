"""pi↔web 로컬 통합 테스트(풀 라운드트립) — 실 HTTP 어댑터 + 단일 FakeHttpServer.

커버리지 감사가 지목한 **핵심 갭**을 닫는다: 각 어댑터는 실 소켓 단위테스트가 있지만
`SenlytDaemon.boot()` 이 **실 SSE 어댑터 + 실 HTTP status 어댑터**로 **한 개의** 로컬 웹 서버에
대해 "부팅→등록→SSE 명령 수신→FakeEngine 제조→상태 역보고"를 한 바퀴 도는 통합 테스트가 없었다.

이 파일은 tests/support_http.py 의 FakeHttpServer(실 127.0.0.1 ThreadingHTTPServer)로
**한 핸들러가 register + SSE(commandSets) + status PATCH + heartbeat + trace + 봉투전이**를
경로(urlparse path)로 분기해 모두 처리하게 하고, 그 URL 을 `SENLYT_SERVER_BASE_URL` 로 준 뒤
`bootstrap.build_components` + `DaemonDeps` 로 **실 어댑터를 조립**해 데몬을 부팅한다.
유일한 mock 은 물리 엔진(FakeEnginePort) — 등록/SSE/역보고는 전부 실 HTTP 왕복이다.

방식: boot() 은 stop 까지 블록하므로 별도 스레드에서 돌리고, 조건 충족까지 폴링 대기(타임아웃
가드)한 뒤 request_stop()+join 으로 정리한다(테스트 hang 금지).
"""

from __future__ import annotations

import json
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Iterator

from senlyt_pi.adapters.fake_engine_adapter import FakeEnginePort
from senlyt_pi.app.bootstrap import (
    build_components,
    build_ledger,
    build_resolver,
)
from senlyt_pi.app.daemon import DaemonDeps, SenlytDaemon
from support_http import FakeHttpServer, RecordedRequest

# ─────────────────────────────────────────────────────────────────────────────
# 단일 웹 핸들러 — 한 서버가 pi↔web 6개 엔드포인트를 path 로 분기 처리
# ─────────────────────────────────────────────────────────────────────────────

STREAM_PATH = "/api/dispenser/orders/stream"
REGISTER_PATH = "/api/dispensers/register"
HEARTBEAT_PATH = "/api/dispenser/heartbeat"
TRACE_PATH = "/api/dispenser/trace"
ORDERS_PREFIX = "/api/dispenser/orders/"
COMMANDSETS_PREFIX = "/api/dispenser/commandsets/"


def _cs_wire(order_id: str, device_id: str, attempt: int = 1) -> dict:
    """SSE snapshot 에 실을 manufacture CommandSet 봉투(queued) 와이어(계약 §8-2)."""
    return {
        "commandSetId": f"{order_id}:{attempt}",
        "deviceId": device_id,
        "kind": "manufacture",
        "steps": [{"idx": 0, "pumpAddr": 1, "flavor": "cola", "volume": 100}],
        "status": "queued",
        "createdAt": "2026-07-10T00:00:00.000Z",
        "createdBy": "server",
        "sourceOrderId": order_id,
        "attempt": attempt,
        "traceId": f"trace-{order_id}",
    }


# 실 web 주문 전이표 근사(orderStatus.ts·§2-3): 단조 전진만·종결 불변.
_ORDER_RANK = {"PENDING": 0, "PROCESSING": 1, "COMPLETED": 2, "FAILED": 2}
_TERMINAL = {"COMPLETED", "FAILED"}


def _can_transition(cur: str, new: str | None) -> bool:
    """cur→new 가 실 web 전이 게이트를 통과하나 — 종결 불변·후진 금지·전진/동일 허용."""
    if new not in _ORDER_RANK:
        return False
    if cur in _TERMINAL:
        return False
    return _ORDER_RANK[new] >= _ORDER_RANK[cur]


class WebHandler:
    """FakeHttpServer 용 라우팅 핸들러 — 실 web 게이트에 **충실**하게(스레드 안전).

    - register(POST)     → {deviceId(echo), dispenserToken, exp, slug}.
    - orders/stream(GET) → SSE snapshot. commandSets 는 **첫 스트림 요청에만** 실어 보냄.
    - orders/{id}(PATCH) → status 역보고. **실 web 처럼**: 주문 미존재 → 404, 후진 전이 →
                           422, status_online=False → 503(단절), 정상 단조전이 → 200 + 적용.
                           (order-web 이 만든 PENDING 주문이 원장에 있어야 수락 — commandSet 의
                           sourceOrderId 를 PENDING 으로 시드해 실 주문 수명주기를 모델.)
    - heartbeat/trace/commandsets → 200(수신 기록만).
    """

    def __init__(
        self,
        *,
        command_sets: list[dict] | None = None,
        dispenser_token: str = "disp-token-1",
        exp: int = 9_999_999_999,
        status_online: bool = True,
    ) -> None:
        self.command_sets = command_sets or []
        self.dispenser_token = dispenser_token
        self.exp = exp
        self.status_online = status_online
        self._lock = threading.Lock()
        self.stream_count = 0
        # 서버측 관측 — 실제로 200 으로 적용(수락)된 status body 들.
        self.applied_status: list[dict] = []
        # 거절된 status(404 미존재·422 후진) — 가짜 계약 방지 관측.
        self.rejected_status: list[dict] = []
        # 주문 원장 — order-web 이 생성한 PENDING 주문(commandSet 의 sourceOrderId). 실 web 은
        # 주문이 존재하고 단조 전이여야 status PATCH 를 수락한다(미존재 404·후진 422).
        self.orders: dict[str, str] = {}
        for cs in self.command_sets:
            oid = cs.get("sourceOrderId")
            if oid:
                self.orders[str(oid)] = "PENDING"

    def __call__(self, req: RecordedRequest) -> dict[str, Any]:
        path = req.path

        if path == REGISTER_PATH:
            return {
                "status": 200,
                "json": {
                    "deviceId": "server-echo-ignored",
                    "dispenserToken": self.dispenser_token,
                    "exp": self.exp,
                    "slug": "cafe-x",
                },
            }

        if path == STREAM_PATH:
            with self._lock:
                n = self.stream_count
                self.stream_count += 1
            css = self.command_sets if n == 0 else []
            snapshot = {"orders": [], "commands": [], "commandSets": css}
            return {"sse": [("snapshot", json.dumps(snapshot))]}

        if path == HEARTBEAT_PATH:
            return {"status": 200, "json": {"ok": True}}

        if path == TRACE_PATH:
            return {"status": 200, "json": {"accepted": 1, "deduped": 0}}

        if path.startswith(COMMANDSETS_PREFIX):
            return {"status": 200, "json": {"applied": True}}

        if path.startswith(ORDERS_PREFIX):  # STREAM 은 위에서 이미 처리됨.
            order_id = path[len(ORDERS_PREFIX):].split("?", 1)[0]
            body = req.json() or {}
            new_status = body.get("status")
            with self._lock:
                if not self.status_online:
                    return {"status": 503, "json": {"error": "update_failed"}}  # 단절 시뮬.
                cur = self.orders.get(order_id)
                if cur is None:
                    # 실 web: 주문 미존재(order-web 이 안 만듦) → 404. pi 는 4xx 를 permanent 로
                    # 처리해 OQ 에서 drop → 실서비스면 이 주문은 영원히 안 올라간다(가짜 계약 방지).
                    self.rejected_status.append({"order": order_id, "code": 404, "to": new_status})
                    return {"status": 404, "json": {"error": "order_not_found"}}
                if not _can_transition(cur, new_status):
                    self.rejected_status.append(
                        {"order": order_id, "from": cur, "to": new_status, "code": 422}
                    )
                    return {"status": 422, "json": {"error": "illegal_transition"}}
                self.orders[order_id] = new_status
                self.applied_status.append(body)
            return {"status": 200, "json": {"applied": True}}

        return {"status": 404, "json": {"error": "not_found"}}


# ─────────────────────────────────────────────────────────────────────────────
# 하네스 — 실 서버 + 실 어댑터 조립 + boot 스레드(타임아웃 가드 teardown)
# ─────────────────────────────────────────────────────────────────────────────


def _make_env(srv_base: str, tmp_path, *, hardware_id: str, mode: str) -> dict[str, str]:
    return {
        "SENLYT_SERVER_BASE_URL": srv_base,  # ServerConfig 탈출구(프리뷰/prod 슬러그 대신 실서버).
        "SENLYT_HARDWARE_ID": hardware_id,  # read_hardware_id → deviceId(수집 시리얼).
        "DISPENSER_PROVISION_KEY": "prov",  # register Authorization: Bearer.
        "SENLYT_ENGINE": "fake",  # 유일 mock — FakeEnginePort.
        "SENLYT_VALVE": "fake",  # FakeValveAdapter(실 GPIO 없이).
        "SENLYT_MODE": mode,  # 구독/역보고 mode 쿼리.
        "PUMP_ADDRESSES": f"{mode}:1,2",  # RR pump_map — 없으면 모든 스텝 drop(토출 0).
        "SENLYT_IDENTITY_PATH": str(tmp_path / "identity.json"),
        "SENLYT_LEDGER_PATH": str(tmp_path / "ledger.log"),
    }


@contextmanager
def running_daemon(
    tmp_path,
    handler: WebHandler,
    *,
    hardware_id: str = "hw-e2e",
    mode: str = "flavor",
    poll_interval_s: float = 0.02,
    heartbeat_interval_s: float = 0.0,
) -> Iterator[tuple[FakeHttpServer, SenlytDaemon, Any]]:
    """실 웹 서버 + 실 어댑터 데몬을 부팅하고, 종료 시 stop+join 으로 정리."""
    with FakeHttpServer() as srv:
        srv.set_handler(handler)
        env = _make_env(srv.base_url, tmp_path, hardware_id=hardware_id, mode=mode)

        # senlytd._run 과 동일한 결선 — 실 등록(HTTP) + 실 SSE/status 어댑터 + 파일 ledger.
        ledger = build_ledger(env)
        components = build_components(env, ledger=ledger)  # register=True(기본) — 실 등록.
        deps = DaemonDeps(
            device_id=components.device_id,
            command_source=components.command_source,
            status_sink=components.status_sink,
            engine=components.engine,
            valve=components.valve,
            ledger=ledger,
            resolver=build_resolver(env),
            commandset_source=components.command_source,  # 동일 SSE 어댑터가 두 축 제공.
            logger=components.logger,
            poll_interval_s=poll_interval_s,
            heartbeat_interval_s=heartbeat_interval_s,
        )
        daemon = SenlytDaemon(deps)

        t = threading.Thread(target=daemon.boot, name="e2e-boot", daemon=True)
        t.start()
        try:
            yield srv, daemon, components
        finally:
            daemon.request_stop()
            t.join(timeout=5.0)
            assert not t.is_alive(), "request_stop 후 boot 루프가 종료돼야(hang 금지)"


def _wait_until(pred: Callable[[], bool], *, timeout: float = 5.0, interval: float = 0.01) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


def _reqs(srv: FakeHttpServer, pred: Callable[[RecordedRequest], bool]) -> list[RecordedRequest]:
    return [r for r in list(srv.requests) if pred(r)]


def _is_status_patch(r: RecordedRequest) -> bool:
    return (
        r.method == "PATCH"
        and r.path.startswith(ORDERS_PREFIX)
        and r.path != STREAM_PATH
    )


# ─────────────────────────────────────────────────────────────────────────────
# 시나리오 1 — 등록: register 수신 + deviceId=시리얼 + 토큰 핸드오프(register→SSE/status Bearer)
# ─────────────────────────────────────────────────────────────────────────────


def test_registration_and_token_handoff_over_real_http(tmp_path) -> None:
    """부팅 시 실 register 왕복 + 발급 dispenserToken 이 후속 SSE/status Bearer 로 이어진다.

    갭 봉합: register 는 인증 헤더 없음(TOFU·공유키 제거), 이후는 dispenserToken 으로 Bearer 가
    생기는 지점을 실 소켓으로 통합 검증(종전엔 어댑터 속성만 확인).
    """
    handler = WebHandler(dispenser_token="disp-token-1")
    with running_daemon(tmp_path, handler, hardware_id="hw-e2e") as (srv, daemon, comp):
        # 등록 요청이 도착할 때까지 대기.
        assert _wait_until(
            lambda: bool(_reqs(srv, lambda r: r.path == REGISTER_PATH))
        ), "register 요청이 서버에 도착해야"
        # 후속 SSE 구독(stream)까지 최소 1회 돌 때까지 대기(토큰 핸드오프 관측용).
        assert _wait_until(
            lambda: bool(_reqs(srv, lambda r: r.path == STREAM_PATH))
        ), "SSE stream 구독 요청이 도착해야"

        reg = _reqs(srv, lambda r: r.path == REGISTER_PATH)[0]
        # deviceId = 수집 시리얼(서버 echo 아님).
        assert reg.method == "POST"
        assert reg.json()["deviceId"] == "hw-e2e"
        # register 는 인증 헤더 없음(TOFU · 공유키 제거).
        assert reg.header("Authorization") is None

        # 데몬이 응답 토큰을 보관.
        assert comp.identity.dispenser_token == "disp-token-1"
        assert comp.device_id == "hw-e2e"

        # 토큰 핸드오프 — 이후 SSE 구독은 dispenserToken 으로 Bearer(provision key 아님).
        stream = _reqs(srv, lambda r: r.path == STREAM_PATH)[0]
        assert stream.header("Authorization") == "Bearer disp-token-1"


# ─────────────────────────────────────────────────────────────────────────────
# 시나리오 2 — 명령 라운드트립: SSE commandSet → FakeEngine 제조 → status COMPLETED 역보고
# ─────────────────────────────────────────────────────────────────────────────


def test_commandset_roundtrip_to_completed(tmp_path) -> None:
    """서버 SSE 로 commandSet 1건 → 데몬 소비 → FakeEngine dispense → 서버가 COMPLETED PATCH 수신."""
    handler = WebHandler(command_sets=[_cs_wire("o1", "hw-e2e")])
    with running_daemon(tmp_path, handler, hardware_id="hw-e2e") as (srv, daemon, comp):
        engine = comp.engine
        assert isinstance(engine, FakeEnginePort)

        # 서버가 COMPLETED status PATCH 를 수락할 때까지 대기(풀 라운드트립 완료 신호).
        assert _wait_until(
            lambda: any(b.get("status") == "COMPLETED" for b in list(handler.applied_status))
        ), "서버가 주문 COMPLETED status PATCH 를 수신해야"

        # 물리 토출 발생(1스텝 레시피 = dispense 1회).
        assert engine.dispense_count >= 1

        # 주문축 — PROCESSING(진행) → COMPLETED(완료) 가 서버에 도달.
        applied = [b.get("status") for b in list(handler.applied_status)]
        assert "PROCESSING" in applied, "진행 중 PROCESSING 역보고가 서버에 도달해야"
        assert applied[-1] == "COMPLETED", "마지막 status 는 COMPLETED"

        # status PATCH 가 dispenserToken Bearer + orderId path(합성키에서 추출)로 갔는지.
        status_reqs = _reqs(srv, _is_status_patch)
        assert status_reqs, "status PATCH 요청이 있어야"
        assert status_reqs[0].path == "/api/dispenser/orders/o1"  # 합성키 o1:1 → orderId o1.
        assert status_reqs[0].header("Authorization") == "Bearer disp-token-1"
        assert "mode=flavor" in status_reqs[0].query

        # 봉투축 — DELIVERED → RUNNING → DONE 전이가 서버에 순서대로 도달.
        cs_reqs = _reqs(srv, lambda r: r.path.startswith(COMMANDSETS_PREFIX))
        cs_statuses = [r.json().get("status") for r in cs_reqs]
        assert cs_statuses == ["delivered", "running", "done"], cs_statuses


# ─────────────────────────────────────────────────────────────────────────────
# 시나리오 3 — heartbeat: 데몬 heartbeat PATCH 가 실 어댑터로 서버에 도달(queueDepth 파생)
# ─────────────────────────────────────────────────────────────────────────────


def test_heartbeat_reaches_server(tmp_path) -> None:
    """짧은 주기 heartbeat 스레드가 실 HttpStatusSinkAdapter 로 /heartbeat 에 PATCH 를 보낸다."""
    handler = WebHandler()
    with running_daemon(
        tmp_path, handler, hardware_id="hw-e2e", heartbeat_interval_s=0.05
    ) as (srv, daemon, comp):
        assert _wait_until(
            lambda: bool(_reqs(srv, lambda r: r.path == HEARTBEAT_PATH))
        ), "heartbeat PATCH 가 서버에 도달해야"

        hb = _reqs(srv, lambda r: r.path == HEARTBEAT_PATH)[0]
        assert hb.method == "PATCH"
        assert hb.header("Authorization") == "Bearer disp-token-1"
        # heartbeat 는 order-scoped 아님 — x-trace-id 헤더 없음(계약).
        assert hb.header("x-trace-id") is None
        body = hb.json()
        assert body["deviceId"] == "hw-e2e"
        assert body["queueDepth"] == 0  # 유휴 파생(제조 없음).


# ─────────────────────────────────────────────────────────────────────────────
# 시나리오 4 — RPi4/5 hardware id: SENLYT_HARDWARE_ID 가 register deviceId 로 그대로 감
# ─────────────────────────────────────────────────────────────────────────────


def test_rpi_hardware_id_becomes_register_device_id(tmp_path) -> None:
    """RPi HW 시리얼(SENLYT_HARDWARE_ID)이 register deviceId = pi 권위 deviceId 로 확정.

    실 register 왕복(HTTP)으로 확인 — 서버 echo 는 무시되고 수집 시리얼이 권위값.
    RPi4(/proc/cpuinfo Serial)·RPi5(/proc/device-tree/serial-number) 모두 이 env 경로로 수렴.
    """
    rpi_serial = "100000003d1b8f42"  # RPi 시리얼 형태(16-hex).
    with FakeHttpServer() as srv:
        handler = WebHandler(dispenser_token="disp-token-9")
        srv.set_handler(handler)
        env = _make_env(srv.base_url, tmp_path, hardware_id=rpi_serial, mode="flavor")
        ledger = build_ledger(env)
        try:
            comp = build_components(env, ledger=ledger)  # 실 등록(boot 불필요).
        finally:
            ledger.close()

        assert comp.device_id == rpi_serial  # 수집 시리얼 = 권위 deviceId.
        reg = _reqs(srv, lambda r: r.path == REGISTER_PATH)[0]
        assert reg.json()["deviceId"] == rpi_serial  # 서버 echo 아닌 시리얼이 전송됨.
        # 정체성 파일에도 시리얼이 영속.
        assert comp.identity.device_id == rpi_serial
        assert comp.identity.dispenser_token == "disp-token-9"


# ─────────────────────────────────────────────────────────────────────────────
# 시나리오 5 — 재연결/오프라인: status PATCH 5xx → OQ 적재 → 200 전환 → FIFO flush 전달
# ─────────────────────────────────────────────────────────────────────────────


def test_offline_queue_flush_on_real_socket_reconnect(tmp_path) -> None:
    """실 소켓 503 단절 중 status 역보고를 OQ 에 무손실 적재 → 200 복구 시 재연결 flush 로 전달.

    갭 봉합: 종전 OQ 재연결은 request seam(HttpTransportError raise)로만 시뮬됐다. 여기선
    실 FakeHttpServer 가 503(서버 일시오류)을 돌려 OQ 에 남기고, 200 으로 전환하면 데몬의
    주기적 flush_offline_queue(sender/heartbeat 결)가 실 소켓으로 FIFO flush 함을 검증한다.
    """
    handler = WebHandler(
        command_sets=[_cs_wire("o1", "hw-e2e")],
        status_online=False,  # 처음엔 status PATCH 503(단절 시뮬).
    )
    with running_daemon(
        tmp_path, handler, hardware_id="hw-e2e", heartbeat_interval_s=0.05
    ) as (srv, daemon, comp):
        sink = comp.status_sink

        # 제조는 성립(토출은 status 역보고와 독립·best-effort) + 단절 역보고는 OQ 에 적재.
        assert _wait_until(lambda: sink._oq.depth > 0), "단절 중 status 역보고는 OQ 에 남아야(무손실)"
        assert comp.engine.dispense_count >= 1, "status 단절과 무관하게 제조는 성립"

        # 재연결 — 서버가 status PATCH 를 200 으로 수락하기 시작.
        with handler._lock:
            handler.status_online = True

        # 주기적 flush_offline_queue 가 OQ 를 실 소켓으로 FIFO flush → 소진.
        assert _wait_until(lambda: sink._oq.depth == 0), "재연결 flush 로 OQ 가 소진돼야(무손실 전송)"

        # 서버가 실제로 COMPLETED status 를 수락(적용)했는지 — 무손실 전달 확인.
        assert _wait_until(
            lambda: any(b.get("status") == "COMPLETED" for b in list(handler.applied_status))
        ), "재연결 후 COMPLETED status 가 서버에 무손실 전달돼야"


# ─────────────────────────────────────────────────────────────────────────────
# 시나리오 6 — CS-08 외부기기 필터(F19 함대 위장 방어): 타 deviceId 봉투 무시
# ─────────────────────────────────────────────────────────────────────────────


def test_cs08_foreign_device_commandset_filtered(tmp_path) -> None:
    """공용 SSE 에 자기 기기(o1)+타 기기(o2) 봉투를 섞어 push → o1 만 소비·COMPLETED,
    o2 는 어떤 status PATCH 도 서버에 도달하면 안 된다(CS-08 — 함대 공유키 기기 위장 방어)."""
    handler = WebHandler(
        command_sets=[
            _cs_wire("o1", "hw-e2e"),     # 자기 기기 — 소비됨
            _cs_wire("o2", "hw-other"),   # 타 기기 — CS-08 drop
        ]
    )
    with running_daemon(tmp_path, handler, hardware_id="hw-e2e") as (srv, daemon, _c):
        assert _wait_until(
            lambda: any(b.get("status") == "COMPLETED" for b in list(handler.applied_status))
        ), "자기 기기 주문(o1)은 COMPLETED 되어야"
        # o1 은 원장이 COMPLETED, o2 는 손도 안 댐(PENDING 유지 = 소비 안 됨).
        assert handler.orders.get("o1") == "COMPLETED"
        assert handler.orders.get("o2") == "PENDING", "타 기기 봉투(o2)는 소비되면 안 됨(CS-08)"
        # o2 로 향한 status PATCH 가 하나도 없어야(pi 가 애초에 dispatch 안 함).
        o2_patches = _reqs(srv, lambda r: r.path.startswith("/api/dispenser/orders/o2"))
        assert o2_patches == [], f"타 기기(o2) 역보고가 새면 안 됨: {[r.path for r in o2_patches]}"


# ─────────────────────────────────────────────────────────────────────────────
# 시나리오 7 — 실 web 게이트 충실성: 미존재 주문 status → 404, 후진 전이 → 422
# ─────────────────────────────────────────────────────────────────────────────


def test_status_for_unknown_order_is_rejected_404(tmp_path) -> None:
    """order-web 이 만들지 않은 주문의 status PATCH 는 실 web 처럼 404 로 거절돼야(가짜 계약 방지).

    commandSet 를 안 실어 원장이 비어 있는데, 데몬이 (직접) 상태를 보고하면 서버가 404 를 준다.
    여기서는 WebHandler 원장 게이트 자체를 검증 — 실 web 이 미존재 주문을 200 으로 삼키지 않음을 고정.
    """
    handler = WebHandler(command_sets=[])  # 원장 빈 상태.
    # 원장 게이트 단위 검증(핸들러를 직접 호출) — 미존재 o9 → 404, 후진(COMPLETED→PROCESSING) → 422.
    def _patch(order_id: str, status: str) -> dict:
        body = json.dumps({"status": status, "requestId": "r"}).encode()
        return handler(
            RecordedRequest(
                method="PATCH", path=f"{ORDERS_PREFIX}{order_id}", query="", headers={}, body=body
            )
        )

    assert _patch("o9", "PROCESSING")["status"] == 404  # 미존재 → 404

    handler.orders["o1"] = "PENDING"
    assert _patch("o1", "PROCESSING")["status"] == 200  # 전진 허용
    assert _patch("o1", "COMPLETED")["status"] == 200   # 전진 허용
    assert _patch("o1", "PROCESSING")["status"] == 422   # 종결 후 후진 → 422
