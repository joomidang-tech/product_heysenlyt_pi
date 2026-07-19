"""봉투 종단(terminal) 신뢰성 하드닝(2026-07-19 P0/P1) 회귀 앵커.

16:25 "2펌프 전량 배출" 실패 조사(전달 지연 291s·SSE 귀머거리)에서 파생된 교착 계열 봉합:
  A. **settled 재전달 = terminal 재보고** — terminal PATCH 유실(fire-once) 시 봉투가
     delivered 로 영구 잔류하면 종단 주체가 우주에 없어 그 기기 큐가 영구 교착했다(P0).
     재전달분을 계기로 ledger 의 terminal(DONE/FAILED)을 status PATCH 로 재보고한다.
     단 트레이스 오염 방지 계약(2026-07-10)은 유지 — 역행(delivered/running) 보고 0·span 0.
  B. **delivered 후 예외 = 반드시 FAILED 종단** — interpret(레거시 폴백) 예외가 봉투를
     delivered 로 방치하면 재전달마다 같은 예외 반복 + 큐 교착(P0). dispatcher 가
     "delivered 보고한 봉투는 반드시 terminal" 불변식을 소유한다.
  C. **DELIVERED claim 게이트** — 서버가 DELIVERED 를 rejected(422 역행·404)로 답하면
     이미 종단/취소된 봉투(estop 큐 취소 레이스)다 — 실행하지 않는다(유령 물리 실행 차단).
  D. **종단 전이 5xx·단절 재시도 큐**(http_status_sink_adapter) — 종전 fire-once 는 Cloud Run
     콜드스타트 5xx 한 번에 terminal 을 영구 유실했다. sender 주기가 재전송한다.
  E. **estop 해제 레이스 유예**(pump_sequencer) — 해제 직후 복구 봉투가 estop 폴(≤1s)보다
     먼저 도착해도 짧은 유예 안에 래치 해제를 재확인해 정상 실행한다(진짜 estop 은 유예 후
     기존대로 중단·모션 0).
"""

from __future__ import annotations

import threading
from pathlib import Path

from senlyt_pi.adapters.fake_engine_adapter import FakeEngineOutcome, FakeEnginePort
from senlyt_pi.adapters.http_client import HttpTransportError
from senlyt_pi.adapters.http_status_sink_adapter import HttpStatusSinkAdapter
from senlyt_pi.app.dispatcher import Dispatcher
from senlyt_pi.core.command_set import CommandSetStatus
from senlyt_pi.core.pump_guard import StatusErrorCode
from senlyt_pi.persistence.file_idempotency_ledger import FileIdempotencyLedger
from senlyt_pi.pipeline.pump_sequencer import JobOutcome, PumpSequencer
from senlyt_pi.pipeline.recipe_resolver import RecipeResolver
from support_http import FakeHttpServer

# 봉투/스텝/sink 더블 관례 재사용(test_commandset_dispatch).
from test_commandset_dispatch import (
    SPEC,
    FakeCommandSource,
    SinkRecorder,
    maintenance,
    manufacture,
    step,
)


def make_harness(tmp_path: Path, fake: FakeEnginePort, *, sink=None, interpret=None):
    ledger = FileIdempotencyLedger.open(tmp_path / "ledger.log")
    seq_counter = iter(range(10_000))
    sequencer = PumpSequencer(
        ledger=ledger,
        engine=fake,
        resolver=RecipeResolver({1: SPEC, 2: SPEC}),
        request_id_gen=lambda: f"req-{next(seq_counter)}",
        now_iso=lambda: "2026-07-19T00:00:00.000Z",
    )
    dispatcher = Dispatcher(
        device_id="dev-A",
        command_source=FakeCommandSource(),
        sequencer=sequencer,
        interpret=interpret if interpret is not None else (lambda c: []),
        commandset_sink=sink,
        now_s=lambda: 1783555200.0,  # created_at(2026-07-09) 과 동일 — 신선도 게이트 통과.
    )
    return dispatcher, ledger


def ack_engine() -> FakeEnginePort:
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    return fake


# ── A. settled(FAILED) 재전달 → failed 재보고(교착 자가치유) ────────────────────


class TestSettledRedeliveryRereportsTerminal:
    def test_failed_settled_redelivery_rereports_failed(self, tmp_path):
        fake = ack_engine()
        fake.script_for(1, [FakeEngineOutcome.PERMANENT])
        sink = SinkRecorder()
        dispatcher, ledger = make_harness(tmp_path, fake, sink=sink)
        try:
            first = dispatcher.dispatch_commandset(manufacture(steps=[step(0, 1, 100)]))
            assert first.outcome is JobOutcome.PARTIAL_FAILED
            assert sink.of("o1:1")[-1] == ("failed", "ENGINE_ERROR_PERMANENT")
            n_before = len(sink.of("o1:1"))

            # terminal PATCH 유실 시나리오의 서버측 재전달 — failed 를 다시 종단 보고한다
            # (원 errorCode 는 원장 미보유 → 코드 없는 failed·서버 게이트가 noop/적용 흡수).
            again = dispatcher.dispatch_commandset(manufacture(steps=[step(0, 1, 100)]))
            assert again is None
            events = sink.of("o1:1")
            assert events[n_before:] == [("failed", None)], "재전달분 = failed 재보고 1건뿐"
            # 역행 보고(delivered/running) 0 — 트레이스 오염 방지 계약 유지.
            assert [e for e in events[n_before:] if e[0] in ("delivered", "running")] == []
        finally:
            ledger.close()

    def test_ledger_without_state_of_stays_silent(self, tmp_path):
        """state_of 미제공 ledger(구/테스트 더블) — 종전 조용한 no-op 유지(하위호환)."""

        class MinimalLedger:
            def check_and_claim(self, cid, tid=""):
                raise AssertionError("실행되면 안 됨")

            def is_settled(self, cid):
                return True

        fake = ack_engine()
        sink = SinkRecorder()
        dispatcher, ledger = make_harness(tmp_path, fake, sink=sink)
        try:
            dispatcher.sequencer.ledger = MinimalLedger()  # type: ignore[assignment]
            assert dispatcher.dispatch_commandset(manufacture(steps=[step(0, 1, 100)])) is None
            assert sink.events == []
        finally:
            ledger.close()


# ── B. delivered 후 예외 → 반드시 FAILED 종단(delivered 방치 금지) ──────────────


class TestDeliveredThenExceptionTerminates:
    def test_interpret_exception_reports_failed_terminal(self, tmp_path):
        def boom(_command):
            raise RuntimeError("legacy interpret boom")

        fake = ack_engine()
        sink = SinkRecorder()
        dispatcher, ledger = make_harness(tmp_path, fake, sink=sink, interpret=boom)
        try:
            r = dispatcher.dispatch_commandset(manufacture("oX:1", steps=None))
            assert r is not None
            assert r.outcome is JobOutcome.VALIDATION_FAILED
            assert r.error_code is StatusErrorCode.CMD_VALIDATION_FAILED
            assert fake.dispense_count == 0
            # delivered 보고 후 예외였어도 terminal(failed)로 반드시 종단 — delivered 잔류 0.
            assert sink.of("oX:1") == [
                ("delivered", None),
                ("failed", "CMD_VALIDATION_FAILED"),
            ]
        finally:
            ledger.close()


# ── C. DELIVERED claim 게이트 — 서버 rejected(이미 종단/취소) 시 실행 skip ──────


class TestDeliveredClaimGate:
    def test_rejected_delivered_skips_execution(self, tmp_path):
        class RejectingSink(SinkRecorder):
            def __call__(self, cs, status, error_code):
                super().__call__(cs, status, error_code)
                if status is CommandSetStatus.DELIVERED:
                    return "rejected"  # 서버: 422 역행(이미 failed·estop 취소) / 404.
                return "applied"

        fake = ack_engine()
        sink = RejectingSink()
        dispatcher, ledger = make_harness(tmp_path, fake, sink=sink)
        try:
            r = dispatcher.dispatch_commandset(maintenance("mnt-c", steps=[step(0, 1, 100)]))
            assert r is None, "취소된 봉투는 실행하지 않는다(유령 물리 실행 차단)"
            assert fake.dispense_count == 0
            # delivered 시도 1건뿐 — running/terminal 보고 없음(서버 상태 그대로).
            assert sink.of("mnt-c") == [("delivered", None)]
            assert not ledger.is_settled("mnt-c")  # ledger 미점유(재시도 여지 보존).
        finally:
            ledger.close()

    def test_none_verdict_proceeds(self, tmp_path):
        """판정 미상(구 sink·None 반환) — 기존 동작 그대로 실행(가용성 우선·하위호환)."""
        fake = ack_engine()
        sink = SinkRecorder()  # None 반환.
        dispatcher, ledger = make_harness(tmp_path, fake, sink=sink)
        try:
            r = dispatcher.dispatch_commandset(manufacture(steps=[step(0, 1, 100)]))
            assert r is not None and r.outcome is JobOutcome.COMPLETED
            assert fake.dispense_count == 1
        finally:
            ledger.close()


# ── D. 종단 전이 재시도 큐(http_status_sink_adapter) ───────────────────────────


def _cs_for_sink(cid: str = "oQ:1"):
    return manufacture(cid, steps=[step(0, 1, 100)])


class TestTerminalTransitionRetryQueue:
    def test_transport_error_enqueues_terminal_and_flush_resends(self):
        online = {"up": False}

        def flaky(method, url, **kw):
            if not online["up"]:
                raise HttpTransportError("단절")
            from senlyt_pi.adapters.http_client import request_json

            return request_json(method, url, **kw)

        with FakeHttpServer() as srv:
            srv.set_handler(lambda req: {"status": 200, "json": {"applied": True}})
            sink = HttpStatusSinkAdapter(base_url=srv.base_url, bearer_token="t", request=flaky)
            verdict = sink.report_command_set_transition(
                _cs_for_sink(), CommandSetStatus.DONE, None
            )
            assert verdict == "retry"
            assert srv.requests == []  # 단절 — 왕복 0.

            online["up"] = True
            assert sink.flush_commandset_retries() == 1
            rec = srv.requests[-1]
            assert rec.method == "PATCH"
            assert rec.path.endswith("/api/dispenser/commandsets/oQ%3A1") or "commandsets" in rec.path
            assert rec.json()["status"] == "done"

    def test_5xx_enqueues_and_422_terminates_retry(self):
        calls = {"n": 0}

        def handler(req):
            calls["n"] += 1
            # 1차(report) = 503 → 재시도 적재. 2차(flush) = 422 → 서버 확정·종결.
            return (
                {"status": 503, "json": {"code": "oops"}}
                if calls["n"] == 1
                else {"status": 422, "json": {"code": "illegal"}}
            )

        with FakeHttpServer() as srv:
            srv.set_handler(handler)
            sink = HttpStatusSinkAdapter(base_url=srv.base_url, bearer_token="t")
            assert (
                sink.report_command_set_transition(
                    _cs_for_sink(), CommandSetStatus.FAILED, StatusErrorCode.PARTIAL_DISPENSE
                )
                == "retry"
            )
            # 422(역행 = 이미 종단) → rejected 로 종결·재적재 없음.
            assert sink.flush_commandset_retries() == 1
            assert sink.flush_commandset_retries() == 0  # 큐 빔.

    def test_non_terminal_transport_error_not_enqueued(self):
        sink = HttpStatusSinkAdapter(
            base_url="http://x",
            bearer_token="t",
            request=lambda *a, **k: (_ for _ in ()).throw(HttpTransportError("단절")),
        )
        assert (
            sink.report_command_set_transition(_cs_for_sink(), CommandSetStatus.DELIVERED, None)
            == "retry"
        )
        assert sink.flush_commandset_retries() == 0  # delivered 는 재시도 대상 아님(재전달이 커버).

    def test_2xx_returns_applied_and_422_returns_rejected(self):
        with FakeHttpServer() as srv:
            srv.set_handler(lambda req: {"status": 200, "json": {"applied": True}})
            sink = HttpStatusSinkAdapter(base_url=srv.base_url, bearer_token="t")
            assert (
                sink.report_command_set_transition(_cs_for_sink(), CommandSetStatus.DELIVERED, None)
                == "applied"
            )
        with FakeHttpServer() as srv:
            srv.set_handler(lambda req: {"status": 422, "json": {"code": "illegal"}})
            sink = HttpStatusSinkAdapter(base_url=srv.base_url, bearer_token="t")
            assert (
                sink.report_command_set_transition(_cs_for_sink(), CommandSetStatus.DELIVERED, None)
                == "rejected"
            )


# ── E. estop 해제 레이스 유예(pump_sequencer) ──────────────────────────────────


def _make_seq(tmp_path: Path, fake: FakeEnginePort, ev: threading.Event) -> tuple[PumpSequencer, FileIdempotencyLedger]:
    ledger = FileIdempotencyLedger.open(tmp_path / "seq.log")
    seq_counter = iter(range(10_000))
    seq = PumpSequencer(
        ledger=ledger,
        engine=fake,
        resolver=RecipeResolver({1: SPEC}),
        request_id_gen=lambda: f"req-{next(seq_counter)}",
        now_iso=lambda: "2026-07-19T00:00:00.000Z",
        estop_event=ev,
    )
    return seq, ledger


class TestEstopClearGrace:
    def test_latch_cleared_within_grace_runs_job(self, tmp_path):
        """estop 해제 직후 복구 봉투 레이스 — 유예 내 래치 해제(estop 폴 모사)면 정상 실행."""
        ev = threading.Event()
        ev.set()  # 서버는 이미 clear 했지만 pi 폴이 아직 못 봤다(래치 잔존).
        fake = ack_engine()
        seq, ledger = _make_seq(tmp_path, fake, ev)
        try:
            seq.estop_clear_grace_s = 1.0
            timer = threading.Timer(0.1, ev.clear)  # estop 폴(≤1s)의 해제 모사.
            timer.start()
            try:
                r = seq.submit(command_id="mnt-r:1", trace_id="t", steps=[step(0, 1, 100)])
            finally:
                timer.cancel()
            assert r.outcome is JobOutcome.COMPLETED
            assert fake.dispense_count == 1
        finally:
            ledger.close()

    def test_latch_kept_set_still_aborts_after_grace(self, tmp_path):
        """진짜 estop(래치 유지) — 유예 후 기존과 동일하게 ESTOP_ABORTED·토출 0(안전 불변)."""
        ev = threading.Event()
        ev.set()
        fake = ack_engine()
        seq, ledger = _make_seq(tmp_path, fake, ev)
        try:
            seq.estop_clear_grace_s = 0.05  # 테스트 신속화 — 유예 자체는 상수와 무관 동작.
            r = seq.submit(command_id="mnt-k:1", trace_id="t", steps=[step(0, 1, 100)])
            assert r.outcome is JobOutcome.ESTOP_ABORTED
            assert r.error_code is StatusErrorCode.INTERRUPTED
            assert fake.dispense_count == 0
        finally:
            ledger.close()
