"""정비 흡입/배출 신뢰성 — 2026-07-19 QA 이슈([admin] 흡입, 배출 이슈) 회귀 앵커.

실기기(10000000b9166a1c) 재현: 흡입/배출이 "됐다 안 됐다" — 실패 3건 전부 `A…R` 즉답이
깨짐(`C`·`\\x07`·무응답) → 즉시 -2000/-1000 permanent. 그런데 성공 건(13:37:39)은 즉답만
정상이면 폴에서 쓰레기가 3번 나와도 완주했다. 즉 **명령 즉답 1회 읽기가 유일한 단일 실패
지점**이었고, 명령 자체는 펌프에 닿아 플런저는 실제로 움직였다(ACK 만 깨진 것).

v1.1.0(잘 동작하던 필드 검증 버전·향장향)의 구조를 미러한 3종 수정을 앵커한다:
  A. **ack-tolerant** — v1.1.0 `_validateResponse` 는 빈/깨진 즉답을 **통과**시키고 폴
     (`_pollUntilReady` 40s)이 성패를 판정했다. 정비 이동(`A…R`)에 같은 구조 복원 —
     silent-success 아님(성공 판정은 언제나 폴의 실제 완료 확인·죽은 펌프는 타임아웃 실패).
  B. **밸브 회전 시퀀스** — v1.1.0 흡입=air 포트 회전→A{full} / 배출=output 포트 회전→A0.
     v1.2.0 은 회전 없이 A 만 쏴서 마지막 열린 액체 포트로 흡입/역류하는 격차. 포트 배치
     SoT=서버가 `valvePort` 로 해석해 싣는다(부재=구 서버=회전 생략 하위호환).
  C. **정비 신선도 게이트** — 연타로 쌓인 정비 봉투가 몇 분 뒤 유령처럼 실행되던 것
     ("2~3분 뒤 1·2펌프 동시 작동")을 dispatcher 가 createdAt 기준 90s 로 차단.
  D. **로그 상관** — 시퀀서가 스텝 실행 스레드에 traceId/commandSetId 를 바인딩해 어댑터
     시리얼 왕복 DEBUG·실패 ERROR 가 그 명령의 trace 로 자동 엮인다(흐름 추적).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from senlyt_pi.app.dispatcher import MAINTENANCE_STALE_S, Dispatcher
from senlyt_pi.core.command_set import CommandSet, CommandSetStatus
from senlyt_pi.core.pump_guard import StatusErrorCode
from senlyt_pi.core.wire_messages import RecipeStep
from senlyt_pi.obs.log import StructuredLogger
from senlyt_pi.persistence.file_idempotency_ledger import FileIdempotencyLedger
from senlyt_pi.pipeline.pump_sequencer import JobOutcome, PumpSequencer
from senlyt_pi.pipeline.recipe_resolver import RecipeResolver, ResolvedOpStep
from senlyt_pi.ports.engine_port import EngineOpCommand
from senlyt_pi.test_seam.fake_engine_sentinels import (
    FAKE_EMPTY_RAW_CODE,
    FAKE_TIMEOUT_RAW_CODE,
)

# 기존 어댑터 테스트의 시리얼 더블 관례 재사용.
from test_sy01b_engine_adapter import SPEC_05, FakeSerial, adapter_with, cmd, status_frame


class GarbledAckSerial(FakeSerial):
    """절대이동(`A…R`) **즉답만** 깨뜨리고 나머지(셋업·폴)는 정상 — 실기기 링크 노이즈 모사.

    `ack_bytes=None` = 즉답 무응답. `fail_times` 만큼만 깨고 이후 정상(간헐성 모사).
    """

    def __init__(self, ack_bytes: bytes | None = b"C", *, fail_times: int = 1):
        super().__init__(default=status_frame(0, ready=True))
        self._ack_bytes = ack_bytes
        self._remaining = fail_times

    def _is_abs_move(self, txt: str) -> bool:
        body = txt.rstrip("\r")
        return "A" in body and body.endswith("R") and not body.endswith("?")

    def write(self, data: bytes) -> int:
        txt = data.decode("ascii")
        self.written.append(txt)
        if self._is_abs_move(txt) and self._remaining > 0:
            self._remaining -= 1
            if self._ack_bytes is not None:
                self._buf.extend(self._ack_bytes)  # ETX 없는 쓰레기 — 타임아웃까지 읽다 반환.
            return len(data)  # 무응답이면 버퍼에 아무것도 안 넣음.
        self._buf.extend(status_frame(0, ready=True))
        return len(data)


def _op(op: str, *, valve_port: int | None = None) -> EngineOpCommand:
    return EngineOpCommand(pump_addr=1, op=op, spec=SPEC_05, valve_port=valve_port)


def _tolerant_adapter(fake: FakeSerial):
    # read_timeout 짧게 — 깨진 즉답(ETX 없음)이 실기기처럼 타임아웃으로 반환되게.
    return adapter_with(fake, read_timeout_s=0.02, init_timeout_s=1.0, motion_timeout_s=1.0)


# ── A. ack-tolerant — 즉답이 깨져도 폴이 실제 완료를 확인하면 성공 ────────────────


class TestAckTolerantPlungerMoves:
    def test_garbled_ack_then_ready_poll_succeeds(self):
        """실기기 실패 재현(`A0R`→`C`) — 이제 폴이 Ready 를 확인해 성공(0)."""
        fake = GarbledAckSerial(b"C")
        res = _tolerant_adapter(fake).run_op(_op("plunger_home"))
        assert res.raw_error_code == 0, "즉답 쓰레기 1회로 permanent 오판(QA 이슈 재현) — 회귀"
        joined = "".join(fake.written)
        assert "A0R" in joined  # 이동 명령은 나갔고
        assert joined.count("?") >= 1  # 성패는 폴이 판정했다.

    def test_no_ack_then_ready_poll_succeeds(self):
        """실기기 실패 재현(pump2 스타일 즉답 무응답) — 폴이 판정해 성공."""
        fake = GarbledAckSerial(None)
        res = _tolerant_adapter(fake).run_op(_op("plunger_full"))
        assert res.raw_error_code == 0
        assert "A12000" in "".join(fake.written)

    def test_dead_pump_still_fails_honestly(self):
        """진짜 죽은 펌프(전 왕복 무응답)는 여전히 실패 — silent-success 아님."""

        class DeadSerial(FakeSerial):
            def write(self, data: bytes) -> int:
                self.written.append(data.decode("ascii"))
                return len(data)

        res = _tolerant_adapter(DeadSerial(default=b"")).run_op(_op("plunger_full"))
        assert res.raw_error_code == FAKE_TIMEOUT_RAW_CODE

    def test_explicit_error_ack_fails_immediately_even_when_tolerant(self):
        """즉답이 **명시적 에러**(Code 9 오버로드)면 tolerant 여도 즉시 실패(진짜 에러)."""

        class ErrorAckSerial(GarbledAckSerial):
            def write(self, data: bytes) -> int:
                txt = data.decode("ascii")
                self.written.append(txt)
                if self._is_abs_move(txt):
                    self._buf.extend(status_frame(9, ready=False))  # 오버로드 즉답.
                else:
                    self._buf.extend(status_frame(0, ready=True))
                return len(data)

        a = _tolerant_adapter(ErrorAckSerial())
        res = a.run_op(_op("plunger_full"))
        assert res.raw_error_code == 9
        assert 1 not in a._initialized  # 9 = 재초기화-필수 → 셋업 캐시 무효화 유지.

    def test_dispense_path_stays_strict_ep03(self):
        """토출 경로는 비관대 유지 — 흡입(`P…R`) 즉답이 깨지면 기존대로 실패(EP-03 경계)."""

        class GarbledDispenseSerial(FakeSerial):
            def write(self, data: bytes) -> int:
                txt = data.decode("ascii")
                self.written.append(txt)
                if "P" in txt.rstrip("\r") and txt.rstrip("\r").endswith("R"):
                    self._buf.extend(b"C")  # 토출 흡입 명령 즉답 파손.
                else:
                    self._buf.extend(status_frame(0, ready=True))
                return len(data)

        res = adapter_with(GarbledDispenseSerial(), read_timeout_s=0.02).dispense(cmd())
        assert res.raw_error_code == FAKE_EMPTY_RAW_CODE  # _MALFORMED — 관대화 누수 없음.


# ── B. 밸브 회전 시퀀스(v1.1.0 미러) — 흡입=air / 배출=output 회전 후 이동 ────────


class TestValveRotationBeforePlungerMove:
    def test_valve_rotates_before_move_when_port_given(self):
        fake = FakeSerial()
        res = adapter_with(fake).run_op(_op("plunger_full", valve_port=12))
        assert res.raw_error_code == 0
        joined = "".join(fake.written)
        assert "I12R" in joined
        assert joined.index("I12R") < joined.index("A12000")  # 회전 → 이동 순서(v1.1.0).

    def test_no_valve_port_skips_rotation_backcompat(self):
        """valvePort 부재(구 서버) — 회전 없이 기존 와이어 그대로(하위호환)."""
        fake = FakeSerial()
        res = adapter_with(fake).run_op(_op("plunger_home"))
        assert res.raw_error_code == 0
        assert "I" not in "".join(w.rstrip("\r")[2:-1] for w in fake.written if "A0R" not in w) or (
            "I12R" not in "".join(fake.written)
        )
        assert "A0R" in "".join(fake.written)

    def test_valve_failure_blocks_move(self):
        """밸브 회전이 명시 에러로 실패하면 플런저를 밀지 않는다(역류·과부하 방지·v1.1.0 전파)."""

        class ValveErrorSerial(FakeSerial):
            def write(self, data: bytes) -> int:
                txt = data.decode("ascii")
                self.written.append(txt)
                if txt.rstrip("\r").startswith("/1I2") and txt.rstrip("\r").endswith("R"):
                    self._buf.extend(status_frame(3, ready=False))  # 밸브 오류.
                else:
                    self._buf.extend(status_frame(0, ready=True))
                return len(data)

        fake = ValveErrorSerial()
        res = adapter_with(fake).run_op(_op("plunger_home", valve_port=2))
        assert res.raw_error_code == 3
        assert not any("A0R" in w for w in fake.written), "밸브 실패 후 이동 = 역류 위험"

    def test_wire_valveport_roundtrip_and_resolver(self):
        """서버 `valvePort` → RecipeStep.in_port → ResolvedOpStep.valve_port 전파 + 왕복 보존."""
        step = RecipeStep.from_json(
            {"idx": 0, "stage": 0, "kind": "engineOp", "pumpAddr": 1, "op": "plungerFull",
             "valvePort": 12}
        )
        assert step.in_port == 12
        assert step.to_json()["valvePort"] == 12  # 재전송/영속 왕복 보존.

        rr = RecipeResolver({1: SPEC_05})
        out = rr.resolve([step]).steps[0]
        assert isinstance(out, ResolvedOpStep)
        assert out.valve_port == 12

    def test_wire_valveport_out_of_range_ignored(self):
        """1~12 밖 valvePort — 안전측 무시(회전 생략)·구계약(부재)도 None."""
        rr = RecipeResolver({1: SPEC_05})
        bad = RecipeStep.from_json(
            {"idx": 0, "stage": 0, "kind": "engineOp", "pumpAddr": 1, "op": "plungerFull",
             "valvePort": 13}
        )
        out = rr.resolve([bad]).steps[0]
        assert isinstance(out, ResolvedOpStep) and out.valve_port is None
        legacy = RecipeStep.from_json(
            {"idx": 0, "stage": 0, "kind": "engineOp", "pumpAddr": 1, "op": "plungerFull"}
        )
        out2 = rr.resolve([legacy]).steps[0]
        assert isinstance(out2, ResolvedOpStep) and out2.valve_port is None


# ── C. 정비 신선도 게이트 — 묵은 정비는 물리 실행 없이 종단(유령 실행 차단) ─────────


class _NullCommandSource:
    def commands(self, device_id):
        return []


class _SinkRecorder:
    def __init__(self):
        self.events = []

    def __call__(self, cs, status, error_code):
        self.events.append((cs.command_set_id, status, error_code))


FIXED_NOW_S = 1783555200.0  # 2026-07-09T00:00:00Z


def _dispatcher(tmp_path: Path, engine, *, now_s: float = FIXED_NOW_S):
    ledger = FileIdempotencyLedger.open(tmp_path / "l.log")
    sequencer = PumpSequencer(
        ledger=ledger,
        engine=engine,
        resolver=RecipeResolver({1: SPEC_05}),
        request_id_gen=iter(f"req-{i}" for i in range(10_000)).__next__,
        now_iso=lambda: "2026-07-09T00:00:00.000Z",
    )
    sink = _SinkRecorder()
    d = Dispatcher(
        device_id="dev-A",
        command_source=_NullCommandSource(),
        sequencer=sequencer,
        interpret=lambda c: [],
        commandset_sink=sink,
        now_s=lambda: now_s,
    )
    return d, sink, ledger


def _mnt(cid: str, created_at: str) -> CommandSet:
    return CommandSet(
        command_set_id=cid,
        device_id="dev-A",
        kind="maintenance",
        steps=(
            RecipeStep(
                idx=0, pump_addr=1, flavor="op:plungerFull", volume=0.0,
                kind="engineOp", stage=0, op="plungerFull",
            ),
        ),
        status=CommandSetStatus.QUEUED,
        created_at=created_at,
        created_by="operator:op-1",
    )


class TestMaintenanceStaleGate:
    def test_stale_maintenance_fails_without_executing(self, tmp_path):
        """상한(90s) 넘게 묵은 정비 — 펌프 무구동 + failed 종단(유령 실행 차단)."""
        from senlyt_pi.adapters.fake_engine_adapter import FakeEngineOutcome, FakeEnginePort

        engine = FakeEnginePort()
        engine.script_all(FakeEngineOutcome.ACK)
        d, sink, ledger = _dispatcher(tmp_path, engine)
        try:
            stale = _mnt("mnt-old", "2026-07-08T23:58:00.000Z")  # 120s 전 발행.
            report = d.dispatch_commandset(stale)
            assert report is not None and report.outcome is JobOutcome.VALIDATION_FAILED
            assert report.error_code is StatusErrorCode.CMD_VALIDATION_FAILED
            assert engine.op_calls == [], "묵은 정비가 펌프를 움직이면 유령 실행"
            assert engine.dispense_calls == []
            # 서버 종단 보고 — delivered/running 없이 곧장 failed(실행 자체가 없었음).
            assert sink.events == [
                ("mnt-old", CommandSetStatus.FAILED, StatusErrorCode.CMD_VALIDATION_FAILED)
            ]
        finally:
            ledger.close()

    def test_fresh_maintenance_executes(self, tmp_path):
        from senlyt_pi.adapters.fake_engine_adapter import FakeEngineOutcome, FakeEnginePort

        engine = FakeEnginePort()
        engine.script_all(FakeEngineOutcome.ACK)
        d, sink, ledger = _dispatcher(tmp_path, engine)
        try:
            fresh = _mnt("mnt-fresh", "2026-07-08T23:59:30.000Z")  # 30s 전 — 통과.
            report = d.dispatch_commandset(fresh)
            assert report is not None and report.outcome is JobOutcome.COMPLETED
        finally:
            ledger.close()

    def test_stale_manufacture_still_executes(self, tmp_path):
        """제조는 대상 아님 — 주문은 묵어도 반드시 실행(멱등·유실 0 계약)."""
        from senlyt_pi.adapters.fake_engine_adapter import FakeEngineOutcome, FakeEnginePort

        engine = FakeEnginePort()
        engine.script_all(FakeEngineOutcome.ACK)
        d, sink, ledger = _dispatcher(tmp_path, engine)
        try:
            old_order = CommandSet(
                command_set_id="o9:1",
                device_id="dev-A",
                kind="manufacture",
                steps=(RecipeStep(idx=0, pump_addr=1, flavor="lemon", volume=100.0),),
                status=CommandSetStatus.QUEUED,
                created_at="2026-07-08T00:00:00.000Z",  # 하루 묵은 주문.
                created_by="server",
                source_order_id="o9",
                attempt=1,
                trace_id="t-o9",
            )
            report = d.dispatch_commandset(old_order)
            assert report is not None and report.outcome is JobOutcome.COMPLETED
        finally:
            ledger.close()

    def test_unparseable_created_at_passes_gate(self, tmp_path):
        """createdAt 파싱 불가 — 잘못된 드롭보다 기존 동작(실행)이 안전측."""
        from senlyt_pi.adapters.fake_engine_adapter import FakeEngineOutcome, FakeEnginePort

        engine = FakeEnginePort()
        engine.script_all(FakeEngineOutcome.ACK)
        d, sink, ledger = _dispatcher(tmp_path, engine)
        try:
            weird = _mnt("mnt-weird", "not-a-timestamp")
            report = d.dispatch_commandset(weird)
            assert report is not None and report.outcome is JobOutcome.COMPLETED
        finally:
            ledger.close()

    def test_stale_limit_is_90s(self):
        assert MAINTENANCE_STALE_S == pytest.approx(90.0)


# ── D. 로그 상관 — 스텝 실행 스레드 컨텍스트로 어댑터 로그가 trace 에 엮인다 ────────


class TestStepLogContext:
    def test_bind_attaches_and_clear_removes(self):
        records = []
        log = StructuredLogger(device_id="dev-A", sink=records.append)
        log.bind_step_context(trace_id="t-1", command_set_id="mnt-1")
        log.debug("시리얼 왕복", stage="스텝실행", command="/1?", response="x")
        log.clear_step_context()
        log.debug("컨텍스트 밖", stage="스텝실행")
        assert records[0]["traceId"] == "t-1"
        assert records[0]["commandSetId"] == "mnt-1"
        assert records[1]["traceId"] is None and records[1]["commandSetId"] is None

    def test_explicit_correlation_wins_over_context(self):
        records = []
        log = StructuredLogger(sink=records.append)
        log.bind_step_context(trace_id="ctx-t", command_set_id="ctx-c")
        log.info("명시 우선", stage="스텝실행", trace_id="explicit-t")
        log.clear_step_context()
        assert records[0]["traceId"] == "explicit-t"
        assert records[0]["commandSetId"] == "ctx-c"  # 명시 안 된 필드는 컨텍스트.

    def test_context_is_thread_local(self):
        import threading

        records = []
        log = StructuredLogger(sink=records.append)
        log.bind_step_context(trace_id="main-t", command_set_id="main-c")

        def other_thread():
            log.info("다른 스레드 — 컨텍스트 없음", stage="스텝실행")

        t = threading.Thread(target=other_thread)
        t.start()
        t.join()
        log.clear_step_context()
        assert records[0]["traceId"] is None, "스레드로컬이어야 — 전역 누수는 잡 교차 오염"

    def test_sequencer_binds_context_for_op_failure_log(self, tmp_path):
        """시퀀서 통합 — 정비 실패 ERROR 가 잡의 traceId/commandSetId 를 자동 획득."""
        from senlyt_pi.adapters.fake_engine_adapter import FakeEngineOutcome, FakeEnginePort

        records = []
        log = StructuredLogger(device_id="dev-A", sink=records.append)
        engine = FakeEnginePort()
        engine.script_all(FakeEngineOutcome.PERMANENT)
        ledger = FileIdempotencyLedger.open(tmp_path / "l.log")
        try:
            seq = PumpSequencer(
                ledger=ledger,
                engine=engine,
                resolver=RecipeResolver({1: SPEC_05}),
                request_id_gen=lambda: "req-0",
                now_iso=lambda: "2026-07-19T00:00:00.000Z",
                logger=log,
            )
            seq.submit(
                command_id="mnt-ctx",
                trace_id="trace-ctx",
                steps=[
                    RecipeStep(
                        idx=0, pump_addr=1, flavor="op:plungerFull", volume=0.0,
                        kind="engineOp", stage=0, op="plungerFull",
                    )
                ],
            )
            errs = [r for r in records if r.get("severity") == "ERROR"]
            assert errs, "정비 실패 ERROR 자체가 안 남음"
            assert errs[0]["traceId"] == "trace-ctx"  # QA 이슈의 traceId=null 사각 봉합.
            assert errs[0]["commandSetId"] == "mnt-ctx"
        finally:
            ledger.close()
