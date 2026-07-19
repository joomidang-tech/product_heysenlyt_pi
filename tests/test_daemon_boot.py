"""SenlytDaemon.boot 상시 소비 루프 실구현 검증 — 스텁 완전 제거(사용자 원칙 2026-07-10).

물리 엔진(FakeEngineAdapter)만 mock 이고, 소비 루프(SSE→멱등→실행→역보고)는 실구현이다.
검증:
  - boot 루프가 CommandSet 봉투를 소비 → FakeEngine 실행 → 주문축(PROCESSING→COMPLETED) +
    봉투축(DELIVERED→RUNNING→DONE) 역보고.
  - 멱등(IL-02): 중복 전달은 1회만 토출.
  - heartbeat(queueDepth 파생) + ship_trace 배치 flush + OQ flush.
  - BootRecovery: RUNNING 잔여 → INTERRUPTED 보고(재토출 0).
  - 네트워크 예외 시 루프 지속(삼킴) + OQ 무손실 flush.
  - senlytd main(SENLYT_RUN=1) 이 boot 호출(엔진=fake).
  - graceful stop(request_stop) → 우아한 종료.
"""

from __future__ import annotations

import time
import types
from collections import deque
from pathlib import Path
from typing import Iterator

import pytest

from senlyt_pi.adapters.fake_engine_adapter import FakeEngineOutcome, FakeEnginePort
from senlyt_pi.adapters.http_client import HttpTransportError
from senlyt_pi.adapters.http_status_sink_adapter import HttpStatusSinkAdapter
from senlyt_pi.app.daemon import DaemonDeps, SenlytDaemon
from senlyt_pi.core.command_set import CommandSet, CommandSetStatus
from senlyt_pi.core.order_status import DispensePhase, phase_to_wire_status
from senlyt_pi.core.pump_guard import StatusErrorCode, SyringeSpec
from senlyt_pi.core.wire_messages import Command, RecipeStep
from senlyt_pi.persistence.file_idempotency_ledger import (
    FileIdempotencyLedger,
    LedgerEntryState,
)
from senlyt_pi.pipeline.recipe_resolver import RecipeResolver

SPEC = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=1.25)


# ─────────────────────────────────────────────────────────────────────────────
# 하네스 — Fake 소스/싱크
# ─────────────────────────────────────────────────────────────────────────────


class FakeCommandSetSource:
    """Fake CommandSetSource — 테스트가 봉투를 push, command_sets() 가 도착분 드레인."""

    def __init__(self) -> None:
        self._pending: deque[CommandSet] = deque()

    def push(self, cs: CommandSet) -> None:
        self._pending.append(cs)

    def command_sets(self, device_id: str) -> Iterator[CommandSet]:
        while self._pending:
            yield self._pending.popleft()


class FakeCommandSource:
    """Fake CommandSource — 테스트가 command 를 push, commands() 가 도착분 드레인."""

    def __init__(self) -> None:
        self._pending: deque[Command] = deque()

    def push(self, c: Command) -> None:
        self._pending.append(c)

    def commands(self, device_id: str) -> Iterator[Command]:
        while self._pending:
            yield self._pending.popleft()


class FakeStatusSink:
    """Fake StatusSinkPort(+봉투전이·OQ flush) — 역보고를 기록. 네트워크 실패 시뮬 가능."""

    def __init__(self, *, raise_on_report: bool = False) -> None:
        self.reports: list = []  # StatusReport
        self.heartbeats: list = []
        self.trace_batches: list = []
        self.transitions: list = []  # (CommandSet, CommandSetStatus, errorCode|None)
        self.flush_calls = 0
        self.raise_on_report = raise_on_report

    def report_status(self, report) -> None:
        if self.raise_on_report:
            raise RuntimeError("네트워크 단절(시뮬)")
        self.reports.append(report)

    def send_heartbeat(self, hb) -> None:
        self.heartbeats.append(hb)

    def ship_trace(self, spans) -> None:
        self.trace_batches.append(list(spans))

    def report_command_set_transition(self, cs, status, error_code) -> None:
        self.transitions.append((cs, status, error_code))

    def flush_offline_queue(self) -> int:
        self.flush_calls += 1
        return 0

    # 관측 헬퍼 --------------------------------------------------------------
    def order_wire_statuses(self) -> list[str]:
        out = []
        for r in self.reports:
            phase = DispensePhase.from_wire(r.phase)
            out.append(phase_to_wire_status(phase).wire if phase else r.phase)
        return out

    def transition_statuses(self) -> list[CommandSetStatus]:
        return [t[1] for t in self.transitions]


def _manufacture_cs(
    order_id: str,
    attempt: int,
    *,
    device_id: str = "dev-A",
    steps: tuple[RecipeStep, ...] | None = None,
    status: CommandSetStatus = CommandSetStatus.DELIVERED,
) -> CommandSet:
    return CommandSet(
        command_set_id=f"{order_id}:{attempt}",
        device_id=device_id,
        kind="manufacture",
        steps=steps if steps is not None else (RecipeStep(idx=0, pump_addr=1, flavor="f", volume=100),),
        status=status,
        created_at="2026-07-10T00:00:00.000Z",
        created_by="server",
        source_order_id=order_id,
        attempt=attempt,
        trace_id=f"tr-{order_id}",
    )


@pytest.fixture
def ledger(tmp_path: Path):
    lg = FileIdempotencyLedger.open(tmp_path / "ledger.log")
    yield lg
    try:
        lg.close()
    except Exception:
        pass


def _daemon(
    ledger: FileIdempotencyLedger,
    *,
    engine: FakeEnginePort | None = None,
    status_sink: object | None = None,
    commandset_source: object | None = None,
    command_source: object | None = None,
    pump_map: dict | None = None,
    heartbeat_interval_s: float = 0.0,
    poll_interval_s: float = 0.01,
    estop_source=None,
) -> SenlytDaemon:
    eng = engine if engine is not None else FakeEnginePort()
    if engine is None:
        eng.script_all(FakeEngineOutcome.ACK)
    return SenlytDaemon(
        DaemonDeps(
            device_id="dev-A",
            command_source=command_source if command_source is not None else FakeCommandSource(),
            status_sink=status_sink if status_sink is not None else FakeStatusSink(),
            engine=eng,
            ledger=ledger,
            resolver=RecipeResolver(pump_map if pump_map is not None else {1: SPEC, 2: SPEC}),
            commandset_source=commandset_source,
            request_id_gen=lambda: "req-x",
            now_iso=lambda: "2026-07-10T00:00:00.000Z",
            heartbeat_interval_s=heartbeat_interval_s,
            poll_interval_s=poll_interval_s,
            estop_source=estop_source,
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# 1) 소비 루프 — 봉투 소비 → FakeEngine 실행 → 양축 역보고
# ─────────────────────────────────────────────────────────────────────────────


def test_poll_consumes_commandset_and_reports_both_axes(ledger):
    sink = FakeStatusSink()
    src = FakeCommandSetSource()
    src.push(
        _manufacture_cs(
            "o1", 1,
            steps=(
                RecipeStep(idx=0, pump_addr=1, flavor="f", volume=100),
                RecipeStep(idx=1, pump_addr=2, flavor="g", volume=100),
            ),
        )
    )
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    daemon = _daemon(ledger, engine=fake, status_sink=sink, commandset_source=src)

    handled = daemon.poll_once()

    assert handled == 1
    # FakeEngine 실토출 = 2스텝(2회 dispense).
    assert fake.dispense_count == 2
    # 주문축(PENDING→)PROCESSING→COMPLETED — 마지막은 COMPLETED, 앞은 PROCESSING.
    wires = sink.order_wire_statuses()
    assert wires[0] == "PROCESSING"
    assert wires[-1] == "COMPLETED"
    assert set(wires) == {"PROCESSING", "COMPLETED"}
    # 봉투축 DELIVERED→RUNNING→DONE.
    assert sink.transition_statuses() == [
        CommandSetStatus.DELIVERED,
        CommandSetStatus.RUNNING,
        CommandSetStatus.DONE,
    ]
    daemon.shutdown()


def test_command_axis_also_consumed(ledger):
    """기존 Command 축(recipe 명시)도 상시 루프에서 소비 — poll() 경로."""
    sink = FakeStatusSink()
    csrc = FakeCommandSource()
    csrc.push(
        Command(
            id="o5:1",
            order_id="o5",
            attempt=1,
            device_id="dev-A",
            recipe=(RecipeStep(idx=0, pump_addr=1, flavor="f", volume=100),),
            trace_id="tr-o5",
            created_at="2026-07-10T00:00:00.000Z",
        )
    )
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    daemon = _daemon(ledger, engine=fake, status_sink=sink, command_source=csrc)

    assert daemon.poll_once() == 1
    assert fake.dispense_count == 1
    assert sink.order_wire_statuses()[-1] == "COMPLETED"
    daemon.shutdown()


# ─────────────────────────────────────────────────────────────────────────────
# 2) 멱등(IL-02) — 중복 전달 1회 토출
# ─────────────────────────────────────────────────────────────────────────────


def test_duplicate_commandset_dispenses_once(ledger):
    sink = FakeStatusSink()
    src = FakeCommandSetSource()
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    daemon = _daemon(ledger, engine=fake, status_sink=sink, commandset_source=src)

    src.push(_manufacture_cs("o1", 1))
    daemon.poll_once()
    src.push(_manufacture_cs("o1", 1))  # 동일 합성키 재전달.
    daemon.poll_once()

    # 1스텝 레시피 × 원판 1회 = dispense 1. 중복은 IL-02 로 추가 토출 0.
    assert fake.dispense_count == 1
    # 봉투 terminal DONE 은 원판 1회만(중복은 DUPLICATE_DROPPED → terminal 생략).
    assert sink.transition_statuses().count(CommandSetStatus.DONE) == 1
    daemon.shutdown()


# ─────────────────────────────────────────────────────────────────────────────
# 3) heartbeat + ship_trace 배치 + OQ flush
# ─────────────────────────────────────────────────────────────────────────────


def test_heartbeat_traces_and_oq_flush(ledger):
    sink = FakeStatusSink()
    src = FakeCommandSetSource()
    src.push(_manufacture_cs("o1", 1))
    daemon = _daemon(ledger, status_sink=sink, commandset_source=src)

    daemon.poll_once()  # trace span 버퍼 채움.
    daemon._emit_heartbeat()

    assert len(sink.heartbeats) == 1
    hb = sink.heartbeats[0]
    assert hb.device_id == "dev-A"
    assert hb.queue_depth == 0  # 유휴(제조 완료 후).
    # per-report 즉시 flush — 진행 span 은 poll 중 각 status 역보고 직후 이미 ship_trace 된다
    # (heartbeat 배치를 기다리지 않음). 배치 1회 이상 + 모든 span 이 pi(dispense.* 이벤트).
    assert len(sink.trace_batches) >= 1
    all_spans = [s for batch in sink.trace_batches for s in batch]
    assert all_spans, "trace span 이 비어있지 않아야"
    assert all(s.service == "pi" for s in all_spans)
    # OQ flush 호출.
    assert sink.flush_calls == 1
    daemon.shutdown()


# ─────────────────────────────────────────────────────────────────────────────
# 4) BootRecovery — RUNNING 잔여 → INTERRUPTED 보고(재토출 0)
# ─────────────────────────────────────────────────────────────────────────────


def test_boot_recovery_reports_interrupted(ledger):
    # 전원 단절로 RUNNING 에 멈춘 합성키를 재현.
    ledger.check_and_claim("o9:1")
    ledger.mark_running("o9:1")

    sink = FakeStatusSink()
    fake = FakeEnginePort()
    daemon = _daemon(ledger, engine=fake, status_sink=sink)

    daemon._recover()

    # INTERRUPTED 보고(재토출 0) — dispense 0.
    assert fake.dispense_count == 0
    assert len(sink.reports) == 1
    r = sink.reports[0]
    assert r.id == "o9:1"
    assert r.phase == DispensePhase.FAILED.wire
    assert r.error_code is StatusErrorCode.INTERRUPTED
    # ledger 는 FAILED 로 종결 — 다음 재기동 재보고 방지.
    assert ledger.state_of("o9:1") is LedgerEntryState.FAILED
    daemon.shutdown()


def test_boot_recovery_uses_ledger_trace_id(ledger):
    """복구 보고가 원장 traceId 를 실어 원 주문 트레이스와 상관된다(갭 봉합).

    제조 중 크래시 재현: claim 시 원 traceId 를 원장에 영속 → RUNNING 마킹 → (크래시) →
    재기동 복구가 그 traceId 로 INTERRUPTED status + dispense.failed span 을 보고.
    """
    # claim 시 원 traceId 영속(pump_sequencer 경로와 동일한 traceId 전달).
    ledger.check_and_claim("o9:1", "trace-order-9")
    ledger.mark_running("o9:1")

    sink = FakeStatusSink()
    fake = FakeEnginePort()
    daemon = _daemon(ledger, engine=fake, status_sink=sink)

    daemon._recover()

    # 재토출 0(자동 재실행 금지) 유지.
    assert fake.dispense_count == 0
    # status 축 — INTERRUPTED 보고가 원 traceId 를 실었다(빈 문자열 아님).
    r = sink.reports[0]
    assert r.error_code is StatusErrorCode.INTERRUPTED
    assert r.trace_id == "trace-order-9"
    # trace 축 — dispense.failed span 이 원 traceId 로 상관된다.
    all_spans = [s for batch in sink.trace_batches for s in batch]
    interrupted = [s for s in all_spans if s.event == "dispense.failed"]
    assert interrupted, "복구 dispense.failed span 이 있어야"
    assert all(s.trace_id == "trace-order-9" for s in interrupted)
    # 봉투 전이 축도 원 traceId 를 보유.
    assert sink.transitions, "봉투 INTERRUPTED 전이가 있어야"
    assert sink.transitions[-1][0].trace_id == "trace-order-9"
    daemon.shutdown()


def test_boot_recovery_trace_id_fallback_when_absent(ledger):
    """구엔트리(claim 시 traceId 미저장) 하위호환 — 복구 보고 traceId 는 빈 문자열 폴백."""
    ledger.check_and_claim("o9:1")  # traceId 미전달(구경로).
    ledger.mark_running("o9:1")

    sink = FakeStatusSink()
    daemon = _daemon(ledger, engine=FakeEnginePort(), status_sink=sink)

    daemon._recover()

    assert sink.reports[0].trace_id == ""  # 안전 폴백(기존 동작 불변).
    daemon.shutdown()


# ─────────────────────────────────────────────────────────────────────────────
# 5) 네트워크 예외 — 루프 지속 + OQ 무손실
# ─────────────────────────────────────────────────────────────────────────────


def test_report_exception_does_not_break_loop(ledger):
    """status_sink 예외를 삼켜 poll 지속 — 제조(토출)는 계속된다(§10-6)."""
    sink = FakeStatusSink(raise_on_report=True)
    src = FakeCommandSetSource()
    src.push(_manufacture_cs("o1", 1))
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    daemon = _daemon(ledger, engine=fake, status_sink=sink, commandset_source=src)

    # 예외가 전파되지 않고(삼킴) 토출은 성립.
    handled = daemon.poll_once()
    assert handled == 1
    assert fake.dispense_count == 1
    daemon.shutdown()


class _ToggleRequest:
    """HttpStatusSinkAdapter request seam — fail=True 면 네트워크 오류, False 면 200."""

    def __init__(self) -> None:
        self.fail = True

    def __call__(self, method, url, *, body=None, headers=None, timeout=None):
        if self.fail:
            raise HttpTransportError("network down")
        return (200, None)


def test_offline_queue_lossless_flush_on_reconnect(ledger):
    """단절 중 역보고는 OQ 에 적재(무손실) → heartbeat 의 flush_offline_queue 로 재연결 flush."""
    req = _ToggleRequest()
    real_sink = HttpStatusSinkAdapter(
        base_url="http://web:3000",
        bearer_token="t",
        request=req,
        request_id_gen=lambda: "req-1",
    )
    src = FakeCommandSetSource()
    src.push(_manufacture_cs("o1", 1))
    daemon = _daemon(ledger, status_sink=real_sink, commandset_source=src)

    # 네트워크 단절 상태로 소비 — 토출은 성립, 역보고는 OQ 에 적재(전송 실패).
    daemon.poll_once()
    assert real_sink._oq.depth > 0, "단절 중 역보고는 OQ 에 남아야(무손실)"

    # 재연결 — heartbeat 주기의 flush_offline_queue 가 FIFO flush.
    req.fail = False
    daemon._emit_heartbeat()
    assert real_sink._oq.depth == 0, "재연결 flush 로 OQ 소진(무손실 전송)"
    daemon.shutdown()


# ─────────────────────────────────────────────────────────────────────────────
# 6) graceful stop — boot 루프를 스레드로 돌리고 request_stop 으로 우아한 종료
# ─────────────────────────────────────────────────────────────────────────────


def test_boot_loop_graceful_stop(ledger):
    import threading

    sink = FakeStatusSink()
    src = FakeCommandSetSource()
    src.push(_manufacture_cs("o1", 1))
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    daemon = _daemon(
        ledger, engine=fake, status_sink=sink, commandset_source=src,
        heartbeat_interval_s=0.05, poll_interval_s=0.01,
    )

    t = threading.Thread(target=daemon.boot, name="boot", daemon=True)
    t.start()

    # 도착분 소비를 기다린다(최대 3s).
    deadline = time.time() + 3.0
    while fake.dispense_count < 1 and time.time() < deadline:
        time.sleep(0.01)
    assert fake.dispense_count == 1, "루프가 도착분을 소비해야"

    daemon.request_stop()
    t.join(timeout=3.0)
    assert not t.is_alive(), "request_stop 후 루프가 종료돼야"
    assert daemon._shutdown_done, "종료 시 shutdown(우아한 종료) 수행"


# ─────────────────────────────────────────────────────────────────────────────
# 7) senlytd main(SENLYT_RUN=1) → boot 호출(엔진=fake)
# ─────────────────────────────────────────────────────────────────────────────


def test_senlytd_run_mode_invokes_boot(tmp_path, monkeypatch):
    from senlyt_pi.app import senlytd

    captured: dict = {}

    class _RecorderDaemon:
        def __init__(self, deps):
            self.deps = deps
            captured["deps"] = deps
            self.boot_called = False

        def request_stop(self):
            pass

        def boot(self):
            self.boot_called = True
            captured["boot_called"] = True

    fake_components = types.SimpleNamespace(
        device_id="dev-A",
        command_source=FakeCommandSource(),
        status_sink=FakeStatusSink(),
        engine=FakeEnginePort(),
        valve=None,  # §9-1 v2 — 밸브 미결선(valve 스텝 없는 시나리오).
        server_config=types.SimpleNamespace(base_url="http://web:3000"),
        logger=None,
    )

    lg = FileIdempotencyLedger.open(tmp_path / "run-ledger.log")

    monkeypatch.setattr(senlytd, "build_ledger", lambda environ: lg)
    monkeypatch.setattr(senlytd, "build_components", lambda environ, **kw: fake_components)
    monkeypatch.setattr(senlytd, "SenlytDaemon", _RecorderDaemon)
    monkeypatch.setenv("SENLYT_RUN", "1")

    rc = senlytd.main()

    assert rc == 0
    assert captured.get("boot_called") is True
    # 엔진 = Fake(유일 mock).
    assert isinstance(captured["deps"].engine, FakeEnginePort)
    lg.close()


# ─────────────────────────────────────────────────────────────────────────────
# 긴급정지 감시(§9-4·2026-07-18) — estop 신호 fast-poll → 전 펌프 즉시 정지 + 하드 중단
# ─────────────────────────────────────────────────────────────────────────────


def test_estop_watcher_triggers_emergency_stop_on_rising_edge(ledger):
    """active 상승엣지 → 데몬 _estop set + engine.emergency_stop_all(pump_map addrs)."""
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    signal = {"v": (True, "2026-07-18T00:00:00.000Z")}
    d = _daemon(ledger, engine=fake, estop_source=lambda: signal["v"],
                pump_map={1: SPEC, 2: SPEC, 3: SPEC})
    d.poll_estop_once()
    assert d._estop.is_set()
    assert fake.estop_all_calls == [[1, 2, 3]]  # 전 펌프 TR


def test_estop_watcher_same_signal_not_retriggered(ledger):
    """같은 requestedAt 은 재처리하지 않는다(반복 TR 회피)."""
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    signal = {"v": (True, "2026-07-18T00:00:00.000Z")}
    d = _daemon(ledger, engine=fake, estop_source=lambda: signal["v"])
    d.poll_estop_once()
    d.poll_estop_once()  # 같은 신호 재폴
    assert len(fake.estop_all_calls) == 1


def test_estop_watcher_clear_releases_latch(ledger):
    """신호 해제(active False) → 데몬 _estop clear + engine.clear_estop."""
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    signal = {"v": (True, "2026-07-18T00:00:00.000Z")}
    d = _daemon(ledger, engine=fake, estop_source=lambda: signal["v"])
    d.poll_estop_once()
    assert d._estop.is_set() and fake._estop.is_set()
    signal["v"] = (False, None)
    d.poll_estop_once()
    assert not d._estop.is_set()
    assert not fake._estop.is_set()  # engine.clear_estop 호출됨


def test_estop_watcher_unknown_poll_keeps_latch(ledger):
    """[안전·fail-SAFE·2026-07-19] 래치 set(estop 활성) 상태에서 폴이 **None(불확정)** 반환(네트워크
    오류·401 만료·403·500)해도 래치를 **유지**한다.

    회귀 방지: 옛 코드는 poll_estop 이 실패를 (False,None)로 흡수 → 데몬이 active=False+래치set 을
    '정상 해제'로 오인해 폴 1회 실패만으로 안전 래치를 풀었다(fail-OPEN — 관제 estop 활성 중 언래치).
    이제 실패=None → 데몬이 래치를 건드리지 않아야 한다(확인불가=정지측).
    """
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    signal = {"v": (True, "2026-07-19T00:00:00.000Z")}
    d = _daemon(ledger, engine=fake, estop_source=lambda: signal["v"])
    d.poll_estop_once()
    assert d._estop.is_set() and fake._estop.is_set()  # estop 발동·래치 set
    # 폴 실패(불확정) — 서버가 '비활성'이라 확인한 게 아니다.
    signal["v"] = None
    d.poll_estop_once()
    assert d._estop.is_set()  # ⛔ 래치 유지(fail-safe) — 옛 코드는 여기서 풀렸다
    assert fake._estop.is_set()  # 엔진 래치도 유지(clear_estop 미호출)
    # 다음 성공 폴에서 진짜 해제 신호가 오면 그때 정상 풀림(수렴).
    signal["v"] = (False, None)
    d.poll_estop_once()
    assert not d._estop.is_set()


def test_estop_watcher_poll_error_is_swallowed(ledger):
    """estop 폴 소스 예외 → 삼킴(다음 폴 재시도·데몬 죽지 않음)."""
    def boom():
        raise RuntimeError("network down")

    d = _daemon(ledger, estop_source=boom)
    d.poll_estop_once()  # 예외 안 나야 함
    assert not d._estop.is_set()


def test_ship_log_ships_all_levels_by_default(ledger):
    """_ship_log — 기본 게이트 DEBUG(전 레벨 DEBUG·INFO·WARN·ERROR 전송) + 필드 매핑 + WARN 즉시-flush.

    2026-07-18 "하드웨어 로그 전량 전송" — 폴 단위 DEBUG 상세까지 서버로 합류시킨다.
    WARN/ERROR 는 _trace_flush_now 를 set 해 sender 가 즉시 전송하게 한다(DEBUG/INFO 는 배치).
    """
    d = _daemon(ledger)
    d._ship_log({"severity": "DEBUG", "message": "폴", "stage": "pi수신"})  # 실림(DEBUG)
    d._ship_log({"severity": "INFO", "message": "명령 수신", "stage": "pi수신"})  # 실림(INFO)
    assert not d._trace_flush_now.is_set()  # DEBUG/INFO 는 즉시 flush 신호 안 켬(배치 대기)
    d._ship_log(
        {
            "severity": "WARN",
            "message": "폴 주기 오류(삼킴)",
            "stage": "오류",
            "traceId": "tr-1",
            "orderId": "o-1",
            "deviceId": "dev-A",
            "commandSetId": "c-1:1",
            "ts": "2026-07-18T00:00:00.000Z",
            "detail": {"error": "SSE timeout"},
        }
    )
    spans = d._trace_buffer
    assert len(spans) == 3  # DEBUG + INFO + WARN 전부 실림(전 레벨 전송)
    assert d._trace_flush_now.is_set()  # WARN = 실패 신호 → 즉시 flush 신호 켜짐
    assert spans[0].event == "pi.log.debug" and spans[0].level == "DEBUG"
    assert spans[1].event == "pi.log.info" and spans[1].level == "INFO"
    s = spans[2]
    assert s.event == "pi.log.warn"
    assert s.level == "WARN"
    assert s.service == "pi"
    assert s.trace_id == "tr-1"
    assert s.order_id == "o-1"
    assert s.detail is not None
    assert s.detail["message"] == "폴 주기 오류(삼킴)"
    assert s.detail["stage"] == "오류"
    assert s.detail["commandSetId"] == "c-1:1"
    assert s.detail["error"] == "SSE timeout"  # 서버 allowlist(trace.ts)가 최종 반출 게이트


def test_ship_log_overflow_spills_to_disk_sink_no_drop(ledger, monkeypatch):
    """버퍼 상한 도달 + sink 가 스풀 지원 → 오래된 절반을 spill_traces 로 배출, 드롭 0 (2026-07-19 유실 0).

    스풀 지원 sink 에선 메모리 overflow 가 더 이상 드롭이 아니다 — 디스크로 넘겨 재연결 시
    서버 합류한다. 드롭 계수(합성 WARN)는 스풀 미지원 sink 전용 폴백으로만 남는다.
    """
    from senlyt_pi.app import daemon as dmod

    monkeypatch.setattr(dmod, "_LOG_TRACE_BUFFER_CAP", 4)

    class SpillingSink(FakeStatusSink):
        def __init__(self):
            super().__init__()
            self.spilled: list = []

        def spill_traces(self, spans) -> None:
            self.spilled.extend(spans)

    sink = SpillingSink()
    d = _daemon(ledger, status_sink=sink)
    for i in range(6):  # cap=4 → 5번째에서 overflow(오래된 2건 배출) 후 계속 적재.
        d._ship_log({"severity": "INFO", "message": f"m{i}", "stage": "pi수신"})
    assert d._trace_dropped == 0, "스풀 지원 sink 에선 드롭이 없어야(유실 0)"
    assert [s.detail["message"] for s in sink.spilled] == ["m0", "m1"]  # 오래된 것부터 디스크로.
    assert [s.detail["message"] for s in d._trace_buffer] == ["m2", "m3", "m4", "m5"]


def test_ship_log_buffer_cap_drops_counted_and_reported(ledger, monkeypatch):
    """버퍼 상한 초과분은 조용히 버리지 않고 건수를 세어 다음 flush 에 합성 WARN 으로 보고(silent 금지)."""
    from senlyt_pi.app import daemon as dmod

    monkeypatch.setattr(dmod, "_LOG_TRACE_BUFFER_CAP", 2)
    sink = FakeStatusSink()
    d = _daemon(ledger, status_sink=sink)
    for i in range(4):  # cap=2 → 앞 2건 실림, 뒤 2건 드롭
        d._ship_log({"severity": "INFO", "message": f"m{i}", "stage": "pi수신"})
    assert d._trace_dropped == 2

    d._flush_traces()
    shipped = sink.trace_batches[-1]
    assert len(shipped) == 3  # 실린 2건 + 드롭 보고 합성 WARN 1건
    warn = shipped[-1]
    assert warn.level == "WARN"
    assert warn.detail is not None and "드롭" in warn.detail["message"]
    assert d._trace_dropped == 0  # 보고 후 리셋
