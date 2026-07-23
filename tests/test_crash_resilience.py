"""크래시/회복력 실효화 검증 — FakeEngine 현실적 스텝 지연 + 트레이스 per-report 즉시 전송.

배경(docker E2E pi-crash 무의미 문제 수리):
  1) FakeEngine 이 즉시 완료(~1초)라 "제조 중" pi 를 kill 하기 전에 이미 COMPLETED →
     env `SENLYT_FAKE_STEP_DELAY_MS` 로 각 물리 동작에 지연을 넣어 제조가 여러 초 걸리게.
     - 기본 0 = 무지연(단위테스트 회귀 방지). 값>0 = 그만큼 지연.
     - stop 신호(signal_stop)에 즉응(취소/우아한 종료 시 블록 최소화).
  2) trace span 이 배치(heartbeat 30s)로만 나가 크래시 시 유실 → 각 status 역보고 직후
     ship_trace flush 로 진행 중 span 이 지체 없이 서버에 도달.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Iterator

import pytest

from senlyt_pi.adapters.fake_engine_adapter import (
    SENLYT_FAKE_STEP_DELAY_MS_ENV,
    FakeEngineOutcome,
    FakeEnginePort,
    _resolve_step_delay_ms,
)
from senlyt_pi.app.daemon import DaemonDeps, SenlytDaemon
from senlyt_pi.core.command_set import CommandSet, CommandSetStatus
from senlyt_pi.core.order_status import DispensePhase
from senlyt_pi.core.pump_guard import StatusErrorCode, SyringeSpec
from senlyt_pi.core.wire_messages import RecipeStep
from senlyt_pi.persistence.file_idempotency_ledger import FileIdempotencyLedger
from senlyt_pi.pipeline.recipe_resolver import RecipeResolver
from senlyt_pi.ports.engine_port import EngineDispenseCommand

SPEC = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=1.25)


def _cmd(pump_addr: int = 1, volume_ul: float = 100.0) -> EngineDispenseCommand:
    return EngineDispenseCommand(pump_addr=pump_addr, volume_ul=volume_ul, steps=10, spec=SPEC)


# ─────────────────────────────────────────────────────────────────────────────
# 1) FakeEngine 지연 env — 기본 0=무지연 / 값>0=지연 호출
# ─────────────────────────────────────────────────────────────────────────────


def test_env_resolver_default_zero_and_positive():
    # 미설정 → 0(무지연).
    assert _resolve_step_delay_ms({}) == 0
    assert _resolve_step_delay_ms({SENLYT_FAKE_STEP_DELAY_MS_ENV: ""}) == 0
    # 파싱 실패·음수 → 안전 폴백 0.
    assert _resolve_step_delay_ms({SENLYT_FAKE_STEP_DELAY_MS_ENV: "abc"}) == 0
    assert _resolve_step_delay_ms({SENLYT_FAKE_STEP_DELAY_MS_ENV: "-5"}) == 0
    # 값>0 → 그대로.
    assert _resolve_step_delay_ms({SENLYT_FAKE_STEP_DELAY_MS_ENV: "1500"}) == 1500


def test_default_zero_delay_is_immediate():
    """기본(env 미설정) = 무지연 — dispense 가 즉시 반환(단위테스트 타이밍 무영향)."""
    fake = FakeEnginePort()  # env 미설정 가정(CI 기본).
    assert fake.step_delay_ms == 0
    fake.script_all(FakeEngineOutcome.ACK)
    t0 = time.perf_counter()
    for _ in range(5):
        res = fake.dispense(_cmd())
        assert res.raw_error_code == 0
    elapsed = time.perf_counter() - t0
    # 5회 즉시 반환 — 지연 없음. 호출은 정상 기록.
    assert elapsed < 0.05
    assert fake.dispense_count == 5


def test_env_positive_delays_each_physical_step(monkeypatch):
    """env>0 → aspirate/dispense/initialize 각 동작이 그만큼 지연(실 펌프 근사)."""
    monkeypatch.setenv(SENLYT_FAKE_STEP_DELAY_MS_ENV, "60")
    fake = FakeEnginePort()  # env 에서 읽음.
    assert fake.step_delay_ms == 60
    fake.script_all(FakeEngineOutcome.ACK)

    t0 = time.perf_counter()
    fake.dispense(_cmd())
    dispense_elapsed = time.perf_counter() - t0
    assert dispense_elapsed >= 0.05, "dispense 가 지연돼야(실 펌프 근사)"

    t0 = time.perf_counter()
    fake.aspirate(_cmd())
    assert time.perf_counter() - t0 >= 0.05, "aspirate 도 지연"

    t0 = time.perf_counter()
    fake.initialize()
    assert time.perf_counter() - t0 >= 0.05, "initialize 도 지연"

    # 지연이 있어도 관찰 카운터는 정상.
    assert fake.dispense_count == 1
    assert len(fake.aspirate_calls) == 1
    assert fake.initialize_count == 1


def test_explicit_param_overrides_env(monkeypatch):
    """명시 step_delay_ms 주입이 env 보다 우선."""
    monkeypatch.setenv(SENLYT_FAKE_STEP_DELAY_MS_ENV, "9999")
    fake = FakeEnginePort(step_delay_ms=0)
    assert fake.step_delay_ms == 0
    t0 = time.perf_counter()
    fake.dispense(_cmd())
    assert time.perf_counter() - t0 < 0.05


# ─────────────────────────────────────────────────────────────────────────────
# 2) stop 신호 즉응 — 긴 지연 중 signal_stop 이면 지체 없이 반환
# ─────────────────────────────────────────────────────────────────────────────


def test_signal_stop_wakes_delay_promptly():
    """긴 지연(5s) 중 signal_stop → 슬립 조각(20ms)에서 즉시 깨어 반환(블록 최소화)."""
    fake = FakeEnginePort(step_delay_ms=5000)
    fake.script_all(FakeEngineOutcome.ACK)

    done = threading.Event()

    def _run() -> None:
        fake.dispense(_cmd())
        done.set()

    t = threading.Thread(target=_run, daemon=True)
    t0 = time.perf_counter()
    t.start()
    time.sleep(0.05)  # 지연 슬립 진입 보장.
    fake.signal_stop()
    t.join(timeout=2.0)
    elapsed = time.perf_counter() - t0

    assert done.is_set(), "signal_stop 후 dispense 가 반환돼야"
    assert elapsed < 1.0, f"5s 지연을 기다리지 않고 즉시 깨어야(실제 {elapsed:.3f}s)"
    # 호출 자체는 기록(물리 시도는 발생).
    assert fake.dispense_count == 1


def test_reset_clears_stop_for_scenario_reuse():
    """reset 은 stop 신호를 해제 — 시나리오 재사용 시 지연이 다시 정상 동작."""
    fake = FakeEnginePort(step_delay_ms=40)
    fake.signal_stop()
    fake.reset()
    assert not fake._stop.is_set()
    assert fake.step_delay_ms == 40  # step_delay 는 유지.
    t0 = time.perf_counter()
    fake.dispense(_cmd())
    assert time.perf_counter() - t0 >= 0.03, "reset 후 지연 재개"


# ─────────────────────────────────────────────────────────────────────────────
# 3) 트레이스 per-report 즉시 flush — 진행 span 이 배치를 기다리지 않고 도달
# ─────────────────────────────────────────────────────────────────────────────


class _TimelineSink:
    """report_status / ship_trace 를 동일 타임라인에 기록 — flush 시점 관찰."""

    def __init__(self) -> None:
        self.reports: list = []
        self.trace_batches: list = []
        self.timeline: list[str] = []  # "report:<phase>" | "ship:<n>"
        self.transitions: list = []

    def report_status(self, report) -> None:
        self.reports.append(report)
        self.timeline.append(f"report:{report.phase}")

    def send_heartbeat(self, hb) -> None:
        pass

    def ship_trace(self, spans) -> None:
        spans = list(spans)
        self.trace_batches.append(spans)
        self.timeline.append(f"ship:{len(spans)}")

    def report_command_set_transition(self, cs, status, error_code) -> None:
        self.transitions.append((cs, status, error_code))

    def flush_offline_queue(self) -> int:
        return 0

    def all_spans(self) -> list:
        return [s for batch in self.trace_batches for s in batch]


def _manufacture_cs(order_id: str, steps: tuple[RecipeStep, ...]) -> CommandSet:
    return CommandSet(
        command_set_id=f"{order_id}:1",
        device_id="dev-A",
        kind="manufacture",
        steps=steps,
        status=CommandSetStatus.DELIVERED,
        created_at="2026-07-10T00:00:00.000Z",
        created_by="server",
        source_order_id=order_id,
        attempt=1,
        trace_id=f"tr-{order_id}",
    )


class _FakeCommandSource:
    def commands(self, device_id: str) -> Iterator:
        return iter(())


class _FakeCommandSetSource:
    def __init__(self, css: list[CommandSet]) -> None:
        self._css = list(css)

    def command_sets(self, device_id: str) -> Iterator[CommandSet]:
        while self._css:
            yield self._css.pop(0)


@pytest.fixture
def ledger(tmp_path: Path):
    lg = FileIdempotencyLedger.open(tmp_path / "ledger.log")
    yield lg
    try:
        lg.close()
    except Exception:
        pass


def _daemon(ledger, sink, commandset_source=None) -> SenlytDaemon:
    fake = FakeEnginePort()  # 무지연(단위 타이밍).
    fake.script_all(FakeEngineOutcome.ACK)
    return SenlytDaemon(
        DaemonDeps(
            device_id="dev-A",
            command_source=_FakeCommandSource(),
            status_sink=sink,
            engine=fake,
            ledger=ledger,
            resolver=RecipeResolver({1: SPEC, 2: SPEC}),
            commandset_source=commandset_source,
            request_id_gen=lambda: "req-x",
            now_iso=lambda: "2026-07-10T00:00:00.000Z",
            heartbeat_interval_s=0.0,  # heartbeat 배치 비활성 — per-report flush 만으로 도달해야.
            poll_interval_s=0.01,
        )
    )


def test_trace_ships_per_report_without_heartbeat(ledger):
    """heartbeat 없이 poll 만으로도 각 status 역보고 직후 span 이 ship_trace 된다.

    2스텝 레시피 → ACCEPTED·PROGRESS·COMPLETED 3회 역보고 → 각 직후 ship_trace flush.
    heartbeat/shutdown 을 전혀 호출하지 않았는데도 span 이 서버(sink)에 도달해야 한다.
    """
    sink = _TimelineSink()
    src = _FakeCommandSetSource([
        _manufacture_cs("o1", (
            RecipeStep(idx=0, pump_addr=1, flavor="f", volume=100),
            RecipeStep(idx=1, pump_addr=2, flavor="g", volume=100),
        ))
    ])
    daemon = _daemon(ledger, sink, commandset_source=src)

    daemon.poll_once()  # heartbeat/shutdown 미호출.

    # 진행 span 이 이미 도달(배치 대기 없음).
    events = [s.event for s in sink.all_spans()]
    assert "dispense.accepted" in events
    assert "dispense.progress" in events
    assert "dispense.completed" in events
    # 타임라인: 각 report 직후 ship 이 뒤따른다(report→ship 페어).
    tl = sink.timeline
    for i, mark in enumerate(tl):
        if mark.startswith("report:"):
            assert i + 1 < len(tl) and tl[i + 1].startswith("ship:"), (
                f"각 status 역보고 직후 ship_trace 가 와야: {tl}"
            )
    daemon.shutdown()


def test_boot_recovery_ships_interrupted_span(ledger):
    """재기동 복구 — RUNNING 잔여를 INTERRUPTED 로 보고하면 그 span 도 즉시 서버에 도달."""
    ledger.check_and_claim("o9:1")
    ledger.mark_running("o9:1")

    sink = _TimelineSink()
    daemon = _daemon(ledger, sink)

    daemon._recover()

    spans = sink.all_spans()
    assert spans, "복구 INTERRUPTED span 이 전송돼야"
    interrupted = [s for s in spans if s.event == "dispense.failed"]
    assert interrupted, "dispense.failed(INTERRUPTED) span 존재"
    s = interrupted[0]
    assert s.level == "ERROR"
    assert s.detail is not None
    assert s.detail.get("errorCode") == StatusErrorCode.INTERRUPTED.wire
    assert s.order_id == "o9"
    # status 역보고도 INTERRUPTED(FAILED).
    assert sink.reports[0].phase == DispensePhase.FAILED.wire
    assert sink.reports[0].error_code is StatusErrorCode.INTERRUPTED
    daemon.shutdown()


def test_boot_recovery_terminates_commandset_envelope(ledger):
    """재기동 복구 — RUNNING 잔여 봉투를 서버 FAILED(INTERRUPTED)로 종단시켜야 게이트 교착 해소.

    단일 in-flight FIFO 게이트는 head 가 running 이면 신규 전달 0(뒤 queued 미노출). 크래시로
    봉투가 서버에 running 으로 남으면 그 기기 큐가 영구 교착한다 — 복구가 봉투 전이(→FAILED)를
    보고해 서버 게이트가 다음 head 를 승격하도록 한다. (주문 status 축과 독립한 관측/전달 축.)
    """
    ledger.check_and_claim("o9:1")
    ledger.mark_running("o9:1")

    sink = _TimelineSink()
    daemon = _daemon(ledger, sink)

    daemon._recover()

    # 봉투 전이 sink 에 FAILED(INTERRUPTED) 가 정확히 그 commandSetId 로 전달돼야(교착 해소 신호).
    assert sink.transitions, "복구가 CommandSet 봉투 전이를 보고해야(게이트 교착 해소)"
    cs, status, error_code = sink.transitions[-1]
    assert cs.command_set_id == "o9:1"
    assert status is CommandSetStatus.FAILED
    assert error_code is StatusErrorCode.INTERRUPTED
    daemon.shutdown()


def test_boot_recovery_commandset_terminate_is_best_effort(ledger):
    """봉투 전이 sink 미제공(report_command_set_transition 없음)이어도 복구는 정상 진행(무크래시)."""

    class _NoCommandSetSink:
        def __init__(self) -> None:
            self.reports: list = []

        def report_status(self, report) -> None:
            self.reports.append(report)

        def send_heartbeat(self, hb) -> None:
            pass

        def ship_trace(self, spans) -> None:
            pass

    ledger.check_and_claim("o9:1")
    ledger.mark_running("o9:1")
    sink = _NoCommandSetSink()
    daemon = _daemon(ledger, sink)

    daemon._recover()  # commandset_sink=None — 예외 없이 order status 만 보고.

    assert sink.reports and sink.reports[0].error_code is StatusErrorCode.INTERRUPTED
    daemon.shutdown()
