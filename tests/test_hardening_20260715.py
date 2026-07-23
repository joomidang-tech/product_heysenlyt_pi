"""하드닝 봉합 검증(감사 P2/P3·2026-07-15) — sender 워커·ledger clear·지수 백오프·SSE 가드.

수정 1(P2): 역보고 송신 전용 워커 — 제조 임계경로에서 네트워크 I/O 분리.
  - sender 경유에도 보고 순서 FIFO 보존(ACCEPTED→PROGRESS→COMPLETED).
  - shutdown 시 큐 잔여분 동기 drain(무손실 지향).
  - boot 없이 _publish_progress 직접 호출 = 기존 동기 경로(즉시 sink 도달·무파괴).
수정 3(P2): BootRecovery CLEAR_AND_FRESH 실동작 — ledger.clear → fresh 재수용.
  - clear 후 check_and_claim FRESH / replay(재open) 대칭 / DONE dedup 유지 / compact 제외.
수정 4(P3): 소비 루프 지수 백오프 — **오류에만**(정상 유휴 미적용)·상한 60s·성공 리셋.
수정 5(P3): SSE connect/read 타임아웃 분리 가드 + 트리클 워치독(_is_stale 순수 판정).
"""

from __future__ import annotations

import threading
import time
import types
from collections import deque
from pathlib import Path
from typing import Iterator

import pytest

from senlyt_pi.adapters.fake_engine_adapter import FakeEngineOutcome, FakeEnginePort
from senlyt_pi.adapters.http_client import (
    HttpTransportError,
    SseStream,
    _upgrade_read_timeout,
    open_sse,
)
from senlyt_pi.adapters import sse_command_source_adapter as sse_mod
from senlyt_pi.adapters.sse_command_source_adapter import (
    SseCommandSourceAdapter,
    _is_stale,
)
from senlyt_pi.app.daemon import DaemonDeps, SenlytDaemon, _error_backoff_s
from senlyt_pi.core.command_set import CommandSet, CommandSetStatus
from senlyt_pi.core.order_status import DispensePhase
from senlyt_pi.core.pump_guard import SyringeSpec
from senlyt_pi.core.wire_messages import RecipeStep
from senlyt_pi.persistence.file_idempotency_ledger import (
    FileIdempotencyLedger,
    LedgerEntryState,
)
from senlyt_pi.persistence.idempotency_ledger import LedgerVerdict
from senlyt_pi.pipeline.recipe_resolver import RecipeResolver
from support_http import FakeHttpServer

SPEC = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=1.25)


# ─────────────────────────────────────────────────────────────────────────────
# 하네스 — Fake 소스/싱크(test_daemon_boot 결 유지·자체 보유)
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
    """Fake CommandSource — 항상 유휴(빈 스트림)."""

    def commands(self, device_id: str) -> Iterator:
        return iter(())


class FlakyCommandSource:
    """처음 fail_times 회는 전송 오류, 이후 정상 유휴 — 백오프 리셋 검증용."""

    def __init__(self, fail_times: int) -> None:
        self.fail_times = fail_times
        self.calls = 0

    def commands(self, device_id: str) -> Iterator:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise HttpTransportError("network down(시뮬)")
        return iter(())


class FakeStatusSink:
    """Fake StatusSinkPort — 역보고·heartbeat·trace·OQ flush 를 기록."""

    def __init__(self) -> None:
        self.reports: list = []
        self.heartbeats: list = []
        self.trace_batches: list = []
        self.transitions: list = []
        self.flush_calls = 0

    def report_status(self, report) -> None:
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


def _manufacture_cs(order_id: str, attempt: int) -> CommandSet:
    return CommandSet(
        command_set_id=f"{order_id}:{attempt}",
        device_id="dev-A",
        kind="manufacture",
        steps=(RecipeStep(idx=0, pump_addr=1, flavor="f", volume=100),),
        status=CommandSetStatus.DELIVERED,
        created_at="2026-07-15T00:00:00.000Z",
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
    status_sink: object | None = None,
    commandset_source: object | None = None,
    command_source: object | None = None,
    heartbeat_interval_s: float = 0.0,
    poll_interval_s: float = 0.01,
) -> SenlytDaemon:
    eng = FakeEnginePort()
    eng.script_all(FakeEngineOutcome.ACK)
    return SenlytDaemon(
        DaemonDeps(
            device_id="dev-A",
            command_source=command_source if command_source is not None else FakeCommandSource(),
            status_sink=status_sink if status_sink is not None else FakeStatusSink(),
            engine=eng,
            ledger=ledger,
            resolver=RecipeResolver({1: SPEC, 2: SPEC}),
            commandset_source=commandset_source,
            request_id_gen=lambda: "req-x",
            now_iso=lambda: "2026-07-15T00:00:00.000Z",
            heartbeat_interval_s=heartbeat_interval_s,
            poll_interval_s=poll_interval_s,
        )
    )


def _publish(daemon: SenlytDaemon, phase: DispensePhase, k: int, n: int, cid: str) -> None:
    daemon._publish_progress(phase, k, n, None, cid, f"tr-{cid}")


# ─────────────────────────────────────────────────────────────────────────────
# 수정 1 — 송신 전용 워커: FIFO 보존 + shutdown drain + 동기 폴백
# ─────────────────────────────────────────────────────────────────────────────


def test_sender_thread_preserves_fifo_order(ledger):
    """sender 경유에도 보고 순서 FIFO 보존(ACCEPTED→PROGRESS→COMPLETED) — 단일 소비 스레드."""
    sink = FakeStatusSink()
    daemon = _daemon(ledger, status_sink=sink)
    daemon._start_sender()
    assert daemon._sender_alive(), "sender 워커가 기동돼야"

    _publish(daemon, DispensePhase.ACCEPTED, 0, 2, "o1:1")
    _publish(daemon, DispensePhase.PROGRESS, 1, 2, "o1:1")
    _publish(daemon, DispensePhase.COMPLETED, 2, 2, "o1:1")

    # shutdown 이 sender 정지 + 큐 잔여 drain — 이후 3건 전부·순서 그대로 sink 도달.
    daemon.shutdown()
    assert [r.phase for r in sink.reports] == ["ACCEPTED", "PROGRESS", "COMPLETED"]
    # trace 도 유실 없이 flush(sender per-report 또는 shutdown 마지막 flush).
    all_spans = [s for batch in sink.trace_batches for s in batch]
    assert len(all_spans) == 3


def test_shutdown_drains_send_queue_residue(ledger):
    """shutdown 시 큐 잔여분 동기 drain — sender 가 못 비운 report 도 무손실 전송."""
    sink = FakeStatusSink()
    daemon = _daemon(ledger, status_sink=sink)
    # sender 미기동 상태로 큐에 직접 적재(워커가 join 후 남긴 잔여분 재현).
    daemon._start_sender()
    daemon._stop.set()  # 워커를 곧 종료시키고,
    daemon._sender_thread.join(timeout=3.0)
    assert not daemon._sender_alive()
    for k, phase in enumerate((DispensePhase.ACCEPTED, DispensePhase.COMPLETED)):
        _publish(daemon, phase, k, 1, "o2:1")  # 워커 죽은 뒤 = 동기 경로로 즉시 도달.
    # 큐에 별도 잔여를 남긴 시나리오 — 직접 put(드레인 경로 검증).
    daemon._send_queue.put(sink.reports[0].__class__(
        id="o3:1", phase=DispensePhase.COMPLETED.wire, step_k=1, step_n=1,
        error_code=None, request_id="req-x", trace_id="tr-o3",
        updated_at="2026-07-15T00:00:00.000Z",
    ))
    daemon.shutdown()
    assert [r.id for r in sink.reports] == ["o2:1", "o2:1", "o3:1"], "잔여분까지 FIFO drain"


def test_publish_without_boot_uses_sync_path(ledger):
    """boot 없이 _publish_progress 직접 호출(기존 테스트 결) = 동기 경로 — 즉시 sink 도달."""
    sink = FakeStatusSink()
    daemon = _daemon(ledger, status_sink=sink)
    assert not daemon._sender_alive()

    _publish(daemon, DispensePhase.COMPLETED, 1, 1, "o1:1")

    # 대기 없이 곧장 도달(큐 미경유) + per-report trace flush 도 즉시.
    assert [r.phase for r in sink.reports] == ["COMPLETED"]
    assert len(sink.trace_batches) == 1
    daemon.shutdown()


def test_boot_loop_reports_via_sender_in_order(ledger):
    """boot 상시 루프(=sender 기동 경로)에서도 주문축 보고가 순서대로 도달한다."""
    sink = FakeStatusSink()
    src = FakeCommandSetSource()
    src.push(_manufacture_cs("o1", 1))
    daemon = _daemon(ledger, status_sink=sink, commandset_source=src)

    t = threading.Thread(target=daemon.boot, name="boot", daemon=True)
    t.start()
    # sender 는 비동기 — COMPLETED 도달을 기다린다(최대 3s).
    deadline = time.time() + 3.0
    while time.time() < deadline:
        if any(r.phase == "COMPLETED" for r in sink.reports):
            break
        time.sleep(0.01)
    daemon.request_stop()
    t.join(timeout=3.0)
    assert not t.is_alive()

    phases = [r.phase for r in sink.reports]
    assert phases[0] == "ACCEPTED" and phases[-1] == "COMPLETED", f"FIFO 보존: {phases}"


def test_heartbeat_skips_flushes_when_sender_alive(ledger):
    """sender 생존 시 heartbeat 는 전송만(정시성) — flush 는 워커 담당. 미기동 시 기존 3종."""
    sink = FakeStatusSink()
    daemon = _daemon(ledger, status_sink=sink)

    # sender 미기동(기존 경로) — heartbeat 가 OQ flush 까지 수행(하위호환).
    daemon._emit_heartbeat()
    assert sink.flush_calls == 1

    # sender 기동 — heartbeat 는 전송만, OQ flush 횟수 불변.
    daemon._start_sender()
    daemon._emit_heartbeat()
    assert len(sink.heartbeats) == 2
    assert sink.flush_calls == 1, "sender 생존 시 heartbeat 는 flush 생략(워커 담당)"
    daemon.shutdown()


# ─────────────────────────────────────────────────────────────────────────────
# 수정 3 — ledger.clear: RECEIVED 잔여 → fresh 재수용(CR-01 비위반)
# ─────────────────────────────────────────────────────────────────────────────


def test_clear_received_key_becomes_fresh(tmp_path: Path):
    lg = FileIdempotencyLedger.open(tmp_path / "l.log")
    assert lg.check_and_claim("o1:1") is LedgerVerdict.FRESH
    assert lg.check_and_claim("o1:1") is LedgerVerdict.DUPLICATE

    lg.clear("o1:1")

    assert lg.state_of("o1:1") is None, "clear 후 인덱스 부재"
    assert lg.check_and_claim("o1:1") is LedgerVerdict.FRESH, "재전달분 fresh 재수용"
    lg.close()


def test_clear_survives_replay_reopen(tmp_path: Path):
    """CLEARED 라인은 replay(재open)에서도 키를 제거 — 영속 대칭."""
    p = tmp_path / "l.log"
    l1 = FileIdempotencyLedger.open(p)
    l1.check_and_claim("o1:1", "tr-1")
    l1.clear("o1:1")
    l1.close()

    l2 = FileIdempotencyLedger.open(p)
    assert l2.state_of("o1:1") is None
    assert l2.trace_id_of("o1:1") == "", "clear 는 traceId 도 제거"
    assert l2.check_and_claim("o1:1") is LedgerVerdict.FRESH
    l2.close()


def test_done_key_dedup_unaffected_by_clear(tmp_path: Path):
    """DONE 종결 키는 clear(다른 키)와 무관하게 dedup 유지 — 재open 후에도."""
    p = tmp_path / "l.log"
    l1 = FileIdempotencyLedger.open(p)
    l1.check_and_claim("done:1")
    l1.mark_settled("done:1", success=True)
    l1.check_and_claim("recv:1")
    l1.clear("recv:1")
    assert l1.check_and_claim("done:1") is LedgerVerdict.DUPLICATE
    l1.close()

    l2 = FileIdempotencyLedger.open(p)
    assert l2.state_of("done:1") is LedgerEntryState.DONE
    assert l2.check_and_claim("done:1") is LedgerVerdict.DUPLICATE
    l2.close()


def test_compact_excludes_cleared_keys(tmp_path: Path):
    """compact 출력에서 CLEARED(=인덱스 부재) 키 미출력 — 로그도 접힌다."""
    p = tmp_path / "l.log"
    lg = FileIdempotencyLedger.open(p)
    lg.check_and_claim("gone:1")
    lg.clear("gone:1")
    lg.check_and_claim("kept:1")
    lg.mark_settled("kept:1", success=True)

    lg.compact()

    text = p.read_text(encoding="utf-8")
    assert "gone:1" not in text, "CLEARED 키는 compact 출력 제외"
    assert "kept:1" in text
    lg.close()

    l2 = FileIdempotencyLedger.open(p)
    assert l2.check_and_claim("gone:1") is LedgerVerdict.FRESH
    assert l2.check_and_claim("kept:1") is LedgerVerdict.DUPLICATE
    l2.close()


def test_recover_clears_received_for_fresh_redelivery(ledger):
    """daemon._recover 의 CLEAR_AND_FRESH 분기가 실제 ledger.clear 를 호출 — 재전달분 fresh."""
    ledger.check_and_claim("o7:1")  # RECEIVED 창(claim 후·모션 전) 크래시 재현.
    sink = FakeStatusSink()
    daemon = _daemon(ledger, status_sink=sink)

    daemon._recover()

    # 물리 토출 0(CR-01) — RECEIVED 는 보고 대상도 아님(재전달 대기).
    assert sink.reports == []
    # 핵심: 재전달분이 DUPLICATE drop 되지 않고 fresh 소비된다.
    assert ledger.check_and_claim("o7:1") is LedgerVerdict.FRESH
    daemon.shutdown()


# ─────────────────────────────────────────────────────────────────────────────
# 수정 4 — 지수 백오프: 오류에만·상한 60s·성공 리셋
# ─────────────────────────────────────────────────────────────────────────────


def test_error_backoff_pure_function():
    """대기시간 = min(poll_interval × 2^연속오류, 60.0) — 순수 계산."""
    assert _error_backoff_s(1.0, 1) == 2.0
    assert _error_backoff_s(1.0, 2) == 4.0
    assert _error_backoff_s(1.0, 5) == 32.0
    assert _error_backoff_s(1.0, 6) == 60.0, "64 → 상한 60s 클램프"
    assert _error_backoff_s(1.0, 30) == 60.0, "큰 지수도 상한 고정(오버플로 아님)"
    assert _error_backoff_s(0.5, 1) == 1.0


class _RecordingStop:
    """threading.Event 대역 — boot 루프의 wait 인자(대기시간)를 기록하고 N회 후 정지."""

    def __init__(self, stop_after: int) -> None:
        self._ev = threading.Event()
        self.waits: list[float] = []
        self._stop_after = stop_after

    def is_set(self) -> bool:
        return self._ev.is_set()

    def set(self) -> None:
        self._ev.set()

    def wait(self, timeout=None) -> bool:
        self.waits.append(timeout)
        if len(self.waits) >= self._stop_after:
            self._ev.set()
        return self._ev.is_set()


def test_boot_loop_backs_off_on_errors_only_and_resets(ledger):
    """연속 오류 → 지수 증가 / 회복(정상 유휴) → poll_interval 로 즉시 리셋(유휴 백오프 없음)."""
    daemon = _daemon(
        ledger,
        command_source=FlakyCommandSource(fail_times=2),
        poll_interval_s=0.5,
    )
    # 워커/heartbeat 를 끄고(대기 기록 오염 방지) 메인 루프 wait 만 관측한다.
    daemon._start_sender = lambda: None  # type: ignore[method-assign]
    stop = _RecordingStop(stop_after=4)
    daemon._stop = stop  # type: ignore[assignment]

    daemon.boot()  # stop_after 회 대기 후 자체 종료.

    # 오류1(2^1)·오류2(2^2) → 회복 후 정상 유휴는 기존 간격 유지(리셋).
    assert stop.waits == [1.0, 2.0, 0.5, 0.5]


def test_boot_loop_error_backoff_caps_at_60s(ledger):
    """상한 60s — poll_interval 이 커도(또는 오류가 길어도) 대기시간이 60 을 넘지 않는다."""
    daemon = _daemon(
        ledger,
        command_source=FlakyCommandSource(fail_times=99),
        poll_interval_s=20.0,
    )
    daemon._start_sender = lambda: None  # type: ignore[method-assign]
    stop = _RecordingStop(stop_after=3)
    daemon._stop = stop  # type: ignore[assignment]

    daemon.boot()

    assert stop.waits == [40.0, 60.0, 60.0]


# ─────────────────────────────────────────────────────────────────────────────
# 수정 5 — SSE connect/read 분리 가드 + 트리클 워치독
# ─────────────────────────────────────────────────────────────────────────────


def test_is_stale_pure_function():
    """마지막 라인 후 limit 초 **초과**면 스테일(경계 포함 아님)."""
    assert _is_stale(now=100.0, last=5.0, limit=90.0) is True
    assert _is_stale(now=95.0, last=5.0, limit=90.0) is False, "정확히 limit 는 스테일 아님"
    assert _is_stale(now=10.0, last=5.0, limit=90.0) is False


def test_open_sse_connect_timeout_still_streams():
    """connect/read 분리(connect_timeout 지정)로도 SSE 프레임 파싱 동일 — 실 소켓."""
    with FakeHttpServer() as srv:
        srv.set_handler(lambda req: {"sse": [("snapshot", '{"n": 1}')]})
        with open_sse(
            f"{srv.base_url}/stream", timeout=5.0, connect_timeout=2.0
        ) as stream:
            events = list(stream.events())
        assert events == [("snapshot", '{"n": 1}')]


def test_upgrade_read_timeout_guarded_fallback_harmless():
    """가드된 소켓 접근 — CPython 세부 미보유(가짜 응답)여도 예외 없이 폴백(무해)."""
    _upgrade_read_timeout(object(), 60.0)  # AttributeError 삼킴 — raise 없으면 성공.

    # 세부 구조를 갖춘 가짜 — settimeout 이 read 타임아웃 값으로 호출된다.
    class _Sock:
        def __init__(self) -> None:
            self.timeouts: list = []

        def settimeout(self, t) -> None:
            self.timeouts.append(t)

    sock = _Sock()
    resp = types.SimpleNamespace(fp=types.SimpleNamespace(raw=types.SimpleNamespace(_sock=sock)))
    _upgrade_read_timeout(resp, 60.0)
    assert sock.timeouts == [60.0]


class _FakeRawResp:
    """SseStream 하부 응답 대역 — 라인(bytes) 순회 + close 기록."""

    def __init__(self, lines: list[bytes]) -> None:
        self._lines = lines
        self.closed = False

    def __iter__(self):
        return iter(self._lines)

    def close(self) -> None:
        self.closed = True


def test_sse_stream_updates_last_line_on_every_line_including_comments():
    """주석 heartbeat(`:`) 라인도 관측점(last_line_monotonic)을 갱신 — 트리클 워치독 근거."""
    stream = SseStream(_FakeRawResp([b": hb\n", b": hb\n"]))
    before = stream.last_line_monotonic
    time.sleep(0.02)
    assert list(stream.events()) == [], "주석만으로는 프레임 미방출(기존 동작 불변)"
    assert stream.last_line_monotonic > before, "모든 라인에서 관측점 갱신"


def test_watchdog_closes_stale_stream(monkeypatch, ledger):
    """트리클 스테일(마지막 라인 90s 초과) → 워치독이 stream.close() — 재연결 유도."""
    monkeypatch.setattr(sse_mod, "WATCHDOG_CHECK_INTERVAL_S", 0.01)
    adapter = SseCommandSourceAdapter(base_url="http://web:3000")
    stream = SseStream(_FakeRawResp([]))
    stream.last_line_monotonic = time.monotonic() - 1000.0  # 스테일 재현.

    stop = adapter._start_watchdog(stream)
    try:
        deadline = time.time() + 2.0
        while not stream._resp.closed and time.time() < deadline:
            time.sleep(0.005)
        assert stream._resp.closed, "스테일 스트림은 워치독이 강제 close"
    finally:
        stop.set()


def test_watchdog_inactive_without_observation_point():
    """관측점(last_line_monotonic) 없는 fake 스트림 — 워치독 미기동(기존 동작·무해)."""
    adapter = SseCommandSourceAdapter(base_url="http://web:3000")

    class _NoAttrStream:
        def close(self) -> None:  # pragma: no cover — 호출되면 안 됨.
            raise AssertionError("워치독이 기동되면 안 된다")

    stop = adapter._start_watchdog(_NoAttrStream())  # type: ignore[arg-type]
    time.sleep(0.05)
    stop.set()  # 반환 Event 는 정상 사용 가능(무해).
