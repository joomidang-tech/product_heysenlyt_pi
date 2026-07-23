"""TraceSpill(관측 로그 디스크 스풀) — 단절 유실 0 회귀 앵커 (2026-07-19).

배경: 관측 로그(pi.log.*)는 best-effort 라 전송 실패 배치가 그대로 버려졌다 — 긴 단절 구간의
DEBUG/INFO 는 서버에 없고 journalctl 로 가야 했다. 운영 원칙 = **"서버가 전부"**(로컬 로그를
아예 볼 일 없게). 그래서 전송 실패분을 디스크 JSONL 에 스풀했다가 재연결 시 FIFO 업로드한다.

이 파일이 지키는 것:
  A. TraceSpill 단위 — append/drain FIFO·부분 실패 보존·상한 trim(+드롭 계수)·깨진 줄 내성
  B. 어댑터 통합 — 단절 중 ship_trace → 디스크 보존 → 재연결 flush 로 전량·순서대로 서버 합류
  C. 재시작 생존 — 스풀 파일만 있으면 새 어댑터(재부팅 모사)가 이어서 업로드
  D. 스풀 미주입(구 어댑터·Fake) — 종전 best-effort 동작 그대로(하위호환)
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

from senlyt_pi.adapters.http_client import HttpTransportError
from senlyt_pi.adapters.http_status_sink_adapter import HttpStatusSinkAdapter
from senlyt_pi.pipeline.trace_spill import TraceSpill
from senlyt_pi.ports.status_sink_port import TraceSpan


def _span(i: int) -> TraceSpan:
    return TraceSpan(
        ts=f"2026-07-19T00:00:{i:02d}.000Z",
        trace_id=f"t-{i}",
        span_id=f"s-{i}",
        service="pi",
        event="pi.log.info",
        level="INFO",
        device_id="dev-1",
        detail={"message": f"m{i}", "stage": "obs"},
    )


class FlakyTransport:
    """온/오프 전환 가능한 전송 seam — 성공 배치 body 를 기록한다."""

    def __init__(self, *, up: bool) -> None:
        self.up = up
        self.sent_batches: list[list[dict]] = []

    def __call__(self, method, url, *, body=None, headers=None, timeout=None):
        if not self.up:
            raise HttpTransportError("단절")
        self.sent_batches.append(list(body["logs"]))
        return (200, {"ok": True})

    @property
    def sent_ids(self) -> list[str]:
        return [d["spanId"] for b in self.sent_batches for d in b]


def _sink(tmp_path: Path, transport: FlakyTransport, *, spill: bool = True) -> HttpStatusSinkAdapter:
    return HttpStatusSinkAdapter(
        base_url="http://server",
        bearer_token="tok",
        trace_spill=TraceSpill(tmp_path / "trace-spill.jsonl") if spill else None,
        request=transport,
    )


# ── A. TraceSpill 단위 ──────────────────────────────────────────────────────────


class TestTraceSpillUnit:
    def test_append_then_drain_fifo_and_file_removed(self, tmp_path):
        sp = TraceSpill(tmp_path / "s.jsonl")
        sp.append([{"spanId": "a"}, {"spanId": "b"}, {"spanId": "c"}])
        assert sp.depth == 3
        got: list[dict] = []
        sent = sp.drain(lambda batch: (got.extend(batch), True)[1], batch_max=2)
        assert sent == 3
        assert [d["spanId"] for d in got] == ["a", "b", "c"]  # FIFO 순서.
        assert sp.depth == 0
        assert not (tmp_path / "s.jsonl").exists()  # 다 비우면 파일 제거.

    def test_partial_failure_keeps_remainder_in_order(self, tmp_path):
        sp = TraceSpill(tmp_path / "s.jsonl")
        sp.append([{"spanId": f"x{i}"} for i in range(5)])
        calls = {"n": 0}

        def send_first_batch_only(batch):
            calls["n"] += 1
            return calls["n"] == 1  # 두 번째 배치부터 단절.

        sent = sp.drain(send_first_batch_only, batch_max=2)
        assert sent == 2
        assert sp.depth == 3  # 실패 배치부터 보존.
        got: list[dict] = []
        sp.drain(lambda b: (got.extend(b), True)[1], batch_max=100)
        assert [d["spanId"] for d in got] == ["x2", "x3", "x4"]  # 순서 유지.

    def test_cap_trims_oldest_and_counts_dropped(self, tmp_path, monkeypatch):
        import senlyt_pi.pipeline.trace_spill as spill_mod

        # trim 은 히스테리시스(_TRIM_SLACK 초과 시에만 O(n) 재작성·상각) — 테스트는 슬랙 0 으로
        #   즉시 발동시켜 FIFO trim·드롭 계수만 검증한다.
        monkeypatch.setattr(spill_mod, "_TRIM_SLACK", 0)
        sp = TraceSpill(tmp_path / "s.jsonl", max_spans=3)
        sp.append([{"spanId": f"o{i}"} for i in range(5)])
        assert sp.depth == 3
        assert sp.pop_dropped() == 2  # 오래된 2건 trim — 조용한 유실 아님(계수→합성 WARN 재료).
        assert sp.pop_dropped() == 0  # 회수 후 0.
        got: list[dict] = []
        sp.drain(lambda b: (got.extend(b), True)[1])
        assert [d["spanId"] for d in got] == ["o2", "o3", "o4"]  # 최신 쪽 생존(FIFO trim).

    def test_max_batches_bounds_one_drain_cycle(self, tmp_path):
        """max_batches — 한 사이클 배출량 상한(대형 스풀이 sender 사이클을 통째로 잡지 않게)."""
        sp = TraceSpill(tmp_path / "s.jsonl")
        sp.append([{"spanId": f"x{i}"} for i in range(5)])
        got: list[dict] = []
        sent = sp.drain(lambda b: (got.extend(b), True)[1], batch_max=2, max_batches=1)
        assert sent == 2 and sp.depth == 3  # 1배치만 배출 — 잔량은 다음 사이클.

    def test_corrupt_line_skipped(self, tmp_path):
        p = tmp_path / "s.jsonl"
        sp = TraceSpill(p)
        sp.append([{"spanId": "ok1"}])
        with p.open("a", encoding="utf-8") as f:
            f.write('{"broken...\n')  # 크래시 잔재 모사(부분 기록).
        sp.append([{"spanId": "ok2"}])
        got: list[dict] = []
        sp.drain(lambda b: (got.extend(b), True)[1])
        assert [d["spanId"] for d in got] == ["ok1", "ok2"]  # 깨진 줄만 조용히 건너뜀.


# ── B. 어댑터 통합 — 단절 → 스풀 → 재연결 전량 합류 ─────────────────────────────


class TestShipTraceWithSpill:
    def test_offline_ship_persists_then_reconnect_flush_uploads_all(self, tmp_path):
        """단절 중 ship_trace 2회 → 디스크 보존(전송 0) → 재연결 flush → 전량·순서대로 서버 합류."""
        tp = FlakyTransport(up=False)
        sink = _sink(tmp_path, tp)
        sink.ship_trace([_span(0), _span(1)])
        sink.ship_trace([_span(2)])
        assert tp.sent_batches == []  # 단절 — 서버로 간 것 없음.
        assert (tmp_path / "trace-spill.jsonl").exists()  # 디스크에 보존(유실 0).

        tp.up = True  # 재연결.
        sent = sink.flush_trace_spill()
        assert sent == 3
        assert tp.sent_ids == ["s-0", "s-1", "s-2"]  # 시간순 보존.
        assert not (tmp_path / "trace-spill.jsonl").exists()  # 스풀 소진.

    def test_new_spans_after_reconnect_go_after_spilled(self, tmp_path):
        """재연결 후 ship_trace — 스풀 잔여를 먼저 배출하고 새 스팬을 보낸다(시간 역전 방지)."""
        tp = FlakyTransport(up=False)
        sink = _sink(tmp_path, tp)
        sink.ship_trace([_span(0)])  # 단절 중 → 스풀.
        tp.up = True
        sink.ship_trace([_span(1)])  # 재연결 후 새 스팬.
        assert tp.sent_ids == ["s-0", "s-1"]  # 스풀 먼저 → 새 스팬.

    def test_still_offline_new_spans_append_behind_spill(self, tmp_path):
        """단절 지속 — 새 스팬도 스풀 뒤에 붙는다(순서 보존·유실 0)."""
        tp = FlakyTransport(up=False)
        sink = _sink(tmp_path, tp)
        sink.ship_trace([_span(0)])
        sink.ship_trace([_span(1)])  # 스풀 잔여 있음 + 여전히 단절.
        tp.up = True
        assert sink.flush_trace_spill() == 2
        assert tp.sent_ids == ["s-0", "s-1"]

    def test_spill_traces_direct_deposit(self, tmp_path):
        """데몬 메모리 버퍼 overflow 배출구 — 전송 시도 없이 곧장 디스크."""
        tp = FlakyTransport(up=True)
        sink = _sink(tmp_path, tp)
        sink.spill_traces([_span(7)])
        assert tp.sent_batches == []  # 전송 안 함(배출만).
        assert sink.flush_trace_spill() == 1
        assert tp.sent_ids == ["s-7"]


# ── E. 동시성 — 전송(드레인) 중 append 가 블록되지 않는다 (리뷰 P1-1 회귀 앵커) ────


class TestConcurrency:
    def test_append_not_blocked_by_slow_send_and_tail_preserved(self, tmp_path):
        """드레인의 네트워크 전송(락 밖) 동안 append(로깅 hot path)가 즉시 반환 + tail 유실 0.

        옛 구조는 drain 이 파일 락을 쥔 채 전송(타임아웃 수 초)해, 버퍼 overflow 배출을 타는
        제조 스레드 로그가 그 시간만큼 스톨했다(리뷰 P1-1). 새 구조 = 스냅샷(락)→전송(락 밖)
        →소비 반영(락). 전송 중 들어온 tail 은 보존돼 다음 드레인에서 나온다.
        """
        sp = TraceSpill(tmp_path / "s.jsonl")
        sp.append([{"spanId": f"a{i}"} for i in range(150)])  # 2배치 분량.
        in_send = threading.Event()
        sent1: list[dict] = []

        def slow_send(batch):
            in_send.set()
            time.sleep(0.3)  # hung-send 모사.
            sent1.extend(batch)
            return True

        t = threading.Thread(target=lambda: sp.drain(slow_send, batch_max=100))
        t.start()
        assert in_send.wait(2.0)
        t0 = time.monotonic()
        sp.append([{"spanId": "tail"}])  # 전송이 걸려 있는 동안의 로깅 배출.
        append_s = time.monotonic() - t0
        t.join(5.0)
        assert append_s < 0.25, f"append 가 전송 락에 물림({append_s:.2f}s) — P1-1 회귀"
        sent2: list[dict] = []
        sp.drain(lambda b: (sent2.extend(b), True)[1])
        ids = [d["spanId"] for d in sent1 + sent2]
        assert "tail" in ids  # 전송 중 tail 보존(유실 0).
        assert {f"a{i}" for i in range(150)} <= set(ids)  # 스냅샷 전량 전송(중복은 수용).


# ── C. 재시작 생존 — 파일이 세션을 넘긴다 ───────────────────────────────────────


class TestSpillSurvivesRestart:
    def test_new_adapter_uploads_previous_session_spill(self, tmp_path):
        tp1 = FlakyTransport(up=False)
        sink1 = _sink(tmp_path, tp1)
        sink1.ship_trace([_span(0), _span(1)])  # 전 세션 — 단절 중 스풀만 남기고 죽음(모사).

        tp2 = FlakyTransport(up=True)
        sink2 = _sink(tmp_path, tp2)  # 재부팅 — 같은 경로의 새 어댑터.
        assert sink2.flush_trace_spill() == 2
        assert tp2.sent_ids == ["s-0", "s-1"]  # 전 세션 로그가 서버 합류(재시작 생존).


# ── D. 스풀 미주입 — 종전 best-effort 하위호환 ──────────────────────────────────


class TestNoSpillBackCompat:
    def test_without_spill_failure_swallowed_no_file(self, tmp_path):
        tp = FlakyTransport(up=False)
        sink = _sink(tmp_path, tp, spill=False)
        sink.ship_trace([_span(0)])  # 예외 없이 삼킴(종전 계약).
        assert not (tmp_path / "trace-spill.jsonl").exists()
        assert sink.flush_trace_spill() == 0  # 스풀 없음 — no-op.
