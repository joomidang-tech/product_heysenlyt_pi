"""OfflineQueue 테스트 — SoT §4-6 / 질의서 Q5(OQ-04·CS-07). Dart `test/offline_queue_test.dart` 포팅.

단절 중 적재·재연결 FIFO flush·멱등(at-least-once·서버 dedup)·fetchSince cursor 누락보정.
"""

from senlyt_pi.core.wire_messages import StatusReport
from senlyt_pi.pipeline.offline_queue import OfflineQueue


def rep(id_: str, phase: str, k: int) -> StatusReport:
    return StatusReport(
        id=id_,
        phase=phase,
        step_k=k,
        step_n=3,
        error_code=None,
        request_id=f"req-{id_}-{phase}-{k}",
        trace_id="t",
        updated_at=f"2026-07-03T00:00:0{k}.000Z",
    )


def test_enqueue_while_offline_flush_only_online():
    """단절 중 적재 — flush 는 online 일 때만 전송."""
    oq = OfflineQueue()
    oq.disconnect()
    oq.enqueue(rep("o:1", "PROGRESS", 1))
    oq.enqueue(rep("o:1", "PROGRESS", 2))
    assert oq.depth == 2

    sent: list[str] = []
    # 단절 중 flush → 0.
    assert oq.flush(lambda r: sent.append(r.phase) or True) == 0
    assert oq.depth == 2

    # 재연결 후 flush → FIFO 순서.
    oq.reconnect()
    sent.clear()

    def send(r: StatusReport) -> bool:
        sent.append(f"{r.phase}-{r.step_k}")
        return True

    assert oq.flush(send) == 2
    assert sent == ["PROGRESS-1", "PROGRESS-2"]
    assert oq.depth == 0


def test_idempotent_no_reenqueue_after_success():
    """멱등 — 이미 성공 전송된 서명 재적재 안 함."""
    oq = OfflineQueue()
    oq.enqueue(rep("o:1", "PROGRESS", 1))
    oq.flush(lambda r: True)
    # 동일 (id, phase, stepK) 재적재 → 무시.
    oq.enqueue(rep("o:1", "PROGRESS", 1))
    assert oq.depth == 0


def test_flush_failure_preserves_order():
    """flush 실패 시 순서 보존 — 실패 지점에서 멈추고 재시도."""
    oq = OfflineQueue()
    oq.enqueue(rep("o:1", "ACCEPTED", 0))
    oq.enqueue(rep("o:1", "PROGRESS", 1))
    oq.enqueue(rep("o:1", "PROGRESS", 2))

    # 두 번째 전송 실패.
    sent1 = oq.flush(lambda r: r.step_k != 1)
    assert sent1 == 1  # ACCEPTED 만 성공.
    assert oq.depth == 2  # PROGRESS 1,2 남음.

    # 재시도 — 이번엔 전부 성공.
    sent2 = oq.flush(lambda r: True)
    assert sent2 == 2
    assert oq.depth == 0


def test_at_least_once_safe_on_raise():
    """at-least-once 안전 — send raise 시 큐 유지(멱등 재전송)."""
    oq = OfflineQueue()
    oq.enqueue(rep("o:1", "PROGRESS", 1))

    def boom(r: StatusReport) -> bool:
        raise RuntimeError("network")

    assert oq.flush(boom) == 0
    assert oq.depth == 1


def test_fetch_since_cursor():
    """fetchSince cursor — createdAt > cursor 만 fresh(OQ-04 누락보정)."""
    oq = OfflineQueue()
    assert oq.is_after_cursor("2026-07-03T00:00:01.000Z") is True  # cursor 없음 → 전부 fresh.
    oq.advance_cursor("2026-07-03T00:00:05.000Z")
    assert oq.is_after_cursor("2026-07-03T00:00:04.000Z") is False  # 이미 처리분.
    assert oq.is_after_cursor("2026-07-03T00:00:06.000Z") is True  # 놓친 후속.


def test_cursor_monotonic():
    """cursor 단조 전진 — 역행 무시."""
    oq = OfflineQueue()
    oq.advance_cursor("2026-07-03T00:00:05.000Z")
    oq.advance_cursor("2026-07-03T00:00:03.000Z")  # 더 이른 값 무시.
    assert oq.cursor == "2026-07-03T00:00:05.000Z"


def test_max_depth_drops_oldest():
    """maxDepth 초과 시 FIFO 앞부분 드롭(폭주 방어)."""
    oq = OfflineQueue(max_depth=2)
    oq.enqueue(rep("o:1", "PROGRESS", 1))
    oq.enqueue(rep("o:1", "PROGRESS", 2))
    oq.enqueue(rep("o:1", "PROGRESS", 3))
    assert oq.depth == 2  # 가장 오래된 것 드롭.
