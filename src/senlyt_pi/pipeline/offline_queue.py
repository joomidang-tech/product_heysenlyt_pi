"""Offline Queue (OQ) + resync — SoT §4-6 / 질의서 Q5(OQ-04·CS-07) / §8-3.

Dart `lib/pipeline/offline_queue.dart` 포팅.

**단절 중 적재·진행 계속**: 네트워크 단절 시 status 역보고를 로컬 큐에 FIFO 적재하고 제조는
계속한다(관측이 제조를 막지 않는다·§10-6). 재연결 시 **FIFO flush**.

**멱등 flush(at-least-once)**: flush 는 재전송해도 무해해야 한다 — 서버가 requestId 로 dedup(§4-6).
  OQ 재시도는 requestId 만 싣고 expectedFrom 미포함 → 서버 CAS 스킵(§4-3). 동일 (id, phase) 1회 보장.

**fetchSince 누락보정(Q5·OQ-04)**: server-mediated uplink 로 A/B(MQTT/Firestore) 선택을 흡수.
  재연결 후 `fetch_since(createdAt > cursor)` 로 단절 중 놓친 command 를 결정적으로 복원(누락 0).

이 큐는 순수 인메모리+영속 훅(선택). 실 http flush 는 StatusSinkPort 어댑터가 담당하고,
이 클래스는 큐잉·FIFO·dedup 키 관리·cursor 만 책임(테스트로 완전 검증 가능).
"""

from __future__ import annotations

from collections import deque
from typing import Callable

from ..core.wire_messages import StatusReport

# flush 시 각 항목을 실제 전송하는 콜백(성공 시 True → 큐에서 제거).
#
# at-least-once: False/raise 면 항목을 큐에 남겨 다음 flush 재시도(멱등이라 안전).
StatusSender = Callable[[StatusReport], bool]


class OfflineQueue:
    """Offline Queue."""

    def __init__(self, *, max_depth: int = 1000) -> None:
        # 큐 상한(폭주 방어). 초과 시 가장 오래된 항목부터 드롭(진행보고 최신성 우선).
        self.max_depth = max_depth
        self._queue: deque[StatusReport] = deque()
        # 이미 flush 성공한 (id, phase, stepK) 서명 — 로컬 중복 방출 억제(서버 dedup 이중화).
        self._sent_signatures: set[str] = set()
        # resync cursor — 마지막으로 성공 소비한 command.createdAt(ISO8601). fetch_since 기준.
        self._cursor: str | None = None
        # 온라인 여부(단절 시뮬레이션).
        self.online = True

    @property
    def depth(self) -> int:
        return len(self._queue)

    @property
    def is_empty(self) -> bool:
        return not self._queue

    @property
    def cursor(self) -> str | None:
        return self._cursor

    @staticmethod
    def _sig(r: StatusReport) -> str:
        return f"{r.id}|{r.phase}|{r.step_k}"

    def enqueue(self, report: StatusReport) -> None:
        """status 보고를 적재(단절 여부와 무관 — flush 시 online 판정).

        로컬 dedup: 이미 성공 전송된 서명은 재적재하지 않는다(멱등·OQ 폭주 완화).
        """
        if self._sig(report) in self._sent_signatures:
            return
        self._queue.append(report)
        # 상한 초과 시 FIFO 앞부분 드롭(최신 진행 우선).
        while len(self._queue) > self.max_depth:
            self._queue.popleft()

    def flush(self, send: StatusSender) -> int:
        """재연결 flush — FIFO 순서로 [send] 호출. 멱등(서버 dedup)·at-least-once.

        online=False 면 아무 것도 보내지 않고 그대로 유지(단절 중 진행 계속).
        반환 = 이번 flush 로 성공 전송된 항목 수.
        """
        if not self.online:
            return 0
        sent = 0
        # FIFO — 앞에서부터. 실패 항목은 남기고 그 뒤도 계속 시도하지 않는다(순서 보존·재시도).
        while self._queue:
            head = self._queue[0]
            sig = self._sig(head)
            if sig in self._sent_signatures:
                # 이미 성공(재적재 방어망) — 조용히 제거.
                self._queue.popleft()
                continue
            try:
                ok = send(head)
            except Exception:
                ok = False
            if not ok:
                break  # 순서 보존 — 실패 지점에서 멈추고 다음 flush 재시도.
            self._queue.popleft()
            self._sent_signatures.add(sig)
            sent += 1
        return sent

    def advance_cursor(self, created_at_iso: str) -> None:
        """성공 소비한 command 의 createdAt 로 cursor 전진(resync 기준·§9-1 createdAt).

        단조 전진만(더 이른 createdAt 은 무시) — 재연결 중복 수신 시 cursor 역행 방지.
        """
        cur = self._cursor
        if cur is None or created_at_iso > cur:
            self._cursor = created_at_iso

    def is_after_cursor(self, created_at_iso: str) -> bool:
        """fetch_since 대상 판별(Q5·OQ-04): createdAt > cursor 인 command 만 재처리 대상.

        ISO8601 밀리초 Z 고정(§5-3·부록A P-3) → 문자열 비교가 시간 비교와 동치.
        """
        cur = self._cursor
        if cur is None:
            return True  # cursor 없으면 전부 fresh.
        return created_at_iso > cur

    def reconnect(self) -> None:
        """재연결 신호."""
        self.online = True

    def disconnect(self) -> None:
        """단절 신호(테스트/실제)."""
        self.online = False
