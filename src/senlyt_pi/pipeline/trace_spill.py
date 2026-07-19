"""TraceSpill — 관측 로그(pi.log.*·dispense 스팬)의 **디스크 스풀** (단절 유실 0 · 2026-07-19).

왜 필요한가: 상태 역보고는 OfflineQueue 로 단절을 버티지만, 관측 로그(trace 스팬)는
best-effort 라 **전송 실패한 배치가 그대로 버려졌다** — 긴 단절 구간의 DEBUG/INFO 가 서버에
영영 없고, 그 구간을 보려면 실기기 journalctl 로 가야 했다. 운영 원칙은 "서버가 전부"
(로컬 로그를 아예 볼 일 없게)이므로, 전송 실패분을 여기(디스크 JSONL)에 쌓았다가
재연결 시 FIFO 로 전량 업로드한다. 데몬 재시작을 넘어도 살아남는다(파일).

설계:
  - 저장 단위 = **직렬화된 span dict**(와이어 그대로) 1줄 1 JSON — 재전송 시 재직렬화 불필요,
    스키마 진화에도 파일이 낡은 TraceSpan 클래스에 묶이지 않는다.
  - `append` = 파일 끝 append + fsync(배치당 1회·수 ms) — **전체 파일을 읽지 않는다**(줄 수는
    메모리 캐시 `_count`). 상한 초과는 매 append 가 아니라 **슬랙(_TRIM_SLACK) 넘칠 때만**
    오래된 것부터 잘라내는 FIFO trim(원자적 tmp+rename) — O(n) 재작성이 상시가 아니라
    슬랙당 1회로 상각된다(리뷰 P1-2). 잘린 건수는 `pop_dropped()` 로 회수해 호출자가 합성
    WARN 으로 서버에 표면화한다(조용한 유실 금지).
  - `drain` = **락 밖 전송**(리뷰 P1-1): 파일 락(_lock)은 스냅샷 읽기와 소비 반영(재작성)
    순간에만 쥐고, 네트워크 전송(send 콜백·타임아웃 수 초) 동안은 놓는다 → 전송이 걸려도
    `append`(로깅 hot path 인접)가 블록되지 않는다. 드레인 자체는 `_drain_lock` 으로 단일
    진입(동시 드레인의 중복 전송 방지). 전송 중 append 로 늘어난 꼬리는 보존되고, 전송 중
    trim 으로 앞이 잘렸으면 `_front_trimmed` 로 보정해 **과소거(유실)를 막는다**(중복 전송은
    at-least-once 로 수용 — 서버 trace 는 spanId 있는 관측 데이터라 중복에 관대).
  - 첫 실패에서 멈추고 나머지를 보존(FIFO). `max_batches` 로 한 번의 드레인 양을 상한해
    sender 사이클이 대형 스풀에 통째로 잡히지 않게 한다(리뷰 P2-5).
  - 깨진 줄(부분 기록·크래시 잔재)은 조용히 건너뛴다 — 스풀이 스풀을 못 읽어 죽으면 본말전도.

⛔ 불변(데드락 방지): `send` 콜백 안에서 **StructuredLogger 로 로그를 찍지 말 것.**
  로거 sink(daemon._ship_log)는 `_trace_lock` 을 잡고, overflow 배출이 이 스풀에 닿는다 —
  send 가 로거를 부르면 락 역전(AB-BA) 재료가 된다. 전송 실패는 반환값(False)으로만 알린다.
"""

from __future__ import annotations

import json
import os
import threading
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

# 스풀 상한(스팬 수). 1줄 ≈ 300~500B → 20k ≈ 6~10MB. 초과분은 오래된 것부터 trim.
DEFAULT_MAX_SPANS = 20_000
# trim 히스테리시스 — 상한 + 이만큼 넘칠 때만 O(n) 재작성(상각). 잘라낼 땐 max_spans 로.
_TRIM_SLACK = 1_024


class TraceSpill:
    """파일 기반 trace 스풀 — append 는 싸게(카운트 캐시·no full read), 전송은 락 밖에서."""

    def __init__(self, path: Path, *, max_spans: int = DEFAULT_MAX_SPANS) -> None:
        self.path = Path(path)
        self.max_spans = max(1, max_spans)
        # _lock = 파일·카운터 보호(짧게만 쥔다). _drain_lock = 드레인 단일 진입(전송 포함).
        self._lock = threading.Lock()
        self._drain_lock = threading.Lock()
        self._count: int | None = None  # 줄 수 캐시(깨진 줄 포함 근사) — lazy 초기화.
        self._dropped = 0  # trim/디스크실패로 잃은 스팬 수 — pop_dropped 로 회수(합성 WARN 재료).
        self._front_trimmed = 0  # 드레인 스냅샷 이후 trim 이 앞에서 잘라낸 줄 수(과소거 보정).

    # ── 조회 ───────────────────────────────────────────────────────────────
    @property
    def depth(self) -> int:
        """스풀에 남은 줄 수(캐시·깨진 줄 포함 근사). 파일이 없으면 0."""
        with self._lock:
            return self._ensure_count_locked()

    def pop_dropped(self) -> int:
        """trim 누적 드롭 건수를 회수(반환 후 0으로) — 호출자가 합성 WARN 으로 표면화."""
        with self._lock:
            n = self._dropped
            self._dropped = 0
            return n

    def restore_dropped(self, n: int) -> None:
        """회수했던 드롭 건수를 되돌린다 — 합성 WARN 전송 실패 시 신호가 조용히 소실되지
        않게(리뷰 P2-4). 다음 flush 가 다시 회수해 재시도한다."""
        if n <= 0:
            return
        with self._lock:
            self._dropped += n

    # ── 적재 ───────────────────────────────────────────────────────────────
    def append(self, span_dicts: Sequence[dict[str, Any]]) -> None:
        """직렬화된 span dict 들을 스풀 끝에 추가(파일 append + fsync — full read 없음)."""
        if not span_dicts:
            return
        with self._lock:
            n = self._ensure_count_locked()
            try:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                with self.path.open("a", encoding="utf-8") as f:
                    for d in span_dicts:
                        f.write(json.dumps(d, ensure_ascii=False) + "\n")
                    f.flush()
                    os.fsync(f.fileno())  # 단절/크래시 대비 스풀 — 내구성이 존재 이유다.
            except OSError:
                # 디스크 실패(가득참·읽기전용)로 관측이 제조를 막으면 안 된다 — 이번 배치는
                #   유실로 계수(다음 drain 의 합성 WARN 으로 서버에 보인다).
                self._dropped += len(span_dicts)
                return
            self._count = n + len(span_dicts)
            # 히스테리시스 trim — 슬랙까지는 그냥 쌓고, 넘칠 때만 1회 재작성(O(n) 상각).
            if self._count > self.max_spans + _TRIM_SLACK:
                self._trim_locked()

    def _trim_locked(self) -> None:
        lines = self._read_lines()
        self._count = len(lines)
        if len(lines) <= self.max_spans:
            return
        cut = len(lines) - self.max_spans
        self._dropped += cut
        self._front_trimmed += cut  # 진행 중 드레인의 소비 반영 보정(과소거 방지).
        self._rewrite_locked(lines[cut:])

    # ── 배출 ───────────────────────────────────────────────────────────────
    def drain(
        self,
        send: Callable[[list[dict[str, Any]]], bool],
        *,
        batch_max: int = 100,
        max_batches: int | None = None,
    ) -> int:
        """FIFO 배치 전송 — 첫 실패에서 멈추고 잔여 보존. 성공 전송 스팬 수 반환.

        `send(batch) -> bool` = 한 배치의 전송 성공 여부. ⛔ send 안에서 로거 사용 금지(모듈
        docstring 불변). **전송은 락 밖** — 스냅샷(락) → 전송(락 없음) → 소비 반영(락).
        `max_batches` 로 한 번에 배출할 양을 상한(None=전량) — sender 사이클 시간 유계.
        """
        with self._drain_lock:
            with self._lock:
                lines = self._read_lines()
                self._count = len(lines)
                self._front_trimmed = 0  # 이 스냅샷 기준으로 보정 카운터 리셋.
            if not lines:
                return 0
            sent_spans = 0
            consumed = 0  # 스냅샷에서 소비(전송 성공)된 줄 수.
            batches = 0
            while consumed < len(lines):
                if max_batches is not None and batches >= max_batches:
                    break
                raw_chunk = lines[consumed : consumed + batch_max]
                batch: list[dict[str, Any]] = []
                for raw in raw_chunk:
                    try:
                        batch.append(json.loads(raw))
                    except ValueError:
                        continue  # 깨진 줄(크래시 잔재) — 건너뛴다(소비로는 계수).
                if batch and not send(batch):  # ← 락 밖 네트워크 I/O.
                    break  # 단절 지속 — 이 배치부터 보존.
                sent_spans += len(batch)
                consumed += len(raw_chunk)
                batches += 1
            if consumed == 0:
                return 0
            with self._lock:
                cur = self._read_lines()
                # 전송 중 trim 이 앞을 잘랐으면 그만큼 덜 지운다(과소거=유실 방지·중복은 수용).
                remove = max(0, consumed - self._front_trimmed)
                remove = min(remove, len(cur))
                self._rewrite_locked(cur[remove:])
                self._count = max(0, len(cur) - remove)
            return sent_spans

    # ── 파일 IO (반드시 _lock 안에서만) ─────────────────────────────────────
    def _ensure_count_locked(self) -> int:
        if self._count is None:
            self._count = len(self._read_lines())
        return self._count

    def _read_lines(self) -> list[str]:
        try:
            with self.path.open("r", encoding="utf-8") as f:
                return [ln.rstrip("\n") for ln in f if ln.strip()]
        except FileNotFoundError:
            return []
        except OSError:
            return []

    def _rewrite_locked(self, lines: list[str]) -> None:
        """잔여 줄로 파일을 원자적 재작성(tmp+rename) — 빈 잔여면 파일 제거."""
        try:
            if not lines:
                self.path.unlink(missing_ok=True)
                self._count = 0
                return
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            with tmp.open("w", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
                f.flush()
                os.fsync(f.fileno())
            tmp.replace(self.path)
            self._count = len(lines)
        except OSError:
            pass  # 재작성 실패 — 다음 drain 이 재시도(중복 전송 가능·수용).
