"""파일 영속 멱등 Ledger — SoT §4-6(pi측 dedup) / 부록A P-2 / 질의서 Q1(IL-04·CR-06).

Dart `lib/persistence/file_idempotency_ledger.dart` 포팅.

**IL-02 게이트(중복토출0)의 물리 보증**: 합성키 `{orderId}:{attempt}` 를 기준으로
[LedgerEntryState] 4상태 **전부**(RECEIVED·RUNNING·DONE·FAILED)를 **한번 본 id = DROP**.
  - Q1(계승): 멱등 DROP 집합에 **FAILED 포함**. 재주문은 attempt 증가로 새 command.id 를 만들어
    fresh 판정을 받는다(status-only 되돌림 금지·§4-4). 같은 합성키는 실패했어도 재토출 안 함.

**crash-safe 영속(부록A·CR-01)**: append-only 로그를 매 write 마다 fsync 로 원자 영속한다.
  - 각 라인 = 1 JSON 레코드(개행 구분). temp 파일 replace atomic swap 은 컴팩션 시 사용.
  - 재부팅 시 로그를 재생(replay)해 마지막 상태를 복원 → on_boot recovery(§9-1) 판단 근거.

순수 표준라이브러리(외부 의존 0). SQLite 대신 append+fsync 로그 — 단일 라이터(pi 데몬) 전제.
"""

from __future__ import annotations

import enum
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .idempotency_ledger import LedgerVerdict


class LedgerEntryState(enum.Enum):
    """Ledger 4상태 — SoT §4-6 / 질의서 Q1. **전부 DROP 집합**(fresh 는 미기록 키에만)."""

    # 명령 수신·예약(claim). 아직 제조 시작 전.
    RECEIVED = "RECEIVED"
    # 제조 진행 중. 재부팅 시 INTERRUPTED 판정 대상(CR-01).
    RUNNING = "RUNNING"
    # 제조 완료(성공 종결).
    DONE = "DONE"
    # 제조 실패 종결. **DROP 집합 포함**(재주문은 새 attempt).
    FAILED = "FAILED"

    @property
    def wire(self) -> str:
        return self.value

    @staticmethod
    def from_wire(v: Any) -> "LedgerEntryState | None":
        if not isinstance(v, str):
            return None
        for s in LedgerEntryState:
            if s.wire == v:
                return s
        return None


@dataclass(frozen=True, slots=True)
class LedgerRecord:
    """Ledger 레코드(replay 로 최종 상태 복원)."""

    command_id: str
    state: LedgerEntryState
    ts: str  # ISO8601 (관찰/디버그용, 판정 무관)
    trace_id: str = ""  # claim 시 기록하는 원 주문 traceId(재기동 복구 상관용·부록A P-3).

    def to_json(self) -> dict[str, Any]:
        m: dict[str, Any] = {
            "commandId": self.command_id,
            "state": self.state.wire,
            "ts": self.ts,
        }
        # traceId 는 값이 있을 때만 방출 — 구엔트리(traceId 없음) 와이어 형태와 동형 유지.
        if self.trace_id:
            m["traceId"] = self.trace_id
        return m


def _default_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class FileIdempotencyLedger:
    """파일 append+fsync 영속 Ledger (IdempotencyLedger 프로토콜 충족).

    check_and_claim: 미기록 키 → FRESH 로 RECEIVED 예약(원자 append+fsync).
      기록된 키(4상태 전부) → DUPLICATE.
    mark_running/mark_settled: 상태 전이 append. is_settled/state_of: replay 인덱스 조회.
    """

    def __init__(
        self,
        path: Path,
        fh: Any,
        index: dict[str, LedgerEntryState],
        trace_index: dict[str, str] | None = None,
    ) -> None:
        """직접 호출 금지 — `FileIdempotencyLedger.open(path)` 사용."""
        self._path = path
        self._fh = fh  # append 바이너리 핸들
        # commandId → 현재(최신) 상태. replay 로 구성·write 마다 갱신.
        self._index = index
        # commandId → claim 시 기록한 원 traceId. replay 로 복원(재기동 복구 상관용).
        self._trace_index: dict[str, str] = trace_index if trace_index is not None else {}
        # 시계 주입(테스트 결정성). 기본 = UTC now ISO8601.
        self.now_iso: Callable[[], str] = _default_now_iso

    @staticmethod
    def open(path: str | Path) -> "FileIdempotencyLedger":
        """로그 파일을 열고(없으면 생성) replay 하여 인메모리 인덱스를 복원한다."""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        index: dict[str, LedgerEntryState] = {}
        trace_index: dict[str, str] = {}

        if p.exists():
            with p.open("r", encoding="utf-8") as f:
                for line in f:
                    trimmed = line.strip()
                    if not trimmed:
                        continue
                    try:
                        parsed = json.loads(trimmed)
                    except ValueError:
                        # 부분 프레임(전원 단절 중 잘린 마지막 라인) — 무시하고 계속(crash-safe).
                        continue
                    if not isinstance(parsed, dict):
                        continue
                    cid = parsed.get("commandId")
                    state = LedgerEntryState.from_wire(parsed.get("state"))
                    if isinstance(cid, str) and state is not None:
                        index[cid] = state  # 마지막 승자(append 순서 = 시간 순서).
                        # traceId 는 있는 레코드에서만 갱신(구엔트리·미보유 레코드는 clobber 금지).
                        tid = parsed.get("traceId")
                        if isinstance(tid, str) and tid:
                            trace_index[cid] = tid

        # append 모드로 열되, replay 후 이어쓰기.
        fh = p.open("ab")
        return FileIdempotencyLedger(p, fh, index, trace_index)

    def _append(self, rec: LedgerRecord) -> None:
        line = json.dumps(rec.to_json(), ensure_ascii=False) + "\n"
        self._fh.write(line.encode("utf-8"))
        self._fh.flush()
        os.fsync(self._fh.fileno())  # fsync — 원자 영속(crash-safe).
        self._index[rec.command_id] = rec.state
        # traceId 는 값이 있을 때만 갱신(미보유 전이 레코드가 claim 시 traceId 를 clobber 금지).
        if rec.trace_id:
            self._trace_index[rec.command_id] = rec.trace_id

    def check_and_claim(self, command_id: str, trace_id: str = "") -> LedgerVerdict:
        # 4상태 전부 DROP — 한번 본 합성키면 fresh 아님(Q1·IL-02).
        if command_id in self._index:
            return LedgerVerdict.DUPLICATE
        self._append(
            LedgerRecord(
                command_id=command_id,
                state=LedgerEntryState.RECEIVED,
                ts=self.now_iso(),
                trace_id=trace_id,  # claim 시 원 traceId 영속(재기동 복구 상관·빈값=미보유).
            )
        )
        return LedgerVerdict.FRESH

    def mark_running(self, command_id: str) -> None:
        """RECEIVED → RUNNING 전이(제조 시작). 재부팅 시 INTERRUPTED 판정 근거(CR-01)."""
        self._append(
            LedgerRecord(command_id=command_id, state=LedgerEntryState.RUNNING, ts=self.now_iso())
        )

    def mark_settled(self, command_id: str, *, success: bool) -> None:
        self._append(
            LedgerRecord(
                command_id=command_id,
                state=LedgerEntryState.DONE if success else LedgerEntryState.FAILED,
                ts=self.now_iso(),
            )
        )

    def is_settled(self, command_id: str) -> bool:
        s = self._index.get(command_id)
        return s is LedgerEntryState.DONE or s is LedgerEntryState.FAILED

    def state_of(self, command_id: str) -> LedgerEntryState | None:
        """현재 상태 조회(on_boot recovery 판단용·§9-1). 미기록 = None."""
        return self._index.get(command_id)

    def trace_id_of(self, command_id: str) -> str:
        """claim 시 기록한 원 traceId 조회(재기동 복구 보고 상관용). 미보유 = ""(하위호환)."""
        return self._trace_index.get(command_id, "")

    def commands_in_state(self, state: LedgerEntryState) -> list[str]:
        """특정 상태의 모든 commandId(재부팅 복구 스캔용)."""
        return [cid for cid, s in self._index.items() if s is state]

    def running_commands(self) -> list[str]:
        """진행 중(RUNNING) 합성키 목록 — CR-01: RUNNING→INTERRUPTED 대상."""
        return self.commands_in_state(LedgerEntryState.RUNNING)

    def received_commands(self) -> list[str]:
        """RECEIVED(수신했으나 미시작) 목록 — CR: 클리어 후 fresh 재실행 대상."""
        return self.commands_in_state(LedgerEntryState.RECEIVED)

    def close(self) -> None:
        self._fh.close()

    def compact(self) -> None:
        """로그 컴팩션(선택) — 최신 상태만 남겨 temp 로 쓰고 atomic replace swap.

        crash-safe: temp 완성·fsync 후에만 replace. 실패 시 원본 유지.
        (Windows 는 열린 파일 위로 replace 불가 → 원본 핸들을 먼저 닫는다 — Dart 와 동일 순서.)
        """
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        with tmp.open("wb") as sink:
            for cid, state in self._index.items():
                rec = LedgerRecord(
                    command_id=cid,
                    state=state,
                    ts=self.now_iso(),
                    trace_id=self._trace_index.get(cid, ""),  # 컴팩션에도 traceId 보존.
                )
                sink.write((json.dumps(rec.to_json(), ensure_ascii=False) + "\n").encode("utf-8"))
            sink.flush()
            os.fsync(sink.fileno())
        self._fh.close()
        os.replace(tmp, self._path)  # atomic swap.
        self._fh = self._path.open("ab")
