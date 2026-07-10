"""Fake EnginePort 시뮬레이션 어댑터 — SoT §6-7 / 질의서 §0·Q8(EP-03·EP-09) 객관 판정 근거.

Dart `test/support/fake_engine_port.dart` 포팅 — Python 에서는 adapters 로 승격
(시뮬레이션 어댑터·실기기 없는 개발/테스트 공용. 실 RS485 는 sy01b_engine_adapter TODO).

**P0 게이트의 관찰 렌즈**: dispense 호출 카운터로 IL-02(중복토출0)·CR-01(재기동 자동재실행
금지)·EP-03(빈응답=실패·silent-success 금지)를 **객관 검증**한다.

주입 가능한 결과(scripted): ack(정상 0) / busy(transient) / permanent / timeout / **empty**(무응답).
  - empty(""·무응답) = 실패로 분류되어야 한다(EP-03·EP-09). silent-success 0.

**현실적 스텝 지연(카오스 테스트용)**: 실 시린지 펌프는 aspirate/dispense/initialize 등 각 물리
동작에 수 초가 걸린다. env `SENLYT_FAKE_STEP_DELAY_MS`(기본 0 — 단위테스트 회귀 방지) 만큼 각
동작을 sleep 시켜 제조가 여러 초 걸리게 근사한다. 값>0 이면 docker E2E 의 pi-crash 시나리오가
"제조 중" pi 를 SIGKILL 할 창을 확보한다. 슬립은 짧은 조각으로 분할해 stop 신호(signal_stop)에
즉응한다(취소/우아한 종료 시 블록 최소화). 기본 0 은 즉시 완료라 단위테스트 타이밍에 무영향.
"""

from __future__ import annotations

import enum
import os
import threading
import time
from dataclasses import dataclass
from typing import Mapping

from ..ports.engine_port import EngineDispenseCommand, EngineResult
from ..test_seam.fake_engine_sentinels import FAKE_EMPTY_RAW_CODE, FAKE_TIMEOUT_RAW_CODE

# 각 물리 동작(aspirate/dispense/initialize)의 지연(ms) 주입 env — 기본 0(무지연).
SENLYT_FAKE_STEP_DELAY_MS_ENV = "SENLYT_FAKE_STEP_DELAY_MS"
# stop 신호 즉응을 위한 슬립 분할 조각(초) — 이 간격마다 stop 이벤트를 검사.
_DELAY_SLICE_S = 0.02


def _resolve_step_delay_ms(environ: Mapping[str, str]) -> int:
    """SENLYT_FAKE_STEP_DELAY_MS(기본 0) → 정수 ms. 파싱 실패·음수는 0 으로 안전 폴백."""
    raw = environ.get(SENLYT_FAKE_STEP_DELAY_MS_ENV, "").strip()
    if not raw:
        return 0
    try:
        ms = int(float(raw))
    except ValueError:
        return 0
    return ms if ms > 0 else 0


class FakeEngineOutcome(enum.Enum):
    """엔진에 주입할 시나리오 결과 종류."""

    # 정상 ack — rawErrorCode 0.
    ACK = "ack"
    # busy — transient(SoT §6-7: 재시도 대상). rawErrorCode 1.
    BUSY = "busy"
    # permanent — 즉시중단(SoT §6-7). rawErrorCode 2.
    PERMANENT = "permanent"
    # timeout — transient(SoT §6-7: ENGINE_TIMEOUT·재시도). rawErrorCode = timeout 표식.
    TIMEOUT = "timeout"
    # empty — 빈 응답(""·무응답). EP-03: **실패**로 분류(silent-success 금지).
    #   Fake 는 이를 rawErrorCode(음수 sentinel)로 노출해 재시도층이 실패 처리하는지 검증.
    EMPTY = "empty"


# timeout/empty sentinel 은 test_seam/fake_engine_sentinels.py 에서 공유
# (EngineExecutor 와 동일 상수를 봐야 EP-03 이 성립).


def _outcome_to_result(o: FakeEngineOutcome) -> EngineResult:
    """FakeEngineOutcome → EngineResult 매핑."""
    if o is FakeEngineOutcome.ACK:
        return EngineResult(raw_error_code=0)
    if o is FakeEngineOutcome.BUSY:
        return EngineResult(raw_error_code=1, detail="busy")
    if o is FakeEngineOutcome.PERMANENT:
        return EngineResult(raw_error_code=2, detail="permanent")
    if o is FakeEngineOutcome.TIMEOUT:
        return EngineResult(raw_error_code=FAKE_TIMEOUT_RAW_CODE, detail="timeout")
    return EngineResult(raw_error_code=FAKE_EMPTY_RAW_CODE, detail="")


@dataclass(frozen=True, slots=True)
class DispenseCall:
    """한 번의 dispense 호출 기록(관찰용)."""

    pump_addr: int
    volume_ul: float
    steps: int


class FakeEnginePort:
    """Fake EnginePort — dispense 호출 카운터 + 결과 주입.

    **호출 카운터가 P0 게이트의 진실**: `dispense_count`/`dispense_calls` 로 실제 물리 토출
    시도 횟수를 객관 관찰한다. Ledger DROP·재기동 no-op·empty 실패 시 카운터가 늘지 않아야 한다.
    """

    def __init__(
        self,
        *,
        step_delay_ms: int | None = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        # pumpAddr 별 결과 스크립트(FIFO 큐). 비면 default_outcome 사용.
        self._script_by_addr: dict[int, list[FakeEngineOutcome]] = {}
        # 스크립트가 없을 때의 기본 결과.
        self.default_outcome = FakeEngineOutcome.ACK
        # dispense 호출 이력(P0 관찰 렌즈).
        self.dispense_calls: list[DispenseCall] = []
        # aspirate 호출 이력.
        self.aspirate_calls: list[DispenseCall] = []
        # initialize 호출 횟수.
        self.initialize_count = 0
        # 각 물리 동작 지연(ms) — 명시 주입 우선, 미주입 시 env(기본 0·무지연).
        self.step_delay_ms = (
            step_delay_ms if step_delay_ms is not None
            else _resolve_step_delay_ms(os.environ)
        )
        # stop 신호(취소/우아한 종료) — set 되면 진행 중 지연 슬립이 즉시 반환.
        self._stop = stop_event if stop_event is not None else threading.Event()

    def signal_stop(self) -> None:
        """진행 중 지연 슬립을 즉시 깨운다(취소/중단·SIGTERM 우아한 종료 시)."""
        self._stop.set()

    def _delay(self) -> None:
        """물리 동작 시간 근사 — step_delay_ms 만큼 sleep(짧은 조각·stop 즉응).

        기본 0 이면 즉시 반환(단위테스트 무영향). 값>0 이면 _DELAY_SLICE_S 조각으로 나눠
        자되, 매 조각마다 stop 이벤트를 검사해 취소/종료 신호에 지체 없이 반응한다.
        """
        if self.step_delay_ms <= 0:
            return
        remaining = self.step_delay_ms / 1000.0
        while remaining > 0 and not self._stop.is_set():
            slice_s = _DELAY_SLICE_S if remaining > _DELAY_SLICE_S else remaining
            time.sleep(slice_s)
            remaining -= slice_s

    @property
    def dispense_count(self) -> int:
        """dispense 총 호출 횟수 — IL-02/CR-01/EP-03 판정의 핵심 카운터."""
        return len(self.dispense_calls)

    def dispense_count_for(self, pump_addr: int) -> int:
        """특정 pumpAddr 의 dispense 호출 횟수."""
        return sum(1 for c in self.dispense_calls if c.pump_addr == pump_addr)

    def script_for(self, pump_addr: int, outcomes: list[FakeEngineOutcome]) -> None:
        """pumpAddr 에 결과 스크립트를 주입(FIFO). 없으면 default_outcome."""
        self._script_by_addr[pump_addr] = list(outcomes)

    def script_all(self, outcome: FakeEngineOutcome) -> None:
        """모든 pumpAddr 에 단일 결과 스크립트를 주입(테스트 편의)."""
        self.default_outcome = outcome
        self._script_by_addr.clear()

    def _next_outcome(self, pump_addr: int) -> FakeEngineOutcome:
        q = self._script_by_addr.get(pump_addr)
        if q:
            return q.pop(0)
        return self.default_outcome

    def aspirate(self, cmd: EngineDispenseCommand) -> EngineResult:
        self.aspirate_calls.append(
            DispenseCall(pump_addr=cmd.pump_addr, volume_ul=cmd.volume_ul, steps=cmd.steps)
        )
        # 물리 흡입 시간 근사(기본 0·무지연). 시도는 위에서 기록 후 지연.
        self._delay()
        # aspirate 도 동일 스크립트 소비 — 실 하드웨어는 흡입/배출이 하나의 물리 사이클.
        return _outcome_to_result(self._next_outcome(cmd.pump_addr))

    def dispense(self, cmd: EngineDispenseCommand) -> EngineResult:
        self.dispense_calls.append(
            DispenseCall(pump_addr=cmd.pump_addr, volume_ul=cmd.volume_ul, steps=cmd.steps)
        )
        # 물리 배출 시간 근사(기본 0·무지연) — 이 구간이 "제조 중" 크래시 주입 창.
        self._delay()
        return _outcome_to_result(self._next_outcome(cmd.pump_addr))

    def initialize(self) -> EngineResult:
        self.initialize_count += 1
        # 물리 homing/purge 시간 근사(기본 0·무지연).
        self._delay()
        return EngineResult(raw_error_code=0)

    def reset(self) -> None:
        """관찰 상태 초기화(재기동 시나리오 사이 카운터 리셋)."""
        self.dispense_calls.clear()
        self.aspirate_calls.clear()
        self.initialize_count = 0
        self._script_by_addr.clear()
        self.default_outcome = FakeEngineOutcome.ACK
        # stop 신호도 해제 — 시나리오 재사용 시 지연이 다시 정상 동작(step_delay_ms 는 유지).
        self._stop.clear()
