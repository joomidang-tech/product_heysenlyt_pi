"""Fake EnginePort 시뮬레이션 어댑터 — SoT §6-7 / 질의서 §0·Q8(EP-03·EP-09) 객관 판정 근거.

Dart `test/support/fake_engine_port.dart` 포팅 — Python 에서는 adapters 로 승격
(시뮬레이션 어댑터·실기기 없는 개발/테스트 공용. 실 RS485 는 sy01b_engine_adapter TODO).

**P0 게이트의 관찰 렌즈**: dispense 호출 카운터로 IL-02(중복토출0)·CR-01(재기동 자동재실행
금지)·EP-03(빈응답=실패·silent-success 금지)를 **객관 검증**한다.

주입 가능한 결과(scripted): ack(정상 0) / busy(transient) / permanent / timeout / **empty**(무응답).
  - empty(""·무응답) = 실패로 분류되어야 한다(EP-03·EP-09). silent-success 0.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass

from ..ports.engine_port import EngineDispenseCommand, EngineResult
from ..test_seam.fake_engine_sentinels import FAKE_EMPTY_RAW_CODE, FAKE_TIMEOUT_RAW_CODE


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

    def __init__(self) -> None:
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
        # aspirate 도 동일 스크립트 소비 — 실 하드웨어는 흡입/배출이 하나의 물리 사이클.
        return _outcome_to_result(self._next_outcome(cmd.pump_addr))

    def dispense(self, cmd: EngineDispenseCommand) -> EngineResult:
        self.dispense_calls.append(
            DispenseCall(pump_addr=cmd.pump_addr, volume_ul=cmd.volume_ul, steps=cmd.steps)
        )
        return _outcome_to_result(self._next_outcome(cmd.pump_addr))

    def initialize(self) -> EngineResult:
        self.initialize_count += 1
        return EngineResult(raw_error_code=0)

    def reset(self) -> None:
        """관찰 상태 초기화(재기동 시나리오 사이 카운터 리셋)."""
        self.dispense_calls.clear()
        self.aspirate_calls.clear()
        self.initialize_count = 0
        self._script_by_addr.clear()
        self.default_outcome = FakeEngineOutcome.ACK
