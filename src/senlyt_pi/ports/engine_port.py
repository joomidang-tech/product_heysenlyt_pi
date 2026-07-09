"""EnginePort — 시린지 펌프 구동 포트(인터페이스만) — SoT §6-7.

Dart `lib/ports/engine_port.dart` 포팅.

⛔ 안전상 유보(이번 웨이브 범위 밖): 펌프 구동 실로직(실토출)·Sequencer 는 구현하지 않는다.
   실어댑터(sy01b 시리얼 RR·pyserial)는 `adapters/` 에 TODO 스텁으로만 둔다.

에러코드 분류·재시도 정책은 core `pump_guard.classify_engine_error_code`(§6-7).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ..core.pump_guard import SyringeSpec


@dataclass(frozen=True, slots=True)
class EngineDispenseCommand:
    """단일 펌프 토출 명령(해석된 스텝) — 서버 recipe step → SyringeSpec 파생 후."""

    pump_addr: int
    volume_ul: float
    # SyringeSpec.steps_for_volume_ul 로 파생된 스텝수(하드코딩 금지·§6-4).
    steps: int
    spec: SyringeSpec


@dataclass(frozen=True, slots=True)
class EngineResult:
    """엔진 실행 결과."""

    # 엔진 raw errorCode(정수) — classify_engine_error_code 입력(§6-7). 0=정상.
    raw_error_code: int
    detail: str | None = None


class EnginePort(Protocol):
    """시린지 펌프 엔진 포트."""

    def aspirate(self, cmd: EngineDispenseCommand) -> EngineResult:
        """단일 스텝 흡입(aspirate). ⛔ 실토출 로직 = 이후 웨이브."""
        ...

    def dispense(self, cmd: EngineDispenseCommand) -> EngineResult:
        """단일 스텝 배출(dispense). ⛔ 실토출 로직 = 이후 웨이브."""
        ...

    def initialize(self) -> EngineResult:
        """초기화(homing/purge). ⛔ 실로직 = 이후 웨이브."""
        ...
