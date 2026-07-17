"""EnginePort — 시린지 펌프 구동 포트(인터페이스만) — SoT §6-7.

Dart `lib/ports/engine_port.dart` 포팅.

⛔ 안전상 유보(이번 웨이브 범위 밖): 펌프 구동 실로직(실토출)·Sequencer 는 구현하지 않는다.
   실어댑터(sy01b 시리얼 RR·pyserial)는 `adapters/` 에 TODO 스텁으로만 둔다.

에러코드 분류·재시도 정책은 core `pump_guard.classify_engine_error_code`(§6-7).
"""

from __future__ import annotations

from collections.abc import Iterable
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
    # ── 회전 밸브 구멍 — 한 번의 토출 = `I{in_port}` → `P{steps}` → `O{out_port}` → `D{steps}`. ──
    #   **서버가 기기설정에서 해석해 준 값**이다(pi 는 배치를 모른다). pump_addr 만으로는 한 펌프
    #   다포트 헤드의 여러 액체를 구분할 수 없어, 이게 없으면 어느 통에서 빨지를 정할 수 없다.
    #
    #   ⚠️ **v1.2.0 실 토출 경로는 flavor·fragrance 모두 두 포트를 항상 싣는다**(서버 조립 게이트가
    #   inPort/outPort 를 강제). None 은 **구계약 스텝**(포트 개념 이전)에만 남는 값이다 — 그 경우
    #   어댑터는 있는 포트만 회전하고 없는 회전은 건너뛴 채 P/D 를 수행한다(밸브가 이전 위치에 머묾).
    #   즉 None 스텝의 흡입/배출 대상은 **직전 밸브 위치에 의존**하므로 실 배치 경로에선 만들지 말 것
    #   (생성형 폴백 flavor_recipe_to_steps 는 빈 스텝→drop 으로 이미 봉인·recipe_resolver 참조).
    in_port: int | None = None
    out_port: int | None = None
    # 속도(Hz)·경사 — 서버가 전역설정 × 포트 오버라이드를 해석해 확정(더 느린 쪽). None = 어댑터 기본.
    aspirate_speed_hz: int | None = None
    dispense_speed_hz: int | None = None
    slope: int | None = None


@dataclass(frozen=True, slots=True)
class EngineResult:
    """엔진 실행 결과."""

    # 엔진 raw errorCode(정수) — classify_engine_error_code 입력(§6-7). 0=정상.
    raw_error_code: int
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class EngineOpCommand:
    """엔진 조작 명령 — 토출이 아닌 정비 동작(관제 정비 버튼).

    **의도만 온다** — `A12000` 같은 펌프 문법은 서버가 모르고, 번역은 어댑터가 한다.
    `spec` 은 조작이 용량 파생을 타기 때문에 필요하다(예: plungerFull = 그 시린지의 풀스트로크).
    """

    pump_addr: int
    # "estop" | "initialize" | "plunger_full" | "plunger_home" — 서버 wire `op` 의 pi 표기.
    op: str
    spec: SyringeSpec


# 엔진 조작 op — 서버 wire `EngineOp` 와 1:1(camelCase → snake_case).
OP_ESTOP = "estop"
OP_INITIALIZE = "initialize"
OP_PLUNGER_FULL = "plunger_full"
OP_PLUNGER_HOME = "plunger_home"
ENGINE_OPS = (OP_ESTOP, OP_INITIALIZE, OP_PLUNGER_FULL, OP_PLUNGER_HOME)


class EnginePort(Protocol):
    """시린지 펌프 엔진 포트."""

    def aspirate(self, cmd: EngineDispenseCommand) -> EngineResult:
        """단일 스텝 흡입(aspirate). ⛔ 실토출 로직 = 이후 웨이브."""
        ...

    def dispense(self, cmd: EngineDispenseCommand) -> EngineResult:
        """단일 스텝 배출(dispense). ⛔ 실토출 로직 = 이후 웨이브."""
        ...

    def initialize(self) -> EngineResult:
        """셋업 캐시 무효화 — 다음 토출 때 그 펌프에 TR+U200+Z 를 다시 건다."""
        ...

    def run_op(self, cmd: EngineOpCommand) -> EngineResult:
        """엔진 조작(정비) 실행 — 의도(op)를 펌프 문법으로 번역해 수행한다."""
        ...

    def emergency_stop_all(self, addrs: "Iterable[int]") -> None:
        """긴급정지(§9-4) — 전 펌프에 즉시 정지(TR)를 걸고 in-flight 모션 폴을 협조적으로 중단한다.

        제조 중에도 감시 스레드에서 안전하게 호출된다(어댑터가 버스 락으로 직렬화). 실토출 어댑터만
        의미가 있고, 테스트 더블/미구현 엔진은 no-op 이어도 무방하다(계약상 존재만 강제).
        """
        ...

    def clear_estop(self) -> None:
        """긴급정지 래치 해제 — 복구(초기화) 경로가 부른다."""
        ...
