"""sy01b 시리얼 EnginePort 실어댑터 — ⛔ TODO 스텁(이번 웨이브 범위 밖).

Dart `lib/adapters/sy01b_engine_adapter.dart` 포팅(스텁 그대로).

안전상 유보(SoT 워크오더): 펌프 구동 실로직(실토출·RS485 시리얼 프로토콜)·실기기 연결은
이후 웨이브에서 구현한다. 지금은 포트 계약만 만족하는 미구현 스텁 — pyserial 은 그때
의존성으로 추가한다(시그니처만 고정, import 없음).

실구현 시 참조: SoT §6-1(SY-01B U200·fullStroke 12000)·§6-4(steps 파생)·§6-7(errorCode).
무응답/타임아웃 시 test_seam.fake_engine_sentinels 의 동일 sentinel 을 방출할 것(EP-03).
"""

from __future__ import annotations

from ..ports.engine_port import EngineDispenseCommand, EngineResult


class Sy01bEngineAdapter:
    """SY-01B 시린지 펌프 시리얼 어댑터 — 미구현 스텁.

    `port`/`baudrate` 는 실구현(pyserial) 시그니처 예약 — 지금은 저장만 한다.
    """

    def __init__(self, *, port: str = "/dev/ttyUSB0", baudrate: int = 9600) -> None:
        self.port = port
        self.baudrate = baudrate

    def aspirate(self, cmd: EngineDispenseCommand) -> EngineResult:
        # TODO(wave-next): RS485 시리얼 프로토콜 실토출. 안전 게이트(0<vol≤maxVolumeUl) 통과분만.
        raise NotImplementedError("sy01b aspirate — 이후 웨이브(실토출 로직 유보)")

    def dispense(self, cmd: EngineDispenseCommand) -> EngineResult:
        # TODO(wave-next): RS485 시리얼 프로토콜 실토출.
        raise NotImplementedError("sy01b dispense — 이후 웨이브(실토출 로직 유보)")

    def initialize(self) -> EngineResult:
        # TODO(wave-next): homing/purge (§6-5 초기화힘 ZR/Z1R/Z2R).
        raise NotImplementedError("sy01b initialize — 이후 웨이브")
