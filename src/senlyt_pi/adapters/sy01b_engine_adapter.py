"""sy01b 시리얼 EnginePort 실어댑터 — ⛔ TODO 스텁(이번 웨이브 범위 밖).

Dart `lib/adapters/sy01b_engine_adapter.dart` 포팅(스텁 그대로).

안전상 유보(SoT 워크오더): 펌프 구동 실로직(실토출·RS485 시리얼 프로토콜)·실기기 연결은
이후 웨이브에서 구현한다. 지금은 포트 계약만 만족하는 미구현 스텁 — pyserial 은 그때
의존성으로 추가한다(시그니처만 고정, import 없음).

실구현 시 참조: SoT §6-1(SY-01B U200·fullStroke 12000)·§6-4(steps 파생)·§6-7(errorCode).
무응답/타임아웃 시 test_seam.fake_engine_sentinels 의 동일 sentinel 을 방출할 것(EP-03).

⛔ **bounded-read 계약(F1 방어 — 실구현 시 필수)**: 모든 시리얼 read 는 반드시 타임아웃을
  건다(`SERIAL_READ_TIMEOUT_S`, pyserial `Serial(timeout=...)`). 펌프가 ACK/status 를 안 주고
  멈춰도 read 는 유한 시간에 반환해 **ENGINE_TIMEOUT**(§6-7 transient)을 방출해야 한다.
  이유: 상위 `pump_sequencer._run_stage` 는 stage 태스크들을 `future.result()`(타임아웃 인자
  없음)로 **완주 대기**한다 — 어댑터가 무한 블록하면 그 대기가 곧 **제조 교착**이다. 배리어
  타임아웃을 시퀀서에 두지 않는 것은 설계 의도다(모션 중 강제중단 금지·설계 §10) — 대신 **시간
  경계는 어댑터의 read 타임아웃**이 책임진다(명령 송신 후 pump 의 유한 모션을 기다릴 뿐이라
  read 타임아웃은 물리 모션을 중단시키지 않는다). Fake 엔진은 이미 유한(step_delay·sentinel)이라
  현 소비 루프는 교착하지 않는다 — 이 계약은 sy01b 실구현 웨이브에 강제된다.
"""

from __future__ import annotations

from ..ports.engine_port import EngineDispenseCommand, EngineResult

# F1 방어(bounded-read) 기본 타임아웃(초) — 실구현 pyserial `Serial(timeout=...)` 에 주입할 값.
# 한 명령의 ACK/status 를 이 시간까지 기다리고, 초과 시 ENGINE_TIMEOUT(transient·§6-7)을 방출한다.
# (실측 캘리브레이션 시 조정 — 최장 stroke 물리 시간 + 여유. 무한 대기 금지가 불변식.)
SERIAL_READ_TIMEOUT_S = 5.0


class Sy01bEngineAdapter:
    """SY-01B 시린지 펌프 시리얼 어댑터 — 미구현 스텁.

    `port`/`baudrate`/`read_timeout_s` 는 실구현(pyserial) 시그니처 예약 — 지금은 저장만 한다.
    `read_timeout_s` = bounded-read 계약(F1) — 실구현은 이 값으로 `Serial(timeout=...)` 을 열어
    무한 블록(제조 교착)을 원천 차단한다.
    """

    def __init__(
        self,
        *,
        port: str = "/dev/ttyUSB0",
        baudrate: int = 9600,
        read_timeout_s: float = SERIAL_READ_TIMEOUT_S,
    ) -> None:
        self.port = port
        self.baudrate = baudrate
        # bounded-read 계약(F1) — 실구현이 pyserial Serial(timeout=read_timeout_s) 로 사용.
        self.read_timeout_s = read_timeout_s

    def aspirate(self, cmd: EngineDispenseCommand) -> EngineResult:
        # TODO(wave-next): RS485 시리얼 프로토콜 실토출. 안전 게이트(0<vol≤maxVolumeUl) 통과분만.
        raise NotImplementedError("sy01b aspirate — 이후 웨이브(실토출 로직 유보)")

    def dispense(self, cmd: EngineDispenseCommand) -> EngineResult:
        # TODO(wave-next): RS485 시리얼 프로토콜 실토출.
        raise NotImplementedError("sy01b dispense — 이후 웨이브(실토출 로직 유보)")

    def initialize(self) -> EngineResult:
        # TODO(wave-next): homing/purge (§6-5 초기화힘 ZR/Z1R/Z2R).
        raise NotImplementedError("sy01b initialize — 이후 웨이브")
