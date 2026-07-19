"""ValvePort — 기주(base liquor) 자동밸브 구동 포트(§9-1 v2 · 2026-07-14 병렬토출 설계 §8).

식향 기주 택1(신기주/베이스) 밸브 — GPIO 시간축(µL 아님·"열고 N초 뒤 닫기").
시린지 RS485 버스와 **완전 독립**(뮤텍스 계층 L3 — L1 버스 락을 절대 잡지 않는다).

안전 불변식(설계 §10 — 어댑터가 보장):
  - 상호배타: open 전 전 밸브 close(한 잔에 밸브 1개).
  - try/finally 닫힘: 오류가 나도 반드시 close.
  - 최대 개방 클램프: openSec ≤ max_open_sec(밸브가 영원히 열리는 것 차단).
  - 시작·종료 시 닫힘(Active-LOW·initial off).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

# base 2종 — 서버 decideBaseLiquor 판정값(D15/D18)과 동일 축.
VALVE_BASES = ("normal", "sour")


@dataclass(frozen=True, slots=True)
class ValveDispenseResult:
    """밸브 토출 결과 — ok=False 는 permanent 취급(시간축 재시도 = 과토출 위험·재시도 없음)."""

    ok: bool
    open_sec: float
    detail: str | None = None


class ValvePort(Protocol):
    """기주 밸브 포트 — volume_ml → openSec 파생(flowRate 설정)은 어댑터 책임."""

    def dispense_volume(self, base: str, volume_ml: float) -> ValveDispenseResult:
        """base("normal"|"sour") 밸브를 volume_ml 에 해당하는 시간만큼 개방 후 닫는다."""
        ...

    def close_all(self) -> None:
        """전 밸브 강제 닫힘 — 시작/종료/graceful/크래시 핸들러에서 호출(멱등)."""
        ...

    def available_bases(self) -> list[str]:
        """GPIO 라인이 클레임된 기주밸브 base 목록 — **비-실행 read-only**(on/off 절대 안 함).

        연결상태 표시용. ⚠️ '핀 사용가능'(라인 살아있음) 판정이지 '실제 밸브 장착' 확인이 아니다
        (GPIO 출력이라 시리얼 펌프처럼 응답 probe 가 물리적으로 불가). admin 라벨에서 구분한다.
        """
        ...
