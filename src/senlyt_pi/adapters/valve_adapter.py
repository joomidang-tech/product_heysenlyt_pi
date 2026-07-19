"""기주 밸브 어댑터 — GPIO 실구동 + Fake(시뮬레이션) — §9-1 v2 · 병렬토출 설계 §8.

HW 사실(2026-07-17 실 배선 정정 — 구 BCM9/11=물리핀21/23 대체·scripts/valve_2ch_test.py 참조 구현):
  - 신 기주(sour) = **BCM17**(물리 핀11) / 베이스(normal) = **BCM27**(물리 핀13).
  - GPIO → Active-LOW 릴레이 → 솔레노이드(24V). 제어 = 열고 N초 뒤 닫기(시간축).
  - openSec = volume_ml ÷ flow_ml_per_sec (기주 20mL 고정 → 캘리브레이션되면 사실상 1값).

⚠️ 핀·flowRate 는 **설정값**(하드코딩 금지 — bootstrap env → 이 어댑터 인자). admin 캘리브레이션
UI(flowRate 실측 입력)는 후속 웨이브 — 값의 SoT 는 admin 설정으로 승격 예정(설계 §9-①).

뮤텍스 계층 L3(설계 §4): 한 잔에 밸브 1개(상호배타) — open 전 `_close_all` + threading.Lock.
RS485 버스 락(L1)과 **교차 의존 금지** — 이 모듈은 시리얼을 모른다.
"""

from __future__ import annotations

import threading
import time
from typing import Callable, Mapping

from ..ports.valve_port import VALVE_BASES, ValveDispenseResult

# 기본 매핑(BCM) — 신기주(sour)=17(물리 핀11) · 베이스(normal)=27(물리 핀13).
# 2026-07-17 실 배선 정정(구 BCM9/11=물리 핀21/23 대체 — RPi 물리핀 11/13 기준).
# 배선 변경 시 SENLYT_VALVE_PINS 로 교체 가능.
DEFAULT_VALVE_PINS: dict[str, int] = {"sour": 17, "normal": 27}
# 기본 유량(mL/s) — 7/13 참조 스크립트 placeholder 와 동일. 벤치 캘리브레이션으로 교체.
DEFAULT_FLOW_ML_PER_SEC = 10.0
# 최대 개방 클램프(s) — 20mL ÷ 10mL/s = 2s 정상 기준의 넉넉한 상한(밸브 영구개방 차단).
DEFAULT_MAX_OPEN_SEC = 15.0


def _validate_base(base: str) -> str | None:
    if base not in VALVE_BASES:
        return f"unknown_base:{base}"
    return None


class FakeValveAdapter:
    """Fake 밸브 — 실 GPIO 없이 개방 기록만 남긴다(단위테스트·E2E·FakeEngine 짝).

    scripted 실패 주입(fail_next)과 개방 지연 시뮬(delay_s — 병렬 타이밍 테스트용) 지원.
    """

    def __init__(
        self,
        *,
        flow_ml_per_sec: float = DEFAULT_FLOW_ML_PER_SEC,
        max_open_sec: float = DEFAULT_MAX_OPEN_SEC,
        delay_s: float = 0.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.flow_ml_per_sec = flow_ml_per_sec
        self.max_open_sec = max_open_sec
        self.delay_s = delay_s
        self._sleep = sleep
        self._lock = threading.Lock()  # L3 상호배타 — 한 잔에 밸브 1개.
        self.dispensed: list[tuple[str, float, float]] = []  # (base, volume_ml, open_sec)
        self.close_all_calls = 0
        self.fail_next = False

    def dispense_volume(self, base: str, volume_ml: float) -> ValveDispenseResult:
        err = _validate_base(base)
        if err is not None:
            return ValveDispenseResult(ok=False, open_sec=0.0, detail=err)
        open_sec = volume_ml / self.flow_ml_per_sec
        # 클램프 발동 = 요청량 미충족(under-dispense) — 조용한 성공 금지(리뷰 P2 봉합).
        # 개방 자체를 거부(fail-closed·기주 낭비 0) — 설정(flow/max) 오류를 즉시 표면화.
        if open_sec > self.max_open_sec:
            return ValveDispenseResult(
                ok=False, open_sec=0.0,
                detail=f"open_sec_exceeds_max({open_sec:.2f}s > {self.max_open_sec:.2f}s)",
            )
        with self._lock:
            if self.fail_next:
                self.fail_next = False
                return ValveDispenseResult(ok=False, open_sec=open_sec, detail="injected_failure")
            if self.delay_s > 0:
                self._sleep(self.delay_s)
            self.dispensed.append((base, volume_ml, open_sec))
        return ValveDispenseResult(ok=True, open_sec=open_sec)

    def available_bases(self) -> list[str]:
        # Fake = 전 base 사용가능(실 GPIO 없음). 연결상태 표시용 read-only.
        return list(VALVE_BASES)

    def close_all(self) -> None:
        self.close_all_calls += 1


class GpioValveAdapter:
    """GPIO 실구동 밸브 — gpiozero OutputDevice(Active-LOW) · 실기기(라즈베리파이) 전용.

    gpiozero 는 **lazy import**(비-pi 환경에서 모듈 로드만으로 죽지 않게 — 생성 시 결선).
    scripts/valve_2ch_test.py 검증 규약 이식: 상호배타(_close_all 선행)·try/finally 닫힘·
    initial_value=False(시작 시 닫힘)·최대 개방 클램프.
    """

    def __init__(
        self,
        *,
        pins: Mapping[str, int] | None = None,
        flow_ml_per_sec: float = DEFAULT_FLOW_ML_PER_SEC,
        max_open_sec: float = DEFAULT_MAX_OPEN_SEC,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if flow_ml_per_sec <= 0:
            raise ValueError(f"flow_ml_per_sec must be > 0 (got {flow_ml_per_sec})")
        from gpiozero import OutputDevice  # lazy — 실기기에서만 존재.

        resolved = dict(pins) if pins is not None else dict(DEFAULT_VALVE_PINS)
        unknown = set(resolved) - set(VALVE_BASES)
        if unknown:
            raise ValueError(f"unknown valve base(s) in pins: {sorted(unknown)}")
        self.flow_ml_per_sec = flow_ml_per_sec
        self.max_open_sec = max_open_sec
        self._sleep = sleep
        self._lock = threading.Lock()  # L3 상호배타.
        # Active-LOW 릴레이 — active_high=False·initial_value=False(시작 시 닫힘).
        self._valves = {
            base: OutputDevice(pin, active_high=False, initial_value=False)
            for base, pin in resolved.items()
        }

    def dispense_volume(self, base: str, volume_ml: float) -> ValveDispenseResult:
        err = _validate_base(base)
        if err is not None:
            return ValveDispenseResult(ok=False, open_sec=0.0, detail=err)
        valve = self._valves.get(base)
        if valve is None:
            return ValveDispenseResult(ok=False, open_sec=0.0, detail=f"unwired_base:{base}")
        open_sec = volume_ml / self.flow_ml_per_sec
        # 클램프 발동 = under-dispense — 개방 전 fail-closed 거부(조용한 성공 금지·리뷰 P2).
        if open_sec > self.max_open_sec:
            return ValveDispenseResult(
                ok=False, open_sec=0.0,
                detail=f"open_sec_exceeds_max({open_sec:.2f}s > {self.max_open_sec:.2f}s)",
            )
        with self._lock:
            self._close_all_unlocked()  # 상호배타 — 한 잔에 밸브 1개.
            valve.on()
            try:
                self._sleep(open_sec)
            finally:
                valve.off()  # 오류가 나도 반드시 닫힘(7/13 규약).
        return ValveDispenseResult(ok=True, open_sec=open_sec)

    def close_all(self) -> None:
        with self._lock:
            self._close_all_unlocked()

    def _close_all_unlocked(self) -> None:
        for valve in self._valves.values():
            valve.off()

    def available_bases(self) -> list[str]:
        # 부팅 시 OutputDevice 클레임 성공한 base = 핀 사용가능. **dict 조회만**(on/off 없음·비-실행).
        return sorted(self._valves.keys())
