#!/usr/bin/env python3
"""식향 기주 2밸브 제어 (라즈베리파이 + Active-LOW 릴레이).

성연님 1밸브 스크립트를 2밸브로 확장. 나중에 pi 데몬 ValveAdapter로 이식할 참조 구현.

  신 기주(sour)   = BCM17 (물리 핀11)   (산 값 >= threshold 일 때)
  베이스 기주(normal) = BCM27 (물리 핀13)  (산 값 <  threshold 일 때)
  ※ 2026-07-17 실 배선 정정 — 구값 BCM9/11(물리 핀21/23)에서 물리 핀11/13(BCM17/27)로.

안전 규칙:
  - 상호배타: 한 잔에 한 밸브만 연다 (기주 택1). open 시 다른 밸브는 강제로 닫는다.
  - 시작 시 두 밸브 모두 닫힘 (initial_value=False).
  - 오류/인터럽트가 나도 반드시 닫힘 (try/finally).

토출량: 밸브는 "몇 초 여느냐"로 제어한다(부피 아님). 기주 20mL는 고정이므로,
  openSec = 20mL / flowRate(ml/s) 로 캘리브레이션한다(밸브마다 유량이 다를 수 있어 개별 보정).
"""

from time import sleep

from gpiozero import OutputDevice

ACTIVE_LOW = True  # 릴레이가 반대로 동작하면 False 로 변경

# base 논리값 → BCM 핀. 배선이 바뀌면 여기만 고친다(코드 하드코딩 금지 원칙).
VALVE_PINS = {
    "sour": 17,  # 신 기주 · BCM17(물리 핀11)
    "normal": 27,  # 베이스 기주 · BCM27(물리 핀13)
}

# 밸브별 유량(ml/s) — 20mL ↔ 초 캘리브레이션. 실측 후 채운다(잠정 placeholder).
FLOW_ML_PER_SEC = {
    "sour": 10.0,
    "normal": 10.0,
}
BASE_ML = 20.0  # 기주 고정 토출량

valves = {
    base: OutputDevice(pin, active_high=not ACTIVE_LOW, initial_value=False)
    for base, pin in VALVE_PINS.items()
}


def _close_all():
    for v in valves.values():
        v.off()


def open_valve(base):
    """base 밸브만 연다. 상호배타 — 나머지는 반드시 닫는다."""
    if base not in valves:
        raise ValueError(f"알 수 없는 기주: {base!r} (sour|normal 만)")
    _close_all()  # 택1 보장: 다른 밸브 먼저 닫고
    valves[base].on()
    print(f"  → {base} 밸브 열림 (딸깍) · BCM{VALVE_PINS[base]}")


def close_valve(base):
    valves[base].off()
    print(f"  → {base} 밸브 닫힘 (딸깍)")


def dispense(base, seconds):
    """base 기주를 seconds 초 동안 토출 (한 잔에 한 번, 택1)."""
    print(f"[{base}] 토출 {seconds}초")
    open_valve(base)
    try:
        sleep(seconds)
    finally:
        close_valve(base)  # 오류가 나도 반드시 닫힘


def dispense_base_20ml(base):
    """기주 20mL 고정 토출 — 유량 캘리브레이션으로 초 환산."""
    flow = FLOW_ML_PER_SEC[base]
    seconds = round(BASE_ML / flow, 2)
    print(f"[{base}] 기주 {BASE_ML}mL → {seconds}초 (유량 {flow}ml/s)")
    dispense(base, seconds)


def cleanup():
    _close_all()
    for v in valves.values():
        v.close()


if __name__ == "__main__":
    try:
        print("=== 2밸브 릴레이 테스트 (24V 미연결 상태) ===")
        print("딸깍 소리와 IN LED를 확인하세요. sour=BCM17(물리핀11), normal=BCM27(물리핀13)\n")

        for base in ("normal", "sour"):
            print(f"[{base} 기주 3회]")
            for i in range(3):
                print(f"  {i + 1}회차")
                dispense(base, 2.0)
                sleep(1.0)
            print()

        print("=== 테스트 완료 ===")

    except KeyboardInterrupt:
        print("\n중단됨")
    finally:
        cleanup()
        print("정리 완료 (두 밸브 모두 닫힘 상태)")
