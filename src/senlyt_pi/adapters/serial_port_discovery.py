"""USB-RS485 시린지펌프 포트 **후보 열거** — 자동 감지(프로브)용.

⚠️ '박아둔 VID/PID 로 매칭'이 아니라, **실제로 펌프가 응답하는 포트를 프로브로 찾는다**(자동 감지).
새 하드웨어·다양한 어댑터에서 VID/PID 를 못 박을 수 있으므로, 이 모듈은 **후보 포트 목록**만
만들고(알려진 어댑터 VID/PID 는 *우선순위 힌트*일 뿐 — 하드필터 아님), 어느 포트에 펌프가
붙었는지는 `pipeline.pump_health.autodetect_bus` 가 **버스 프로브**로 확정한다.

정본 참고: v1.1.0 `find_pump_port.py`(pyserial comports). 그때는 CH340 VID/PID 로 좁혔지만,
지금은 펌프/어댑터가 늘어(한 버스 최대 10 펌프) VID/PID 미상일 수 있어 **전체 포트를 후보로**
두고 프로브가 결정한다. 알려진 어댑터는 먼저 시도(빠른 성공).

우선순위: ① env `SENLYT_SERIAL_PORT`(명시 — 단일 후보) → ② 알려진 어댑터 VID/PID 포트(우선) →
③ 나머지 전체 시리얼 포트.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Callable, Mapping

# 명시 포트 override env — 다장치/개발 고정 시(프로브 없이 이 포트만 후보).
SERIAL_PORT_ENV = "SENLYT_SERIAL_PORT"

# 알려진 RS485 USB 어댑터 (VID, PID) — **우선순위 힌트**(하드필터 아님). CH340=v1.1.0, FT232=FTDI.
KNOWN_ADAPTER_VID_PID: dict[tuple[int, int], str] = {
    (0x1A86, 0x7523): "CH340",
    (0x1A86, 0x5523): "CH341",
    (0x0403, 0x6001): "FT232R",
}

# 블루투스/디버그 등 명백한 비-펌프 포트 제외(v1.1.0 findDevicePort 스킵 패턴).
_EXCLUDE_HINTS: tuple[str, ...] = ("bluetooth", "debug-console", "wlan")


@dataclass(frozen=True, slots=True)
class SerialPortInfo:
    """포트 열거 1건 — pyserial comports() 미러(device·vid·pid). vid/pid 미조회 시 None."""

    device: str
    vid: int | None = None
    pid: int | None = None


# seam: 포트 목록 제공자(테스트가 가짜 목록 주입 → pyserial 무의존 테스트).
PortLister = Callable[[], "list[SerialPortInfo]"]


def _pyserial_lister() -> list[SerialPortInfo]:
    """실 열거 — pyserial `serial.tools.list_ports`(v1.1.0 find_pump_port.py 동일). 미설치 시 []."""
    try:
        from serial.tools import list_ports  # lazy — pyserial 은 실기기 배포에만 필요.
    except ImportError:
        return []
    out: list[SerialPortInfo] = []
    try:
        for p in list_ports.comports():
            out.append(SerialPortInfo(device=p.device, vid=p.vid, pid=p.pid))
    except Exception:  # noqa: BLE001 — 열거 실패는 빈 목록.
        return []
    return out


def _is_excluded(device: str) -> bool:
    d = device.lower()
    return any(h in d for h in _EXCLUDE_HINTS)


def _is_known_adapter(p: SerialPortInfo) -> bool:
    return (
        p.vid is not None and p.pid is not None and (p.vid, p.pid) in KNOWN_ADAPTER_VID_PID
    )


def list_candidate_ports(
    env: Mapping[str, str] | None = None,
    *,
    port_lister: PortLister | None = None,
) -> list[str]:
    """프로브할 **후보 포트 목록**(우선순위 정렬). env override 시 그 포트 하나만.

    알려진 어댑터(VID/PID) 포트를 앞에 두고 나머지를 뒤에 둔다 — 프로브가 앞에서부터
    시도해 빠르게 성공하되, VID/PID 미상 포트도 후보라 **박아둔 값에 의존하지 않는다**.
    """
    e = env if env is not None else os.environ
    override = e.get(SERIAL_PORT_ENV)
    if isinstance(override, str) and override.strip():
        return [override.strip()]

    ports = [p for p in (port_lister or _pyserial_lister)() if not _is_excluded(p.device)]
    known = [p.device for p in ports if _is_known_adapter(p)]
    rest = [p.device for p in ports if not _is_known_adapter(p)]
    # 중복 제거하며 순서 보존(known 먼저).
    seen: set[str] = set()
    ordered: list[str] = []
    for dev in known + rest:
        if dev not in seen:
            seen.add(dev)
            ordered.append(dev)
    return ordered


def discover_serial_port(
    env: Mapping[str, str] | None = None,
    *,
    port_lister: PortLister | None = None,
) -> str | None:
    """후보 중 첫 포트(간이 사용·프로브 없이). 정밀 자동감지는 pump_health.autodetect_bus."""
    cands = list_candidate_ports(env, port_lister=port_lister)
    return cands[0] if cands else None
