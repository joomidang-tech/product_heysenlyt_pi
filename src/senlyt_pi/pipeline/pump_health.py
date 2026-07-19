"""부팅 자동 감지 + 자가진단(self-test) — 포트·시린지펌프(최대 10)·GPIO 밸브 연동 체크.

핵심: **박아둔 VID/PID 로 찾지 않고, 실제로 펌프가 응답하는지 프로브해서 자동 감지**한다.
  - 포트 자동감지: 후보 포트(serial_port_discovery.list_candidate_ports)를 하나씩 열어
    RS485 버스 주소 1..N(기본 1..10)을 프로브 → **펌프가 응답하는 포트**를 그 버스로 확정.
  - 펌프 ID 자동인식: 그 버스에서 응답한 주소 = 장착 펌프 id(한 버스 **최대 10개**).
  - 자동 매핑: 발견한 id → SyringeSpec(모드 용량) pump_map 을 자동 생성(정적 PUMP_ADDRESSES 불필요).

`probe(addr)->bool` 은 엔진 어댑터가 제공(sy01b 실구현=RS485 상태쿼리 / Fake=스크립트).
v1.1.0 계승: 단일 버스·주소로 펌프 구분(1=TOP·2=MIDDLE·3=BASE …). 펌프 증설·교체는 재부팅 스캔으로 재확정.

배선 현황(2026-07-18 정정 — 과현혹 주석 제거·실동작 명시):
  - **wired**: `discover_pumps`·`auto_pump_map` — `app.bootstrap.build_resolver` 가 부팅 시
    엔진 `probe` 로 모드 예상 주소(식향 1,2 / 향장향 1,2,3)를 스캔해 pump_map 을 구성한다.
  - **미배선(진단 헬퍼)**: `autodetect_bus`·`run_self_test`·`HealthReport`·`scan_addresses` —
    포트까지 훑는 **풀 버스-자동감지 self-test** 흐름용이나 현 부팅 경로엔 아직 결선돼 있지
    않다(sy01b 어댑터가 `open_probe` seam 을 제공 — 풀 흐름 승격 시 사용). 부팅 시점 판정은
    `build_resolver` 가 대신한다.
  - **EP-08/CR-07 "자가진단 통과 전 제조 보류(fail-closed·토출 0)"의 실제 강제 지점**:
    ① `build_resolver` 가 **응답한 펌프만** 매핑(부재 펌프 미매핑) → ② `RecipeResolver` 가
    미매핑 pumpAddr 스텝을 CMD_VALIDATION_FAILED 로 drop(토출 0) → ③ `SenlytDaemon._boot_self_test`
    가 매핑 펌프 0 을 부팅 시 경고로 표면화(heartbeat 지속). 즉 fail-closed 는 **살아있되**
    별도 formal self-test 게이트가 아니라 이 3중 결합으로 성립한다(종전 docstring 의 "self-test
    게이트가 돈다"는 과현혹 표현이었음).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Mapping, Sequence

from ..core.pump_guard import PUMP_PRESETS, SyringeSpec

# RS485 버스에서 한 주소가 응답하는지 프로브(엔진 어댑터 제공). 예외/무응답 = False.
PumpProbe = Callable[[int], bool]
# 포트를 열어 그 버스용 프로브를 만드는 seam(port -> probe). 실구현=시리얼 open, 테스트=가짜.
OpenBusProbe = Callable[[str], PumpProbe]

# 자동 스캔 상한 = 주소 1..9. ⚠️ 10 이상을 스캔하면 안 된다(유령 펌프 — 2026-07-19 실기기 실측):
#   프레임이 `/{addr}` 문자열 인코딩이라 `/10?` 은 펌프에게 "주소 1 + 명령 0?" 로 읽혀 **pump1 이
#   대답**한다 → 존재하지 않는 펌프 10 이 발견·등록된다. 한 자리 주소(1..9)만이 이 인코딩에서
#   유일하게 안전하다(스위치 10 이상 주소는 별도 문자 체계 — 현 하드웨어 미사용).
DEFAULT_SCAN_MAX = 9


def scan_addresses(max_addr: int = DEFAULT_SCAN_MAX) -> tuple[int, ...]:
    """스캔할 주소 후보 1..max_addr(포함·최대 9 — 두 자리 주소는 유령 응답, 상수 주석)."""
    return tuple(range(1, min(9, max(1, max_addr)) + 1))


def discover_pumps(
    probe: PumpProbe, addresses: Sequence[int] | None = None
) -> list[int]:
    """RS485 주소를 프로브해 **응답하는 주소**(장착 펌프 id)만 오름차순으로 반환.

    addresses 미지정 → 1..10 전수 스캔(자동인식). 프로브 예외 = 미장착(스캔 지속·견고).
    """
    cands = tuple(addresses) if addresses is not None else scan_addresses()
    found: list[int] = []
    for addr in cands:
        try:
            if probe(addr):
                found.append(addr)
        except Exception:  # noqa: BLE001 — 무응답/예외 = 미장착(스캔 지속).
            continue
    return sorted(set(found))


@dataclass(frozen=True, slots=True)
class BusDiscovery:
    """자동 감지 결과 — 펌프가 응답한 포트 + 그 버스의 펌프 id 목록."""

    port: str | None
    pump_ids: tuple[int, ...]


def autodetect_bus(
    candidate_ports: Sequence[str],
    open_probe: OpenBusProbe,
    *,
    scan: Sequence[int] | None = None,
) -> BusDiscovery:
    """후보 포트를 하나씩 프로브해 **펌프가 응답하는 포트/버스**를 자동 감지.

    각 포트를 열어(open_probe) 주소 1..10 을 스캔, **펌프가 하나라도 응답하면 그 포트로 확정**
    (VID/PID 하드매칭 아님 — 실제 응답이 근거). 아무 포트에도 없으면 port=None.
    open_probe 가 예외를 던지면(열기 실패) 그 포트는 건너뛴다.
    """
    for port in candidate_ports:
        try:
            probe = open_probe(port)
        except Exception:  # noqa: BLE001 — 포트 열기 실패 = 다음 후보.
            continue
        ids = discover_pumps(probe, scan)
        if ids:
            return BusDiscovery(port=port, pump_ids=tuple(ids))
    return BusDiscovery(port=None, pump_ids=())


def auto_pump_map(
    pump_ids: Sequence[int],
    *,
    capacity_ml: float,
    full_stroke: int | None = None,
) -> dict[int, SyringeSpec]:
    """발견한 펌프 id → SyringeSpec 자동 매핑(정적 PUMP_ADDRESSES 대체).

    모든 펌프는 같은 스트로크(기본 sy01b 12000·`full_stroke` 로 서버 프리셋 오버라이드 가능)와
    같은 용량(`capacity_ml`). id(주소)가 곧 pump_map 키다. 서버 portLayout(`pumpPorts` — 어느
    펌프 몇 번 구멍에 어떤 액체가 꽂혔나)이 배치 의미를 얹는다(스텝의 inPort/outPort) — 여기선
    물리 pumpAddr→SyringeSpec 매핑만 한다(pi 는 배치를 모른다).
    """
    stroke = full_stroke if full_stroke is not None else PUMP_PRESETS["sy01b"].pump_full_stroke
    spec = SyringeSpec(pump_full_stroke=stroke, syringe_capacity_ml=capacity_ml)
    return {int(a): spec for a in sorted(set(pump_ids))}


@dataclass(frozen=True, slots=True)
class HealthReport:
    """부팅 자가진단 결과 — ok 이면 제조 트래픽 수용, 아니면 보류(fail-closed).

    pumps_found = 자동인식된 장착 펌프 주소(= 사용 가능한 펌프 id). reasons 비면 통과.
    """

    ok: bool
    serial_port: str | None
    serial_ok: bool
    pumps_found: tuple[int, ...]
    min_pumps: int
    expected_pump_addrs: tuple[int, ...] | None
    valve_present: bool
    valve_required: bool
    reasons: tuple[str, ...] = field(default_factory=tuple)


def run_self_test(
    *,
    serial_port: str | None,
    pumps_found: Sequence[int],
    expected_pump_addrs: Sequence[int] | None = None,
    min_pumps: int = 1,
    valve_present: bool,
    valve_required: bool = True,
) -> HealthReport:
    """부팅 self-test 판정 — 포트 발견 + 펌프 자동인식 결과 + 밸브 연동을 게이트한다.

    (펌프 발견 자체는 autodetect_bus/discover_pumps 가 먼저 수행 — 여기선 그 결과를 판정.)
    - 시리얼 포트 미발견 → 실패.
    - `expected_pump_addrs` 지정 시 → 그 주소가 전부 응답해야 통과(고정 구성 검증).
    - 미지정 시 → 발견 펌프 수 ≥ `min_pumps`(기본 1)면 통과(자동인식·개수 유연).
    - 밸브 필요 시(식향 기주) 밸브 결선 확인.
    """
    reasons: list[str] = []
    found = tuple(sorted(set(pumps_found)))

    serial_ok = bool(serial_port)
    if not serial_ok:
        reasons.append("serial_port_not_found (펌프 응답 포트 미발견)")

    expected = tuple(sorted(set(expected_pump_addrs))) if expected_pump_addrs is not None else None
    if serial_ok:
        if expected is not None:
            missing = [a for a in expected if a not in found]
            if missing:
                reasons.append(f"pumps_missing={missing} (기대 {list(expected)}·응답 {list(found)})")
        elif len(found) < min_pumps:
            reasons.append(f"pumps_insufficient={len(found)}<{min_pumps} (응답 {list(found)})")

    if valve_required and not valve_present:
        reasons.append("valve_not_wired (기주 밸브 미결선 — 식향 기주 토출 불가)")

    ok = not reasons
    return HealthReport(
        ok=ok,
        serial_port=serial_port,
        serial_ok=serial_ok,
        pumps_found=found,
        min_pumps=min_pumps,
        expected_pump_addrs=expected,
        valve_present=valve_present,
        valve_required=valve_required,
        reasons=tuple(reasons),
    )
