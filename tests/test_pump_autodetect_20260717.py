"""시린지펌프 자동 감지(프로브) + 자동 매핑 + 자가진단 (2026-07-17).

핵심: **박아둔 VID/PID 로 찾지 않고, 실제 펌프 응답을 프로브해 자동 감지**한다.
  - 포트 후보: 전체 시리얼 포트(알려진 어댑터 우선·하드필터 아님).
  - 자동 감지: 후보 포트를 프로브 → 펌프 응답하는 포트/버스 확정.
  - 펌프 id: 버스 주소 1..9 스캔(두 자리 주소=유령 응답이라 10↑ 금지) → 응답분 = 장착 펌프.
  - 자동 매핑: 발견 id → SyringeSpec pump_map 생성.
"""

from senlyt_pi.adapters.serial_port_discovery import (
    SerialPortInfo,
    discover_serial_port,
    list_candidate_ports,
)
from senlyt_pi.pipeline.pump_health import (
    DEFAULT_SCAN_MAX,
    autodetect_bus,
    auto_pump_map,
    discover_pumps,
    run_self_test,
    scan_addresses,
)


def _p(device, vid=None, pid=None):
    return SerialPortInfo(device=device, vid=vid, pid=pid)


# ── 포트 후보 열거 (VID/PID 하드필터 아님·우선순위 힌트) ──────────────────────

def test_env_override_is_single_candidate():
    assert list_candidate_ports(
        {"SENLYT_SERIAL_PORT": "/dev/myport"},
        port_lister=lambda: [_p("/dev/ttyUSB0", 0x1A86, 0x7523)],
    ) == ["/dev/myport"]


def test_known_adapter_first_but_unknown_still_candidate():
    """VID/PID 미상 포트도 후보에 포함(박힌 값에 의존 X) — 알려진 어댑터만 앞으로."""
    cands = list_candidate_ports(
        {},
        port_lister=lambda: [
            _p("/dev/ttyUSB0", 0x2341, 0x0043),      # 미상(아두이노) — 후보 유지
            _p("/dev/ttyUSB1", 0x1A86, 0x7523),      # CH340 — 우선
            _p("/dev/ttyUSB2", None, None),          # VID/PID 미조회 — 후보 유지
        ],
    )
    assert cands[0] == "/dev/ttyUSB1"              # 알려진 어댑터 먼저
    assert set(cands) == {"/dev/ttyUSB0", "/dev/ttyUSB1", "/dev/ttyUSB2"}  # 전부 후보


def test_bluetooth_debug_excluded():
    cands = list_candidate_ports(
        {}, port_lister=lambda: [_p("/dev/cu.Bluetooth-Incoming"), _p("/dev/ttyUSB0", None, None)]
    )
    assert cands == ["/dev/ttyUSB0"]


def test_discover_serial_port_first_candidate():
    assert discover_serial_port({}, port_lister=lambda: [_p("/dev/ttyUSB0", None, None)]) == "/dev/ttyUSB0"
    assert discover_serial_port({}, port_lister=lambda: []) is None


# ── 자동 감지 (프로브로 펌프 응답 포트 확정) ─────────────────────────────────

def test_autodetect_finds_port_with_pumps():
    """후보 2개 중 펌프가 응답하는 포트를 프로브로 확정 — VID/PID 무관."""
    bus = {"/dev/ttyUSB0": set(), "/dev/ttyUSB1": {1, 2, 3}}
    d = autodetect_bus(
        ["/dev/ttyUSB0", "/dev/ttyUSB1"],
        open_probe=lambda port: (lambda addr: addr in bus[port]),
    )
    assert d.port == "/dev/ttyUSB1" and d.pump_ids == (1, 2, 3)


def test_autodetect_up_to_9_pumps():
    """한 버스 최대 9개 — 1..9 응답 전부 인식. ⚠️ 10 은 스캔 금지(유령 펌프 —
    `/10?` 이 "주소1+명령0?" 으로 오독돼 pump1 이 대답, 2026-07-19 실기기 실측)."""
    bus = {"/dev/ttyUSB0": set(range(1, 11))}
    d = autodetect_bus(["/dev/ttyUSB0"], open_probe=lambda p: (lambda a: a in bus[p]))
    assert d.pump_ids == tuple(range(1, 10))


def test_autodetect_skips_port_that_fails_to_open():
    def open_probe(port):
        if port == "/dev/ttyUSB0":
            raise OSError("open failed")
        return lambda a: a in (1, 2)
    d = autodetect_bus(["/dev/ttyUSB0", "/dev/ttyUSB1"], open_probe=open_probe)
    assert d.port == "/dev/ttyUSB1" and d.pump_ids == (1, 2)


def test_autodetect_none_when_no_pumps_anywhere():
    d = autodetect_bus(["/dev/ttyUSB0"], open_probe=lambda p: (lambda a: False))
    assert d.port is None and d.pump_ids == ()


# ── 버스 스캔 (최대 9 — 두 자리 주소는 유령 응답·pump_health.DEFAULT_SCAN_MAX 주석) ──

def test_scan_default_is_1_to_9():
    assert DEFAULT_SCAN_MAX == 9
    assert scan_addresses() == tuple(range(1, 10))


def test_discover_pumps_sparse():
    # 10 에 응답이 있어도(=pump1 유령) 스캔 범위 밖이라 등록되지 않는다.
    assert discover_pumps(lambda a: a in {1, 3, 7, 10}) == [1, 3, 7]


def test_probe_exception_absent():
    def probe(a):
        if a == 2:
            raise OSError("no resp")
        return a in (1, 2, 5)
    assert discover_pumps(probe) == [1, 5]


# ── 자동 매핑 ───────────────────────────────────────────────────────────────

def test_auto_pump_map_builds_specs():
    m = auto_pump_map([1, 2, 3], capacity_ml=0.5)
    assert set(m.keys()) == {1, 2, 3}
    assert m[1].syringe_capacity_ml == 0.5 and m[1].pump_full_stroke == 12000
    assert m[1].max_volume_ul == 500.0  # 0.5mL 게이트 상한


# ── 자가진단 게이트 ─────────────────────────────────────────────────────────

def test_self_test_ok_min_pumps():
    r = run_self_test(serial_port="/dev/ttyUSB0", pumps_found=[1, 2, 3, 4], min_pumps=2, valve_present=True)
    assert r.ok is True and r.pumps_found == (1, 2, 3, 4)


def test_self_test_expected_missing_fails():
    r = run_self_test(serial_port="/dev/ttyUSB0", pumps_found=[1], expected_pump_addrs=[1, 2], valve_present=True)
    assert r.ok is False and any("pumps_missing" in x for x in r.reasons)


def test_self_test_insufficient_fails():
    r = run_self_test(serial_port="/dev/ttyUSB0", pumps_found=[1], min_pumps=3, valve_present=True)
    assert r.ok is False and any("pumps_insufficient" in x for x in r.reasons)


def test_self_test_serial_missing_fails():
    r = run_self_test(serial_port=None, pumps_found=[], valve_present=True)
    assert r.ok is False and any("serial_port_not_found" in x for x in r.reasons)


def test_self_test_valve_required_absent_fails():
    r = run_self_test(serial_port="/dev/ttyUSB0", pumps_found=[1, 2], valve_present=False, valve_required=True)
    assert r.ok is False and any("valve_not_wired" in x for x in r.reasons)
