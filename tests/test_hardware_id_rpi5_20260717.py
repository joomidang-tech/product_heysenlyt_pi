"""read_hardware_id RPi 4·5 크로스모델 HW 시리얼 (2026-07-17).

RPi 4 = /proc/cpuinfo `Serial` / RPi 5 = /proc/device-tree/serial-number(공통).
둘 다 **동일 HW 시리얼**을 deviceId 로 얻어야 한다. machine-id 는 최후 폴백(HW 아님).
"""

from pathlib import Path

from senlyt_pi.adapters.registration_client import read_hardware_id


def _paths(tmp_path: Path):
    return {
        "cpuinfo_path": tmp_path / "cpuinfo",
        "devicetree_serial_path": tmp_path / "serial-number",
        "machine_id_path": tmp_path / "machine-id",
    }


def test_rpi4_reads_cpuinfo_serial(tmp_path):
    p = _paths(tmp_path)
    p["cpuinfo_path"].write_text("processor\t: 0\nSerial\t\t: 10000000abcd1234\n")
    hid = read_hardware_id(env={}, **p)
    assert hid == "10000000abcd1234"


def test_rpi5_falls_to_devicetree_serial_when_cpuinfo_has_no_serial(tmp_path):
    """RPi 5: cpuinfo 에 Serial 없음 → devicetree serial-number(NUL 종단) 사용."""
    p = _paths(tmp_path)
    p["cpuinfo_path"].write_text("processor\t: 0\nModel\t\t: Raspberry Pi 5\n")  # Serial 없음
    p["devicetree_serial_path"].write_bytes(b"9f00abcd12345678\x00")  # NUL 종단
    hid = read_hardware_id(env={}, **p)
    assert hid == "9f00abcd12345678"


def test_devicetree_all_zero_is_invalid_falls_to_machine_id(tmp_path):
    p = _paths(tmp_path)
    p["cpuinfo_path"].write_text("Model\t: Raspberry Pi 5\n")
    p["devicetree_serial_path"].write_bytes(b"0000000000000000\x00")  # 전부 0 = 무효
    p["machine_id_path"].write_text("deadbeefcafebabe\n")
    hid = read_hardware_id(env={}, **p)
    assert hid == "deadbeefcafebabe"


def test_machine_id_last_resort_when_no_hw_serial(tmp_path):
    p = _paths(tmp_path)
    p["cpuinfo_path"].write_text("Model\t: unknown\n")   # Serial 없음
    # devicetree 파일 없음
    p["machine_id_path"].write_text("aabbccddeeff0011\n")
    hid = read_hardware_id(env={}, **p)
    assert hid == "aabbccddeeff0011"


def test_env_override_wins(tmp_path):
    p = _paths(tmp_path)
    p["cpuinfo_path"].write_text("Serial\t: shouldnotwin\n")
    hid = read_hardware_id(env={"SENLYT_HARDWARE_ID": "dev-override-1"}, **p)
    assert hid == "dev-override-1"


def test_all_missing_returns_none(tmp_path):
    p = _paths(tmp_path)  # 아무 파일도 안 만듦
    assert read_hardware_id(env={}, **p) is None
