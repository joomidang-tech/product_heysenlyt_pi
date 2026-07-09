"""디바이스 등록 테스트 — RegisterRequest/RegisterResponse 계약 (2026-07-09).

등록 성공/실패 재시도(기존 R=3 준용)·permanent(400/401) 즉시중단·정체성 파일 영속·
만료 재등록(ensure_registered)·hardwareId seam 을 고정한다. 전송은 전부 fake(seam 주입).
"""

from pathlib import Path

import pytest

from senlyt_pi.adapters.device_identity_store import (
    DeviceIdentity,
    DeviceIdentityStore,
    is_identity_expired,
)
from senlyt_pi.adapters.registration_client import (
    RegistrationClient,
    RegistrationError,
    build_register_request,
    ensure_registered,
    read_hardware_id,
)

OK_BODY = {"deviceId": "dev-A", "dispenserToken": "tok-1", "exp": 2_000_000_000}


class ScriptedTransport:
    """호출별 응답 스크립트 — (status, body) 또는 Exception 인스턴스."""

    def __init__(self, script):
        self.script = list(script)
        self.calls: list[dict] = []

    def __call__(self, request):
        self.calls.append(request)
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def client(transport, **over) -> RegistrationClient:
    kw = dict(hardware_id="hw-serial-1", name="매장1", sleep=lambda _s: None)
    kw.update(over)
    return RegistrationClient(transport, **kw)


class TestRegisterRequestWire:
    def test_wire_shape(self):
        """hardwareId 필수 + name includeIfNull:false."""
        assert build_register_request("hw-1", "이름") == {"hardwareId": "hw-1", "name": "이름"}
        assert build_register_request("hw-1") == {"hardwareId": "hw-1"}

    def test_empty_hardware_id_rejected(self):
        with pytest.raises(ValueError):
            build_register_request("")


class TestRegistrationRetry:
    def test_success_first_try(self):
        t = ScriptedTransport([(200, OK_BODY)])
        identity = client(t).register()
        assert identity.device_id == "dev-A"
        assert identity.dispenser_token == "tok-1"
        assert identity.exp == 2_000_000_000
        assert identity.hardware_id == "hw-serial-1"
        assert len(t.calls) == 1
        assert t.calls[0] == {"hardwareId": "hw-serial-1", "name": "매장1"}

    def test_retryable_5xx_then_success(self):
        """500 register_failed·503 provisioning_not_configured → 재시도 후 성공."""
        t = ScriptedTransport([(500, None), (503, None), (200, OK_BODY)])
        identity = client(t).register()
        assert identity.device_id == "dev-A"
        assert len(t.calls) == 3

    def test_transport_exception_is_retryable(self):
        t = ScriptedTransport([ConnectionError("down"), (200, OK_BODY)])
        assert client(t).register().device_id == "dev-A"
        assert len(t.calls) == 2

    def test_retries_exhausted_r3(self):
        """기존 수치 준용(R=3) — 첫 시도 + 3 재시도 = 총 4회 후 retryable 실패 표면화."""
        t = ScriptedTransport([(500, None)] * 10)
        with pytest.raises(RegistrationError) as ei:
            client(t).register()
        assert ei.value.retryable is True
        assert ei.value.code == "register_failed"
        assert len(t.calls) == 4  # 1 + max_retries(3).

    def test_permanent_401_no_retry(self):
        """401 invalid_provision_key — 구성 오류는 재시도 없이 즉시중단(1회 호출)."""
        t = ScriptedTransport([(401, {"error": "invalid_provision_key"}), (200, OK_BODY)])
        with pytest.raises(RegistrationError) as ei:
            client(t).register()
        assert ei.value.retryable is False
        assert ei.value.code == "invalid_provision_key"
        assert len(t.calls) == 1

    def test_permanent_400_no_retry(self):
        t = ScriptedTransport([(400, None)])
        with pytest.raises(RegistrationError) as ei:
            client(t).register()
        assert ei.value.retryable is False
        assert ei.value.code == "invalid_request"
        assert len(t.calls) == 1

    def test_malformed_success_body_retried(self):
        """2xx 인데 계약 위반 본문(deviceId 누락 등) → 방어적 재시도."""
        t = ScriptedTransport([(200, {"deviceId": ""}), (200, {"exp": "soon"}), (200, OK_BODY)])
        assert client(t).register().device_id == "dev-A"
        assert len(t.calls) == 3

    def test_retry_sleeps_between_attempts(self):
        slept: list[float] = []
        t = ScriptedTransport([(500, None)] * 4)
        with pytest.raises(RegistrationError):
            client(t, sleep=slept.append).register()
        assert slept == [1.0, 2.0, 4.0]  # 재시도 전에만·소진 시퀀스.


class TestIdentityStore:
    def test_save_load_roundtrip(self, tmp_path: Path):
        store = DeviceIdentityStore(tmp_path / "identity.json")
        identity = DeviceIdentity(
            device_id="dev-A", dispenser_token="tok", exp=123, hardware_id="hw-1"
        )
        store.save(identity)
        assert store.load() == identity

    def test_missing_and_corrupt_return_none(self, tmp_path: Path):
        store = DeviceIdentityStore(tmp_path / "identity.json")
        assert store.load() is None  # 부재.
        (tmp_path / "identity.json").write_text("{broken", encoding="utf-8")
        assert store.load() is None  # 파손 → 재등록 유도(crash 금지).
        (tmp_path / "identity.json").write_text('{"deviceId": ""}', encoding="utf-8")
        assert store.load() is None  # 계약 위반.

    def test_clear(self, tmp_path: Path):
        store = DeviceIdentityStore(tmp_path / "identity.json")
        store.save(
            DeviceIdentity(device_id="d", dispenser_token="t", exp=1, hardware_id="h")
        )
        store.clear()
        assert store.load() is None
        store.clear()  # 이미 없어도 무해.

    def test_expiry_strict(self):
        identity = DeviceIdentity(
            device_id="d", dispenser_token="t", exp=100, hardware_id="h"
        )
        assert is_identity_expired(identity, now_seconds=100)  # exp==now → 만료(strict).
        assert is_identity_expired(identity, now_seconds=101)
        assert not is_identity_expired(identity, now_seconds=99)


class TestEnsureRegistered:
    def test_boot_registers_and_persists(self, tmp_path: Path):
        store = DeviceIdentityStore(tmp_path / "identity.json")
        t = ScriptedTransport([(200, OK_BODY)])
        identity = ensure_registered(store, client(t), now_seconds=1_000)
        assert identity.device_id == "dev-A"
        assert store.load() == identity  # 재부팅 대비 영속.

    def test_valid_identity_reused_without_network(self, tmp_path: Path):
        store = DeviceIdentityStore(tmp_path / "identity.json")
        saved = DeviceIdentity(
            device_id="dev-A", dispenser_token="tok-old", exp=5_000, hardware_id="hw-serial-1"
        )
        store.save(saved)
        t = ScriptedTransport([])  # 호출되면 IndexError — 네트워크 0 검증.
        assert ensure_registered(store, client(t), now_seconds=1_000) == saved
        assert t.calls == []

    def test_expired_token_reregisters(self, tmp_path: Path):
        """만료(12h TTL 경과) → 재등록으로 재발급(계약 RegisterResponse)."""
        store = DeviceIdentityStore(tmp_path / "identity.json")
        store.save(
            DeviceIdentity(
                device_id="dev-A", dispenser_token="tok-old", exp=999, hardware_id="hw-serial-1"
            )
        )
        t = ScriptedTransport([(200, OK_BODY)])
        identity = ensure_registered(store, client(t), now_seconds=1_000)
        assert identity.dispenser_token == "tok-1"
        assert len(t.calls) == 1
        assert store.load() == identity

    def test_hardware_id_change_reregisters(self, tmp_path: Path):
        """저장분 hardwareId 불일치(기판 교체) → 재등록(레지스트리 자연키 = hardwareId)."""
        store = DeviceIdentityStore(tmp_path / "identity.json")
        store.save(
            DeviceIdentity(
                device_id="dev-A", dispenser_token="tok-old", exp=5_000, hardware_id="hw-OLD"
            )
        )
        t = ScriptedTransport([(200, OK_BODY)])
        identity = ensure_registered(store, client(t), now_seconds=1_000)
        assert len(t.calls) == 1
        assert identity.hardware_id == "hw-serial-1"


class TestHardwareIdSeam:
    def test_env_override_first(self, tmp_path: Path):
        got = read_hardware_id(
            env={"SENLYT_HARDWARE_ID": " hw-env "},
            cpuinfo_path=tmp_path / "none",
            machine_id_path=tmp_path / "none",
        )
        assert got == "hw-env"

    def test_cpuinfo_serial(self, tmp_path: Path):
        cpuinfo = tmp_path / "cpuinfo"
        cpuinfo.write_text(
            "processor\t: 0\nHardware\t: BCM2835\nSerial\t\t: 10000000abcd1234\n",
            encoding="utf-8",
        )
        got = read_hardware_id(env={}, cpuinfo_path=cpuinfo, machine_id_path=tmp_path / "none")
        assert got == "10000000abcd1234"

    def test_all_zero_serial_falls_through(self, tmp_path: Path):
        """Serial 0000…(미노출 커널) 은 자연키로 부적격 → machine-id 폴백."""
        cpuinfo = tmp_path / "cpuinfo"
        cpuinfo.write_text("Serial\t\t: 0000000000000000\n", encoding="utf-8")
        mid = tmp_path / "machine-id"
        mid.write_text("abcdef123456\n", encoding="utf-8")
        assert read_hardware_id(env={}, cpuinfo_path=cpuinfo, machine_id_path=mid) == "abcdef123456"

    def test_nothing_available_returns_none(self, tmp_path: Path):
        """식별자 전무 → None(임의값 생성 금지 — 자연키 안정성)."""
        got = read_hardware_id(
            env={}, cpuinfo_path=tmp_path / "none", machine_id_path=tmp_path / "none"
        )
        assert got is None
