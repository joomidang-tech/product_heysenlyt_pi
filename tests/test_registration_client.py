"""디바이스 등록 테스트 — RegisterRequest/RegisterResponse 계약 (05_api §8-1, D-A 2026-07-10).

[D-A] deviceId = pi 수집 시리얼 그대로 — pi 가 시리얼을 제시하고, 서버 응답의 deviceId(echo)는
정체성을 덮어쓰지 않는다(자기 시리얼이 권위). 등록 성공/실패 재시도(기존 R=3 준용)·permanent(400)
즉시중단·**TOFU 202 pending 승인 폴링**·정체성 파일 영속·만료 재등록(ensure_registered)·시리얼 seam
을 고정한다. 공유키(프로비저닝 키) 제거(2026-07-17) — 보안은 pending+운영자 승인으로. 전송은 fake(seam).
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

# 서버는 제시한 시리얼을 echo 하지만, pi 는 이를 정체성으로 쓰지 않는다(자기 시리얼이 권위).
# echo 값을 시리얼과 **다르게** 두어 "서버발급 id 를 덮어쓰지 않음"을 강하게 고정한다.
OK_BODY = {"deviceId": "server-echo-ignored", "dispenserToken": "tok-1", "exp": 2_000_000_000}
SERIAL = "hw-serial-1"


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
    kw = dict(device_id=SERIAL, name="매장1", sleep=lambda _s: None)
    kw.update(over)
    return RegistrationClient(transport, **kw)


class TestRegisterRequestWire:
    def test_wire_shape(self):
        """deviceId(=시리얼) 필수 + name includeIfNull:false."""
        assert build_register_request("serial-1", "이름") == {"deviceId": "serial-1", "name": "이름"}
        assert build_register_request("serial-1") == {"deviceId": "serial-1"}

    def test_empty_device_id_rejected(self):
        with pytest.raises(ValueError):
            build_register_request("")


class TestRegistrationRetry:
    def test_success_first_try(self):
        t = ScriptedTransport([(200, OK_BODY)])
        identity = client(t).register()
        # [D-A] deviceId = 자기 시리얼 — 서버 echo("server-echo-ignored")를 쓰지 않는다.
        assert identity.device_id == SERIAL
        assert identity.dispenser_token == "tok-1"
        assert identity.exp == 2_000_000_000
        assert len(t.calls) == 1
        assert t.calls[0] == {"deviceId": SERIAL, "name": "매장1"}

    def test_server_echoed_device_id_is_ignored(self):
        """서버가 다른 deviceId 를 echo 해도 정체성은 자기 시리얼(권위) — 덮어쓰기 없음."""
        t = ScriptedTransport([(200, {"deviceId": "dsp-legacyhash", "dispenserToken": "tok-1", "exp": 2_000_000_000})])
        identity = client(t, device_id="10000000abcd1234").register()
        assert identity.device_id == "10000000abcd1234"

    def test_response_without_device_id_still_ok(self):
        """서버가 deviceId 를 아예 안 줘도(등록만) dispenserToken·exp 만으로 정체성 성립."""
        t = ScriptedTransport([(200, {"dispenserToken": "tok-1", "exp": 2_000_000_000})])
        identity = client(t).register()
        assert identity.device_id == SERIAL
        assert identity.dispenser_token == "tok-1"

    def test_retryable_5xx_then_success(self):
        """5xx(register_failed) → 재시도 후 성공."""
        t = ScriptedTransport([(500, None), (503, None), (200, OK_BODY)])
        identity = client(t).register()
        assert identity.device_id == SERIAL
        assert len(t.calls) == 3

    def test_pending_202_returns_none(self):
        """TOFU: 202 pending(승인 대기) → register() 는 None(오류 아님·폴링은 상위)."""
        t = ScriptedTransport([(202, {"deviceId": SERIAL, "status": "pending"})])
        assert client(t).register() is None
        assert len(t.calls) == 1

    def test_approved_response_carries_mode(self):
        """승인(200) 응답의 mode(서버 배정)를 정체성에 싣는다 — SENLYT_MODE env 대체."""
        t = ScriptedTransport(
            [(200, {"dispenserToken": "tok-1", "exp": 2_000_000_000, "mode": "fragrance"})]
        )
        identity = client(t).register()
        assert identity.mode == "fragrance"

    def test_approved_without_mode_is_none(self):
        """mode 미배정 승인 → identity.mode=None(pi 는 env→flavor 폴백)."""
        assert client(ScriptedTransport([(200, OK_BODY)])).register().mode is None

    def test_transport_exception_is_retryable(self):
        t = ScriptedTransport([ConnectionError("down"), (200, OK_BODY)])
        assert client(t).register().device_id == SERIAL
        assert len(t.calls) == 2

    def test_retries_exhausted_r3(self):
        """기존 수치 준용(R=3) — 첫 시도 + 3 재시도 = 총 4회 후 retryable 실패 표면화."""
        t = ScriptedTransport([(500, None)] * 10)
        with pytest.raises(RegistrationError) as ei:
            client(t).register()
        assert ei.value.retryable is True
        assert ei.value.code == "register_failed"
        assert len(t.calls) == 4  # 1 + max_retries(3).

    def test_permanent_400_no_retry(self):
        t = ScriptedTransport([(400, None)])
        with pytest.raises(RegistrationError) as ei:
            client(t).register()
        assert ei.value.retryable is False
        assert ei.value.code == "invalid_request"
        assert len(t.calls) == 1

    def test_malformed_success_body_retried(self):
        """2xx 인데 계약 위반 본문(dispenserToken 누락·exp 형식 오류) → 방어적 재시도."""
        t = ScriptedTransport(
            [(200, {"dispenserToken": ""}), (200, {"dispenserToken": "t", "exp": "soon"}), (200, OK_BODY)]
        )
        assert client(t).register().device_id == SERIAL
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
        identity = DeviceIdentity(device_id="10000000abcd1234", dispenser_token="tok", exp=123)
        store.save(identity)
        assert store.load() == identity

    def test_server_base_url_roundtrip(self, tmp_path: Path):
        """서버 바인딩 필드(2026-07-23) 저장/복원 왕복 + JSON 키 serverBaseUrl."""
        store = DeviceIdentityStore(tmp_path / "identity.json")
        identity = DeviceIdentity(
            device_id="10000000abcd1234",
            dispenser_token="tok",
            exp=123,
            server_base_url="https://dev-env.senlyt.com",
        )
        store.save(identity)
        assert store.load() == identity
        assert '"serverBaseUrl": "https://dev-env.senlyt.com"' in (
            tmp_path / "identity.json"
        ).read_text(encoding="utf-8")

    def test_missing_and_corrupt_return_none(self, tmp_path: Path):
        store = DeviceIdentityStore(tmp_path / "identity.json")
        assert store.load() is None  # 부재.
        (tmp_path / "identity.json").write_text("{broken", encoding="utf-8")
        assert store.load() is None  # 파손 → 재등록 유도(crash 금지).
        (tmp_path / "identity.json").write_text('{"deviceId": ""}', encoding="utf-8")
        assert store.load() is None  # 계약 위반.

    def test_legacy_file_with_hardware_id_loads_ignoring_it(self, tmp_path: Path):
        """구 정체성 파일(hardwareId 포함)도 로드 — hardwareId 는 무시(deviceId 가 곧 시리얼)."""
        store = DeviceIdentityStore(tmp_path / "identity.json")
        (tmp_path / "identity.json").write_text(
            '{"deviceId": "dsp-oldhash", "dispenserToken": "t", "exp": 5000, "hardwareId": "hw-1"}',
            encoding="utf-8",
        )
        loaded = store.load()
        assert loaded == DeviceIdentity(device_id="dsp-oldhash", dispenser_token="t", exp=5000)
        assert not hasattr(loaded, "hardware_id")

    def test_clear(self, tmp_path: Path):
        store = DeviceIdentityStore(tmp_path / "identity.json")
        store.save(DeviceIdentity(device_id="d", dispenser_token="t", exp=1))
        store.clear()
        assert store.load() is None
        store.clear()  # 이미 없어도 무해.

    def test_expiry_strict(self):
        identity = DeviceIdentity(device_id="d", dispenser_token="t", exp=100)
        assert is_identity_expired(identity, now_seconds=100)  # exp==now → 만료(strict).
        assert is_identity_expired(identity, now_seconds=101)
        assert not is_identity_expired(identity, now_seconds=99)


class TestEnsureRegistered:
    def test_boot_registers_and_persists(self, tmp_path: Path):
        store = DeviceIdentityStore(tmp_path / "identity.json")
        t = ScriptedTransport([(200, OK_BODY)])
        identity = ensure_registered(store, client(t), now_seconds=1_000)
        assert identity.device_id == SERIAL  # 자기 시리얼 = deviceId(서버 echo 무시).
        assert store.load() == identity  # 재부팅 대비 영속.

    def test_valid_identity_reused_without_network(self, tmp_path: Path):
        store = DeviceIdentityStore(tmp_path / "identity.json")
        saved = DeviceIdentity(device_id=SERIAL, dispenser_token="tok-old", exp=5_000)
        store.save(saved)
        t = ScriptedTransport([])  # 호출되면 IndexError — 네트워크 0 검증.
        assert ensure_registered(store, client(t), now_seconds=1_000) == saved
        assert t.calls == []

    def test_expired_token_reregisters(self, tmp_path: Path):
        """만료(토큰 exp 경과) → 재등록으로 재발급(계약 RegisterResponse)."""
        store = DeviceIdentityStore(tmp_path / "identity.json")
        store.save(DeviceIdentity(device_id=SERIAL, dispenser_token="tok-old", exp=999))
        t = ScriptedTransport([(200, OK_BODY)])
        identity = ensure_registered(store, client(t), now_seconds=1_000)
        assert identity.dispenser_token == "tok-1"
        assert len(t.calls) == 1
        assert store.load() == identity

    def test_serial_change_reregisters(self, tmp_path: Path):
        """저장분 deviceId 불일치(기판 교체·구 dsp-<hash> 승격) → 재등록(upsert 키=deviceId=시리얼)."""
        store = DeviceIdentityStore(tmp_path / "identity.json")
        store.save(DeviceIdentity(device_id="dsp-oldhash", dispenser_token="tok-old", exp=5_000))
        t = ScriptedTransport([(200, OK_BODY)])
        identity = ensure_registered(store, client(t), now_seconds=1_000)
        assert len(t.calls) == 1
        assert identity.device_id == SERIAL  # 현재 시리얼로 승격.

    def test_same_server_reuses_without_network(self, tmp_path: Path):
        """서버 바인딩(2026-07-23): 저장 정체성의 서버 == 현재 서버 → 재사용(네트워크 0)."""
        store = DeviceIdentityStore(tmp_path / "identity.json")
        saved = DeviceIdentity(
            device_id=SERIAL,
            dispenser_token="tok-old",
            exp=5_000,
            server_base_url="https://dev-env.senlyt.com",
        )
        store.save(saved)
        t = ScriptedTransport([])  # 호출되면 IndexError.
        got = ensure_registered(
            store, client(t), now_seconds=1_000, server_base_url="https://dev-env.senlyt.com"
        )
        assert got == saved
        assert t.calls == []

    def test_different_server_reregisters(self, tmp_path: Path):
        """★핵심 회귀★: 저장 정체성이 **다른 서버** 것이면(URL 만 바꿔 재설치) → 현재 서버에 재등록.
        (안 그러면 옛 서버 정체성을 재사용해 새 서버 admin 후보에 안 떠 페어링 실패 — 2026-07-23 dev 실측)"""
        store = DeviceIdentityStore(tmp_path / "identity.json")
        store.save(
            DeviceIdentity(
                device_id=SERIAL,
                dispenser_token="tok-v120",
                exp=5_000,
                server_base_url="https://v1-2-0.env.senlyt.com",
            )
        )
        t = ScriptedTransport([(200, OK_BODY)])
        got = ensure_registered(
            store, client(t), now_seconds=1_000, server_base_url="https://dev-env.senlyt.com"
        )
        assert len(t.calls) == 1  # 재등록 발생(네트워크 1회).
        assert got.dispenser_token == "tok-1"  # 새 서버 토큰.
        assert got.server_base_url == "https://dev-env.senlyt.com"  # 현재 서버로 각인.
        assert store.load() == got  # 영속에도 새 서버 각인.

    def test_legacy_identity_without_server_reregisters(self, tmp_path: Path):
        """구 정체성 파일(serverBaseUrl 부재=None) + 서버 지정 → 서버 미상이라 재등록(안전 fail-safe).
        배포된 기존 Pi 자가치유 경로 — 이미 승인된 서버면 서버가 즉시 200 재발급(무중단)."""
        store = DeviceIdentityStore(tmp_path / "identity.json")
        (tmp_path / "identity.json").write_text(
            '{"deviceId": "hw-serial-1", "dispenserToken": "tok-old", "exp": 5000}',
            encoding="utf-8",
        )
        assert store.load().server_base_url is None  # 구 파일 = 서버 미상.
        t = ScriptedTransport([(200, OK_BODY)])
        got = ensure_registered(
            store, client(t), now_seconds=1_000, server_base_url="https://senlyt.com"
        )
        assert len(t.calls) == 1
        assert got.server_base_url == "https://senlyt.com"

    def test_server_compare_ignores_trailing_slash(self, tmp_path: Path):
        """trailing slash 차이만이면 같은 서버로 보고 재사용(오탐 재등록 방지)."""
        store = DeviceIdentityStore(tmp_path / "identity.json")
        store.save(
            DeviceIdentity(
                device_id=SERIAL,
                dispenser_token="tok-old",
                exp=5_000,
                server_base_url="https://senlyt.com",
            )
        )
        t = ScriptedTransport([])
        got = ensure_registered(
            store, client(t), now_seconds=1_000, server_base_url="https://senlyt.com/"
        )
        assert t.calls == []  # 재사용(재등록 안 함).
        assert got.dispenser_token == "tok-old"

    def test_no_server_arg_preserves_legacy_reuse(self, tmp_path: Path):
        """server_base_url 미지정(구 호출·테스트) → 서버 비교 스킵, 기존 재사용 동작 보존(하위호환)."""
        store = DeviceIdentityStore(tmp_path / "identity.json")
        saved = DeviceIdentity(device_id=SERIAL, dispenser_token="tok-old", exp=5_000)
        store.save(saved)
        t = ScriptedTransport([])
        assert ensure_registered(store, client(t), now_seconds=1_000) == saved
        assert t.calls == []

    def test_polls_pending_until_approved(self, tmp_path: Path):
        """TOFU: 202 pending 이 이어지면 운영자 승인(200)까지 폴링 — 사이마다 sleep."""
        store = DeviceIdentityStore(tmp_path / "identity.json")
        t = ScriptedTransport(
            [(202, {"status": "pending"}), (202, {"status": "pending"}), (200, OK_BODY)]
        )
        slept: list[float] = []
        identity = ensure_registered(
            store, client(t), now_seconds=1_000, pending_poll_interval_seconds=3.0, sleep=slept.append
        )
        assert identity.device_id == SERIAL
        assert len(t.calls) == 3  # pending·pending·approved
        assert slept == [3.0, 3.0]  # 두 pending 뒤에만 대기
        assert store.load() == identity  # 승인 후 영속

    def test_pending_exhausted_raises(self, tmp_path: Path):
        """승인이 pending_max_polls 안에 안 오면 registration_pending 예외 → restart 재시도."""
        store = DeviceIdentityStore(tmp_path / "identity.json")
        t = ScriptedTransport([(202, {"status": "pending"})] * 10)
        with pytest.raises(RegistrationError) as ei:
            ensure_registered(
                store, client(t), now_seconds=1_000, pending_max_polls=3, sleep=lambda _s: None
            )
        assert ei.value.code == "registration_pending"
        assert ei.value.http_status == 202
        assert len(t.calls) == 4  # 1 + pending_max_polls(3)
        assert store.load() is None  # 미승인 = 정체성 미영속


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
