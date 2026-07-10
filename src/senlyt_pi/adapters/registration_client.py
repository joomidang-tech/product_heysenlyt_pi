"""디바이스 등록 클라이언트 — 계약 RegisterRequest/RegisterResponse (2026-07-09).

pi 부팅 시 POST /api/dispensers/register { hardwareId, name? }
(Authorization: Bearer <DISPENSER_PROVISION_KEY>) → { deviceId, dispenserToken, exp }.
멱등: 같은 hardwareId → 같은 deviceId(registeredAt 불변·name 만 갱신) — 부팅마다 호출해도 안전.

재시도 정책 = 기존 수치 준용(EngineExecutor R=3 — 첫 시도 + 최대 3 재시도):
  - retryable: 전송 예외(네트워크) · 5xx(500 register_failed·503 provisioning_not_configured)
    · 성공(2xx)인데 본문이 계약 위반(과도기 서버 방어).
  - permanent(즉시 중단·재시도 없음): 4xx(400 invalid_request·401 invalid_provision_key) —
    재시도로 해소되지 않는 구성 오류. 사용자 개입 필요.

실 HTTP 전송은 seam(`RegisterTransport`)으로 주입 — 테스트 결정성 + 이후 웨이브에서
urllib/실클라이언트로 교체. hardwareId 도 seam(`read_hardware_id` 파라미터 주입 가능).
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from ..core.wire_json import put_if_present
from .device_identity_store import DeviceIdentity, DeviceIdentityStore, is_identity_expired
from .http_client import (
    DEFAULT_TIMEOUT_SECONDS,
    HttpTransportError,
    bearer_headers,
    request_json,
)

# 프로비저닝 키 env 키 — POST /api/dispensers/register 의 Authorization: Bearer.
DISPENSER_PROVISION_KEY_ENV = "DISPENSER_PROVISION_KEY"

# 전송 seam — RegisterRequest 와이어(dict)를 보내고 (HTTP status, 응답 body json|None) 반환.
# 네트워크 오류는 예외 raise(→ retryable). 프로비저닝 키 헤더는 transport 조립 책임.
RegisterTransport = Callable[[dict[str, Any]], "tuple[int, Mapping[str, Any] | None]"]

# 재시도 횟수 — 기존 수치 준용(EngineExecutor.max_retries = SoT §6-7 R=3).
DEFAULT_REGISTER_MAX_RETRIES = 3

# 재시도 간격(초) — attempt 별 소진 시퀀스(마지막 값 유지). 테스트는 sleep 주입으로 무력화.
DEFAULT_RETRY_DELAYS_SECONDS: tuple[float, ...] = (1.0, 2.0, 4.0)

# hardwareId env 오버라이드 키(테스트·개발 기기).
HARDWARE_ID_ENV_KEY = "SENLYT_HARDWARE_ID"


class RegistrationError(Exception):
    """등록 실패. `retryable=False` 는 구성 오류(키·요청) — 재시도 없이 즉시 표면화."""

    def __init__(self, code: str, *, retryable: bool, http_status: int | None = None) -> None:
        self.code = code
        self.retryable = retryable
        self.http_status = http_status
        super().__init__(f"RegistrationError({code} retryable={retryable} http={http_status})")


def build_register_request(hardware_id: str, name: str | None = None) -> dict[str, Any]:
    """RegisterRequest 와이어 조립 — name 은 includeIfNull:false(부록A P-4)."""
    if not isinstance(hardware_id, str) or hardware_id == "":
        raise ValueError("hardwareId 는 비어있지 않은 문자열(계약 minLength 1)")
    m: dict[str, Any] = {"hardwareId": hardware_id}
    put_if_present(m, "name", name)
    return m


def parse_register_response(body: Mapping[str, Any] | None, hardware_id: str) -> DeviceIdentity:
    """RegisterResponse 방어 파싱 — 계약 위반 본문은 retryable RegistrationError."""
    if body is None:
        raise RegistrationError("malformed_response", retryable=True)
    device_id = body.get("deviceId")
    token = body.get("dispenserToken")
    exp = body.get("exp")
    if not isinstance(device_id, str) or device_id == "":
        raise RegistrationError("malformed_response", retryable=True)
    if not isinstance(token, str) or token == "":
        raise RegistrationError("malformed_response", retryable=True)
    if isinstance(exp, bool) or not isinstance(exp, int):
        raise RegistrationError("malformed_response", retryable=True)
    return DeviceIdentity(
        device_id=device_id, dispenser_token=token, exp=exp, hardware_id=hardware_id
    )


class RegistrationClient:
    """등록 호출기 — 재시도 정책 포함(전송은 seam)."""

    def __init__(
        self,
        transport: RegisterTransport,
        *,
        hardware_id: str,
        name: str | None = None,
        max_retries: int = DEFAULT_REGISTER_MAX_RETRIES,
        retry_delays_seconds: tuple[float, ...] = DEFAULT_RETRY_DELAYS_SECONDS,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        self.transport = transport
        self.hardware_id = hardware_id
        self.name = name
        self.max_retries = max_retries
        self.retry_delays_seconds = retry_delays_seconds
        self._sleep = sleep if sleep is not None else time.sleep

    def register(self) -> DeviceIdentity:
        """등록 실행 — 첫 시도 + 최대 max_retries 재시도. 실패 시 RegistrationError."""
        request = build_register_request(self.hardware_id, self.name)
        last_error: RegistrationError | None = None

        for attempt in range(self.max_retries + 1):
            if attempt > 0:
                delays = self.retry_delays_seconds
                delay = delays[min(attempt - 1, len(delays) - 1)] if delays else 0.0
                if delay > 0:
                    self._sleep(delay)
            try:
                status, body = self.transport(request)
            except RegistrationError:
                raise  # transport 가 이미 분류한 오류는 그대로.
            except Exception:
                # 네트워크/전송 예외 = retryable.
                last_error = RegistrationError("transport_error", retryable=True)
                continue

            if 200 <= status < 300:
                try:
                    return parse_register_response(body, self.hardware_id)
                except RegistrationError as e:
                    last_error = e  # 계약 위반 본문 — retryable(과도기 서버 방어).
                    continue

            if 400 <= status < 500:
                # 400 invalid_request · 401 invalid_provision_key — 재시도 무의미(구성 오류).
                code = "invalid_provision_key" if status == 401 else "invalid_request"
                raise RegistrationError(code, retryable=False, http_status=status)

            # 5xx — 500 register_failed · 503 provisioning_not_configured → 재시도.
            code = "provisioning_not_configured" if status == 503 else "register_failed"
            last_error = RegistrationError(code, retryable=True, http_status=status)

        assert last_error is not None
        raise last_error


def ensure_registered(
    store: DeviceIdentityStore,
    client: RegistrationClient,
    *,
    now_seconds: int | None = None,
    force: bool = False,
) -> DeviceIdentity:
    """부팅 진입점 — 저장된 정체성이 유효(미만료·동일 hardwareId)하면 재사용, 아니면 등록.

    토큰 만료(계약 12h) 시 재등록으로 재발급. hardwareId 가 바뀌었으면(기판 교체 등)
    저장분을 버리고 재등록(레지스트리 자연키 = hardwareId).
    """
    now = now_seconds if now_seconds is not None else int(time.time())
    if not force:
        existing = store.load()
        if (
            existing is not None
            and existing.hardware_id == client.hardware_id
            and not is_identity_expired(existing, now_seconds=now)
        ):
            return existing

    identity = client.register()
    store.save(identity)
    return identity


def make_http_register_transport(
    register_url: str,
    provision_key: str,
    *,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    request: Callable[..., "tuple[int, Mapping[str, Any] | None]"] = request_json,
) -> RegisterTransport:
    """실 HTTP RegisterTransport 조립 — POST {register_url} (Bearer 프로비저닝 키).

    스텁 제거: RegistrationClient 의 seam 에 꽂는 **실 클라이언트**(표준 urllib).
      - 요청 body 는 이미 build_register_request 가 만든 와이어(dict) 그대로 전송.
      - Authorization: Bearer <provision_key>(등록 라우트 계약·503 provisioning_not_configured 방지).
      - HTTP 응답(2xx/4xx/5xx)은 (status, body) 로 반환 → RegistrationClient 가 분류.
      - 네트워크 실패(HttpTransportError)는 그대로 raise → RegistrationClient 가 retryable 처리.

    `request` 는 테스트 주입 seam(기본 = http_client.request_json).
    """

    def transport(req_body: dict[str, Any]) -> "tuple[int, Mapping[str, Any] | None]":
        return request(
            "POST",
            register_url,
            body=req_body,
            headers=bearer_headers(provision_key),
            timeout=timeout,
        )

    return transport


def read_provision_key(env: Mapping[str, str] | None = None) -> str:
    """프로비저닝 키 읽기 — DISPENSER_PROVISION_KEY(부재 시 빈 문자열).

    빈 값이면 서버가 401 invalid_provision_key(또는 503)로 거부 → 부팅 로그로 표면화.
    """
    e = env if env is not None else os.environ
    v = e.get(DISPENSER_PROVISION_KEY_ENV)
    return v.strip() if isinstance(v, str) else ""


def read_hardware_id(
    *,
    env: Mapping[str, str] | None = None,
    cpuinfo_path: Path | str = "/proc/cpuinfo",
    machine_id_path: Path | str = "/etc/machine-id",
) -> str | None:
    """기기 고유 HW 식별자 seam — 계약 RegisterRequest.hardwareId(레지스트리 자연키).

    우선순위: ① env SENLYT_HARDWARE_ID(테스트·개발 주입) → ② /proc/cpuinfo `Serial`
    (Pi CPU 시리얼 — 계약 예시) → ③ /etc/machine-id 폴백. 전부 실패 → None
    (호출측이 등록 불가로 표면화 — silent 임의값 생성 금지·레지스트리 자연키 안정성).
    """
    e = env if env is not None else os.environ
    override = e.get(HARDWARE_ID_ENV_KEY)
    if isinstance(override, str) and override.strip() != "":
        return override.strip()

    try:
        text = Path(cpuinfo_path).read_text(encoding="utf-8")
        for line in text.splitlines():
            key, sep, value = line.partition(":")
            if sep and key.strip() == "Serial":
                serial = value.strip()
                if serial and set(serial) != {"0"}:
                    return serial
    except (FileNotFoundError, OSError, UnicodeDecodeError):
        pass

    try:
        machine_id = Path(machine_id_path).read_text(encoding="utf-8").strip()
        if machine_id:
            return machine_id
    except (FileNotFoundError, OSError, UnicodeDecodeError):
        pass

    return None
