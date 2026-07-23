"""디바이스 등록 클라이언트 — 계약 RegisterRequest/RegisterResponse (05_api §8-1 · TOFU 2026-07-17).

pi 부팅 시 POST /api/dispensers/register { deviceId, name? } (**인증 헤더 없음** · TOFU 공유키 제거)
  → 200 { dispenserToken, exp, mode }(승인) 또는 202 { status:"pending" }(운영자 승인 대기).

**[D-A] deviceId = pi 수집 하드웨어 시리얼 그대로** — pi 가 자기 시리얼(read_hardware_id 의 값)을
`deviceId` 로 **제시**하고 서버는 **등록만** 한다(서버 발급/파생 왕복 없음·구 dsp-<hash> 폐기).
응답의 deviceId 는 제시값 echo 확인일 뿐 — pi 는 **자기 시리얼을 권위 deviceId 로 유지**하고
서버 echo 로 덮어쓰지 않는다(parse_register_response). 서버가 발급하는 것은 dispenserToken·exp·mode.
멱등: 같은 시리얼(deviceId) 재등록 → 같은 문서 + 새 토큰 — 부팅마다 호출해도 안전.

**TOFU 승인 게이트**: 최초 등록은 202 pending(토큰 미발급) — 운영자가 device-centric /admin 에서
  승인(status→approved·mode 배정)해야 200 으로 전환된다. ensure_registered 가 승인까지 폴링한다.
재시도 정책:
  - retryable: 전송 예외(네트워크) · 5xx(register_failed) · 성공(2xx)인데 본문 계약 위반(과도기 방어).
  - pending(202): 오류 아님 — 승인 폴링(ensure_registered).
  - permanent(즉시 중단·재시도 없음): 4xx invalid_request — 구성 오류. (공유키 제거로 401 경로 없음.)

실 HTTP 전송은 seam(`RegisterTransport`)으로 주입 — 테스트 결정성 + 실 urllib 클라이언트.
hardwareId 도 seam(`read_hardware_id` 파라미터 주입 가능).
"""

from __future__ import annotations

import os
import time
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable, Mapping

from ..core.wire_json import put_if_present
from .device_identity_store import DeviceIdentity, DeviceIdentityStore, is_identity_expired
from .http_client import DEFAULT_TIMEOUT_SECONDS, request_json

# 전송 seam — RegisterRequest 와이어(dict)를 보내고 (HTTP status, 응답 body json|None) 반환.
# 네트워크 오류는 예외 raise(→ retryable). TOFU: 인증 헤더 없음(transport 는 body 만 전송).
RegisterTransport = Callable[[dict[str, Any]], "tuple[int, Mapping[str, Any] | None]"]

# 재시도 횟수 — 기존 수치 준용(EngineExecutor.max_retries = SoT §6-7 R=3).
DEFAULT_REGISTER_MAX_RETRIES = 3

# 재시도 간격(초) — attempt 별 소진 시퀀스(마지막 값 유지). 테스트는 sleep 주입으로 무력화.
DEFAULT_RETRY_DELAYS_SECONDS: tuple[float, ...] = (1.0, 2.0, 4.0)

# TOFU 승인 대기 폴링(202 pending) — 운영자가 /admin 에서 승인할 때까지 재등록을 반복한다.
#   전송오류 재시도(max_retries=3)와 **별개** — 승인은 사람 게이트라 훨씬 오래 걸릴 수 있다.
DEFAULT_PENDING_POLL_INTERVAL_SECONDS = 3.0
DEFAULT_PENDING_MAX_POLLS = 200  # ≈10분(3s×200) 후 포기 → 컨테이너 restart 가 재시도(멱등).

# hardwareId env 오버라이드 키(테스트·개발 기기).
HARDWARE_ID_ENV_KEY = "SENLYT_HARDWARE_ID"


class RegistrationError(Exception):
    """등록 실패. `retryable=False` 는 구성 오류(키·요청) — 재시도 없이 즉시 표면화."""

    def __init__(self, code: str, *, retryable: bool, http_status: int | None = None) -> None:
        self.code = code
        self.retryable = retryable
        self.http_status = http_status
        super().__init__(f"RegistrationError({code} retryable={retryable} http={http_status})")


def build_register_request(device_id: str, name: str | None = None) -> dict[str, Any]:
    """RegisterRequest 와이어 조립 — deviceId(=수집 시리얼·D-A) 제시. name 은 includeIfNull:false."""
    if not isinstance(device_id, str) or device_id == "":
        raise ValueError("deviceId(시리얼)는 비어있지 않은 문자열(계약 minLength 1)")
    m: dict[str, Any] = {"deviceId": device_id}
    put_if_present(m, "name", name)
    return m


def parse_register_response(body: Mapping[str, Any] | None, device_id: str) -> DeviceIdentity:
    """RegisterResponse 방어 파싱 — 서버가 발급한 dispenserToken·exp·mode 를 취한다(승인·200).

    [D-A] deviceId 는 **pi 자기 시리얼(`device_id` 인자)이 권위** — 서버 응답의 deviceId(echo)로
    덮어쓰지 않는다. 서버 발급값이 없어져도(등록만) 안전하도록 body 의 deviceId 는 읽지 않는다.
    dispenserToken·exp 계약 위반(누락·형식)만 retryable RegistrationError(과도기 서버 방어).
    mode(선택·flavor|fragrance) = 승인 시 서버가 배정한 기기 모드(부재/null 이면 None → env 폴백).
    """
    if body is None:
        raise RegistrationError("malformed_response", retryable=True)
    token = body.get("dispenserToken")
    exp = body.get("exp")
    if not isinstance(token, str) or token == "":
        raise RegistrationError("malformed_response", retryable=True)
    if isinstance(exp, bool) or not isinstance(exp, int):
        raise RegistrationError("malformed_response", retryable=True)
    # mode 화이트리스트 — 서버(신뢰경계)가 준 값도 flavor|fragrance 만 수용(그 외=None→env/flavor 폴백).
    #   손상 서버/MITM 의 비정상 mode 로 SSE 구독 경로가 오염되는 것 방지(평가자 P2 봉합).
    raw_mode = body.get("mode")
    mode = raw_mode if raw_mode in ("flavor", "fragrance") else None
    return DeviceIdentity(device_id=device_id, dispenser_token=token, exp=exp, mode=mode)


class RegistrationClient:
    """등록 호출기 — 재시도 정책 포함(전송은 seam)."""

    def __init__(
        self,
        transport: RegisterTransport,
        *,
        device_id: str,
        name: str | None = None,
        max_retries: int = DEFAULT_REGISTER_MAX_RETRIES,
        retry_delays_seconds: tuple[float, ...] = DEFAULT_RETRY_DELAYS_SECONDS,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        self.transport = transport
        self.device_id = device_id  # = 수집 HW 시리얼(D-A). 등록 요청에 제시·정체성 권위값.
        self.name = name
        self.max_retries = max_retries
        self.retry_delays_seconds = retry_delays_seconds
        self._sleep = sleep if sleep is not None else time.sleep

    def register(self) -> DeviceIdentity | None:
        """등록 1회 실행(전송오류·5xx 는 bounded 재시도).

        반환:
          - DeviceIdentity: 승인됨(200 — dispenserToken·mode 발급).
          - None: **TOFU pending**(202 — 운영자 승인 대기). 승인 폴링은 ensure_registered 가 담당.
        예외:
          - RegistrationError(retryable=False): 4xx invalid_request(구성 오류·재시도 무의미).
          - RegistrationError(retryable=True): 전송/5xx/계약위반 재시도 소진.
        """
        request = build_register_request(self.device_id, self.name)
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

            if status == 202:
                # TOFU pending — 운영자 승인 대기(토큰 미발급). 오류 아님 → None 반환(폴링은 상위).
                return None

            if 200 <= status < 300:
                try:
                    return parse_register_response(body, self.device_id)
                except RegistrationError as e:
                    last_error = e  # 계약 위반 본문 — retryable(과도기 서버 방어).
                    continue

            if 400 <= status < 500:
                # 400 invalid_request — 재시도 무의미(구성 오류). (공유키 제거로 401 경로 없음·TOFU.)
                raise RegistrationError("invalid_request", retryable=False, http_status=status)

            # 5xx — 500 register_failed 등 → 재시도.
            code = "register_failed"
            last_error = RegistrationError(code, retryable=True, http_status=status)

        assert last_error is not None
        raise last_error


def ensure_registered(
    store: DeviceIdentityStore,
    client: RegistrationClient,
    *,
    now_seconds: int | None = None,
    force: bool = False,
    server_base_url: str | None = None,
    pending_max_polls: int = DEFAULT_PENDING_MAX_POLLS,
    pending_poll_interval_seconds: float = DEFAULT_PENDING_POLL_INTERVAL_SECONDS,
    sleep: Callable[[float], None] | None = None,
) -> DeviceIdentity:
    """부팅 진입점 — 저장된 정체성이 유효(미만료·동일 deviceId·**동일 서버**)하면 재사용, 아니면 등록.

    토큰 만료(계약상 장수명 10년·2026-07-19·사실상 안 만료) 시 재등록으로 재발급. [D-A] 저장된 deviceId 가 현재 시리얼과 다르면
    (기판 교체·구 dsp-<hash> 파일에서 승격 등) 저장분을 버리고 재등록(레지스트리 upsert 키 = deviceId=시리얼).

    **서버 바인딩(2026-07-23)**: `server_base_url` 이 주어지면(부팅 시 항상), 저장된 정체성의
    `server_base_url` 과 다를 때(=서버를 바꿔 재설치했을 때, 또는 구 파일이라 None일 때) 저장분을 버리고
    **현재 서버에 재등록**한다. 토큰·deviceId 는 발급 서버 레지스트리에서만 의미가 있어(서버마다 DB·HMAC
    서명키 상이), 옛 서버 정체성을 새 서버에 재사용하면 그 서버엔 register 가 안 가 admin 후보에 안 떠
    페어링이 안 된다(2026-07-23 dev 검증에서 실측). 새로 발급된 정체성에는 현재 `server_base_url` 을 각인해
    저장한다. 비교는 trailing slash 무시(server_config.base_url 은 이미 정규화되나 방어적으로 rstrip).

    **TOFU 승인 대기(2026-07-17)**: 등록이 202 pending(client.register()→None)이면 운영자가 /admin 에서
    승인할 때까지 `pending_poll_interval_seconds` 간격으로 재등록을 폴링한다(최대 `pending_max_polls`).
    승인 전엔 토큰이 없어 부팅이 여기서 대기하는 것이 정상(승인=사람 게이트). 소진 시 registration_pending
    예외 → 컨테이너/서비스 restart 가 재시도(멱등·같은 deviceId).
    """
    now = now_seconds if now_seconds is not None else int(time.time())

    def _same_server(a: str | None, b: str | None) -> bool:
        # 둘 다 None(=서버 바인딩 미사용 경로)이면 같다고 본다. 한쪽만 None(구 파일)이면 다름 → 재등록.
        if a is None and b is None:
            return True
        if a is None or b is None:
            return False
        return a.rstrip("/") == b.rstrip("/")

    if not force:
        existing = store.load()
        if (
            existing is not None
            and existing.device_id == client.device_id
            and _same_server(existing.server_base_url, server_base_url)
            and not is_identity_expired(existing, now_seconds=now)
        ):
            return existing

    _sleep = sleep if sleep is not None else time.sleep
    for poll in range(pending_max_polls + 1):
        identity = client.register()
        if identity is not None:
            # 발급 정체성에 현재 서버를 각인(서버 바인딩) — 서버가 응답에 base URL 을 주지 않으므로
            #   pi 가 자신이 등록한 서버를 기록한다. server_base_url 미지정(테스트 등)이면 그대로 둔다.
            if server_base_url is not None:
                identity = replace(identity, server_base_url=server_base_url)
            store.save(identity)
            return identity
        # None = TOFU pending(202) — 운영자 승인 대기. 폴링 계속.
        if poll < pending_max_polls:
            _sleep(pending_poll_interval_seconds)
    raise RegistrationError("registration_pending", retryable=True, http_status=202)


def make_http_register_transport(
    register_url: str,
    *,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    request: Callable[..., "tuple[int, Mapping[str, Any] | None]"] = request_json,
) -> RegisterTransport:
    """실 HTTP RegisterTransport 조립 — POST {register_url} (TOFU · **인증 헤더 없음**·2026-07-17).

    스텁 제거: RegistrationClient 의 seam 에 꽂는 **실 클라이언트**(표준 urllib).
      - 요청 body 는 이미 build_register_request 가 만든 와이어(dict) 그대로 전송(deviceId·name?).
      - 공유키(Bearer 프로비저닝 키) 제거 — 보안은 서버측 pending + 운영자 승인(TOFU)으로 이동.
      - HTTP 응답(200 승인·202 pending·4xx/5xx)은 (status, body) 로 반환 → RegistrationClient 가 분류.
      - 네트워크 실패(HttpTransportError)는 그대로 raise → RegistrationClient 가 retryable 처리.

    `request` 는 테스트 주입 seam(기본 = http_client.request_json).
    """

    def transport(req_body: dict[str, Any]) -> "tuple[int, Mapping[str, Any] | None]":
        return request("POST", register_url, body=req_body, timeout=timeout)

    return transport


def read_hardware_id(
    *,
    env: Mapping[str, str] | None = None,
    cpuinfo_path: Path | str = "/proc/cpuinfo",
    devicetree_serial_path: Path | str = "/proc/device-tree/serial-number",
    machine_id_path: Path | str = "/etc/machine-id",
) -> str | None:
    """기기 고유 HW 식별자 seam — [D-A] 이 값이 **곧 deviceId**(RegisterRequest.deviceId·레지스트리 키).

    우선순위: ① env SENLYT_HARDWARE_ID(테스트·개발 주입) → ② /proc/cpuinfo `Serial`
    (Pi 1~4 CPU 시리얼) → ③ **/proc/device-tree/serial-number**(RPi 2~5 공통 HW 시리얼 —
    RPi 5 는 cpuinfo 에 `Serial` 이 없을 수 있어 이 소스로 크로스모델 안정화) → ④ /etc/machine-id
    폴백(⚠️ HW 시리얼 아님 = OS 설치 UUID·재플래시 시 변동). 전부 실패 → None
    (호출측이 등록 불가로 표면화 — silent 임의값 생성 금지·deviceId 안정성).

    ⚠️ **RPi 4·5 호환(2026-07-17)**: RPi 4 는 ②(cpuinfo Serial)로, RPi 5 는 ③(devicetree
    serial-number)로 **동일한 HW 시리얼**을 얻는다. ④(machine-id)는 최후 폴백일 뿐 —
    HW 키가 아니므로 재플래시 시 deviceId 가 바뀌어 재등록·구 등록 고아화 위험(경고 로그 대상).
    seam 이름(read_hardware_id)·env 키(SENLYT_HARDWARE_ID)는 유지 — 반환값의 의미만 deviceId 로 확정.
    """
    e = env if env is not None else os.environ
    override = e.get(HARDWARE_ID_ENV_KEY)
    if isinstance(override, str) and override.strip() != "":
        return override.strip()

    # ② /proc/cpuinfo `Serial` (RPi 1~4).
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

    # ③ /proc/device-tree/serial-number (RPi 2~5 공통 — RPi 5 대응). 값은 NUL 종단 문자열.
    try:
        raw = Path(devicetree_serial_path).read_bytes()
        serial = raw.split(b"\x00", 1)[0].decode("ascii", "ignore").strip()
        # 선행 0 만으로 이뤄진 16-hex(전부 0)나 빈값은 무효(미프로비저닝 EEPROM) → 다음 폴백.
        if serial and set(serial) != {"0"}:
            return serial
    except (FileNotFoundError, OSError, ValueError):
        pass

    # ④ /etc/machine-id (⚠️ HW 시리얼 아님 — 최후 폴백).
    try:
        machine_id = Path(machine_id_path).read_text(encoding="utf-8").strip()
        if machine_id:
            return machine_id
    except (FileNotFoundError, OSError, UnicodeDecodeError):
        pass

    return None
