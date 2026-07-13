"""서버 타겟 결정 — 브랜치=환경 → 서버 base URL (2026-07-09 사용자 확정).

pi 도 web 과 대칭으로 **브랜치(main/dev/v1.x.x)로 관리**하고, 브랜치=환경에 따라
register·SSE·commandsets·heartbeat·status 를 보내는 **서버 base URL 이 환경 설정 하나로 결정**된다.
프리뷰 기계가 prod 를 조용히 보거나 그 반대가 **구조적으로 불가능**해야 한다.

결정 규칙 (우선순위):
  1. 명시 `SENLYT_SERVER_BASE_URL` 이 있으면 **최우선**(탈출구 — 로컬/임시/신환경).
  2. 없으면 `SENLYT_ENV`(prod|dev|v1_2_0|v1_1_0) → 매핑 테이블로 base URL 결정.
  3. 둘 다 없으면 **fail-fast** (`ServerTargetError`) — prod 로 조용히 붙는 사고를 막기 위해
     안전 기본값을 두지 않는다(명시 요구).

브랜치 ↔ 환경 ↔ base URL:
  | 브랜치   | SENLYT_ENV | 서버 base URL                    |
  | main    | prod       | https://senlyt.com               |
  | dev     | dev        | https://dev-env.senlyt.com           |
  | v1.2.0  | v1_2_0     | https://v1-2-0.env.senlyt.com    |
  | v1.1.0  | v1_1_0     | https://v1-1-0.env.senlyt.com    |

배포: systemd `EnvironmentFile=/etc/senlyt/device.env` 에 `SENLYT_ENV=` 주입 —
버전 브랜치별 배포 산출물(device.env)이 자기 환경값을 갖는다(02_infra §4.2·§4.9).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Mapping
from urllib.parse import quote, urlencode, urlsplit

# 환경 선택 env 키 (브랜치=환경 → 이 값 하나로 서버 타겟이 고정).
SENLYT_ENV_KEY = "SENLYT_ENV"

# 명시 base URL 탈출구 env 키 (있으면 SENLYT_ENV 보다 우선 — 로컬/임시/신환경).
SENLYT_SERVER_BASE_URL_KEY = "SENLYT_SERVER_BASE_URL"

# ENV → 서버 base URL 매핑 (정본 테이블 — 코드에 고정, 브랜치별 배포가 SENLYT_ENV 만 주입).
# 값은 trailing slash 없음(join_url 이 항상 정규화하지만 소스도 정규형 유지).
ENV_TO_BASE_URL: dict[str, str] = {
    "prod": "https://senlyt.com",
    "dev": "https://dev-env.senlyt.com",
    "v1_2_0": "https://v1-2-0.env.senlyt.com",
    "v1_1_0": "https://v1-1-0.env.senlyt.com",
}

# 브랜치명 → SENLYT_ENV 정규화 규칙 SoT (로컬 provision·CI 검증 공유·web 슬러그 파생과 대칭).
_VERSION_BRANCH_RE = re.compile(r"^v\d+\.\d+\.\d+$")


def branch_to_env(branch: str | None) -> str | None:
    """git 브랜치명 → SENLYT_ENV 값. 미지원 브랜치는 None.

    브랜치=환경 규칙의 단일 SoT — 로컬 provision 스크립트와 CI 검증이 공유한다.
    web deploy 워크플로의 `${github.ref_name//./-}`(슬러그) 파생과 같은 원리이나,
    pi 의 SENLYT_ENV 키는 언더스코어를 쓴다:
      main → prod / dev → dev / vX.Y.Z → vX_Y_Z / 그 외 → None.
    반환 env 가 ENV_TO_BASE_URL 키가 아니어도(예: 미배포 버전) 서버 타겟 해석 시 fail-fast 로 걸린다.
    """
    if branch is None:
        return None
    b = branch.strip()
    if b == "main":
        return "prod"
    if b == "dev":
        return "dev"
    if _VERSION_BRANCH_RE.match(b):
        return b.replace(".", "_")
    return None


class ServerTargetError(Exception):
    """서버 타겟(base URL) 결정 실패 — 미설정/미지원 환경/잘못된 URL.

    fail-fast 신호: 안전 기본값(prod 조용한 접속)을 두지 않으므로, 환경이 명시되지 않으면
    부팅 시점에 이 예외로 즉시 표면화한다(오배포·오접속 방지).
    """


def _normalize_base_url(url: str) -> str:
    """base URL 정규화 — 공백 제거 + trailing slash 제거. 스킴 검증은 호출측."""
    return url.strip().rstrip("/")


def _validate_base_url(url: str) -> str:
    """base URL 스킴 검증(http/https + netloc 존재). 위반 시 ServerTargetError."""
    normalized = _normalize_base_url(url)
    parts = urlsplit(normalized)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise ServerTargetError(
            f"잘못된 서버 base URL(스킴은 http/https·호스트 필수): {url!r}"
        )
    return normalized


def resolve_server_base_url(env: str | None, explicit_url: str | None = None) -> str:
    """서버 base URL 결정 — 순수 함수(테스트 가능·부작용 없음).

    우선순위:
      1. `explicit_url` 이 비어있지 않은 문자열 → 정규화·스킴 검증 후 반환(탈출구·최우선).
      2. `env` 가 매핑 테이블의 알려진 키 → 매핑 base URL 반환.
      3. 그 외(둘 다 없음·미지원 env) → `ServerTargetError`(fail-fast).

    Args:
        env: SENLYT_ENV 값(prod|dev|v1_2_0|v1_1_0). 앞뒤 공백·대소문자는 정규화.
        explicit_url: SENLYT_SERVER_BASE_URL 명시값. 빈 문자열/공백은 미설정으로 취급.

    Raises:
        ServerTargetError: 결정 불가(미설정) 또는 미지원 env 또는 잘못된 explicit_url.
    """
    # ── 1. 명시 URL 탈출구(최우선). 빈/공백 문자열은 미설정으로 취급해 env 로 폴백. ──
    if explicit_url is not None and explicit_url.strip() != "":
        return _validate_base_url(explicit_url)

    # ── 2·3. ENV 매핑. 미설정/미지원은 fail-fast. ──
    if env is None or env.strip() == "":
        raise ServerTargetError(
            f"서버 타겟 미설정 — {SENLYT_ENV_KEY} 또는 {SENLYT_SERVER_BASE_URL_KEY} 중 "
            f"하나를 반드시 지정해야 합니다(안전 기본값 없음·prod 조용한 접속 방지)."
        )

    normalized_env = env.strip().lower()
    base = ENV_TO_BASE_URL.get(normalized_env)
    if base is None:
        supported = ", ".join(sorted(ENV_TO_BASE_URL))
        raise ServerTargetError(
            f"미지원 {SENLYT_ENV_KEY}={env!r} — 지원 환경: {supported}. "
            f"(신환경은 {SENLYT_SERVER_BASE_URL_KEY} 로 명시하거나 매핑 테이블에 추가)"
        )
    return base


def resolve_from_environ(environ: Mapping[str, str]) -> str:
    """환경변수 매핑에서 서버 base URL 결정 — 진입점(os.environ 또는 주입 매핑).

    `SENLYT_SERVER_BASE_URL`(명시·우선) 과 `SENLYT_ENV`(매핑) 를 읽어
    `resolve_server_base_url` 에 위임한다. 미설정 시 `ServerTargetError`(fail-fast).
    """
    return resolve_server_base_url(
        environ.get(SENLYT_ENV_KEY),
        environ.get(SENLYT_SERVER_BASE_URL_KEY),
    )


def join_url(base_url: str, path: str) -> str:
    """base URL + 경로 결합 — 단일 슬래시 보장(엔드포인트 조립 SoT).

    base 의 trailing slash·path 의 leading slash 유무와 무관하게 정확히 하나로 결합한다.
    """
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


# ── 디스펜서 축 엔드포인트 경로(05_api §12 계약 — path 정본) ──
PATH_REGISTER = "/api/dispensers/register"
PATH_LOGIN = "/api/dispenser/login"
PATH_HEARTBEAT = "/api/dispenser/heartbeat"
PATH_ORDERS = "/api/dispenser/orders"
PATH_ORDERS_STREAM = "/api/dispenser/orders/stream"
PATH_SETTINGS_STREAM = "/api/dispenser/settings"
PATH_COMMANDSETS = "/api/dispenser/commandsets"
PATH_TRACE = "/api/dispenser/trace"


@dataclass(frozen=True, slots=True)
class ServerConfig:
    """결정된 서버 타겟 — base URL + 디스펜서 축 엔드포인트 조립의 단일 소비 지점.

    register/SSE/heartbeat/status/commandsets 어댑터는 하드코딩 URL 대신 이 객체의
    엔드포인트만 사용한다. `from_environ` 이 유일한 생성 경로(부팅 시 1회 결정).
    """

    base_url: str

    @classmethod
    def from_environ(cls, environ: Mapping[str, str]) -> "ServerConfig":
        """환경변수에서 서버 타겟을 결정해 구성 — 미설정 시 ServerTargetError(fail-fast)."""
        return cls(base_url=resolve_from_environ(environ))

    def url(self, path: str) -> str:
        """임의 경로의 절대 URL 조립(엔드포인트 SoT)."""
        return join_url(self.base_url, path)

    @property
    def register_url(self) -> str:
        return self.url(PATH_REGISTER)

    @property
    def login_url(self) -> str:
        return self.url(PATH_LOGIN)

    @property
    def heartbeat_url(self) -> str:
        return self.url(PATH_HEARTBEAT)

    @property
    def orders_stream_url(self) -> str:
        return self.url(PATH_ORDERS_STREAM)

    @property
    def settings_stream_url(self) -> str:
        return self.url(PATH_SETTINGS_STREAM)

    @property
    def commandsets_url(self) -> str:
        return self.url(PATH_COMMANDSETS)

    @property
    def trace_url(self) -> str:
        return self.url(PATH_TRACE)

    def order_url(self, order_id: str, mode: str | None = None) -> str:
        """단건 주문 PATCH URL — /api/dispenser/orders/{orderId}?mode=(선택).

        orderId 는 path 세그먼트 인코딩(합성키 콜론 등 안전). mode 지정 시 쿼리로 부착.
        """
        base = join_url(self.base_url, f"{PATH_ORDERS}/{quote(order_id, safe='')}")
        if mode:
            return f"{base}?{urlencode({'mode': mode})}"
        return base

    def commandset_url(self, command_set_id: str) -> str:
        """단건 CommandSet PATCH URL — /api/dispenser/commandsets/{id}.

        commandSetId 는 콜론 포함 가능(manufacture `{orderId}:{attempt}`) → path 인코딩.
        """
        return join_url(
            self.base_url, f"{PATH_COMMANDSETS}/{quote(command_set_id, safe='')}"
        )

    def orders_stream_query_url(self, *, mode: str, view: str, device_id: str | None) -> str:
        """주문 큐 SSE 구독 URL — mode/view/deviceId(CS-08 라우팅) 쿼리 부착."""
        params: dict[str, str] = {"mode": mode, "view": view}
        if device_id:
            params["deviceId"] = device_id
        return f"{self.orders_stream_url}?{urlencode(params)}"
