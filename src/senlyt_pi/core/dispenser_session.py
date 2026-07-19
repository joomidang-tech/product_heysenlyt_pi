"""디스펜서 Bearer 세션 토큰 (수신·검증측 미러) — SoT §7-4 (축 B).

Dart `lib/core/dispenser_session.dart` 포팅. **정본 = 서버 `dispenserSession.ts`.**
pi 는 POST /api/dispenser/login 으로 토큰을 **발급받아 저장·전송**만 한다(서명·검증은 서버).
따라서 이 파일은:
  - 토큰 payload 구조/키 순서(sub,role,iat,exp·부록A P-5)를 **알기만** 하고,
  - 서명은 pi 가 직접 만들지 않는다(토큰은 opaque). getSessionSecret 이 pi 에 없음(§7-8).

⚠️ 부록A P-5: 토큰은 pi 에서 **opaque 로만** 다룬다 — payload 를 재직렬화하려 하지 말 것
   (JSON 키 순서 한 글자만 달라도 서명 깨짐). 만료(exp) 판단을 위해 **파싱만** 허용.

crypto(HMAC 검증)는 pi 범위 밖(서버 책임) — 이번 웨이브는 payload 파싱·만료 판단만.
"""

from __future__ import annotations

import base64
import binascii
import json
import time
from dataclasses import dataclass
from typing import Any

# 디스펜서 role 상수 — SoT §7-4.
DISPENSER_ROLE = "dispenser"

# 세션 TTL(초) — 장수명 10년 (2026-07-19 야간 오프라인 해결·서버 dispenserSession 미러).
#   참고용(발급·서명은 서버). pi 는 만료를 이 상수가 아니라 토큰 payload 의 exp 로 판단한다.
DISPENSER_SESSION_TTL_SECONDS = 60 * 60 * 24 * 365 * 10

# 서명 도메인 prefix — SoT §7-4 / 부록A P-6. **참고 상수**(pi 는 서명 안 함).
# 교차 재사용 차단의 근거 — 축 A(prefix 없음)·축 B(dispenser)·축 C(operator).
DISPENSER_SIG_DOMAIN = "dispenser-session:v1:"


@dataclass(frozen=True, slots=True)
class DispenserTokenPayload:
    """토큰 payload(읽기 전용) — 키 순서 sub,role,iat,exp (부록A P-5, opaque 원칙)."""

    sub: str
    role: str
    iat: int
    exp: int


def _pad(b64url: str) -> str:
    """base64url 패딩 복원(Python b64decode 는 패딩 필요 — Dart 와 동일)."""
    mod = len(b64url) % 4
    return b64url if mod == 0 else b64url + "=" * (4 - mod)


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def peek_token_payload(token: str | None) -> DispenserTokenPayload | None:
    """로컬 만료 사전판단(선택) — opaque 토큰의 exp 만 base64url payload 에서 읽는다.

    ⚠️ 이것은 **서버 검증의 대체가 아니다**. 서명 검증은 서버가 수행한다.
    pi 는 만료 임박 시 재로그인 트리거를 위해 exp 만 참조한다(네트워크 절약).
    형식·role 이 어긋나면 None(방어).
    """
    if token is None or token == "":
        return None
    dot = token.rfind(".")
    if dot <= 0:
        return None
    payload_b64 = token[:dot]
    try:
        json_str = base64.urlsafe_b64decode(_pad(payload_b64)).decode("utf-8")
        decoded = json.loads(json_str)
    except (binascii.Error, UnicodeDecodeError, ValueError):
        return None
    if not isinstance(decoded, dict):
        return None
    sub = decoded.get("sub")
    role = decoded.get("role")
    exp = decoded.get("exp")
    iat = decoded.get("iat")
    if not isinstance(sub, str) or sub == "":
        return None
    if not isinstance(role, str) or role != DISPENSER_ROLE:
        return None
    if not _is_int(exp):
        return None
    return DispenserTokenPayload(
        sub=sub,
        role=role,
        iat=iat if _is_int(iat) else 0,
        exp=exp,
    )


def is_token_expired(payload: DispenserTokenPayload, *, now_seconds: int | None = None) -> bool:
    """exp(epoch sec) 가 now 이하이면 만료 — SoT §7-4 (strict, =이면 만료)."""
    now = now_seconds if now_seconds is not None else int(time.time())
    return payload.exp <= now
