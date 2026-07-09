"""Bearer 세션 토큰 만료판단 — SoT §7-4 (축 B) / 부록A P-5 (opaque 원칙).

Dart 오라클엔 전용 테스트 파일이 없어 신규 작성(Stage 1 범위: payload 파싱·만료 판단).
서명 검증은 서버 책임 — 여기서는 exp/role 파싱과 strict 만료(=이면 만료)만 검증.
"""

import base64
import json

from senlyt_pi.core.dispenser_session import (
    DISPENSER_ROLE,
    DISPENSER_SESSION_TTL_SECONDS,
    is_token_expired,
    peek_token_payload,
)


def _token(payload: dict) -> str:
    b64 = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    return f"{b64.rstrip('=')}.fakesig"  # 서명은 opaque — 파싱만 검증.


def test_peek_valid_payload():
    """정상 payload — sub/role/iat/exp 파싱."""
    p = peek_token_payload(_token({"sub": "dev-1", "role": "dispenser", "iat": 100, "exp": 43300}))
    assert p is not None
    assert p.sub == "dev-1"
    assert p.role == DISPENSER_ROLE
    assert p.iat == 100
    assert p.exp == 43300


def test_peek_rejects_malformed():
    """형식·role 이 어긋나면 None(방어)."""
    assert peek_token_payload(None) is None
    assert peek_token_payload("") is None
    assert peek_token_payload("no-dot-token") is None
    assert peek_token_payload(".sig") is None
    assert peek_token_payload("!!!notbase64.sig") is None
    # role 불일치(operator 토큰 교차 재사용 차단 — 축 C).
    assert peek_token_payload(_token({"sub": "s", "role": "operator", "iat": 0, "exp": 1})) is None
    # sub 빈 문자열 / exp 비정수.
    assert peek_token_payload(_token({"sub": "", "role": "dispenser", "iat": 0, "exp": 1})) is None
    assert peek_token_payload(_token({"sub": "s", "role": "dispenser", "iat": 0, "exp": "1"})) is None


def test_expiry_strict():
    """exp(epoch sec) ≤ now 이면 만료 — strict(=이면 만료 · §7-4)."""
    p = peek_token_payload(_token({"sub": "d", "role": "dispenser", "iat": 0, "exp": 1000}))
    assert p is not None
    assert is_token_expired(p, now_seconds=999) is False
    assert is_token_expired(p, now_seconds=1000) is True  # = 이면 만료.
    assert is_token_expired(p, now_seconds=1001) is True


def test_ttl_constant():
    """세션 TTL = 12h (§7-2, 참고 상수)."""
    assert DISPENSER_SESSION_TTL_SECONDS == 12 * 60 * 60
