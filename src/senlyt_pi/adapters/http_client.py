"""표준 라이브러리 HTTP 클라이언트 — 실 전송 어댑터의 왕복/스트리밍 공용 하부.

⛔ 외부 의존 0 (pyproject 원칙 "표준 라이브러리 우선"). urllib.request 만 사용한다.
   register/status/heartbeat/trace 왕복(JSON)과 SSE 구독(스트리밍)을 이 모듈이 담당하고,
   각 어댑터는 요청 shaping·재시도·OQ 정책만 갖는다(전송은 여기로 위임).

분류 규약(어댑터 재시도층이 기대하는 계약):
  - HTTP 응답(2xx/4xx/5xx)은 **예외가 아니라** `(status, body)` 로 반환한다
    (4xx/5xx 본문도 파싱해 돌려줌 — urllib.error.HTTPError 를 흡수).
  - **네트워크/전송 실패**(연결 거부·타임아웃·DNS·소켓)는 `HttpTransportError` 로 raise
    → 어댑터가 retryable(등록 R=3·OQ 재적재)로 처리한다.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any, Iterator, Mapping

# 기본 타임아웃(초) — 왕복은 짧게(관측이 제조를 막지 않도록). SSE 는 별도(길게/None).
DEFAULT_TIMEOUT_SECONDS = 10.0


class HttpTransportError(Exception):
    """네트워크/전송 실패(연결 거부·타임아웃·DNS 등) — retryable 신호.

    HTTP 상태 응답(4xx/5xx)은 이 예외가 아니라 (status, body) 로 반환된다.
    """


def _parse_body(raw: bytes | None) -> dict[str, Any] | None:
    """응답 본문 JSON 파싱 — 비어있거나 JSON 이 아니면 None(방어)."""
    if not raw:
        return None
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    return decoded if isinstance(decoded, dict) else None


def request_json(
    method: str,
    url: str,
    *,
    body: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> tuple[int, dict[str, Any] | None]:
    """JSON 왕복 — (HTTP status, 파싱된 body|None) 반환.

    body 가 있으면 Content-Type: application/json 으로 직렬화 전송한다.
    네트워크 실패는 HttpTransportError. HTTP 상태(4xx/5xx 포함)는 정상 반환.
    """
    data = json.dumps(dict(body)).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method.upper())
    if data is not None:
        req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", None) or resp.getcode()
            return int(status), _parse_body(resp.read())
    except urllib.error.HTTPError as e:
        # 4xx/5xx — 상태·본문을 그대로 반환(예외 아님). 본문 파싱 실패는 None.
        try:
            raw = e.read()
        except Exception:
            raw = None
        return int(e.code), _parse_body(raw)
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise HttpTransportError(f"전송 실패: {e}") from e


class SseStream:
    """SSE(text/event-stream) 스트리밍 구독 — urllib 응답을 프레임 단위로 순회.

    with 문(컨텍스트) 또는 close()로 반드시 정리한다(연결 누수 방지·§F8 결).
    `events()` 는 (event, data_str) 튜플을 방출한다 — SSE 주석(`:` heartbeat)은 건너뛴다.
    """

    def __init__(self, response: Any) -> None:
        self._resp = response
        # 트리클 워치독 관측점(감사 P3 봉합·2026-07-15) — 마지막 라인 수신 monotonic 시각.
        # events() 가 **모든 라인**(주석 heartbeat 포함) 처리 시 갱신. 초기값 = 생성(연결) 시각
        # → 연결 직후 무수신(트리클/행업)도 스테일 판정 가능. 어댑터 워치독이 이를 검사한다.
        self.last_line_monotonic: float = time.monotonic()

    def events(self) -> Iterator[tuple[str, str]]:
        """SSE 프레임 순회 — `event:`/`data:` 누적, 빈 줄에서 1프레임 방출.

        data 가 여러 줄이면 개행으로 이어 붙인다(SSE 규격). event 미지정 프레임은 'message'.
        스트림이 닫히면 순회가 끝난다.
        """
        event = "message"
        data_lines: list[str] = []
        for raw in self._resp:
            # 모든 라인(빈 줄·주석 heartbeat 포함)에서 관측점 갱신 — 트리클 워치독 근거.
            self.last_line_monotonic = time.monotonic()
            line = raw.decode("utf-8", errors="replace").rstrip("\n").rstrip("\r")
            if line == "":
                # 프레임 경계 — data 가 있으면 방출.
                if data_lines:
                    yield event, "\n".join(data_lines)
                event = "message"
                data_lines = []
                continue
            if line.startswith(":"):
                continue  # 주석(heartbeat) — 무시.
            field, _, value = line.partition(":")
            if value.startswith(" "):
                value = value[1:]  # SSE 규격: 콜론 뒤 공백 1개 제거.
            if field == "event":
                event = value
            elif field == "data":
                data_lines.append(value)
            # id/retry 등 기타 필드는 무시(현 계약 미사용).

    def close(self) -> None:
        try:
            self._resp.close()
        except Exception:
            pass

    def __enter__(self) -> "SseStream":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


def _upgrade_read_timeout(resp: Any, timeout: float | None) -> bool:
    """연결 성공 후 하부 소켓 read 타임아웃을 상향(connect/read 분리·감사 P3 봉합·2026-07-15).

    ⚠️ `resp.fp.raw._sock` 류는 CPython(http.client → socket.makefile) **구현 세부**다 —
    후보 경로를 순서대로 시도하고, 전부 실패하면 False 를 반환한다(예외는 전부 삼킴).
    False 시 호출측(open_sse)이 **단일 타임아웃으로 재연결**해 구 의미론을 복원한다 —
    "조용한 폴백 = read 타임아웃이 connect(20s)로 고정" 은 서버 heartbeat(15s) 대비 여유가
    5s 뿐이라 느린 링크에서 불필요한 재연결 churn 을 만들었다(리뷰 P3 봉합·2026-07-15).
    """
    for getter in (
        lambda r: r.fp.raw._sock,  # noqa: SLF001 — CPython http.client 표준 경로.
        lambda r: r.fp._sock,  # noqa: SLF001 — 변형(버퍼 없는 makefile).
        lambda r: r.raw._sock,  # noqa: SLF001 — 방어적 후보.
    ):
        try:
            getter(resp).settimeout(timeout)
            return True
        except Exception:  # noqa: BLE001 — 다음 후보.
            continue
    return False


def open_sse(
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    timeout: float | None = None,
    connect_timeout: float | None = None,
) -> SseStream:
    """SSE 구독 시작 — 응답 스트림을 감싼 SseStream 반환(호출측이 close 책임).

    timeout=None 이면 소켓 무한 대기(스트리밍 특성). 연결 실패는 HttpTransportError.
    connect_timeout 이 주어지면 **연결은 짧게**(urlopen timeout=connect_timeout) 시도하고,
    연결 성공 후 read 타임아웃을 `timeout` 으로 올린다(connect/read 분리 가드 —
    느린 핸드셰이크는 빨리 실패, 정상 스트림의 유휴 read 는 길게 허용·감사 P3).
    """
    req = urllib.request.Request(url, method="GET")
    req.add_header("Accept", "text/event-stream")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    def _open(t: float | None) -> Any:
        try:
            return urllib.request.urlopen(req, timeout=t)
        except urllib.error.HTTPError as e:
            # 스트림 시작 자체가 4xx/5xx(401 unauthorized·403 forbidden_device 등) — 전송 오류로 표면화.
            raise HttpTransportError(f"SSE 시작 거부(status={e.code})") from e
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            raise HttpTransportError(f"SSE 연결 실패: {e}") from e

    effective_timeout = connect_timeout if connect_timeout is not None else timeout
    resp = _open(effective_timeout)
    if connect_timeout is not None and not _upgrade_read_timeout(resp, timeout):
        # 소켓 업그레이드 실패(비-CPython 등) — read=connect(20s)로 두면 heartbeat(15s) 여유가
        # 5s 뿐이라 churn 위험(리뷰 P3). 구 단일 타임아웃 의미론으로 재연결(연결·read 둘 다 timeout).
        try:
            resp.close()
        except Exception:  # noqa: BLE001
            pass
        resp = _open(timeout)
    return SseStream(resp)


def bearer_headers(token: str, extra: Mapping[str, str] | None = None) -> dict[str, str]:
    """Authorization: Bearer 헤더 조립(+ 추가 헤더 병합)."""
    h: dict[str, str] = {}
    if token:
        h["Authorization"] = f"Bearer {token}"
    if extra:
        h.update(extra)
    return h
