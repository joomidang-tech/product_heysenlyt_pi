"""서버 SSE CommandSource/CommandSetSource 실어댑터 — 실 HTTP 구독(스텁 제거).

정본 계약: 05_api §8 · GET /api/dispenser/orders/stream (Bearer dispenser).

스텁 제거(사용자 원칙 2026-07-10): 이전 웨이브의 `raise NotImplementedError` 를 걷어내고
**실 SSE 클라이언트**(표준 urllib·http_client.open_sse)로 서버 큐를 구독한다.
  - 서버가 event:snapshot{orders, commands, commandSets} 를 push → 이 어댑터가 파싱.
  - `commands(device_id)` = snapshot.commands → Command 파생 → **CS-08 자기 deviceId 필터** → yield.
  - `command_sets(device_id)` = snapshot.commandSets → CommandSet 파생(queued|delivered·CS-08) → yield.
    (CommandSourcePort + CommandSetSourcePort 두 축을 한 어댑터가 제공 — 동일 snapshot 소비.)
  - deviceId 는 스트림 쿼리(`?deviceId=`)로도 서버가 1차 필터하고, 어댑터가 2차 필터(이중방어).

서버 base URL 은 하드코딩하지 않고 `ServerConfig`(config.server_target)가 환경별로 결정한
단일 base 를 소비한다(프리뷰가 prod 를 보는 사고 구조적 차단). SSE 는 스트리밍이라
`commands()`/`command_sets()` 는 연결이 살아있는 동안 도착분을 순차 방출하는 **무한 제너레이터**
(스트림 종료 시 순회 종료). 실 소비 루프(재연결·resync)는 daemon 이 조립한다.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Callable, Iterator, Mapping

from ..config.server_target import ServerConfig
from ..core.command_set import CommandSet, command_sets_from_snapshot
from ..core.wire_messages import Command
from ..obs.log import STAGE_PI_RECEIVED, StructuredLogger
from .http_client import DEFAULT_TIMEOUT_SECONDS, SseStream, bearer_headers, open_sse

# SSE 구독 소켓 타임아웃(초) — 스트리밍이므로 짧은 왕복 타임아웃보다 길게(무응답 감지용).
# None 이면 무한 대기. 기본은 서버 15s heartbeat 의 여유 배수.
DEFAULT_SSE_TIMEOUT_SECONDS = 60.0

# SSE 연결(핸드셰이크) 타임아웃(초) — read 타임아웃(60s)과 분리(감사 P3 봉합·2026-07-15).
# 느린 링크의 연결 지연은 빨리 실패시키고, 정상 스트림의 유휴 read 는 길게 허용한다.
DEFAULT_SSE_CONNECT_TIMEOUT_SECONDS = 20.0

# 트리클 워치독(감사 P3·2026-07-15) — read 타임아웃은 "완전 무수신"만 잡는다. 바이트가
# 찔끔찔끔 오는(트리클) 반죽은 연결은 read 를 계속 깨워 타임아웃이 영영 안 터진다 →
# 마지막 **라인** 수신 후 STALE_LIMIT 초과면 스트림을 강제 close(블록 read 가 예외로 깨져
# 소비 루프가 재연결). 검사 주기 = CHECK_INTERVAL. 서버 heartbeat 15s 의 여유 배수.
WATCHDOG_CHECK_INTERVAL_S = 15.0
WATCHDOG_STALE_LIMIT_S = 90.0

# ⛔ 스트림 수명 상한(강제 로테이션·2026-07-19 실기기 2회 실측) — **좀비 스트림 방어**.
#   서버 SSE 가 하트비트는 계속 보내는데 데이터 push 만 죽는 상태(좀비)가 실재한다:
#   06:32~06:35(191s)·06:58~07:03(293s) 두 번, 발행된 봉투가 스트림 종료(서버 ~5분 로테이션)
#   후의 재연결 snapshot 에서야 도착 → 정비 신선도 게이트(90s)에 익사했다. 트리클 워치독은
#   하트비트 **라인**도 수신으로 치므로 좀비를 못 잡는다(설계 한계 — 유휴와 좀비는 구분 불가).
#   대책 = 수명 상한: 스트림을 60s 마다 스스로 닫고 재연결한다. 재연결 즉시 서버가 전체
#   snapshot 을 push 하므로 어떤 유실·좀비도 **최대 60s 안에 자가 회복**된다(신선도 90s 안쪽).
#   비용 = 60s 당 핸드셰이크 1회(무시 가능).
MAX_STREAM_AGE_S = 60.0

# open_sse seam — (url, headers, timeout[, connect_timeout]) → SseStream. 테스트가 fake 주입.
OpenStream = Callable[..., SseStream]


def _is_stale(now: float, last: float, limit: float) -> bool:
    """트리클 스테일 판정(순수 함수·단위테스트용) — 마지막 라인 후 limit 초 초과면 스테일."""
    return (now - last) > limit


def commands_from_snapshot(
    snapshot: Mapping[str, Any], device_id: str
) -> list[Command]:
    """SSE snapshot data → 소비 대상 Command 목록 — CS-08 자기 deviceId 필터.

    - `commands` 필드(부재 시 빈 목록). 항목 단위 방어 파싱(깨진 항목 skip).
    - 자기 deviceId 만(다매장 라우팅·CS-08). 서버 1차 필터의 2차 방어.
    """
    raw = snapshot.get("commands")
    if not isinstance(raw, list):
        return []
    out: list[Command] = []
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        try:
            cmd = Command.from_json(item)
        except (KeyError, TypeError, ValueError):
            continue  # 깨진 command 는 skip(전체 snapshot 을 죽이지 않음).
        if cmd.device_id != device_id:
            continue
        out.append(cmd)
    return out


class SseCommandSourceAdapter:
    """서버 SSE command/commandSet 구독 실어댑터."""

    def __init__(
        self,
        *,
        server_config: ServerConfig | None = None,
        base_url: str = "",
        bearer_token: str = "",
        mode: str = "flavor",
        view: str = "pending",
        timeout: float | None = DEFAULT_SSE_TIMEOUT_SECONDS,
        open_stream: OpenStream = open_sse,
        logger: StructuredLogger | None = None,
        stop_event: "threading.Event | None" = None,
    ) -> None:
        # 서버 base 는 ServerConfig(환경별 결정) 우선 — 하드코딩 URL 금지.
        # base_url 인자는 하위호환(테스트·직접 주입)용. server_config 가 있으면 그 base 를 쓴다.
        self.server_config = server_config
        self.base_url = server_config.base_url if server_config is not None else base_url
        self.bearer_token = bearer_token
        self.mode = mode
        self.view = view
        self.timeout = timeout
        self._open_stream = open_stream
        self._log = logger
        # 종료 신호(감사 P3 봉합) — SSE 순회 중 set 되면 제너레이터를 즉시 종료해 boot 루프가
        #   while 조건을 재검사(SIGTERM 우아한 종료 지연 방지). 미주입이면 종료 검사 없음(기존 동작).
        self._stop = stop_event

    def set_stop_event(self, stop_event: "threading.Event") -> None:
        """종료 신호 주입(생성 후 결선) — daemon 이 자기 _stop 을 연결한다(bootstrap 시점엔 미존재)."""
        self._stop = stop_event

    def _config(self) -> ServerConfig:
        return self.server_config or ServerConfig(base_url=self.base_url)

    def _stream_url(self, device_id: str) -> str:
        return self._config().orders_stream_query_url(
            mode=self.mode, view=self.view, device_id=device_id
        )

    def _open(self, device_id: str) -> SseStream:
        # connect/read 타임아웃 분리(감사 P3) — 연결 20s·read 60s(기존). open_sse 는
        # connect_timeout 미지원 하부(가드 실패)면 단일 타임아웃으로 폴백한다(무해).
        return self._open_stream(
            self._stream_url(device_id),
            headers=bearer_headers(self.bearer_token),
            timeout=self.timeout,
            connect_timeout=DEFAULT_SSE_CONNECT_TIMEOUT_SECONDS,
        )

    def _start_watchdog(self, stream: SseStream) -> threading.Event:
        """트리클 워치독 기동(감사 P3 봉합·2026-07-15) — 정지 Event 반환(호출측 finally 가 set).

        데몬 스레드가 WATCHDOG_CHECK_INTERVAL_S 간격으로 스트림의 마지막 라인 수신 시각을
        검사, STALE_LIMIT 초과면 stream.close() — 블록된 read 가 예외로 깨져 소비 루프가
        재연결한다. 스트림이 정상 닫히면(finally) Event 로 워치독도 정리. 관측점
        (last_line_monotonic) 미보유 스트림(테스트 fake 등)이면 스레드를 띄우지 않는다.
        """
        stop = threading.Event()
        if not hasattr(stream, "last_line_monotonic"):
            return stop  # 관측점 없음 — 워치독 비활성(기존 동작·fake 스트림 무해).

        def _watch() -> None:
            while not stop.wait(WATCHDOG_CHECK_INTERVAL_S):
                last = getattr(stream, "last_line_monotonic", None)
                if not isinstance(last, float):
                    return  # 관측점 소실 — 판정 불가·워치독 종료(안전측).
                if _is_stale(time.monotonic(), last, WATCHDOG_STALE_LIMIT_S):
                    try:
                        stream.close()  # 블록 read 를 예외로 깨워 재연결 유도.
                    except Exception:  # noqa: BLE001 — close 실패는 삼킴(best-effort).
                        pass
                    return

        threading.Thread(target=_watch, name="senlyt-sse-watchdog", daemon=True).start()
        return stop

    def _snapshots(self, stream: SseStream) -> Iterator[dict[str, Any]]:
        """SseStream → snapshot data(dict) 순회 — event:snapshot 만·본문 JSON 파싱.

        ⚠️ 종료 신호(감사 P3): 각 SSE 이벤트(서버 heartbeat 코멘트 포함·주기 ≤15s) 처리 전에
          stop 을 검사해 SIGTERM 시 제너레이터를 즉시 종료 → boot 루프가 while 조건 재검사.
          소켓 read 자체가 블록돼도 서버 주기 heartbeat 가 루프를 돌려 최대 그 주기 내 반응한다.

        ⚠️ 수명 상한(MAX_STREAM_AGE_S·좀비 방어): 상한을 넘기면 제너레이터를 종료해 소비 루프가
          재연결하게 한다 — 하트비트만 살아있는 좀비 스트림도 최대 60s 안에 fresh snapshot 으로
          회복된다(상수 주석의 2026-07-19 실기기 근거). 검사도 이벤트 단위(하트비트 ≤15s)라
          유휴 스트림도 상한+15s 안에 로테이션된다.
        """
        opened_at = time.monotonic()
        for event, data in stream.events():
            if self._stop is not None and self._stop.is_set():
                return
            if time.monotonic() - opened_at > MAX_STREAM_AGE_S:
                if self._log is not None:
                    self._log.debug(
                        "SSE 스트림 수명 상한 — 강제 로테이션(좀비 방어·재연결 snapshot 재동기)",
                        stage=STAGE_PI_RECEIVED,
                        ageS=round(time.monotonic() - opened_at, 1),
                    )
                return  # 소비 루프가 재연결(연결 시 서버가 전체 snapshot push = 재동기).
            if event != "snapshot":
                continue  # error/기타 이벤트는 이 축에서 무시(재연결은 daemon 책임).
            try:
                parsed = json.loads(data)
            except ValueError:
                continue
            if isinstance(parsed, dict):
                yield parsed

    def commands(self, device_id: str) -> Iterator[Command]:
        """자기 deviceId Command 스트림(CS-08). snapshot 도착분을 순차 방출."""
        with self._open(device_id) as stream:
            watchdog_stop = self._start_watchdog(stream)  # 트리클 워치독(감사 P3).
            try:
                yield from self._yield_commands(stream, device_id)
            finally:
                watchdog_stop.set()  # 정상 종료 시 워치독 정리.

    def _yield_commands(self, stream: SseStream, device_id: str) -> Iterator[Command]:
        for snapshot in self._snapshots(stream):
            for cmd in commands_from_snapshot(snapshot, device_id):
                if self._log is not None:
                    self._log.info(
                        "SSE snapshot 에서 command 수신",
                        stage=STAGE_PI_RECEIVED,
                        trace_id=cmd.trace_id,
                        order_id=cmd.order_id,
                        device_id=device_id,
                        command_id=cmd.id,
                        attempt=cmd.attempt,
                    )
                yield cmd

    def poll_batches(
        self, device_id: str
    ) -> "Iterator[tuple[list[CommandSet], list[Command]]]":
        """**단일 스트림**에서 두 축(봉투+command)을 snapshot 단위로 함께 방출 — 귀머거리 창 제거.

        ⛔ 기존 `command_sets()`/`commands()` 를 번갈아 소비하면 **각자 스트림을 따로 열어**
        한 축을 듣는 동안(스트림 수명 내내) 다른 축에 귀머거리가 된다 — 2026-07-19 실기기에서
        봉투 발행이 최대 293s 뒤에야 수신돼 신선도 게이트(90s)에 익사한 근본 구조.
        snapshot 하나에 orders/commands/commandSets 가 **전부 실려 오므로** 스트림은 1개면 된다.

        yield = snapshot 당 `(command_sets, commands)` (각각 CS-08 자기 deviceId 필터 완료).
        스트림 종료(수명 상한 로테이션·서버 로테이션·오류) 시 순회가 끝난다 — 재연결은 소비
        루프(dispatcher/daemon) 책임. 항목 단위 로그는 기존 두 메서드와 동일하게 남긴다.
        """
        with self._open(device_id) as stream:
            watchdog_stop = self._start_watchdog(stream)  # 트리클 워치독(감사 P3).
            try:
                for snapshot in self._snapshots(stream):
                    sets = command_sets_from_snapshot(snapshot, device_id)
                    cmds = commands_from_snapshot(snapshot, device_id)
                    if self._log is not None:
                        for cs in sets:
                            self._log.info(
                                "SSE snapshot 에서 CommandSet 봉투 수신",
                                stage=STAGE_PI_RECEIVED,
                                trace_id=cs.trace_id,
                                order_id=cs.source_order_id,
                                device_id=device_id,
                                command_set_id=cs.command_set_id,
                                kind=cs.kind,
                                status=cs.status.wire,
                            )
                        for cmd in cmds:
                            self._log.info(
                                "SSE snapshot 에서 command 수신",
                                stage=STAGE_PI_RECEIVED,
                                trace_id=cmd.trace_id,
                                order_id=cmd.order_id,
                                device_id=device_id,
                                command_id=cmd.id,
                                attempt=cmd.attempt,
                            )
                    yield (sets, cmds)
            finally:
                watchdog_stop.set()  # 정상/예외 종료 공통 — 워치독 정리(스트림 누수 방지).

    def command_sets(self, device_id: str) -> Iterator[CommandSet]:
        """자기 deviceId CommandSet 봉투 스트림(queued|delivered·CS-08). snapshot 순차 방출."""
        with self._open(device_id) as stream:
            watchdog_stop = self._start_watchdog(stream)  # 트리클 워치독(감사 P3).
            try:
                yield from self._yield_command_sets(stream, device_id)
            finally:
                watchdog_stop.set()  # 정상 종료 시 워치독 정리.

    def _yield_command_sets(self, stream: SseStream, device_id: str) -> Iterator[CommandSet]:
        for snapshot in self._snapshots(stream):
            for cs in command_sets_from_snapshot(snapshot, device_id):
                if self._log is not None:
                    self._log.info(
                        "SSE snapshot 에서 CommandSet 봉투 수신",
                        stage=STAGE_PI_RECEIVED,
                        trace_id=cs.trace_id,
                        order_id=cs.source_order_id,
                        device_id=device_id,
                        command_set_id=cs.command_set_id,
                        kind=cs.kind,
                        status=cs.status.wire,
                    )
                yield cs
