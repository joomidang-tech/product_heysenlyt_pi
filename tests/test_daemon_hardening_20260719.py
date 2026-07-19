"""데몬 견고화(2026-07-19) 회귀 앵커 — 실기기 사가에서 나온 4가지 대응.

이 파일이 지키는 것:
  A. **단일 스트림 소비**(`poll_batches`/`poll_stream`) — 봉투+command 두 축을 한 스트림에서.
     구식(축별 무한 스트림 번갈아 소비)은 한 축을 듣는 동안 다른 축에 최대 스트림 수명(수 분)
     귀머거리 → 신선도 게이트(90s) 익사(실기기 191s·293s 실측)의 근본이었다.
  B. **스트림 수명 상한**(MAX_STREAM_AGE_S) — 하트비트만 살아있는 좀비 스트림도 상한 안에
     재연결·재동기로 자가 회복(트리클 워치독은 하트비트 라인도 수신으로 쳐 좀비를 못 잡는다).
  C. **항목 예외 격리 + 제너레이터 close** — 봉투 1건 dispatch 예외가 스트림을 버리면(with
     미정리) 연결이 누수돼 중복 스트림이 쌓인다(06:32 실측 난사).
  D. **시리얼 핫플러그 자가 재연결** — USB 재연결로 장치 노드가 옮겨 붙어도(ttyUSB0→1 실측)
     재시작 없이 후보 재탐색+재오픈. + **주기 HW 건강 프로브**(ok/garbled/silent) 하트비트 실측.
"""

from __future__ import annotations

import json

import pytest

from senlyt_pi.adapters import sse_command_source_adapter as sse_mod
from senlyt_pi.adapters.sse_command_source_adapter import SseCommandSourceAdapter
from senlyt_pi.adapters.sy01b_engine_adapter import Sy01bEngineAdapter
from support_http import FakeHttpServer

# 어댑터 시리얼 더블 관례 재사용.
from test_sy01b_engine_adapter import FakeSerial, status_frame

MINE = "dev-A"


def _cs(order_id: str, device_id: str, status: str = "queued") -> dict:
    return {
        "commandSetId": f"{order_id}:1",
        "kind": "manufacture",
        "deviceId": device_id,
        "status": status,
        "sourceOrderId": order_id,
        "attempt": 1,
        "steps": [{"idx": 0, "pumpAddr": 1, "flavor": "cola", "volume": 100}],
        "createdAt": "2026-07-19T00:00:00.000Z",
        "createdBy": "server",
        "traceId": f"trace-{order_id}",
    }


def _cmd(order_id: str, device_id: str) -> dict:
    return {
        "id": f"{order_id}:1",
        "orderId": order_id,
        "attempt": 1,
        "deviceId": device_id,
        "recipe": None,
        "traceId": f"trace-{order_id}",
        "createdAt": "2026-07-19T00:00:00.000Z",
    }


# ── A. 단일 스트림 소비 — snapshot 한 장에서 두 축이 함께 나온다 ────────────────


class TestPollBatchesSingleStream:
    def test_batches_yield_both_axes_from_one_snapshot(self) -> None:
        snapshot = {
            "orders": [],
            "commands": [_cmd("oC", MINE)],
            "commandSets": [_cs("oS", MINE)],
        }
        with FakeHttpServer() as srv:
            srv.set_handler(lambda req: {"sse": [("snapshot", json.dumps(snapshot))]})
            adapter = SseCommandSourceAdapter(base_url=srv.base_url, timeout=5.0)
            batches = list(adapter.poll_batches(MINE))
            assert len(batches) == 1
            sets, cmds = batches[0]
            assert [c.command_set_id for c in sets] == ["oS:1"]
            assert [c.order_id for c in cmds] == ["oC"]
            # 핵심 계약: 스트림 연결이 **1개**뿐(구식 = 축마다 1개씩 2개).
            stream_reqs = [r for r in srv.requests if "stream" in r.path]
            assert len(stream_reqs) == 1


# ── B. 스트림 수명 상한 — 좀비 스트림 자가 로테이션 ────────────────────────────


class TestStreamMaxAge:
    def test_rotation_ends_generator_after_age_limit(self, monkeypatch) -> None:
        # 수명 상한을 0 으로 — 첫 이벤트 처리 전에 상한 초과 → 즉시 종료(재연결은 소비 루프 몫).
        monkeypatch.setattr(sse_mod, "MAX_STREAM_AGE_S", 0.0)
        snapshot = {"commands": [_cmd("o1", MINE)], "commandSets": []}
        with FakeHttpServer() as srv:
            srv.set_handler(
                lambda req: {"sse": [("snapshot", json.dumps(snapshot))] * 50}
            )
            adapter = SseCommandSourceAdapter(base_url=srv.base_url, timeout=5.0)
            got = list(adapter.poll_batches(MINE))
            assert got == []  # 상한이 0 이라 한 장도 처리 전 로테이션 — 무한 소비 안 함.


# ── C. 항목 예외 격리 + 스트림 정리 (dispatcher.poll_stream) ────────────────────


class _BatchSource:
    """poll_batches 만 제공하는 소스 더블 — close 관측용."""

    def __init__(self, batches):
        self._batches = batches
        self.closed = False

    def poll_batches(self, device_id: str):
        try:
            yield from self._batches
        finally:
            self.closed = True


class _RecordingSequencer:
    class _Resolver:
        pump_map: dict = {}

    def __init__(self):
        self.submitted = []
        self.resolver = self._Resolver()
        self.queue_depth = 0
        self.is_busy = False

        class _Ledger:
            def is_settled(self, _id):
                return False

        self.ledger = _Ledger()


def _mk_dispatcher(source):
    from senlyt_pi.app.dispatcher import Dispatcher

    return Dispatcher(
        device_id=MINE,
        command_source=source,
        sequencer=_RecordingSequencer(),
        interpret=lambda c: [],
        commandset_source=source,
    )


class TestPollStreamIsolation:
    def test_item_exception_does_not_kill_stream(self, monkeypatch) -> None:
        from senlyt_pi.core.command_set import command_sets_from_snapshot
        from senlyt_pi.core.wire_messages import Command

        good = Command.from_json(_cmd("good", MINE))
        bad = Command.from_json(_cmd("bad", MINE))
        src = _BatchSource([([], [bad]), ([], [good])])
        d = _mk_dispatcher(src)

        calls = []

        def _on_command(cmd):
            if cmd.order_id == "bad":
                raise RuntimeError("boom")
            calls.append(cmd.order_id)
            return None

        monkeypatch.setattr(d, "_on_command", _on_command)
        d.poll_stream()  # bad 가 스트림을 죽이면 good 이 소비되지 않는다.
        assert calls == ["good"]
        assert src.closed  # finally close — 스트림 누수 방지 계약.

    def test_fallback_when_no_poll_batches(self) -> None:
        # poll_batches 미제공(테스트 Fake 등) → 구식 순차 소비 폴백(하위호환).
        class _LegacySource:
            def commands(self, device_id):
                return iter(())

            def command_sets(self, device_id):
                return iter(())

        d = _mk_dispatcher(_LegacySource())
        assert d.poll_stream() == 0


# ── D. 시리얼 핫플러그 자가 재연결 + 건강 프로브 ────────────────────────────────


class _VanishingSerial(FakeSerial):
    """N 번째 write 부터 OSError(장치 소멸) — USB 뽑힘 모사."""

    def __init__(self, die_after: int = 0):
        super().__init__()
        self._die_after = die_after
        self.writes = 0

    def write(self, data: bytes) -> int:
        self.writes += 1
        if self.writes > self._die_after:
            raise OSError("device disconnected")
        return super().write(data)


class TestSerialHotplugReconnect:
    def test_txn_reconnects_to_new_port_and_retries(self) -> None:
        dead = _VanishingSerial(die_after=0)  # 첫 write 부터 죽음(뽑힌 상태).
        alive = FakeSerial(responses=[status_frame(0, ready=True)])
        made: list[str] = []

        def factory(port, baud, timeout):
            made.append(port)
            return dead if port == "/dev/ttyUSB0" else alive

        ad = Sy01bEngineAdapter(
            port="/dev/ttyUSB0",
            serial_factory=factory,
            port_resolver=lambda: ["/dev/ttyUSB1"],  # 재열거 = 새 노드
        )
        code, ready = ad._query_status(1)
        assert (code, ready) == (0, True)  # 재연결 후 재시도로 정상 응답.
        assert ad.port == "/dev/ttyUSB1"  # 포트 자가 전환.
        assert made == ["/dev/ttyUSB0", "/dev/ttyUSB1"]

    def test_reconnect_failure_propagates_original_error(self) -> None:
        dead = _VanishingSerial(die_after=0)
        ad = Sy01bEngineAdapter(
            port="/dev/ttyUSB0",
            serial_factory=lambda *_a: dead,  # 어느 포트든 죽은 시리얼 — 회복 불가 상황.
            port_resolver=lambda: [],
        )
        # 재연결도 같은 죽은 팩토리라 결국 실패 — 예외가 정직하게 전파(호출자 실패 처리).
        with pytest.raises(OSError):
            ad._txn(1, "?")

    def test_motion_command_is_not_resent_after_reconnect(self) -> None:
        """모션 명령(`D…R` 등)은 재연결 후 **재전송하지 않는다**(물리 이중 토출 방어·2026-07-19 P1).

        OSError 는 write 성공 후 read 대기 중에도 난다 — 그 시점 펌프는 이미 배출을 시작했을
        수 있다. 재전송하면 (a) 한가한 펌프 = 이중 배출, (b) 바쁜 펌프 = busy NAK 경유 3번째
        전송. 그래서 재연결만 해 두고(다음 트랜잭션 회복) 이번 트랜잭션은 정직한 실패(raise).
        멱등 명령(`?`·TR)만 재전송한다(위 test_txn_reconnects... 가 `?` 재전송을 앵커).
        """
        dead = _VanishingSerial(die_after=0)  # 첫 write 부터 죽음.
        alive = FakeSerial(responses=[status_frame(0, ready=True)])
        ad = Sy01bEngineAdapter(
            port="/dev/ttyUSB0",
            serial_factory=lambda port, *_a: dead if port == "/dev/ttyUSB0" else alive,
            port_resolver=lambda: ["/dev/ttyUSB1"],
        )
        with pytest.raises(OSError):
            ad._txn(1, "D2400R")  # 배출 모션 — 재전송 금지·정직한 실패.
        assert ad.port == "/dev/ttyUSB1", "재연결 자체는 수행(다음 트랜잭션 회복)"
        assert alive.written == [], "새 포트로 모션 프레임이 재전송되지 않았다(이중 토출 0)"
        # 재연결된 핸들로 다음 트랜잭션은 정상 — 회복은 유지된다.
        ad._txn(1, "TR")
        assert any("/1TR" in w for w in alive.written)


class TestHealthProbe:
    def test_ok_garbled_silent(self) -> None:
        ok = FakeSerial(responses=[status_frame(0, ready=True)])
        assert Sy01bEngineAdapter(serial_factory=lambda *_a: ok).health_probe(1) == "ok"

        garbled = FakeSerial(responses=[b"7o["])  # ETX 없는 파편(오늘 오전 실기기 그 모양)
        assert (
            Sy01bEngineAdapter(serial_factory=lambda *_a: garbled).health_probe(1) == "garbled"
        )

        silent = FakeSerial(responses=[b""])
        assert (
            Sy01bEngineAdapter(serial_factory=lambda *_a: silent).health_probe(1) == "silent"
        )


# ── E. 실시간 HW 감시 — 부팅 인식 실패여도 기대 주소를 계속 프로브 (2026-07-19 확정) ──


class TestHwWatchRealtime:
    def test_health_probe_uses_watch_addrs_when_pump_map_empty(self):
        """pump_map 이 비어도(어댑터 미장착 부팅) hw_watch_addrs 를 프로브해 pumpHealth 를
        만든다 — admin 이 '부팅 스냅샷'이 아니라 실시간 무응답(빨강)을 본다."""
        from senlyt_pi.app.daemon import DaemonDeps, SenlytDaemon
        from senlyt_pi.persistence.idempotency_ledger import InMemoryIdempotencyLedger

        class _Probe:
            def __init__(self):
                self.asked: list[int] = []

            def health_probe(self, addr: int) -> str:
                self.asked.append(addr)
                return "silent"  # 어댑터 없음 — 전 주소 무응답(정직한 빨강)

        class _Sink:
            def send_heartbeat(self, hb):
                self.last = hb

            def report_status(self, r):
                pass

        engine = _Probe()
        d = SenlytDaemon(
            DaemonDeps(
                device_id="dev-A",
                command_source=type("S", (), {"commands": lambda s, i: iter(())})(),
                status_sink=_Sink(),
                engine=engine,  # type: ignore[arg-type]
                ledger=InMemoryIdempotencyLedger(),  # type: ignore[arg-type]
                heartbeat_interval_s=0,
                hw_watch_addrs=(1, 2),
            )
        )
        assert sorted(d._sequencer.resolver.pump_map) == []  # 부팅 인식 실패 상황
        d._refresh_hw_health()
        assert engine.asked == [1, 2]  # 기대 주소를 실측했다(실시간 판단)
        assert d._pump_health == {1: "silent", 2: "silent"}
        assert d._hw_checked_at is not None


# ── F. 기주 밸브 openSec 직접 지정 (점검 "N초 열기" · 2026-07-19) ────────────────


class TestValveOpenSec:
    def test_open_sec_overrides_flow_derivation_and_clamps(self):
        from senlyt_pi.adapters.valve_adapter import FakeValveAdapter
        from senlyt_pi.core.wire_messages import RecipeStep

        v = FakeValveAdapter(flow_ml_per_sec=10.0, max_open_sec=15.0)
        # 직접 지정 — flowRate 파생(20/10=2s) 대신 5s 개방.
        r = v.dispense_volume("sour", 0.0, 5.0)
        assert r.ok and r.open_sec == 5.0
        # 상한 초과 = fail-closed 거부(개방 0).
        assert not v.dispense_volume("sour", 0.0, 16.0).ok
        # 0 이하 거부.
        assert not v.dispense_volume("normal", 0.0, 0.0).ok
        # wire 파싱 — openSec 키가 RecipeStep.open_sec 로 실린다(volumeMl 부재 허용).
        step = RecipeStep.from_json({"kind": "valve", "idx": 0, "base": "sour", "openSec": 5})
        assert step.open_sec == 5.0 and step.volume_ml == 0.0
        # 구 계약(volumeMl 파생)은 불변.
        legacy = v.dispense_volume("normal", 20.0)
        assert legacy.ok and legacy.open_sec == 2.0
