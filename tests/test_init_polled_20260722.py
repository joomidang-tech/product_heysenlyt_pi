"""주소지정 폴링 초기화(`initialize_polled`) 계약 테스트 (2026-07-22).

배경: 초기화가 HOME_SETTLE_S(30s) 고정 대기(fire-and-forget)라 제조 시작이 ~31.6s 지연.
실기기 프로브(scripts/init_poll_probe.py·2026-07-22)가 "주소지정 `?` 폴은 홈(Z) 모션 중에도
100% clean(2펌프 동시 홈 포함 177+ 폴·garbled 0·silent 0·err15 0)"을 확정 → 폴 조기완료 도입.
설계 정본: developer/hey_senlyt/v1.2.0/99_daily/2026-07-21-초기화지연-30초-해결방안-주소지정폴링-설계.html

3개의 안전 기둥이 이 테스트가 잠그는 계약:
  1. 폴은 조기 출발에만 — 깨짐/무응답 폴은 실패 보고가 아니라 deadline 폴백(성공 간주).
  2. 브로드캐스트 안 씀 — 발사도 주소지정(`/{addr}...`), 펌프별 포트 각자 가능.
  3. 조기 출발은 유효 프레임 "연속 2회 idle"일 때만 — 1회 idle 후 busy 재관측이면 리셋.
"""

from __future__ import annotations

import threading
import time

import pytest
from test_sy01b_broadcast_init_20260719 import BusScriptedSerial
from test_sy01b_engine_adapter import SPEC_05, FakeSerial, adapter_with, status_frame

import senlyt_pi.adapters.sy01b_engine_adapter as mod

PORTS = {1: (12, 2), 2: (12, 2)}


@pytest.fixture(autouse=True)
def fast_poll_waits(monkeypatch):
    """폴/발사 대기를 테스트 속도로 축소 — 로직(순서·게이트·판정)은 그대로 검증된다."""
    monkeypatch.setattr(mod, "INIT_FIRE_STEP_GAP_S", 0.001)
    monkeypatch.setattr(mod, "INIT_POLL_GRACE_S", 0.001)
    monkeypatch.setattr(mod, "INIT_POLL_INTERVAL_S", 0.001)
    monkeypatch.setattr(mod, "INIT_POLL_READ_TIMEOUT_S", 0.05)


def polled_adapter(fake, **kw):
    kw.setdefault("read_timeout_s", 0.1)
    kw.setdefault("init_timeout_s", 1.0)
    return adapter_with(fake, **kw)


class TestEarlyExit:
    def test_early_exit_without_settle_wait(self, monkeypatch):
        """조기완료 — 깨끗한 idle 2연속이면 HOME_SETTLE_S(30s)를 기다리지 않는다."""
        fake = FakeSerial()  # 기본 응답 = clean ready idle err0.
        a = polled_adapter(fake)
        t0 = time.monotonic()
        results = a.initialize_polled([1, 2], SPEC_05, ports_by_addr=PORTS)
        elapsed = time.monotonic() - t0
        assert results == {1: 0, 2: 0}
        assert elapsed < 5.0, f"조기완료가 30s 를 기다림({elapsed:.1f}s) — 폴 게이트 미동작"
        # 캐시 등록 — 다음 토출이 셋업(TR·U·Z)을 건너뛴다.
        assert a._initialized == {1, 2}

    def test_addressed_fire_no_broadcast(self):
        """기둥 2 — 발사가 전부 주소지정. 브로드캐스트(`/_`) 프레임이 하나도 없어야 한다."""
        fake = FakeSerial()
        a = polled_adapter(fake)
        a.initialize_polled([1, 2], SPEC_05, ports_by_addr=PORTS)
        assert not any(w.startswith("/_") for w in fake.written), "브로드캐스트 사용 금지(7/19 오염)"
        # 펌프별 TR → U(스톨전류) → Z(포트 지정) 순서로 발사된다.
        p1 = [w for w in fake.written if w.startswith("/1")]
        assert p1[0] == "/1TR\r" and p1[1] == "/1U200,5R\r" and p1[2] == "/1Z1,12,2R\r"

    def test_per_pump_ports(self):
        """기둥 2 덤 — 펌프별 상이 포트: 각자 자기 (air, out)으로 Z + 자기 배출구 주차."""
        fake = FakeSerial()
        a = polled_adapter(fake)
        results = a.initialize_polled(
            [1, 2], SPEC_05, ports_by_addr={1: (12, 2), 2: (11, 3)}
        )
        assert results == {1: 0, 2: 0}
        assert any(w == "/1Z1,12,2R\r" for w in fake.written)
        assert any(w == "/2Z1,11,3R\r" for w in fake.written)
        assert any(w == "/1I2R\r" for w in fake.written), "주차 = 자기 배출구"
        assert any(w == "/2I3R\r" for w in fake.written)


class TestConsecutiveIdleGate:
    def test_single_idle_then_busy_resets(self):
        """기둥 3 — idle 1회 후 busy 재관측이면 연속성 리셋(우연 오독 조기출발 차단)."""
        busy = status_frame(0, ready=False)
        idle = status_frame(0, ready=True)
        fake = BusScriptedSerial(
            # 폴 스크립트: idle → busy(리셋) → busy → idle → idle(여기서야 완료).
            poll_scripts={1: [idle, busy, busy, idle, idle]},
            poll_default={1: idle},
        )
        a = polled_adapter(fake)
        results = a.initialize_polled([1], SPEC_05, ports_by_addr={1: (12, 2)})
        assert results == {1: 0}
        # `?` 폴이 최소 5회(스크립트 소진) 나갔다 = 첫 idle 로 조기 완료하지 않았다.
        polls = [w for w in fake.written if w == "/1?\r"]
        assert len(polls) >= 5, "idle 1회로 조기 출발 — 연속 2회 게이트 미동작(err15 재발 경로)"

    def test_garbled_polls_fall_back_to_deadline_success(self, monkeypatch):
        """기둥 1 — 깨진 폴 지속 = 실패 보고가 아니라 deadline 폴백·성공 간주(+캐시 등록)."""
        monkeypatch.setattr(mod, "HOME_SETTLE_S", 0.2)  # 폴백 상한만 축소(로직 불변).
        garbled = b"\x07>F["  # ETX 없는 쓰레기 — 7/19 실기기에서 본 그 프레임.
        fake = BusScriptedSerial(poll_scripts={}, poll_default={1: garbled})
        a = polled_adapter(fake)
        results = a.initialize_polled([1], SPEC_05, ports_by_addr={1: (12, 2)})
        assert results == {1: 0}, "garbled 폴이 실패로 새면 7/19 오탐 재발"
        assert a._initialized == {1}

    def test_all_silent_reports_failure(self, monkeypatch):
        """전 펌프 완전 무응답(전원/케이블) — 거짓 완료 방지를 위해 실패 보고(캐시 미등록)."""
        monkeypatch.setattr(mod, "HOME_SETTLE_S", 0.2)
        fake = BusScriptedSerial(poll_scripts={}, poll_default={1: None, 2: None})
        a = polled_adapter(fake)
        results = a.initialize_polled([1, 2], SPEC_05, ports_by_addr=PORTS)
        assert set(results.values()) == {mod._NO_RESPONSE}
        assert a._initialized == set()


class TestHonestErrors:
    def test_overload_is_honest_failure_with_tr(self):
        """Code 9(오버로드) — TR+재초기화 예약+정직한 실패. 형제 펌프는 정상 완료."""
        overload = status_frame(9, ready=False)
        fake = BusScriptedSerial(poll_scripts={1: [overload]})  # 나머지 폴 = 기본 idle.
        a = polled_adapter(fake)
        results = a.initialize_polled([1, 2], SPEC_05, ports_by_addr=PORTS)
        assert results[1] == 9, "오버로드를 성공으로 위장하면 씰/모터 손상(현행 개선점)"
        assert results[2] == 0
        assert 1 not in a._initialized and 2 in a._initialized
        # TR 이 오버로드 직후 다시 나갔다(발사 TR + 오버로드 처리 TR = 2회 이상).
        assert len([w for w in fake.written if w == "/1TR\r"]) >= 2

    def test_idle_err15_is_honest_failure_with_tr(self):
        """idle+err15(latched 겹침+정지) — TR+재초기화 예약+정직한 실패 15(33초 매달림 금지)."""
        latched15 = status_frame(15, ready=True)  # 0x6F 계열 — 정지했는데 err15 latched.
        fake = BusScriptedSerial(poll_scripts={1: [latched15]})
        a = polled_adapter(fake)
        results = a.initialize_polled([1, 2], SPEC_05, ports_by_addr=PORTS)
        assert results[1] == 15 and results[2] == 0
        assert 1 not in a._initialized and 2 in a._initialized
        assert len([w for w in fake.written if w == "/1TR\r"]) >= 2, "latched 는 TR 로 지운다"

    def test_shutdown_aborts_without_cache_and_without_fire(self):
        """shutdown 선점 — 물리 명령이 **한 발도 안 나가고**(선-검사) 전 펌프 실패·캐시 미등록."""
        stop = threading.Event()
        stop.set()
        fake = FakeSerial()
        a = polled_adapter(fake, stop_event=stop)
        results = a.initialize_polled([1, 2], SPEC_05, ports_by_addr=PORTS)
        assert set(results.values()) == {mod._NO_RESPONSE}
        assert a._initialized == set()
        assert fake.written == [], "래치 선 뒤 Z/TR 이 나가면 안 된다(송신 전 게이트·리뷰 P2-2)"

    def test_estop_during_poll_aborts_before_parking(self):
        """폴 도중 estop — 즉시 이탈: 주차(I·밸브 회전)도 안 보내고 전 펌프 실패·캐시 미등록."""

        class EstopOnPollSerial(BusScriptedSerial):
            """N번째 `?` 폴에서 estop 래치를 세우는 더블 — '폴 도중 estop' 재현."""

            def __init__(self, estop_event, trigger_at=2):
                super().__init__(poll_default={1: status_frame(0, ready=False)})  # 계속 busy.
                self._estop_event = estop_event
                self._trigger_at = trigger_at
                self._polls = 0

            def write(self, data):
                if data.decode("ascii").rstrip("\r").endswith("?"):
                    self._polls += 1
                    if self._polls >= self._trigger_at:
                        self._estop_event.set()
                return super().write(data)

        estop = threading.Event()
        fake = EstopOnPollSerial(estop)
        a = polled_adapter(fake, estop_event=estop)
        results = a.initialize_polled([1], SPEC_05, ports_by_addr={1: (12, 2)})
        assert results == {1: mod._NO_RESPONSE}
        assert a._initialized == set()
        assert not any("I2R" in w for w in fake.written), "estop 후 밸브 회전(주차) 송신 금지"


class TestSequencerRouting:
    def test_polled_preferred_over_broadcast(self):
        """getattr 사다리 — initialize_polled 가 있으면 그 경로(펌프별 포트 전달)."""
        from senlyt_pi.pipeline.recipe_resolver import RecipeResolver
        from senlyt_pi.core.wire_messages import RecipeStep

        calls: dict = {}

        class PolledEngine:
            def initialize_polled(self, addrs, spec, **kw):
                calls["addrs"] = list(addrs)
                calls["ports_by_addr"] = kw.get("ports_by_addr")
                return {1: 15, 2: 0}  # 펌프1 = 정직한 transient(err15) — 라벨 보존 검증.

            def initialize_broadcast(self, *a, **kw):  # 폴링이 있으면 안 불려야 한다.
                calls["broadcast"] = True
                return {}

        steps = [
            RecipeStep.from_json(
                {"idx": i, "stage": 0, "kind": "engineOp", "pumpAddr": p, "op": "initialize",
                 "initInPort": 12, "initOutPort": out}
            )
            for i, (p, out) in enumerate([(1, 2), (2, 3)])
        ]
        resolved = RecipeResolver({1: SPEC_05, 2: SPEC_05}).resolve(steps).steps

        from senlyt_pi.pipeline.pump_sequencer import PumpSequencer

        seq = PumpSequencer.__new__(PumpSequencer)  # 라우팅 단위 검증 — 최소 속성만 주입.
        seq._executor = type("X", (), {"engine": PolledEngine()})()
        seq._log = None
        seq._bind_step_log_ctx = lambda: None
        seq._clear_step_log_ctx = lambda: None
        out = seq._maybe_broadcast_init(resolved)
        # err15 = SoT §6-7 TRANSIENT — blanket PERMANENT 로 뭉개면 "다시 누르면 복구"가
        #   영구 실패로 오표시된다(리뷰 P2-1 회귀 잠금).
        from senlyt_pi.core.pump_guard import StatusErrorCode

        assert out == [(False, StatusErrorCode.ENGINE_ERROR_TRANSIENT), (True, None)]
        assert calls["addrs"] == [1, 2]
        assert calls["ports_by_addr"] == {1: (12, 2), 2: (12, 3)}, "펌프별 포트가 각자 전달"
        assert "broadcast" not in calls, "폴링 지원 엔진에서 브로드캐스트로 새면 안 된다"

    def test_broadcast_fallback_when_no_polled(self):
        """구 어댑터(폴링 미지원) — 기존 브로드캐스트 경로 그대로(하위호환)."""
        from senlyt_pi.pipeline.recipe_resolver import RecipeResolver
        from senlyt_pi.core.wire_messages import RecipeStep

        calls: dict = {}

        class BroadcastOnlyEngine:
            def initialize_broadcast(self, addrs, spec, **kw):
                calls["addrs"] = list(addrs)
                calls["ports"] = (kw.get("init_in_port"), kw.get("init_out_port"))
                return {a: 0 for a in addrs}

        steps = [
            RecipeStep.from_json(
                {"idx": i, "stage": 0, "kind": "engineOp", "pumpAddr": p, "op": "initialize",
                 "initInPort": 12, "initOutPort": 2}
            )
            for i, p in enumerate([1, 2])
        ]
        resolved = RecipeResolver({1: SPEC_05, 2: SPEC_05}).resolve(steps).steps

        from senlyt_pi.pipeline.pump_sequencer import PumpSequencer

        seq = PumpSequencer.__new__(PumpSequencer)
        seq._executor = type("X", (), {"engine": BroadcastOnlyEngine()})()
        seq._log = None
        seq._bind_step_log_ctx = lambda: None
        seq._clear_step_log_ctx = lambda: None
        out = seq._maybe_broadcast_init(resolved)
        assert out == [(True, None), (True, None)]
        assert calls == {"addrs": [1, 2], "ports": (12, 2)}
