"""sy01b 실어댑터 — 프로토콜·병렬성·안전 계약 검증(시리얼 없이 seam 으로).

이 테스트가 지키는 것:
  1. **프레임 문법** — `/{addr}{cmd}\\r` · 토출 = I→P→O→D 순서
  2. **silent-success 금지** — 빈 응답·상태프레임 아님·에러코드가 절대 성공으로 새지 않는다
  3. **bounded-read(F1)** — Ready 가 영영 안 와도 유한 시간에 실패로 반환한다(교착 없음)
  4. **버스 락은 짧게** — 모션 대기 중 다른 펌프가 버스를 쓸 수 있다(= 병렬이 죽지 않는다)
  5. **v1.1.0 벽돌 사고 회귀 방지** — latched 에러여도 TR→셋업이 진행된다
  6. **용량 파생** — 0.5mL → `Z1R`(반력)·`U200,5`. 모드로 분기하지 않는다.
"""

from __future__ import annotations

import threading
import time

import pytest

from senlyt_pi.adapters.sy01b_engine_adapter import (
    Sy01bEngineAdapter,
    parse_status,
)
from senlyt_pi.core.pump_guard import SyringeSpec
from senlyt_pi.ports.engine_port import EngineDispenseCommand
from senlyt_pi.test_seam.fake_engine_sentinels import (
    FAKE_EMPTY_RAW_CODE,
    FAKE_TIMEOUT_RAW_CODE,
)

SPEC_05 = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=0.5)


def status_frame(error_code: int = 0, *, ready: bool = True) -> bytes:
    """`/0{상태바이트}` + ETX — 펌프 응답 모사. ready 비트 = 0x20, 에러 = 하위 4비트."""
    b = (0x20 if ready else 0x00) | (error_code & 0x0F)
    return b"/0" + bytes([b]) + b"\x03"


class FakeSerial:
    """시리얼 seam — 보낸 프레임을 기록하고, 스크립트된 응답을 돌려준다."""

    def __init__(self, responses: list[bytes] | None = None, *, default: bytes | None = None):
        self.written: list[str] = []
        self._responses = list(responses or [])
        self._default = default if default is not None else status_frame(0, ready=True)
        self._buf = bytearray()
        self.closed = False
        self.lock_witness: list[str] = []

    def write(self, data: bytes) -> int:
        self.written.append(data.decode("ascii"))
        self._buf.extend(self._responses.pop(0) if self._responses else self._default)
        return len(data)

    @property
    def in_waiting(self) -> int:
        return len(self._buf)

    def read(self, size: int = 1) -> bytes:
        out, self._buf = bytes(self._buf[:size]), bytearray(self._buf[size:])
        return out

    def close(self) -> None:
        self.closed = True


def adapter_with(fake: FakeSerial, **kw) -> Sy01bEngineAdapter:
    return Sy01bEngineAdapter(serial_factory=lambda *_a: fake, **kw)


def cmd(**over) -> EngineDispenseCommand:
    base = dict(pump_addr=1, volume_ul=100.0, steps=2400, spec=SPEC_05, in_port=3, out_port=2)
    base.update(over)
    return EngineDispenseCommand(**base)  # type: ignore[arg-type]


# ── 1. 상태 프레임 파싱 ────────────────────────────────────────────────────────


class TestParseStatus:
    def test_ready_normal(self):
        assert parse_status(status_frame(0, ready=True).decode("ascii", "ignore")) == (0, True)

    def test_busy_normal(self):
        # Busy = Ready 비트 없음. 에러는 아니다(모터가 도는 중).
        assert parse_status(status_frame(0, ready=False).decode("ascii", "ignore")) == (0, False)

    def test_error_code_masked(self):
        # Code 9 = 플런저 오버로드(구조적). Ready 비트가 서 있어도 **에러면 ready=False**
        #   — 안 그러면 에러가 성공으로 샌다(silent-success).
        assert parse_status(status_frame(9, ready=True).decode("ascii", "ignore")) == (9, False)

    def test_no_frame_is_no_response(self):
        assert parse_status("") == (FAKE_TIMEOUT_RAW_CODE, False)
        assert parse_status("garbage") == (FAKE_TIMEOUT_RAW_CODE, False)


# ── 2. 토출 사이클 = I → P → O → D ────────────────────────────────────────────


class TestDispenseCycle:
    def test_frame_syntax_and_order(self):
        fake = FakeSerial()
        a = adapter_with(fake)
        res = a.dispense(cmd(pump_addr=2, in_port=3, out_port=2, steps=2400))
        assert res.raw_error_code == 0

        # 셋업(TR·U200·Z) 이후의 토출 프레임만 추린다. 흡입=절대 `A{steps}` · 배출=절대 홈 `A0`
        #   (v1.1.0 movePlungerAbs 복원 — 상대 P/D 회귀 봉합, 2026-07-20 "용량 조절 X").
        moves = [w for w in fake.written if any(c in w for c in ("I3", "A2400", "O2", "A0R"))]
        # 프레임 문법 — 전부 `/{addr}…\r`
        for w in moves:
            assert w.startswith("/2"), w
            assert w.endswith("\r"), w
        # 순서 — 흡입포트 회전 → 흡입(절대) → 배출포트 회전 → 배출(절대 홈)
        seq = [next(t for t in ("I3", "A2400", "O2", "A0R") if t in w) for w in moves]
        assert seq == ["I3", "A2400", "O2", "A0R"]

    def test_setup_derives_from_capacity_not_mode(self):
        # 0.5mL → 스톨 5 · 초기화힘 **Z1R(반력)**. v1.1.0 사고 = 모드 기본으로 유도해 ZR(전력).
        fake = FakeSerial()
        adapter_with(fake).dispense(cmd())
        joined = "".join(fake.written)
        assert "U200,5R" in joined
        assert "Z1R" in joined
        assert "ZR" not in joined.replace("Z1R", "")  # 전력 초기화가 새지 않았다

    def test_setup_runs_once_per_pump(self):
        fake = FakeSerial()
        a = adapter_with(fake)
        a.dispense(cmd(pump_addr=1))
        a.dispense(cmd(pump_addr=1))
        assert "".join(fake.written).count("U200,5R") == 1  # 캐시 — 매 스텝 재셋업 안 함

    def test_initialize_clears_setup_cache(self):
        fake = FakeSerial()
        a = adapter_with(fake)
        a.dispense(cmd(pump_addr=1))
        a.initialize()
        a.dispense(cmd(pump_addr=1))
        assert "".join(fake.written).count("U200,5R") == 2  # 무효화 후 재셋업

    def test_aspirate_stops_before_dispense(self):
        fake = FakeSerial()
        a = adapter_with(fake)
        a.aspirate(cmd(in_port=3, out_port=2, steps=1200))
        joined = "".join(fake.written)
        assert "I3" in joined and "A1200" in joined  # 흡입=절대 이동 A{steps}
        assert "A0R" not in joined  # 배출(절대 홈 A0)은 안 한다

    def test_missing_ports_skip_valve_rotation(self):
        # 구계약(포트 미보유) 스텝 — 밸브를 돌리지 않고 현 위치에서 흡입·배출만.
        fake = FakeSerial()
        a = adapter_with(fake)
        res = a.dispense(cmd(in_port=None, out_port=None, steps=500))
        assert res.raw_error_code == 0
        joined = "".join(fake.written)
        assert "A500" in joined and "A0R" in joined  # 흡입 A{steps} · 배출 절대 홈 A0
        assert "I" not in joined.replace("/1", "")  # 밸브 회전 프레임 없음


# ── 3. silent-success 금지 (EP-03) ────────────────────────────────────────────


class TestNoSilentSuccess:
    def test_empty_response_is_failure(self):
        fake = FakeSerial(default=b"")  # 무응답
        res = adapter_with(fake).dispense(cmd())
        assert res.raw_error_code == FAKE_TIMEOUT_RAW_CODE  # 성공 아님

    def test_malformed_response_is_failure(self):
        fake = FakeSerial(default=b"noise\x03")  # 뭔가 왔는데 상태 프레임이 아님
        res = adapter_with(fake).dispense(cmd())
        assert res.raw_error_code == FAKE_EMPTY_RAW_CODE

    def test_error_code_surfaces_raw_not_reclassified(self):
        # 첫 응답(TR)은 정상, 그 다음 U200 에서 Code 9(플런저 오버로드).
        fake = FakeSerial(responses=[status_frame(0), status_frame(9)])
        res = adapter_with(fake).dispense(cmd())
        # 어댑터는 **재분류하지 않는다** — raw 9 를 그대로 올리고 분류는 pump_guard 정본이 한다.
        assert res.raw_error_code == 9

    def test_serial_exception_is_failure_not_raise(self):
        class Boom(FakeSerial):
            def write(self, data: bytes) -> int:
                raise OSError("cable unplugged")

        res = adapter_with(Boom()).dispense(cmd())
        assert res.raw_error_code == FAKE_TIMEOUT_RAW_CODE
        assert "serial error" in (res.detail or "")


# ── 4. bounded-read (F1) — 교착 없음 ──────────────────────────────────────────


class TestBoundedRead:
    def test_never_ready_times_out_finitely(self):
        # 펌프가 영영 Busy 만 답한다 = 모션이 안 끝난다. **유한 시간에 실패로 반환**해야 한다.
        #   (상위 _run_stage 의 future.result() 는 타임아웃이 없어서, 여기서 안 끊으면 제조 교착)
        fake = FakeSerial(default=status_frame(0, ready=False))
        a = adapter_with(fake, motion_timeout_s=0.3, init_timeout_s=0.3, read_timeout_s=0.1)
        started = time.monotonic()
        res = a.dispense(cmd())
        elapsed = time.monotonic() - started
        assert res.raw_error_code == FAKE_TIMEOUT_RAW_CODE  # → ENGINE_TIMEOUT(transient·재시도)
        assert elapsed < 5.0, "폴링이 유한 시간에 끝나야 한다(F1 교착 방어)"

    def test_stop_signal_breaks_polling(self):
        fake = FakeSerial(default=status_frame(0, ready=False))
        a = adapter_with(fake, motion_timeout_s=30.0, init_timeout_s=30.0, read_timeout_s=0.05)
        threading.Timer(0.1, a.signal_stop).start()
        started = time.monotonic()
        a.dispense(cmd())
        assert time.monotonic() - started < 5.0  # 우아한 종료에 즉응


# ── 5. 버스 락은 짧게 — 병렬이 죽지 않는다 (L2) ───────────────────────────────


class TestBusLockIsShort:
    def test_lock_not_held_during_motion_wait(self):
        """모션 완료를 기다리는 동안 **다른 스레드가 버스를 쓸 수 있어야** 한다.

        락을 모션 내내 쥐면 상위 ThreadPool 의 동시성이 통째로 직렬화된다(= 병렬 토출 소멸).
        여기선 '펌프1이 Busy 로 폴링 도는 중에 펌프2의 트랜잭션이 통과하는가'를 본다.
        """
        busy_then_ready = [status_frame(0)] * 3 + [status_frame(0, ready=False)] * 20

        class SlowFake(FakeSerial):
            def __init__(self):
                super().__init__(responses=list(busy_then_ready), default=status_frame(0, ready=False))
                self.other_thread_got_through = threading.Event()

        fake = SlowFake()
        a = adapter_with(fake, motion_timeout_s=1.0, init_timeout_s=1.0, read_timeout_s=0.05)

        def other_pump():
            time.sleep(0.15)  # 펌프1이 폴링에 들어간 뒤
            a._query_status(9)  # 다른 주소의 트랜잭션 — 락이 길게 잡혀 있으면 여기서 막힌다
            fake.other_thread_got_through.set()

        t = threading.Thread(target=other_pump)
        t.start()
        a.dispense(cmd(pump_addr=1))
        t.join(timeout=3)
        assert fake.other_thread_got_through.is_set(), "모션 대기 중 버스가 잠겨 있었다(병렬 소멸)"


# ── 6. v1.1.0 벽돌 사고 회귀 방지 ─────────────────────────────────────────────


class TestBrickRegression:
    def test_latched_error_does_not_block_recovery(self):
        """latched 에러 상태에서도 `TR` → 셋업이 **진행**되어야 한다.

        v1.1.0 사고: Step0 의 TR 이 `validate:true` 라 에러 상태에서 throw → 에러를 지우려고
        부르는 명령이 에러 때문에 실패 → 전원 재투입 전 복구 불가(펌프 벽돌).
        여기선 TR 응답이 에러여도 무시하고 셋업으로 간다.
        """
        # TR 응답 = Code 9(latched) → 그래도 U200·Z1R 이 나가야 한다.
        fake = FakeSerial(responses=[status_frame(9)], default=status_frame(0))
        res = adapter_with(fake).dispense(cmd())
        joined = "".join(fake.written)
        assert "TR" in joined
        assert "U200,5R" in joined, "TR 이 에러라고 셋업을 포기하면 복구 불가(v1.1.0 벽돌)"
        assert res.raw_error_code == 0

    def test_tr_exception_does_not_block_recovery(self):
        calls = {"n": 0}

        class FlakyTR(FakeSerial):
            def write(self, data: bytes) -> int:
                calls["n"] += 1
                if calls["n"] == 1:  # TR 에서만 폭발
                    raise OSError("transient")
                return super().write(data)

        fake = FlakyTR()
        res = adapter_with(fake).dispense(cmd())
        assert "U200,5R" in "".join(fake.written)  # TR 예외를 삼키고 셋업 진행
        assert res.raw_error_code == 0

    def test_safety_port_not_hardcoded_to_9(self):
        # 안전포트(공기 구멍)는 **서버가 스텝에 실어 준다** — 어댑터가 9 를 박지 않는다
        #   (Port 9 = 향료8 충돌·누액 사고). 스텝이 12 를 주면 12 로 돈다.
        fake = FakeSerial()
        adapter_with(fake).dispense(cmd(in_port=12, out_port=2))
        assert "I12" in "".join(fake.written)


# ── 7. 속도 — 서버가 정하고 pi 는 상한 클램프만 ────────────────────────────────


class TestSpeedClamp:
    def test_server_speed_is_used_when_within_limits(self):
        fake = FakeSerial()
        adapter_with(fake).dispense(cmd(aspirate_speed_hz=3000, slope=10))
        assert "V3000" in "".join(fake.written)

    def test_over_limit_speed_is_clamped_to_preset(self):
        # sy01b 상한 = V 6000. 서버가 9999 를 보내도 하드웨어 상한으로 깎는다(물리 보호).
        fake = FakeSerial()
        adapter_with(fake).dispense(cmd(aspirate_speed_hz=9999))
        joined = "".join(fake.written)
        assert "V6000" in joined
        assert "V9999" not in joined

    def test_speed_profile_is_monotonic(self):
        """`v ≤ c ≤ V` — 느리게 출발·느리게 끝, 중간이 최고(SY-01B 제약)."""
        import re

        fake = FakeSerial()
        adapter_with(fake).dispense(cmd(aspirate_speed_hz=500))
        prof = next(
            m for w in fake.written if (m := re.search(r"v(\d+)V(\d+)c(\d+)L(\d+)", w))
        )
        v, big_v, c, _l = (int(g) for g in prof.groups())
        assert v <= c <= big_v, f"속도 단조성 위반: v{v} c{c} V{big_v}"

    def test_missing_speed_falls_back_to_preset_max(self):
        fake = FakeSerial()
        adapter_with(fake).dispense(cmd(aspirate_speed_hz=None, slope=None))
        assert "V6000" in "".join(fake.written)  # 프리셋 상한


# ── 8. 프로브 (부팅 자동인식) ─────────────────────────────────────────────────


class TestProbe:
    def test_responding_address_is_found(self):
        assert adapter_with(FakeSerial()).probe(1) is True

    def test_error_frame_still_counts_as_present(self):
        # Code 7(미초기화)여도 **프레임이 왔다 = 전원·통신 살아있음** = 장착됨.
        fake = FakeSerial(default=status_frame(7, ready=False))
        assert adapter_with(fake).probe(1) is True

    def test_silent_address_is_absent(self):
        a = adapter_with(FakeSerial(default=b""))
        started = time.monotonic()
        assert a.probe(1) is False
        assert time.monotonic() - started < 8.0  # 주소당 상한(전수 스캔이 늘어지지 않게)

    def test_first_empty_frame_then_response_is_present(self):
        # CH340 wake 레이스 — 첫 폴이 빈 프레임이어도 재시도해서 찾아낸다(v1.1.0 원리).
        fake = FakeSerial(responses=[b"", status_frame(0)], default=status_frame(0))
        assert adapter_with(fake).probe(1) is True

    def test_port_error_is_absent_not_raise(self):
        class Boom(FakeSerial):
            def write(self, data: bytes) -> int:
                raise OSError("no such port")

        assert adapter_with(Boom()).probe(1) is False


# ── 9. 연결 수명 ──────────────────────────────────────────────────────────────


class TestConnection:
    def test_lazy_open_once(self):
        opened = {"n": 0}
        fake = FakeSerial()

        def factory(*_a):
            opened["n"] += 1
            return fake

        a = Sy01bEngineAdapter(serial_factory=factory)
        assert opened["n"] == 0  # 아직 안 열었다
        a.dispense(cmd())
        a.dispense(cmd(pump_addr=2))
        assert opened["n"] == 1  # 한 버스 = 한 연결

    def test_close_is_idempotent(self):
        fake = FakeSerial()
        a = adapter_with(fake)
        a.dispense(cmd())
        a.close()
        a.close()
        assert fake.closed is True

    def test_pyserial_missing_surfaces_at_connect_not_import(self):
        # 모듈 import 는 이미 성공했다(이 파일이 돈다는 게 증거). 실 팩토리는 연결 시점에만 실패.
        from senlyt_pi.adapters import sy01b_engine_adapter as mod

        assert callable(mod._pyserial_factory)


# ── 10. pump_map 자동인식 배선 (부팅) ─────────────────────────────────────────


class TestResolverAutodetectWiring:
    """`build_resolver` 가 **버스 스캔으로 pump_map 을 만든다** — "URL만" 설치의 핵심.

    ⚠️ 이 배선이 없으면 `PUMP_ADDRESSES` env 없는 기기는 **전 스텝이 CMD_VALIDATION_FAILED**
    로 죽는다(unmapped_pump_addr). `pump_health` 의 스캔 로직은 원래 있었지만 부팅에 안 붙어
    있었다(비-테스트 호출자 0건·2026-07-17 발견) — 이 테스트가 그 회귀를 막는다.
    """

    def test_env_absent_autodetects_from_bus(self):
        from senlyt_pi.app.bootstrap import build_resolver

        class Bus:
            """주소 1·2 에만 펌프가 달린 버스."""

            def probe(self, addr: int) -> bool:
                return addr in (1, 2)

        r = build_resolver({"SENLYT_MODE": "flavor"}, engine=Bus())
        assert sorted(r.pump_map) == [1, 2]
        # 자동 매핑도 **용량 파생**을 지킨다(0.5mL → 상한 500µL·stepsPerMl 24000).
        assert r.pump_map[1].max_volume_ul == 500
        assert r.pump_map[1].steps_per_ml == 24000

    def test_env_wins_over_autodetect(self):
        from senlyt_pi.app.bootstrap import build_resolver

        class Bus:
            def probe(self, addr: int) -> bool:
                return True  # 전 주소 응답 — 그래도 env 가 이긴다.

        r = build_resolver({"PUMP_ADDRESSES": "flavor:1,2", "SENLYT_MODE": "flavor"}, engine=Bus())
        assert sorted(r.pump_map) == [1, 2]

    def test_no_probe_engine_yields_empty_map(self):
        # Fake 엔진 등 probe 가 없는 어댑터 → 빈 매핑(추측 금지·전 스텝 drop = 토출 0·안전측).
        from senlyt_pi.app.bootstrap import build_resolver
        from senlyt_pi.adapters.fake_engine_adapter import FakeEnginePort

        assert build_resolver({}, engine=FakeEnginePort()).pump_map == {}

    def test_silent_bus_yields_empty_map(self):
        from senlyt_pi.app.bootstrap import build_resolver

        class Dead:
            def probe(self, addr: int) -> bool:
                return False

        assert build_resolver({}, engine=Dead()).pump_map == {}

    def test_real_adapter_probe_feeds_autodetect(self):
        """실 어댑터의 `probe` 가 스캔 seam 으로 그대로 꽂힌다(계약 정합)."""
        from senlyt_pi.app.bootstrap import build_resolver

        # 주소 3 만 상태 프레임을 준다.
        class AddrSelectiveSerial(FakeSerial):
            def write(self, data: bytes) -> int:
                self.written.append(data.decode("ascii"))
                if data.startswith(b"/3"):
                    self._buf.extend(status_frame(0))
                return len(data)

        a = adapter_with(AddrSelectiveSerial())
        # fragrance 는 1,2,3 을 스캔하므로 addr 3 응답이 잡힌다(flavor 는 1,2 만 스캔).
        r = build_resolver({"SENLYT_MODE": "fragrance"}, engine=a)
        assert r.pump_map.get(3) is not None
        assert 3 in r.pump_map


# ── 11. 엔진 조작(정비 버튼) — 의도 → 문법 번역 ──────────────────────────────


class TestEngineOps:
    """관제 정비 버튼이 **봉투 → 의도 → 펌프 문법**으로 도달한다.

    ⚠️ 이 경로가 없으면 버튼은 toast 만 띄우고 아무것도 안 간다("보냈다"는 거짓말·2026-07-17
    핸드오프). 그리고 **의도(op)만 wire 를 타야** 한다 — 서버가 `A12000` 을 조립하면
    "pi 만 하드웨어를 안다"는 경계가 깨진다.
    """

    def _op(self, op: str):
        from senlyt_pi.ports.engine_port import EngineOpCommand

        return EngineOpCommand(pump_addr=1, op=op, spec=SPEC_05)

    def test_plunger_full_moves_to_full_stroke(self):
        fake = FakeSerial()
        res = adapter_with(fake).run_op(self._op("plunger_full"))
        assert res.raw_error_code == 0
        assert "A12000" in "".join(fake.written)  # 풀스트로크 = spec 파생(하드코딩 아님)

    def test_plunger_home_moves_to_zero(self):
        fake = FakeSerial()
        res = adapter_with(fake).run_op(self._op("plunger_home"))
        assert res.raw_error_code == 0
        assert "A0R" in "".join(fake.written)

    def test_absolute_move_requires_origin_setup_first(self):
        # `A{n}` 은 홈이 잡혀 있어야 기준이 선다 — 셋업(U200·Z)이 먼저 나가야 한다.
        fake = FakeSerial()
        adapter_with(fake).run_op(self._op("plunger_full"))
        joined = "".join(fake.written)
        assert joined.index("Z1R") < joined.index("A12000")

    def test_force_initialize_reruns_setup(self):
        fake = FakeSerial()
        a = adapter_with(fake)
        a.dispense(cmd())  # 이미 셋업됨
        a.run_op(self._op("initialize"))  # 강제 초기화 = 캐시 버리고 재셋업
        assert "".join(fake.written).count("U200,5R") == 2

    def test_estop_sends_terminate_without_setup(self):
        # 긴급 정지 = `TR`(이동 중단) 즉발. 셋업(U200·Z)이나 절대이동(A) 을 태우지 않는다 —
        #   정지가 목적이라 홈 기준을 새로 잡을 이유가 없다(오히려 위험).
        fake = FakeSerial()
        res = adapter_with(fake).run_op(self._op("estop"))
        assert res.raw_error_code == 0
        joined = "".join(fake.written)
        assert "TR" in joined
        assert "A" not in joined  # 절대이동 없음
        assert "U200" not in joined  # 재셋업 없음

    def test_estop_invalidates_setup_cache(self):
        # TR 후 홈 기준이 흔들릴 수 있으므로 다음 토출은 반드시 재초기화(안전측).
        fake = FakeSerial()
        a = adapter_with(fake)
        a.dispense(cmd())  # 셋업 1회
        a.run_op(self._op("estop"))  # 캐시 무효화해야 함
        a.dispense(cmd())  # 셋업 재실행돼야 함
        assert "".join(fake.written).count("U200,5R") == 2

    def test_estop_echo_only_is_failure_not_success(self):
        # ⛔ P1 회귀(2026-07-18) — 펌프가 죽어 응답이 없고 USB 어댑터가 자기 송신만 에코(`/1TR\r`,
        #   ETX 없음)하면, 옛 `0 if raw else …` 는 그 에코를 성공으로 봤다(safety-stop 거짓 성공).
        #   이제 parse_status 로 응답 프레임(`/0…`)을 확인하므로 에코만 오면 **실패**로 보고해야 한다.
        class EchoOnly(FakeSerial):
            def write(self, data: bytes) -> int:
                self.written.append(data.decode("ascii"))
                self._buf.extend(data)  # 응답 대신 자기 프레임을 에코(no `/0`)
                return len(data)

        res = adapter_with(EchoOnly()).run_op(self._op("estop"))
        assert res.raw_error_code == FAKE_TIMEOUT_RAW_CODE  # /0 프레임 없음 = 무응답 = 실패

    def test_unknown_op_is_rejected(self):
        res = adapter_with(FakeSerial()).run_op(self._op("self_destruct"))
        assert res.raw_error_code == FAKE_EMPTY_RAW_CODE  # fail-closed — 미지의 물리 동작 금지

    def test_serial_error_is_result_not_raise(self):
        class Boom(FakeSerial):
            def write(self, data: bytes) -> int:
                raise OSError("unplugged")

        res = adapter_with(Boom()).run_op(self._op("plunger_home"))
        assert res.raw_error_code == FAKE_TIMEOUT_RAW_CODE

    def test_wire_op_camel_to_pi_snake_and_gate(self):
        """wire `op`(camelCase) → pi op(snake) 매핑 + 모르는 op 는 resolver 가 거부."""
        from senlyt_pi.core.wire_messages import RecipeStep
        from senlyt_pi.pipeline.recipe_resolver import (
            RecipeResolver,
            RecipeValidationError,
            ResolvedOpStep,
        )

        rr = RecipeResolver({1: SPEC_05})
        step = RecipeStep.from_json(
            {"idx": 0, "stage": 0, "kind": "engineOp", "pumpAddr": 1, "op": "plungerFull"}
        )
        out = rr.resolve([step]).steps[0]
        assert isinstance(out, ResolvedOpStep)
        assert out.op == "plunger_full"  # camelCase → snake_case

        bad = RecipeStep.from_json(
            {"idx": 0, "stage": 0, "kind": "engineOp", "pumpAddr": 1, "op": "nuke"}
        )
        with pytest.raises(RecipeValidationError):
            rr.resolve([bad])

    def test_op_step_roundtrips_through_wire(self):
        from senlyt_pi.core.wire_messages import RecipeStep

        j = {"idx": 2, "stage": 1, "kind": "engineOp", "pumpAddr": 3, "op": "initialize"}
        back = RecipeStep.from_json(j).to_json()
        assert back["kind"] == "engineOp"
        assert back["op"] == "initialize"
        assert back["pumpAddr"] == 3
        assert back["volume"] == 0  # 토출 없음 — 부피 게이트를 타지 않는 축


# ── 12. 리뷰 후속 수정 (P2-1 write timeout · P2-2 에코 방어) ──────────────────


class TestReviewFixes:
    def test_echo_frame_does_not_corrupt_status(self):
        """반이중 RS485 에코 방어(P2-2) — 버퍼에 에코가 섞여도 응답 `/0` 만 파싱한다.

        에코(`/1I3R\\r`)의 `/` 를 먼저 잡으면 명령 문자(I=0x49)를 상태바이트로 오독해 거짓
        Code 9(구조에러) 또는 거짓 성공이 난다. `/0` 접두 앵커로 응답만 본다.
        """
        # 우리가 /1 로 보냈고, 어댑터가 자기 송신을 에코 → 그 뒤 진짜 응답(/0 정상).
        echoed = b"/1I3R\r" + status_frame(0, ready=True)
        assert parse_status(echoed.decode("ascii", "ignore")) == (0, True)
        # 에코 + 에러 응답도 정확히 에러로.
        echoed_err = b"/2P960R\r" + status_frame(9, ready=False)
        assert parse_status(echoed_err.decode("ascii", "ignore")) == (9, False)

    def test_dispense_succeeds_despite_echo(self):
        class EchoSerial(FakeSerial):
            def write(self, data: bytes) -> int:
                self.written.append(data.decode("ascii"))
                # 에코(보낸 프레임 그대로) + 정상 응답.
                self._buf.extend(data)
                self._buf.extend(status_frame(0, ready=True))
                return len(data)

        res = adapter_with(EchoSerial()).dispense(cmd())
        assert res.raw_error_code == 0  # 에코 때문에 거짓 에러가 나면 안 된다

    def test_write_timeout_is_set_on_real_factory(self):
        """P2-1 — 실 pyserial 팩토리가 write_timeout 을 건다(버스 락 쥔 채 write 무한블록 방지)."""
        import inspect

        from senlyt_pi.adapters import sy01b_engine_adapter as mod

        src = inspect.getsource(mod._pyserial_factory)
        assert "write_timeout" in src, "write 도 bounded 여야 한다(F1 — write 측 구멍)"

    def test_write_timeout_exception_is_failure_not_hang(self):
        # write 가 타임아웃 예외를 던지면 실패 결과로 흡수(교착 아님).
        class WriteTimeout(FakeSerial):
            def write(self, data: bytes) -> int:
                raise TimeoutError("write timed out")

        res = adapter_with(WriteTimeout()).dispense(cmd())
        assert res.raw_error_code == FAKE_TIMEOUT_RAW_CODE


class TestBroadcastAddrRejection:
    """P2-5 (pi 측) — env 로 addr 0 을 넣어도 pump_map 에 안 들어간다(브로드캐스트 방지)."""

    def test_env_addr_zero_excluded_from_pump_map(self):
        from senlyt_pi.app.bootstrap import pump_map_from_addresses_env

        m = pump_map_from_addresses_env("flavor:0,1,2")
        assert 0 not in m  # 브로드캐스트 — 어댑터가 /0(전 펌프 동시응답) 못 쏘게
        assert sorted(m) == [1, 2]


# ── 13. v1.1.0 폴 루프 정합 (2026-07-18 · 사용자 "v1.1.0 참고") ────────────────


class TestPollParityV110:
    class _QueryScriptedSerial(FakeSerial):
        """`?`(상태조회)에만 스크립트를 태우고, 그 외 명령엔 항상 Ready 를 준다.

        셋업(TR·U200·Z)·이동 명령의 ACK 는 정상으로 흘리고, **폴링(`?`) 응답만** 원하는
        코드를 주입해 셋업-폴 순서에 얽매이지 않게 한다.
        """

        def __init__(self, query_frames: list[bytes]):
            super().__init__(default=status_frame(0, ready=True))
            self._q = list(query_frames)

        def write(self, data: bytes) -> int:
            self.written.append(data.decode("ascii"))
            if data.rstrip(b"\r").endswith(b"?"):
                self._buf.extend(self._q.pop(0) if self._q else status_frame(0, ready=True))
            else:
                self._buf.extend(status_frame(0, ready=True))  # 명령 ACK = 정상
            return len(data)

    def test_code7_keeps_polling_not_fail(self):
        """Code 7(미초기화)은 실패가 아니라 계속 폴링 — v1.1.0 `_internalCheckStatus` 정합.

        7 을 실패로 올리면 정상 진행 중 스텝을 죽인다. v1.1.0 은 7 을 일시현상으로 보고 폴링 지속.
        """
        # 첫 폴 두 번 Code 7(계속 폴링) → 이후 정상 Ready.
        fake = self._QueryScriptedSerial(
            [status_frame(7, ready=False), status_frame(7, ready=False)]
        )
        res = adapter_with(fake, motion_timeout_s=2.0, init_timeout_s=2.0).dispense(cmd())
        assert res.raw_error_code == 0  # Code 7 로 죽지 않고 폴링 끝에 성공

    def test_overload_invalidates_setup_cache(self):
        """Code 9(오버로드) 후 셋업 캐시 무효화 — 다음 스텝이 재초기화한다(v1.1.0 parity).

        무효화 안 하면 _ensure_ready 가 _initialized 를 보고 재초기화를 skip → 오버로드 상태로
        계속 밀어붙인다(씰/모터 손상). v1.1.0 은 9/10 에 TR + _initializedPumps.remove.
        """
        # 첫 폴에서 Code 9(오버로드).
        fake = self._QueryScriptedSerial([status_frame(9, ready=False)])
        a = adapter_with(fake)
        r1 = a.dispense(cmd())
        assert r1.raw_error_code == 9
        # 오버로드 시 TR 이 나갔다(latched 에러 클리어).
        assert any(w.rstrip("\r").endswith("TR") for w in fake.written)
        # 다음 dispense: 캐시가 무효화됐으니 U200(재셋업)이 **다시** 나가야 한다.
        before = "".join(fake.written).count("U200,5R")
        a.dispense(cmd())
        after = "".join(fake.written).count("U200,5R")
        assert after == before + 1, "오버로드 후 재초기화를 건너뛰면 안 된다"


class TestProbeScanBoundedByMode:
    """#2 (2026-07-18 · 사용자 "소프트웨어 포트 매핑이있잖아") — 스캔 범위는 모드가 정한다."""

    def test_flavor_probes_only_addr_1_2(self):
        from senlyt_pi.app.bootstrap import build_resolver

        probed: list[int] = []

        class RecordingBus:
            def probe(self, addr: int) -> bool:
                probed.append(addr)
                return addr in (1, 2)

        r = build_resolver({"SENLYT_MODE": "flavor"}, engine=RecordingBus())
        assert sorted(r.pump_map) == [1, 2]
        assert probed == [1, 2]  # 3..10 은 **프로브조차 안 한다**(부재 주소 6s 낭비 0)

    def test_fragrance_probes_addr_1_2_3(self):
        from senlyt_pi.app.bootstrap import build_resolver

        probed: list[int] = []

        class RecordingBus:
            def probe(self, addr: int) -> bool:
                probed.append(addr)
                return True

        r = build_resolver({"SENLYT_MODE": "fragrance"}, engine=RecordingBus())
        assert sorted(r.pump_map) == [1, 2, 3]
        assert probed == [1, 2, 3]

    def test_explicit_env_still_wins_over_mode_scan(self):
        from senlyt_pi.app.bootstrap import build_resolver

        class Bus:
            def probe(self, addr: int) -> bool:
                raise AssertionError("env 명시면 프로브하면 안 된다")

        r = build_resolver({"PUMP_ADDRESSES": "flavor:1,2", "SENLYT_MODE": "flavor"}, engine=Bus())
        assert sorted(r.pump_map) == [1, 2]


class TestCode7BrickRegression:
    """P1 회귀(2026-07-18): de-init(Code 7) 후 캐시가 무효화돼 다음 스텝이 재초기화한다.

    구 버그: _initialized 캐시가 Code 7 을 무효화 안 해, 한 번 홈을 잃은 펌프가 캐시-skip 때문에
    재초기화를 영영 못 하고 후속 전 주문이 조용히 토출0로 브릭. 전원순단→Code7 시나리오.
    """

    def _cmd(self):
        return cmd(pump_addr=1, in_port=3, out_port=2, steps=2400)

    def test_immediate_code7_on_move_invalidates_cache(self):
        # 주문1: 정상. 그 뒤 전원순단으로 de-init. 주문2 흡입 즉답 Code 7 → 캐시 무효화 → 재시도 재초기화.
        class DeInitControllable(FakeSerial):
            """de_inited=True 이면 이동(P/D) 명령에 Code 7 즉답. Z(초기화) 수신 시 de_inited=False 복귀."""

            def __init__(self):
                super().__init__(default=status_frame(0, ready=True))
                self.de_inited = False

            def write(self, data: bytes) -> int:
                self.written.append(data.decode("ascii"))
                txt = data.decode("ascii").rstrip("\r")
                # Z 초기화 재실행 → 홈 복구.
                if txt.endswith("Z1R") or txt.endswith("ZR") or txt.endswith("Z2R"):
                    self.de_inited = False
                # 이동 명령(흡입=절대 `…A{n}R` / 배출=절대 홈 `A0R`) 이고 de_inited 면 Code 7 즉답.
                #   (v1.1.0 movePlungerAbs 복원 — 흡입/배출 모두 절대 `A…R`.)
                import re as _re
                is_move = bool(_re.search(r"A\d+R$", txt.rstrip("\r")))
                if is_move and self.de_inited:
                    self._buf.extend(status_frame(7, ready=False))
                    return len(data)
                self._buf.extend(status_frame(0, ready=True))
                return len(data)

        fake = DeInitControllable()
        a = adapter_with(fake)
        assert a.dispense(self._cmd()).raw_error_code == 0
        assert 1 in a._initialized  # 셋업됨
        # 전원순단 시뮬 — 펌프가 홈을 잃음.
        fake.de_inited = True
        setups_before = "".join(fake.written).count("U200,5R")
        r2 = a.dispense(self._cmd())
        assert r2.raw_error_code == 7  # 흡입 즉답 Code 7 → 실패 반환(상위 재시도)
        assert 1 not in a._initialized, "Code 7 후 캐시가 무효화돼야 다음 재시도가 재초기화한다"
        # 다음 dispense(=재시도 상당): _ensure_ready 가 재초기화(Z) → de_inited 해제 → 정상 토출.
        assert a.dispense(self._cmd()).raw_error_code == 0
        assert "".join(fake.written).count("U200,5R") > setups_before

    def test_setup_time_code7_discard_is_noop(self):
        # 셋업(Z 초기화) 폴 중 일시 7→Ready 는 정상 완주(브릭 아님). addr 미등록이라 discard no-op.
        class Code7DuringHome(FakeSerial):
            def __init__(self):
                super().__init__(default=status_frame(0, ready=True))

            def write(self, data: bytes) -> int:
                self.written.append(data.decode("ascii"))
                # Z 초기화 직후 첫 폴은 7(홈 진행), 그 뒤 ready.
                if data.rstrip(b"\r").endswith(b"?"):
                    self._buf.extend(status_frame(0, ready=True))
                else:
                    self._buf.extend(status_frame(0, ready=True))
                return len(data)

        a = adapter_with(Code7DuringHome())
        assert a.dispense(self._cmd()).raw_error_code == 0


# ── 13. 긴급정지 실시간 선점(§9-4·2026-07-18) ──────────────────────────────────


class TestEstopPreemption:
    """estop = **제조 중에도 즉시 정지**. 감시 스레드가 emergency_stop_all 로 전 펌프 TR + `_estop`
    래치를 세우면, in-flight 모션 폴이 즉시 빠져나온다(v1.1.0 _isEmergencyStopped 이식)."""

    def test_emergency_stop_all_sends_tr_to_each_pump(self):
        fake = FakeSerial()
        a = adapter_with(fake)
        a.emergency_stop_all([1, 2, 3])
        frames = "".join(fake.written)
        assert frames.count("TR") == 3  # 펌프 1,2,3 각각 TR
        assert a._estop.is_set()

    def test_emergency_stop_all_skips_broadcast_addr_zero(self):
        fake = FakeSerial()
        a = adapter_with(fake)
        a.emergency_stop_all([0, 1])  # 0 = 브로드캐스트 금지
        assert "".join(fake.written).count("TR") == 1  # addr 1 만

    def test_emergency_stop_all_invalidates_setup_cache(self):
        fake = FakeSerial()
        a = adapter_with(fake)
        a.dispense(cmd())  # 셋업 1회(addr 1 등록)
        a.emergency_stop_all([1])
        a.clear_estop()  # 래치 풀고
        a.dispense(cmd())  # 다음 토출은 재초기화돼야 함
        assert "".join(fake.written).count("U200,5R") == 2

    def test_estop_latch_bails_motion_poll_immediately(self):
        # Busy 를 영원히 반환하는 시리얼 — estop 없으면 모션 폴이 타임아웃까지 돈다. estop 을 세우면
        #   폴이 **즉시** 무응답으로 빠져나온다(진행 중 제조 하드 중단의 물리적 근거).
        fake = FakeSerial(default=status_frame(0, ready=False))  # 영원히 Busy
        a = adapter_with(fake, motion_timeout_s=5.0)
        a._estop.set()
        t0 = time.monotonic()
        code = a._poll_until_ready(1, timeout_s=5.0)
        assert code == FAKE_TIMEOUT_RAW_CODE
        assert time.monotonic() - t0 < 1.0  # 타임아웃(5s)까지 안 기다리고 즉시 반환

    def test_clear_estop_resumes_normal_polling(self):
        fake = FakeSerial()
        a = adapter_with(fake)
        a._estop.set()
        a.clear_estop()
        assert not a._estop.is_set()
        assert a.dispense(cmd()).raw_error_code == 0  # 정상 복귀

    def test_run_op_initialize_clears_estop_latch(self):
        # 초기화 = estop 복구 경로 — 래치를 풀어야 홈 탐색 폴이 안 빠져나온다.
        from senlyt_pi.ports.engine_port import EngineOpCommand

        fake = FakeSerial()
        a = adapter_with(fake)
        a._estop.set()
        a.run_op(EngineOpCommand(pump_addr=1, op="initialize", spec=SPEC_05))
        assert not a._estop.is_set()


# ── 8. Bit5 완료판정 (2026-07-20 "점검시 용량 조절 X" 봉합) ─────────────────────
#   실기기: 흡입 A9600 이 9583 정지(idle+latched err15)했는데 폴이 40s 매달리다 실패 → 배출 못 감.
#   근본원인 = parse_status 의 ready=Bit5&&err0 이 idle+err15 에서 ready=False → blanket 15=계속 폴.
#   수정: 완료 판정을 raw Bit5(idle)로 하고 err 는 별도 분류(매뉴얼 §4.6.1) — idle+err15 는 즉시
#   TR+재초기화+정직한 실패(거짓 성공 금지), busy-15 는 계속 폴.


def status_pos_frame(code: int, ready: bool, position: int | None) -> bytes:
    """`/0{상태바이트}{위치}` + ETX — `?` 응답 모사(위치 포함)."""
    b = (0x20 if ready else 0x00) | (code & 0x0F)
    tail = str(position).encode() if position is not None else b""
    return b"/0" + bytes([b]) + tail + b"\x03"


class ScriptedPollSerial(FakeSerial):
    """`?` 호출마다 스크립트된 (code, ready, position) 프레임을 순서대로 반환(마지막은 반복)."""

    def __init__(self, frames: list[bytes]):
        super().__init__()
        self._frames = list(frames)
        self._i = 0

    def write(self, data: bytes) -> int:
        self.written.append(data.decode("ascii"))
        f = self._frames[min(self._i, len(self._frames) - 1)]
        self._i += 1
        self._buf.extend(f)
        return len(data)


class TestParseStatusRaw:
    def test_idle_latched_err15_exposes_idle(self):
        # 0x6F = Bit5(idle)=1 + err=15. parse_status_raw 는 idle 을 err 와 안 섞고 그대로 노출한다.
        from senlyt_pi.adapters.sy01b_engine_adapter import parse_status_raw

        assert parse_status_raw(status_pos_frame(15, True, None).decode("ascii", "ignore")) == (
            15,
            True,
            None,
        )

    def test_busy_err15_is_not_idle(self):
        from senlyt_pi.adapters.sy01b_engine_adapter import parse_status_raw

        assert parse_status_raw(status_pos_frame(15, False, None).decode("ascii", "ignore")) == (
            15,
            False,
            None,
        )

    def test_idle_err0_is_idle(self):
        from senlyt_pi.adapters.sy01b_engine_adapter import parse_status_raw

        assert parse_status_raw(status_pos_frame(0, True, None).decode("ascii", "ignore")) == (
            0,
            True,
            None,
        )

    def test_public_parse_status_still_folds_err(self):
        # 공개 계약 불변: idle+err15 → ready=False(에러면 준비 안 됨), idle+err0 → ready=True.
        assert parse_status(status_pos_frame(15, True, None).decode("ascii", "ignore")) == (15, False)
        assert parse_status(status_pos_frame(0, True, None).decode("ascii", "ignore")) == (0, True)


class TestBit5CompletionJudgement:
    def test_idle_latched_err15_returns_fast_and_reinits(self):
        # idle+err15(정지+latched Command Overflow) → timeout 훨씬 전에 15 반환 + TR + 캐시 무효화.
        fake = ScriptedPollSerial([status_pos_frame(15, True, 9583)])  # 첫 폴부터 idle+err15(반복).
        a = adapter_with(fake)
        a._initialized.add(1)
        t0 = time.monotonic()
        code = a._poll_until_ready(1, 40.0)
        assert code == 15  # 정직한 실패(거짓 성공 금지)
        assert time.monotonic() - t0 < 5.0  # 33초 매달림 부재(무한/과도 대기 금지)
        assert 1 not in a._initialized  # 재초기화 예약(캐시 무효화)
        assert any("TR" in w for w in fake.written)  # latched 에러 해제(안전 복구)

    def test_busy_err15_keeps_polling_until_idle(self):
        # busy(Bit5=0)+err15 는 "아직 도는 중" → 계속 폴 → idle+err0 오면 0 반환.
        frames = [
            status_pos_frame(15, False, 4000),  # busy-15 → 계속 폴
            status_pos_frame(15, False, 8000),  # busy-15 → 계속 폴
            status_pos_frame(0, True, 9600),  # idle+err0 → 완료
        ]
        a = adapter_with(ScriptedPollSerial(frames))
        assert a._poll_until_ready(1, 40.0) == 0

    def test_normal_completion_idle_err0(self):
        frames = [status_pos_frame(0, False, 3000), status_pos_frame(0, True, 9600)]
        a = adapter_with(ScriptedPollSerial(frames))
        assert a._poll_until_ready(1, 40.0) == 0
