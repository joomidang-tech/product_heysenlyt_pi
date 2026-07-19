"""브로드캐스트 초기화 = fire-and-forget — 2026-07-19 실기기 오탐 회귀 앵커.

실기기 실측 2회(기기 10000000b9166a1c · flavor 2펌프):
  1차(10:13) — 매 브로드캐스트 뒤 개별 `?` 단발 확인(`_verify_alive`)이 버스 오염에
    -1000 오탐 → 단발 확인 제거(91e3b08).
  2차(12:09, 91e3b08 이후에도 지속) — **끝의 펌프별 Ready 폴조차** 이 기기에서 오탐:
    펌프1 = 매번 다른 ETX 없는 쓰레기(`[`·`>F[`·`6f7`…) · 펌프2 = 5s 무응답 반복 →
    `-1000 → ENGINE_ERROR_PERMANENT`. 그동안 `/_Z1R` 은 나가 홈은 **실제 수행됨**.

결론(2026-07-19 사용자 승인): 초기화는 open-loop — **명령만 쏘고 어떤 `?` 판정도 하지
않는다**(fire-and-forget). 홈은 `HOME_SETTLE_S` 고정 대기로만 보장하고 전 펌프 성공(0)
반환. v1.0.0 Flask 기기설정 툴 `api_broadcast(action=init)`(poll 없이 sleep 후 성공)과
동일 방식 — 실기기 검증된 유일한 방식이다. silent-success 는 **정비 한정 허용**:
진짜 죽은 펌프는 이후 토출 경로의 Ready 폴(`_settle`/`_ensure_ready` — 유지됨)에서 드러난다.

이 파일이 지키는 것:
  A. fire-and-forget 앵커 — 시리얼 링크가 어떤 상태든(쓰레기·무응답) 전 펌프 성공(0)
  B. `?` 프레임 0 — 초기화 시퀀스에서 상태 조회가 한 발도 안 나간다(폴 회귀 방지)
  C. 와이어 순서 — /_TR → /_U… → /_{init} → /_I12R 브로드캐스트 4발뿐
  D. 토출 경계 — 토출 셋업(`_setup`)의 Ready 폴은 그대로다(silent-success 토출 전파 방지)
"""

from __future__ import annotations

import pytest

from senlyt_pi.adapters import sy01b_engine_adapter as mod
from senlyt_pi.test_seam.fake_engine_sentinels import FAKE_TIMEOUT_RAW_CODE

# 기존 어댑터 테스트의 시리얼 더블(SerialLike seam) 관례를 재사용한다.
from test_sy01b_engine_adapter import SPEC_05, FakeSerial, adapter_with, cmd, status_frame


@pytest.fixture(autouse=True)
def fast_broadcast_waits(monkeypatch):
    """브로드캐스트 대기(500ms×3 + 홈 4s)를 줄여 테스트를 빠르게 — 로직엔 영향 없음."""
    monkeypatch.setattr(mod, "BROADCAST_STEP_GAP_S", 0.01)
    monkeypatch.setattr(mod, "BROADCAST_SETTLE_S", 0.01)
    monkeypatch.setattr(mod, "HOME_SETTLE_S", 0.01)


class BusScriptedSerial(FakeSerial):
    """브로드캐스트(`/_`)엔 무응답(물리 속성), 주소지정 `?` 폴엔 스크립트 응답을 주는 더블.

    `poll_scripts[addr]` = 그 주소의 `?` 폴에 순서대로 줄 바이트들. 소진되면 `poll_default
    [addr]`(기본 Ready). `None` 응답 = 무응답(버퍼에 아무것도 안 넣음).
    fire-and-forget 에선 초기화 중 `?` 가 아예 안 나가야 하므로, 스크립트는 주로
    "폴이 나갔다면 실패했을 상황"을 깔아 두는 용도다.
    """

    def __init__(
        self,
        poll_scripts: dict[int, list[bytes | None]] | None = None,
        poll_default: dict[int, bytes | None] | None = None,
    ):
        super().__init__(default=status_frame(0, ready=True))
        self._poll_scripts = {a: list(v) for a, v in (poll_scripts or {}).items()}
        self._poll_default = dict(poll_default or {})

    def write(self, data: bytes) -> int:
        txt = data.decode("ascii")
        self.written.append(txt)
        if txt.startswith("/_"):
            return len(data)  # 브로드캐스트 = 무응답(응답을 읽지도 않는다).
        if txt.rstrip("\r").endswith("?"):
            addr = int(txt[1:-2])  # `/{addr}?\r`
            script = self._poll_scripts.get(addr)
            if script:
                resp = script.pop(0)
            else:
                resp = self._poll_default.get(addr, status_frame(0, ready=True))
            if resp is not None:
                self._buf.extend(resp)
            return len(data)
        self._buf.extend(status_frame(0, ready=True))  # 그 외 명령 ACK = 정상.
        return len(data)


def _queries(written: list[str]) -> list[str]:
    return [w for w in written if w.rstrip("\r").endswith("?")]


# ── A. fire-and-forget 앵커 — 링크 상태와 무관하게 전 펌프 성공 ──────────────────


class TestFireAndForgetSucceedsRegardlessOfLink:
    def test_corrupt_and_silent_link_still_all_success(self):
        """펌프1 = 영원히 쓰레기 프레임 · 펌프2 = 영원 무응답이어도 전 펌프 0.

        옛 구조(끝 Ready 폴)는 정확히 이 시나리오에서 건강한 두 펌프를 -1000 →
        ENGINE_ERROR_PERMANENT 로 오탐했다(2026-07-19 12:09 실기기). 새 구조는 응답을
        아예 읽지 않으므로 링크가 어떤 상태든 성공이다.
        """
        garbage = b"\x07x\xb73;k_"  # 실기기 트레이스 모사 — ETX(0x03) 없음.
        fake = BusScriptedSerial(
            poll_default={1: garbage, 2: None}  # 폴이 나갔다면 실패했을 상황.
        )
        a = adapter_with(fake, read_timeout_s=0.05, init_timeout_s=2.0)
        results = a.initialize_broadcast([1, 2], SPEC_05)
        assert results == {1: 0, 2: 0}, "fire-and-forget — 링크 상태가 초기화 판정에 개입하면 안 된다"
        # silent-success(정비 한정): 전 펌프 캐시 등록 — 죽은 펌프는 토출에서 드러난다.
        assert a._initialized == {1, 2}


# ── B. `?` 프레임 0 — 폴 회귀 방지(이번 버그의 재발 방지 핵심) ───────────────────


class TestNoStatusQueryDuringInit:
    def test_no_query_frames_at_all(self):
        """초기화 시퀀스에서 상태 조회(`?`)가 **한 발도** 나가지 않는다.

        단발 확인(10:13 오탐)이든 Ready 폴(12:09 오탐)이든, 브로드캐스트 초기화 안의
        `?` 는 이 기기에서 오탐원이다. 어떤 형태로든 되살아나면 회귀다.
        """
        fake = BusScriptedSerial()
        a = adapter_with(fake, read_timeout_s=0.1, init_timeout_s=1.0)
        a.initialize_broadcast([1, 2], SPEC_05)
        assert _queries(fake.written) == [], "초기화 중 `?` 부활 = 2026-07-19 오탐 회귀"


# ── C. 와이어 순서 — 눈감고 브로드캐스트 4발뿐 ─────────────────────────────────


class TestWireOrder:
    def test_broadcast_only_sequence(self):
        fake = BusScriptedSerial()
        a = adapter_with(fake, read_timeout_s=0.1, init_timeout_s=1.0)
        results = a.initialize_broadcast([1, 2], SPEC_05)
        assert results == {1: 0, 2: 0}
        # 순서: 상태리셋 → 스톨전류 → 홈 → 안전포트(0.5mL → U200,5 · Z1R · I12).
        assert fake.written == ["/_TR\r", "/_U200,5R\r", "/_Z1R\r", "/_I12R\r"], (
            "초기화 와이어 = 브로드캐스트 4발뿐 — 주소지정 프레임이 끼면 회귀"
        )


# ── D. 토출 경계 — 셋업 Ready 폴은 그대로(정비 밖 silent-success 금지) ───────────


class DeadSerial(FakeSerial):
    """모든 프레임(브로드캐스트·주소지정)에 무응답 — 죽은/링크 끊긴 펌프 모사."""

    def write(self, data: bytes) -> int:
        self.written.append(data.decode("ascii"))
        return len(data)


class TestDispenseSetupStillPolls:
    def test_setup_still_fails_on_dead_pump(self):
        """`_setup`(토출 셋업)은 여전히 응답을 검증한다 — 죽은 펌프는 여기서 걸린다.

        fire-and-forget 은 **정비(initialize) 한정**이다. 초기화가 오성공시킨 죽은 펌프도
        토출 전 셋업/명령의 Ready 폴이 무응답으로 잡아야 EP-03(토출 silent-success 금지)이
        유지된다.
        """
        fake = DeadSerial(default=b"")
        a = adapter_with(fake, read_timeout_s=0.02, init_timeout_s=0.1)
        code = a._setup(1, SPEC_05)
        assert code != 0, "토출 셋업까지 fire-and-forget 이 번지면 EP-03 위반(위험한 회귀)"

    def test_dead_pump_registered_by_init_still_fails_at_dispense(self):
        """통합 경로 앵커(리뷰 P2) — 오성공 캐시 등록 → 다음 토출에서 실패로 드러난다.

        fire-and-forget 초기화는 죽은 펌프도 캐시에 등록해 다음 토출이 `_ensure_ready`
        셋업을 건너뛴다. 그래도 `_cycle` 의 첫 주소지정 명령(`I{port}R` Ready 폴)이
        무응답을 잡아야 "silent-success 가 토출까지 전파"가 안 된다 — 이 사슬이 정비
        한정 허용(트레이드오프 §4)의 안전 근거이므로 통합으로 앵커한다.
        """
        fake = DeadSerial(default=b"")
        a = adapter_with(fake, read_timeout_s=0.02, init_timeout_s=0.1)
        results = a.initialize_broadcast([1], SPEC_05)
        assert results == {1: 0}  # 정비는 오성공(허용).
        assert a._initialized == {1}  # 캐시 등록 → 다음 토출 셋업 스킵.
        res = a.dispense(cmd())
        assert res.raw_error_code == FAKE_TIMEOUT_RAW_CODE, (
            "죽은 펌프가 토출에서도 성공하면 silent-success 전파 — 물리 안전 회귀"
        )


# ── E. estop 우선 — fire-and-forget 창을 estop 이 뚫는다(리뷰 P1·P2) ─────────────


class EstopTriggerSerial(FakeSerial):
    """특정 프레임 송신 순간 estop 이벤트를 세우는 더블 — 정비 중 감시 스레드 발동 모사."""

    def __init__(self, trigger_frame: str):
        super().__init__(default=status_frame(0, ready=True))
        self._trigger_frame = trigger_frame
        self.estop_event = None  # 테스트가 주입.

    def write(self, data: bytes) -> int:
        txt = data.decode("ascii")
        self.written.append(txt)
        if txt == self._trigger_frame and self.estop_event is not None:
            self.estop_event.set()
        return len(data)


class TestEstopInterruptsFireAndForget:
    def test_estop_mid_sequence_aborts_and_does_not_register_cache(self):
        """시퀀스 도중 estop → 남은 물리 이동(홈·안전포트) 미발사 + 캐시 미등록 + 전 펌프 실패.

        리뷰 P1 회귀 앵커: 무조건 `_initialized.add()` 는 estop 의 discard(안전 무효화)를
        덮어, estop 이 홈을 중단시킨 펌프가 재홈 없이 다음 토출을 받게 된다. estop 이 서면
        ① 이후 브로드캐스트가 나가면 안 되고 ② 캐시가 비어 있어야 하며 ③ 결과는 실패여야
        한다(운영자 재시도 유도).
        """
        import threading

        ev = threading.Event()
        fake = EstopTriggerSerial(trigger_frame="/_U200,5R\r")  # [1/4] 송신 순간 estop.
        fake.estop_event = ev
        a = adapter_with(fake, read_timeout_s=0.05, init_timeout_s=1.0, estop_event=ev)
        results = a.initialize_broadcast([1, 2], SPEC_05)
        assert results == {1: FAKE_TIMEOUT_RAW_CODE, 2: FAKE_TIMEOUT_RAW_CODE}, (
            "estop 중단은 성공으로 보고되면 안 된다"
        )
        assert a._initialized == set(), "estop 후 캐시 등록 = 재홈 없는 토출 허용(P1 회귀)"
        assert "/_Z1R\r" not in fake.written, "estop 후 홈 브로드캐스트 발사 금지"
        assert "/_I12R\r" not in fake.written, "estop 후 안전포트 브로드캐스트 발사 금지"
