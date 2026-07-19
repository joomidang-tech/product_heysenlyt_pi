"""브로드캐스트 초기화 판정 구조 — 2026-07-19 실기기 오탐 회귀 앵커.

실기기 실측(10:13~10:17 · 기기 10000000b9166a1c · flavor 2펌프): 브로드캐스트 직후 버스가
일시 오염돼(펌프1 = ETX 없는 쓰레기 바이트 `\\x07x·3;k_` 5초·펌프2 = 완전 무응답) 옛 구조의
"매 브로드캐스트 뒤 개별 `?` 단발 확인(`_verify_alive`)"이 건강한 펌프 둘 다 -1000
(_NO_RESPONSE) 오탐 + 5s×2=10초 지연을 냈다. `/_Z1R` 은 그대로 나가 펌프는 실제 홈을 잡는데
"잡은 실패"로 보고된 것. 새 구조 = **눈감고 브로드캐스트 3발 + 끝에 펌프별 Ready 폴**
(v1.1.0 initializeAll 검증 구조) — 일과성 오염은 폴 재시도로 자연 통과하고, 진짜 죽은
펌프만 타임아웃으로 드러난다.

이 파일이 지키는 것 (기존 커버리지 0 이 이번 버그가 새어나간 이유):
  A. 오탐 회귀 앵커 — 브로드캐스트 무응답 + 초반 폴 쓰레기여도 전 펌프 성공(0) 판정
  B. 진짜 죽은 펌프 — 영원 무응답 펌프만 _NO_RESPONSE, 산 펌프는 0
  C. 와이어 순서 — /_TR → /_U… → /_{init} → /_I12R, 브로드캐스트들 사이에 주소지정 `?` 없음
"""

from __future__ import annotations

import pytest

from senlyt_pi.adapters import sy01b_engine_adapter as mod
from senlyt_pi.test_seam.fake_engine_sentinels import FAKE_TIMEOUT_RAW_CODE

# 기존 어댑터 테스트의 시리얼 더블(SerialLike seam) 관례를 재사용한다.
from test_sy01b_engine_adapter import SPEC_05, FakeSerial, adapter_with, status_frame


@pytest.fixture(autouse=True)
def fast_broadcast_gap(monkeypatch):
    """브로드캐스트 스텝 간격(500ms×2)을 줄여 테스트를 빠르게 — 로직엔 영향 없음."""
    monkeypatch.setattr(mod, "BROADCAST_STEP_GAP_S", 0.01)
    monkeypatch.setattr(mod, "BROADCAST_SETTLE_S", 0.01)


class BusScriptedSerial(FakeSerial):
    """브로드캐스트(`/_`)엔 무응답(물리 속성), 주소지정 `?` 폴엔 스크립트 응답을 주는 더블.

    `poll_scripts[addr]` = 그 주소의 `?` 폴에 순서대로 줄 바이트들. 소진되면 `poll_default
    [addr]`(기본 Ready). `None` 응답 = 무응답(버퍼에 아무것도 안 넣음).
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


# ── A. 오탐 회귀 앵커 — 일과성 버스 오염은 성공으로 통과한다 ─────────────────────


class TestTransientBusNoiseIsNotFailure:
    def test_garbage_then_ready_means_all_pumps_succeed(self):
        """브로드캐스트 무응답 + 첫 폴 몇 번 ETX 없는 쓰레기 → 이후 Ready = 전 펌프 0.

        옛 구조(매 발 뒤 `?` 단발 확인)는 정확히 이 시나리오에서 건강한 두 펌프를 -1000 으로
        오탐했다. 새 구조는 끝 폴이 쓰레기/무응답을 "아직 준비 안 됨"으로 재시도해 통과한다.
        """
        garbage = b"\x07x\xb73;k_"  # 실기기 트레이스 모사 — ETX(0x03 단독 프레임) 없음.
        fake = BusScriptedSerial(
            poll_scripts={
                1: [garbage, garbage],  # 펌프1 — 오염 두 번 뒤 Ready(기본값).
                2: [None],  # 펌프2 — 한 번 완전 무응답 뒤 Ready(기본값).
            }
        )
        a = adapter_with(fake, read_timeout_s=0.05, init_timeout_s=2.0)
        results = a.initialize_broadcast([1, 2], SPEC_05)
        assert results == {1: 0, 2: 0}, "일과성 버스 오염이 실패로 오탐되면 안 된다(2026-07-19 회귀)"
        assert a._initialized == {1, 2}  # 성공 펌프는 셋업 캐시 등록.


# ── B. 진짜 죽은 펌프 — 타임아웃으로만 드러난다 ─────────────────────────────────


class TestTrulyDeadPumpStillFails:
    def test_forever_silent_pump_times_out_alive_pump_succeeds(self):
        fake = BusScriptedSerial(poll_default={2: None})  # 펌프2 = 영원 무응답.
        a = adapter_with(fake, read_timeout_s=0.02, init_timeout_s=0.3)
        results = a.initialize_broadcast([1, 2], SPEC_05)
        assert results[1] == 0
        assert results[2] == FAKE_TIMEOUT_RAW_CODE  # 진짜 죽은 펌프만 실패.
        assert a._initialized == {1}  # 죽은 펌프는 캐시 미등록.


# ── C. 와이어 순서 — 눈감고 3발 + 안전포트, 사이에 단발 확인 없음 ────────────────


class TestWireOrder:
    def test_broadcast_sequence_and_no_probe_between_broadcasts(self):
        fake = BusScriptedSerial()
        a = adapter_with(fake, read_timeout_s=0.1, init_timeout_s=1.0)
        results = a.initialize_broadcast([1, 2], SPEC_05)
        assert results == {1: 0, 2: 0}
        broadcasts = [w for w in fake.written if w.startswith("/_")]
        # 순서: 상태리셋 → 스톨전류 → 홈 → 안전포트(0.5mL → U200,5 · Z1R · I12).
        assert broadcasts == ["/_TR\r", "/_U200,5R\r", "/_Z1R\r", "/_I12R\r"]
        # TR·U 뒤 개별 `?` 확인 제거 검증 — 첫 세 프레임이 **연달아** 브로드캐스트다
        #   (사이에 주소지정 `?` 가 끼면 옛 `_verify_alive` 구조로 회귀한 것).
        assert fake.written[:3] == ["/_TR\r", "/_U200,5R\r", "/_Z1R\r"]
        # 판정 폴(`?`)은 홈 브로드캐스트 **뒤에만** 나온다.
        first_query = next(i for i, w in enumerate(fake.written) if w.rstrip("\r").endswith("?"))
        assert first_query > fake.written.index("/_Z1R\r")
