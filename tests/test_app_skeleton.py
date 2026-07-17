"""app 골격 테스트 — senlytd 진입점·실어댑터 결선.

스텁 제거(2026-07-10): 소비 루프(daemon.boot)는 실구현됐고 `SENLYT_RUN=1` 이 켜는 스위치다.
이 테스트는 (1) 무설정 기본 경로가 펌프를 구동하지 않고 안전 종료하며, (2) SSE/status 어댑터가
실 HTTP 클라이언트로 구성되고, (3) 실 RS485 엔진 어댑터만 여전히 TODO 스텁임을 고정한다.
(boot 상시 소비 루프의 실구현 검증은 test_daemon_boot.py.)
"""

import pytest

from senlyt_pi.adapters.http_status_sink_adapter import HttpStatusSinkAdapter
from senlyt_pi.adapters.sse_command_source_adapter import SseCommandSourceAdapter
from senlyt_pi.adapters.sy01b_engine_adapter import Sy01bEngineAdapter
from senlyt_pi.app.senlytd import main
from senlyt_pi.core.pump_guard import SyringeSpec
from senlyt_pi.ports.engine_port import EngineDispenseCommand


def test_senlytd_entry_exits_zero_without_dispensing(capsys, monkeypatch):
    """senlytd — 무설정 기본 경로는 네트워크 없이 결선 준비 안내 후 종료(exit 0)·펌프 미구동."""
    monkeypatch.delenv("SENLYT_RUN", raising=False)
    monkeypatch.delenv("SENLYT_SELFTEST", raising=False)
    assert main() == 0
    err = capsys.readouterr().err
    # 상시 소비 루프는 SENLYT_RUN=1 스위치로 실행 — 무설정은 안전 종료.
    assert "SENLYT_RUN" in err


def test_sse_and_status_adapters_are_real_not_stubs():
    """SSE/status 어댑터 = 실 클라이언트 — 구성 시 NotImplementedError 없음(스텁 제거)."""
    sse = SseCommandSourceAdapter(base_url="http://web:3000", bearer_token="t")
    status = HttpStatusSinkAdapter(base_url="http://web:3000", bearer_token="t")
    # 스트림 URL·엔드포인트가 실 base 를 소비(하드코딩 URL 아님).
    assert sse._stream_url("dev-A").startswith("http://web:3000/api/dispenser/orders/stream")
    # ship_trace([]) 는 실 어댑터에서 no-op(왕복 없음·예외 없음).
    status.ship_trace([])


def test_sy01b_engine_adapter_is_implemented():
    """실 RS485 엔진 어댑터가 **구현됐다**(2026-07-17) — 구 스텁 단언의 역전.

    이전엔 `NotImplementedError` 를 던지는 게 계약이었다(실토출 유보). 이제 어댑터는 시리얼
    seam 을 받아 I→P→O→D 를 실제로 조립한다 — 시리얼이 없는 이 환경에선 **예외를 던지지 않고
    실패 결과로 흡수**하는 것이 계약이다(상위 stage 태스크가 형제를 완주시켜야 하므로).
    상세 계약 검증은 tests/test_sy01b_engine_adapter.py.
    """
    spec = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=0.5)
    cmd = EngineDispenseCommand(pump_addr=1, volume_ul=100, steps=2400, spec=spec)

    class DeadSerial:
        def write(self, data):
            raise OSError("no device")

        def read(self, size=1):
            return b""

        @property
        def in_waiting(self):
            return 0

        def close(self):
            pass

    engine = Sy01bEngineAdapter(serial_factory=lambda *_a: DeadSerial())
    # 예외가 아니라 **실패 결과**로 나온다(silent-success 도 아니고 raise 도 아니다).
    assert engine.dispense(cmd).raw_error_code != 0
    assert engine.aspirate(cmd).raw_error_code != 0
    assert engine.initialize().raw_error_code == 0  # 캐시 무효화 — 시리얼 불필요
