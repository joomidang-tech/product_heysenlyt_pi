"""app 골격 테스트 — senlytd 진입점·데몬 결선·실어댑터 스텁의 안전 유보 확인.

Dart 와 동일 단계: 실행 시 펌프를 구동하지 않고 명확히 미구현임을 알린다(안전상 유보).
"""

import pytest

from senlyt_pi.adapters.fake_engine_adapter import FakeEnginePort
from senlyt_pi.adapters.http_status_sink_adapter import HttpStatusSinkAdapter
from senlyt_pi.adapters.sse_command_source_adapter import SseCommandSourceAdapter
from senlyt_pi.adapters.sy01b_engine_adapter import Sy01bEngineAdapter
from senlyt_pi.app.daemon import DaemonDeps, SenlytDaemon
from senlyt_pi.app.senlytd import main
from senlyt_pi.core.pump_guard import SyringeSpec
from senlyt_pi.persistence.idempotency_ledger import InMemoryIdempotencyLedger
from senlyt_pi.ports.engine_port import EngineDispenseCommand


def test_senlytd_entry_exits_zero_without_dispensing(capsys):
    """senlytd — 골격 안내 후 종료(exit 0)·펌프 미구동."""
    assert main() == 0
    err = capsys.readouterr().err
    assert "미구현" in err


def test_daemon_boot_is_reserved():
    """SenlytDaemon.boot — 소비 루프 유보(NotImplementedError)·shutdown 은 무해."""
    daemon = SenlytDaemon(
        DaemonDeps(
            device_id="dev-A",
            command_source=SseCommandSourceAdapter(),
            status_sink=HttpStatusSinkAdapter(),
            engine=FakeEnginePort(),
            ledger=InMemoryIdempotencyLedger(),
        )
    )
    with pytest.raises(NotImplementedError):
        daemon.boot()
    daemon.shutdown()  # no-op(골격) — 예외 없음.


def test_real_adapters_are_stubs():
    """실기기/실서버 어댑터 = TODO 스텁 — 어떤 경로로도 실토출/실전송 불가."""
    spec = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=1.25)
    cmd = EngineDispenseCommand(pump_addr=1, volume_ul=100, steps=960, spec=spec)
    engine = Sy01bEngineAdapter()
    with pytest.raises(NotImplementedError):
        engine.dispense(cmd)
    with pytest.raises(NotImplementedError):
        engine.aspirate(cmd)
    with pytest.raises(NotImplementedError):
        engine.initialize()
    with pytest.raises(NotImplementedError):
        next(SseCommandSourceAdapter().commands("dev-A"))
    with pytest.raises(NotImplementedError):
        HttpStatusSinkAdapter().ship_trace([])
