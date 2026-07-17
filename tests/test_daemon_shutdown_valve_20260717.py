"""daemon 종료 시 밸브 안전 — F2 설계 의도 회귀 잠금 (2026-07-17).

설계 의도: SIGTERM/SIGINT → senlytd 시그널 핸들러 → daemon.request_stop() →
boot() finally → shutdown() → **valve.close_all()**. 이 우아한 종료 경로가 밸브를 반드시
닫는다는 것을 잠근다(회귀 방지).

⚠️ SIGKILL/전원차단은 shutdown 을 안 거치므로 소프트웨어가 밸브를 못 닫는다 — 하드웨어
   fail-safe(스프링복귀 솔레노이드 = 무전원 시 닫힘)가 최종 방어다. 이 테스트 범위 밖(물리).
"""

from pathlib import Path

from senlyt_pi.adapters.fake_engine_adapter import FakeEnginePort
from senlyt_pi.adapters.valve_adapter import FakeValveAdapter
from senlyt_pi.app.daemon import DaemonDeps, SenlytDaemon
from senlyt_pi.persistence.file_idempotency_ledger import FileIdempotencyLedger


class _NoopSink:
    """StatusSinkPort 최소 구현 — 종료 경로 검증에 네트워크 불필요."""

    def report_status(self, report):  # noqa: ANN001, D102
        pass

    def send_heartbeat(self, hb):  # noqa: ANN001, D102
        pass

    def ship_trace(self, spans):  # noqa: ANN001, D102
        pass


class _NoSource:
    """CommandSource 최소 구현 — 도착분 없음(종료만 검증)."""

    def commands(self, device_id):  # noqa: ANN001, D102
        return iter(())

    def command_sets(self, device_id):  # noqa: ANN001, D102
        return iter(())


def _daemon(tmp_path: Path, valve: FakeValveAdapter):
    led = FileIdempotencyLedger.open(tmp_path / "l.log")
    deps = DaemonDeps(
        device_id="dev-A",
        command_source=_NoSource(),
        status_sink=_NoopSink(),
        engine=FakeEnginePort(),
        ledger=led,
        valve=valve,
        heartbeat_interval_s=0.0,  # heartbeat 스레드 비활성(테스트).
    )
    return SenlytDaemon(deps), led


def test_shutdown_closes_valve(tmp_path):
    valve = FakeValveAdapter()
    daemon, led = _daemon(tmp_path, valve)
    daemon.shutdown()
    assert valve.close_all_calls >= 1
    led.close()


def test_sigterm_path_boot_then_stop_closes_valve(tmp_path):
    """SIGTERM 경로 근사: stop 선세팅(시그널 핸들러가 하는 일) → boot() 즉시 종료하며 close_all."""
    valve = FakeValveAdapter()
    daemon, led = _daemon(tmp_path, valve)
    daemon.request_stop()  # signal handler → request_stop
    daemon.boot()          # stop 이 이미 서서 소비 0회 → finally shutdown → close_all
    assert valve.close_all_calls >= 1
    led.close()


def test_shutdown_is_idempotent_closes_valve_once(tmp_path):
    valve = FakeValveAdapter()
    daemon, led = _daemon(tmp_path, valve)
    daemon.shutdown()
    daemon.shutdown()  # 멱등(shutdown_done 가드) — 2회째 no-op
    assert valve.close_all_calls == 1
    led.close()
