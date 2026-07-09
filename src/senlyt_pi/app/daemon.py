"""senlytd 데몬 조립(wiring) — 헥사고날 코어 ↔ 포트 ↔ 어댑터 결선.

Dart `lib/app/daemon.dart` 포팅.

이번 웨이브 = **골격**. 실제 명령 소비 루프(Sequencer 상시 구동)·펌프 구동은 유보(안전상
이후 웨이브). 이 클래스는 포트 의존성 주입 구조와 부팅/종료 뼈대만 제공한다.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..persistence.idempotency_ledger import IdempotencyLedger
from ..ports.command_source_port import CommandSourcePort
from ..ports.engine_port import EnginePort
from ..ports.status_sink_port import StatusSinkPort


@dataclass(frozen=True, slots=True)
class DaemonDeps:
    """데몬 의존성 묶음(포트 주입)."""

    device_id: str
    command_source: CommandSourcePort
    status_sink: StatusSinkPort
    engine: EnginePort
    ledger: IdempotencyLedger


class SenlytDaemon:
    """headless 디스펜서 데몬 골격."""

    def __init__(self, deps: DaemonDeps) -> None:
        self.deps = deps

    def boot(self) -> None:
        """부팅 — 실 루프(SSE 구독→멱등 판정→Sequencer→status 역보고)는 이후 웨이브."""
        # TODO(wave-next): BootRecovery.plan() → INTERRUPTED 보고 →
        #   deps.command_source.commands(device_id) 소비 →
        #   ledger.check_and_claim(command.id) → EnginePort 실토출(Sequencer) →
        #   status_sink.report_status / heartbeat 30s / ship_trace.
        # 이번 웨이브는 결선 구조·계약 포팅까지만.
        raise NotImplementedError(
            "SenlytDaemon.boot — 소비 루프는 이후 웨이브(안전상 유보)"
        )

    def shutdown(self) -> None:
        """우아한 종료."""
        # TODO(wave-next): Sequencer.request_drain → OQ flush → 시리얼 close → heartbeat 정지.
