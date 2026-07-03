/// senlytd 데몬 조립(wiring) — 헥사고날 코어 ↔ 포트 ↔ 어댑터 결선.
///
/// 이번 웨이브 = **골격**. 실제 명령 소비 루프(Sequencer)·펌프 구동은 유보(안전상 이후 웨이브).
/// 이 클래스는 포트 의존성 주입 구조와 부팅/종료 뼈대만 제공한다.
library;

import '../ports/command_source_port.dart';
import '../ports/engine_port.dart';
import '../ports/status_sink_port.dart';
import '../persistence/idempotency_ledger.dart';

/// 데몬 의존성 묶음(포트 주입).
class DaemonDeps {
  const DaemonDeps({
    required this.deviceId,
    required this.commandSource,
    required this.statusSink,
    required this.engine,
    required this.ledger,
  });

  final String deviceId;
  final CommandSourcePort commandSource;
  final StatusSinkPort statusSink;
  final EnginePort engine;
  final IdempotencyLedger ledger;
}

/// headless 디스펜서 데몬 골격.
class SenlytDaemon {
  SenlytDaemon(this.deps);

  final DaemonDeps deps;

  /// 부팅 — 실 루프(SSE 구독→멱등 판정→Sequencer→status 역보고)는 이후 웨이브.
  Future<void> boot() async {
    // TODO(wave-2): deps.commandSource.commands(deviceId) 구독 →
    //   ledger.checkAndClaim(command.id) → EnginePort 실토출(Sequencer) →
    //   statusSink.reportStatus / heartbeat 30s / shipTrace.
    // 이번 웨이브는 결선 구조·계약 포팅까지만.
    throw UnimplementedError('SenlytDaemon.boot — 소비 루프는 이후 웨이브(안전상 유보)');
  }

  /// 우아한 종료.
  Future<void> shutdown() async {
    // TODO(wave-2): OQ flush·시리얼 close·heartbeat 정지.
  }
}
