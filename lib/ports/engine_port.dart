/// EnginePort — 시린지 펌프 구동 포트(인터페이스만) — SoT §6-7.
///
/// ⛔ 안전상 유보(이번 웨이브 범위 밖): 펌프 구동 실로직(실토출)·Sequencer 는 구현하지 않는다.
///    실어댑터(sy01b 시리얼 RR)는 `lib/adapters/` 에 TODO 스텁으로만 둔다.
///
/// 에러코드 분류·재시도 정책은 core `pump_guard.dart` `classifyEngineErrorCode`(§6-7).
library;

import '../core/pump_guard.dart' show SyringeSpec;

/// 단일 펌프 토출 명령(해석된 스텝) — 서버 recipe step → SyringeSpec 파생 후.
class EngineDispenseCommand {
  const EngineDispenseCommand({
    required this.pumpAddr,
    required this.volumeUl,
    required this.steps,
    required this.spec,
  });

  final int pumpAddr;
  final double volumeUl;

  /// SyringeSpec.stepsForVolumeUl 로 파생된 스텝수(하드코딩 금지·§6-4).
  final int steps;
  final SyringeSpec spec;
}

/// 엔진 실행 결과.
class EngineResult {
  const EngineResult({required this.rawErrorCode, this.detail});

  /// 엔진 raw errorCode(정수) — classifyEngineErrorCode 입력(§6-7). 0=정상.
  final int rawErrorCode;
  final String? detail;
}

/// 시린지 펌프 엔진 포트.
abstract interface class EnginePort {
  /// 단일 스텝 흡입(aspirate). ⛔ 실토출 로직 = 이후 웨이브.
  Future<EngineResult> aspirate(EngineDispenseCommand cmd);

  /// 단일 스텝 배출(dispense). ⛔ 실토출 로직 = 이후 웨이브.
  Future<EngineResult> dispense(EngineDispenseCommand cmd);

  /// 초기화(homing/purge). ⛔ 실로직 = 이후 웨이브.
  Future<EngineResult> initialize();
}
