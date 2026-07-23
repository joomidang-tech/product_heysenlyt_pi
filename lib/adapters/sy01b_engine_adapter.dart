/// sy01b 시리얼 EnginePort 실어댑터 — ⛔ TODO 스텁(이번 웨이브 범위 밖).
///
/// 안전상 유보(SoT 워크오더): 펌프 구동 실로직(실토출·RR 시리얼 프로토콜)·Sequencer 는
/// 이후 웨이브에서 구현한다. 지금은 포트 계약만 만족하는 미구현 스텁.
///
/// 실구현 시 참조: SoT §6-1(SY-01B U200·fullStroke 12000)·§6-4(steps 파생)·§6-7(errorCode).
library;

import '../ports/engine_port.dart';

/// SY-01B 시린지 펌프 시리얼 어댑터 — 미구현 스텁.
class Sy01bEngineAdapter implements EnginePort {
  @override
  Future<EngineResult> aspirate(EngineDispenseCommand cmd) {
    // TODO(wave-2): RR 시리얼 프로토콜 실토출. 안전 게이트(0<vol≤maxVolumeUl) 통과분만.
    throw UnimplementedError('sy01b aspirate — 이후 웨이브(실토출 로직 유보)');
  }

  @override
  Future<EngineResult> dispense(EngineDispenseCommand cmd) {
    // TODO(wave-2): RR 시리얼 프로토콜 실토출.
    throw UnimplementedError('sy01b dispense — 이후 웨이브(실토출 로직 유보)');
  }

  @override
  Future<EngineResult> initialize() {
    // TODO(wave-2): homing/purge (§6-5 초기화힘 ZR/Z1R/Z2R).
    throw UnimplementedError('sy01b initialize — 이후 웨이브');
  }
}
