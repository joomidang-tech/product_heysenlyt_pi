/// Fake EnginePort 하네스 — SoT §6-7 / 질의서 §0·Q8(EP-03·EP-09) 객관 판정 근거.
///
/// **P0 게이트의 관찰 렌즈**: dispense 호출 카운터로 IL-02(중복토출0)·CR-01(재기동 자동재실행
/// 금지)·EP-03(빈응답=실패·silent-success 금지)를 **객관 검증**한다.
///
/// 주입 가능한 결과(scripted): ack(정상 0) / busy(transient) / permanent / timeout / **empty**(무응답).
///   - empty(""·무응답) = 실패로 분류되어야 한다(EP-03·EP-09). silent-success 0.
///
/// 이 파일은 test 하네스 전용 — 실 sy01b 시리얼 어댑터(lib/adapters/sy01b_engine_adapter.dart)는
/// 계속 TODO 스텁으로 둔다(실기 PoC 유보).
library;

import 'package:heysenlyt_pi/heysenlyt_pi.dart';
import 'package:heysenlyt_pi/test_seam/fake_engine_sentinels.dart';

/// 엔진에 주입할 시나리오 결과 종류.
enum FakeEngineOutcome {
  /// 정상 ack — rawErrorCode 0.
  ack,

  /// busy — transient(SoT §6-7: 재시도 대상). rawErrorCode 1.
  busy,

  /// permanent — 즉시중단(SoT §6-7). rawErrorCode 2.
  permanent,

  /// timeout — transient(SoT §6-7: ENGINE_TIMEOUT·재시도). rawErrorCode = timeout 표식.
  timeout,

  /// empty — 빈 응답(""·무응답). EP-03: **실패**로 분류(silent-success 금지).
  ///   Fake 는 이를 rawErrorCode(음수 sentinel)로 노출해 EnginePort 재시도층이 실패 처리하는지 검증.
  empty,
}

// timeout/empty sentinel 은 lib/test_seam/fake_engine_sentinels.dart 에서 공유
// (EngineExecutor 와 동일 상수를 봐야 EP-03 이 성립).

/// FakeEngineOutcome → EngineResult 매핑.
EngineResult _outcomeToResult(FakeEngineOutcome o) {
  switch (o) {
    case FakeEngineOutcome.ack:
      return const EngineResult(rawErrorCode: 0);
    case FakeEngineOutcome.busy:
      return const EngineResult(rawErrorCode: 1, detail: 'busy');
    case FakeEngineOutcome.permanent:
      return const EngineResult(rawErrorCode: 2, detail: 'permanent');
    case FakeEngineOutcome.timeout:
      return const EngineResult(rawErrorCode: kFakeTimeoutRawCode, detail: 'timeout');
    case FakeEngineOutcome.empty:
      return const EngineResult(rawErrorCode: kFakeEmptyRawCode, detail: '');
  }
}

/// 한 번의 dispense 호출 기록(관찰용).
class DispenseCall {
  const DispenseCall({required this.pumpAddr, required this.volumeUl, required this.steps});

  final int pumpAddr;
  final double volumeUl;
  final int steps;
}

/// Fake EnginePort — dispense 호출 카운터 + 결과 주입.
///
/// **호출 카운터가 P0 게이트의 진실**: [dispenseCount]/[dispenseCalls] 로 실제 물리 토출 시도
/// 횟수를 객관 관찰한다. Ledger DROP·재기동 no-op·empty 실패 시 카운터가 늘지 않아야 한다.
class FakeEnginePort implements EnginePort {
  FakeEnginePort();

  /// pumpAddr 별 결과 스크립트(FIFO 큐). 비면 [defaultOutcome] 사용.
  final Map<int, List<FakeEngineOutcome>> _scriptByAddr = <int, List<FakeEngineOutcome>>{};

  /// 스크립트가 없을 때의 기본 결과.
  FakeEngineOutcome defaultOutcome = FakeEngineOutcome.ack;

  /// dispense 호출 이력(P0 관찰 렌즈).
  final List<DispenseCall> dispenseCalls = <DispenseCall>[];

  /// aspirate 호출 이력.
  final List<DispenseCall> aspirateCalls = <DispenseCall>[];

  /// initialize 호출 횟수.
  int initializeCount = 0;

  /// dispense 총 호출 횟수 — IL-02/CR-01/EP-03 판정의 핵심 카운터.
  int get dispenseCount => dispenseCalls.length;

  /// 특정 pumpAddr 의 dispense 호출 횟수.
  int dispenseCountFor(int pumpAddr) =>
      dispenseCalls.where((c) => c.pumpAddr == pumpAddr).length;

  /// pumpAddr 에 결과 스크립트를 주입(FIFO). 없으면 defaultOutcome.
  void scriptFor(int pumpAddr, List<FakeEngineOutcome> outcomes) {
    _scriptByAddr[pumpAddr] = List<FakeEngineOutcome>.from(outcomes);
  }

  /// 모든 pumpAddr 에 단일 결과 스크립트를 주입(테스트 편의).
  void scriptAll(FakeEngineOutcome outcome) {
    defaultOutcome = outcome;
    _scriptByAddr.clear();
  }

  FakeEngineOutcome _nextOutcome(int pumpAddr) {
    final q = _scriptByAddr[pumpAddr];
    if (q != null && q.isNotEmpty) return q.removeAt(0);
    return defaultOutcome;
  }

  @override
  Future<EngineResult> aspirate(EngineDispenseCommand cmd) async {
    aspirateCalls.add(
      DispenseCall(pumpAddr: cmd.pumpAddr, volumeUl: cmd.volumeUl, steps: cmd.steps),
    );
    // aspirate 도 동일 스크립트 소비 — 실 하드웨어는 흡입/배출이 하나의 물리 사이클.
    return _outcomeToResult(_nextOutcome(cmd.pumpAddr));
  }

  @override
  Future<EngineResult> dispense(EngineDispenseCommand cmd) async {
    dispenseCalls.add(
      DispenseCall(pumpAddr: cmd.pumpAddr, volumeUl: cmd.volumeUl, steps: cmd.steps),
    );
    return _outcomeToResult(_nextOutcome(cmd.pumpAddr));
  }

  @override
  Future<EngineResult> initialize() async {
    initializeCount++;
    return const EngineResult(rawErrorCode: 0);
  }

  /// 관찰 상태 초기화(재기동 시나리오 사이 카운터 리셋).
  void reset() {
    dispenseCalls.clear();
    aspirateCalls.clear();
    initializeCount = 0;
    _scriptByAddr.clear();
    defaultOutcome = FakeEngineOutcome.ack;
  }
}
