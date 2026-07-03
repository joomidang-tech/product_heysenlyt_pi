/// Boot Recovery (on_boot) — SoT §9-1 / 질의서 Q4(CR-01·CR-02) / §6-7(INTERRUPTED).
///
/// 재부팅 시 Ledger replay 로 복원된 각 합성키 상태별 결정:
///   - **RUNNING → INTERRUPTED**(CR-01): 전원 단절로 중단된 제조. **자동 재실행 절대 금지**
///     (dispense 0). phase=FAILED, errorCode=INTERRUPTED 로 보고만(§6-7·Q7). 재시도는 운영자 축
///     (FAILED→PENDING·attempt++)으로만 — pi 가 임의로 재토출하면 이중토출(IL-02 위반).
///   - **RECEIVED → 클리어 후 fresh**(CR-02): 수신했으나 제조 미시작(dispense 전) → 안전하게
///     클리어하고 새 명령처럼 fresh 처리 가능(아직 물리 토출 없음).
///   - **DONE → 무동작**: 이미 완료 종결. 재보고·재토출 없음.
///   - **FAILED → 무동작**: 이미 실패 종결(멱등 DROP 집합). 재실행 없음(재주문은 새 attempt).
///
/// **CR-01 게이트(재기동 자동재실행 금지)**: 이 모듈은 dispense 를 **호출하지 않는다**(엔진 미주입).
///   결정(RecoveryDecision)만 산출하고, INTERRUPTED 보고는 StatusReporter/Sink 가 담당.
library;

import '../persistence/file_idempotency_ledger.dart';

/// 재부팅 복구 액션.
enum RecoveryAction {
  /// RUNNING → INTERRUPTED 보고(자동 재실행 금지·dispense0).
  reportInterrupted,

  /// RECEIVED → 클리어 후 fresh 재처리 허용(물리 토출 전).
  clearAndFresh,

  /// DONE/FAILED → 무동작.
  none,
}

/// 합성키별 복구 결정.
class RecoveryDecision {
  const RecoveryDecision({required this.commandId, required this.action, required this.fromState});

  final String commandId;
  final RecoveryAction action;
  final LedgerEntryState fromState;

  @override
  String toString() => 'RecoveryDecision($commandId: ${fromState.wire} -> $action)';
}

/// Boot Recovery 플래너 — Ledger 상태를 스캔해 결정 목록만 산출(부작용 0).
///
/// **엔진 미주입** — 이 클래스는 어떤 상황에서도 물리 토출을 시작하지 않는다(CR-01 구조적 보장).
class BootRecovery {
  const BootRecovery(this.ledger);

  final FileIdempotencyLedger ledger;

  /// 재부팅 복구 결정 목록.
  ///
  /// RUNNING(진행중 중단) → reportInterrupted. RECEIVED(미시작) → clearAndFresh.
  /// DONE/FAILED → none. 순서: RUNNING 우선(안전 보고), 그 다음 RECEIVED.
  List<RecoveryDecision> plan() {
    final decisions = <RecoveryDecision>[];

    for (final cid in ledger.runningCommands()) {
      decisions.add(RecoveryDecision(
        commandId: cid,
        action: RecoveryAction.reportInterrupted,
        fromState: LedgerEntryState.running,
      ));
    }
    for (final cid in ledger.receivedCommands()) {
      decisions.add(RecoveryDecision(
        commandId: cid,
        action: RecoveryAction.clearAndFresh,
        fromState: LedgerEntryState.received,
      ));
    }
    // DONE/FAILED 는 결정 목록에 넣지 않음(none = 무동작). 명시적으로 원하면 아래 주석 참조.
    return decisions;
  }
}
