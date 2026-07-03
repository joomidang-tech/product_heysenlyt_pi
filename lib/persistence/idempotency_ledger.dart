/// 멱등 Ledger 인터페이스 — SoT §4-6 (pi측 dedup) / 부록A P-2.
///
/// pi측 dedup 키 = 합성키 `{orderId}:{attempt}`(= command.id). 재시도 시 attempt 증가로
/// **fresh 판정** → 재제조 성립. attempt 증가 없는 status-only 되돌림은 계약 위반(§4-4).
///
/// 이번 웨이브 = 인터페이스만. 실제 로컬 영속(SQLite/파일 WAL)은 이후 웨이브(TODO).
library;

/// Ledger 판정 결과 — SoT §4-6.
enum LedgerVerdict {
  /// 처음 본 합성키 — 제조 진행.
  fresh,

  /// 이미 처리된(또는 진행중인) 합성키 — DROP(재제조 no-op 방지 대상).
  duplicate,
}

/// 멱등 Ledger — 합성키 기준 at-most-once 제조 보장.
///
/// 구현체는 crash-safe(전원 단절·재부팅 후에도 판정 유지)를 목표로 한다(이후 웨이브).
abstract interface class IdempotencyLedger {
  /// 합성키 `{orderId}:{attempt}` 를 처음 보는가.
  ///
  /// fresh 면 예약(claim)까지 원자적으로 수행해 동시 중복 처리를 막는 것을 권장한다.
  Future<LedgerVerdict> checkAndClaim(String commandId);

  /// 제조 완료/실패 종결 기록(재부팅 후 재판정 안정화).
  Future<void> markSettled(String commandId, {required bool success});

  /// 진행중 여부(재부팅 복구·recover.decision 판단용).
  Future<bool> isSettled(String commandId);
}

/// 인메모리 Ledger — **테스트/스켈레톤 전용**(영속 아님).
///
/// ⚠️ 프로덕션 미사용 — 실 crash-safe 영속 어댑터가 이후 웨이브에서 대체한다.
class InMemoryIdempotencyLedger implements IdempotencyLedger {
  final Set<String> _claimed = <String>{};
  final Map<String, bool> _settled = <String, bool>{};

  @override
  Future<LedgerVerdict> checkAndClaim(String commandId) async {
    if (_claimed.contains(commandId)) return LedgerVerdict.duplicate;
    _claimed.add(commandId);
    return LedgerVerdict.fresh;
  }

  @override
  Future<void> markSettled(String commandId, {required bool success}) async {
    _settled[commandId] = success;
  }

  @override
  Future<bool> isSettled(String commandId) async => _settled.containsKey(commandId);
}
