/// 주문 상태 전이표 — 순수 도메인 (SoT §4).
///
/// **정본 = heysenlyt-web `lib/server/orderStatus.ts`.** 이 파일은 그 TS 전이표를
/// **바이트 동일** 포팅한다 — 한 셀이라도 다르면 이중토출·완료유실이 재발한다(SoT 부록A P-1).
///
/// firebase / http 를 모른다(순수 함수만) — 단위테스트가 하드웨어·네트워크 없이 통과.
///
/// ★F2 반영(SoT §4-2): PENDING→COMPLETED 직행 허용(완료 유실 방지). 전진(forward)은
///   모두 허용하고, 막는 것은 비가역 역행(un-complete)뿐이다.
library;

/// 와이어 상태 4종 — SoT §4-1. 정확히 이 4개 문자열 리터럴만 경계를 넘는다.
///
/// pi 내부 도메인은 `ERROR` 를 쓸 수 있으나 **경계에서 반드시 FAILED 로 coercion**(역방향 동일).
enum WireStatus {
  pending('PENDING'),
  processing('PROCESSING'),
  completed('COMPLETED'),
  failed('FAILED');

  const WireStatus(this.wire);

  /// 와이어 문자열(대문자 리터럴) — JSON 직렬화·전이표의 진실값.
  final String wire;

  /// 와이어 문자열 → enum. 4종 외 → null(라우트 입력 검증용, TS `isWireStatus` 대응).
  static WireStatus? fromWire(Object? v) {
    if (v is! String) return null;
    for (final s in WireStatus.values) {
      if (s.wire == v) return s;
    }
    return null;
  }
}

/// 알려진 WireStatus 문자열인지 검사(라우트 입력 검증용) — TS `isWireStatus` 대응.
bool isWireStatus(Object? v) => WireStatus.fromWire(v) != null;

/// 전이표 (ALLOWED) — SoT §4-2. TS `ALLOWED` 와 **바이트 동일**.
///
/// - PENDING    → {PROCESSING, COMPLETED, FAILED}  (COMPLETED 직행 허용·F2)
/// - PROCESSING → {COMPLETED, FAILED}
/// - COMPLETED  → {}                               (terminal — un-complete 금지)
/// - FAILED     → {PENDING}                        (운영자 재시도 유일경로)
const Map<WireStatus, Set<WireStatus>> _allowed = {
  WireStatus.pending: {WireStatus.processing, WireStatus.completed, WireStatus.failed},
  WireStatus.processing: {WireStatus.completed, WireStatus.failed},
  WireStatus.completed: <WireStatus>{},
  WireStatus.failed: {WireStatus.pending},
};

/// 전이 판정 결과 — TS `TransitionVerdict`.
enum TransitionVerdict { noop, apply, illegal }

/// from → to 전이 평가 — TS `evaluateTransition` 바이트 동일.
///
///   - from == to           → noop   (멱등 재적용)
///   - ALLOWED[from] ∋ to    → apply
///   - 그 외                 → illegal
TransitionVerdict evaluateTransition(WireStatus from, WireStatus to) {
  if (from == to) return TransitionVerdict.noop;
  return _allowed[from]!.contains(to) ? TransitionVerdict.apply : TransitionVerdict.illegal;
}

/// updateStatus 실패 신호 — 서버 라우트가 HTTP 상태로 매핑(illegal→422, conflict→409).
///
/// TS `StatusTransitionError` 대응. pi 는 status 전진 write 주체(§4-5)이므로 전이 판정을
/// 로컬에서 선검사해 불필요한 PATCH 를 줄이는 데 사용한다(서버 CAS 가 최종 게이트).
class StatusTransitionError implements Exception {
  StatusTransitionError(this.kind, this.current, this.attempted);

  /// 'illegal' | 'conflict'.
  final String kind;
  final WireStatus current;
  final WireStatus attempted;

  @override
  String toString() => kind == 'illegal'
      ? 'StatusTransitionError: illegal transition ${current.wire} -> ${attempted.wire}'
      : 'StatusTransitionError: expectedFrom mismatch (current=${current.wire})';
}

/// pi 내부 제조 phase — SoT §9-2 status.phase 4종. 단조·역행 금지.
enum DispensePhase {
  accepted('ACCEPTED'),
  progress('PROGRESS'),
  completed('COMPLETED'),
  failed('FAILED');

  const DispensePhase(this.wire);
  final String wire;

  static DispensePhase? fromWire(Object? v) {
    if (v is! String) return null;
    for (final p in DispensePhase.values) {
      if (p.wire == v) return p;
    }
    return null;
  }
}

/// pi phase → WireStatus 매핑 — SoT §4-5 / §9-2 (단조·역행 금지).
///
///   ACCEPTED / PROGRESS → PROCESSING
///   COMPLETED           → COMPLETED
///   FAILED              → FAILED
WireStatus phaseToWireStatus(DispensePhase phase) {
  switch (phase) {
    case DispensePhase.accepted:
    case DispensePhase.progress:
      return WireStatus.processing;
    case DispensePhase.completed:
      return WireStatus.completed;
    case DispensePhase.failed:
      return WireStatus.failed;
  }
}
