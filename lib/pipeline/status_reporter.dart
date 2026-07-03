/// Status Reporter — phase 단조·역행거부·멱등·errorCode 표준·PII 미포함 — SoT §9-2 / §4-5.
///
/// 책임:
///   - phase **단조** 진행 강제(ACCEPTED → PROGRESS → COMPLETED|FAILED). 역행 보고 **거부**(SR).
///   - **멱등**: 동일 (id, phase, stepK) 재보고는 새 requestId 를 만들지 않고 마지막 것 재사용
///     하지 않는다 — 대신 서버 dedup(§4-6)에 위임하되, pi 는 동일 진행보고를 중복 생성하지 않는다.
///   - errorCode 표준 7종(§6-7)만 방출. PII 미포함(StatusReport 는 orderId·진행도·상관메타만).
///   - phase→WireStatus 매핑은 order_status.dart(단조·역행 금지).
///
/// **정본 = pi(status 전진 write 주체·§4-5)**. 이 reporter 는 StatusReport 를 조립만 하고
/// 실제 전송(PATCH·OQ flush)은 StatusSinkPort(어댑터) 책임 — 관측이 제조를 막지 않도록 분리.
library;

import '../core/order_status.dart';
import '../core/pump_guard.dart' show StatusErrorCode;
import '../core/wire_messages.dart' show StatusReport;

/// phase 순서 등급(단조성 판정용). ACCEPTED < PROGRESS < 종결(COMPLETED|FAILED).
///
/// 종결 두 값은 동급(둘 다 최종) — 서로 다른 종결로의 전이는 없다(한 주문 1종결).
int _phaseRank(DispensePhase p) {
  switch (p) {
    case DispensePhase.accepted:
      return 0;
    case DispensePhase.progress:
      return 1;
    case DispensePhase.completed:
    case DispensePhase.failed:
      return 2;
  }
}

/// phase 역행 시도 예외(SR — 단조 위반).
class PhaseRegressionError implements Exception {
  PhaseRegressionError(this.last, this.attempted);

  final DispensePhase last;
  final DispensePhase attempted;

  @override
  String toString() =>
      'PhaseRegressionError: ${last.wire} -> ${attempted.wire} (단조 위반·역행 금지)';
}

/// requestId 발급기(테스트 결정성 주입). 기본 = UUID v4 유사(랜덤).
typedef RequestIdGen = String Function();

/// 주문(합성키 id) 단위 Status Reporter.
///
/// 인스턴스는 한 command.id(제조 1건)의 진행 상태를 추적하며, 단조성·멱등을 로컬 강제한다.
class StatusReporter {
  StatusReporter({
    required this.commandId,
    required this.traceId,
    required RequestIdGen requestIdGen,
    String Function()? nowIso,
  })  : _requestIdGen = requestIdGen,
        _nowIso = nowIso ?? (() => DateTime.now().toUtc().toIso8601String());

  /// `{orderId}:{attempt}` — StatusReport.id.
  final String commandId;
  final String traceId;

  final RequestIdGen _requestIdGen;
  final String Function() _nowIso;

  DispensePhase? _lastPhase;
  int _lastStepK = -1;

  /// 마지막으로 보고한 phase(관찰).
  DispensePhase? get lastPhase => _lastPhase;

  /// 진행 보고를 조립한다. 단조 위반(역행)은 [PhaseRegressionError] throw.
  ///
  /// 멱등: 동일 (phase, stepK) 재보고는 [reportProgress] 가 아니라 호출측이 중복 억제하도록
  ///   [wouldBeDuplicate] 로 판별할 수 있게 한다. 여기서는 계약상 유효 보고만 조립한다.
  StatusReport report({
    required DispensePhase phase,
    required int stepK,
    required int stepN,
    StatusErrorCode? errorCode,
  }) {
    final last = _lastPhase;
    if (last != null && _phaseRank(phase) < _phaseRank(last)) {
      throw PhaseRegressionError(last, phase);
    }
    // 종결(COMPLETED|FAILED) 이후 추가 보고 금지(멱등·단조).
    if (last != null && (last == DispensePhase.completed || last == DispensePhase.failed)) {
      throw PhaseRegressionError(last, phase);
    }

    _lastPhase = phase;
    _lastStepK = stepK;

    return StatusReport(
      id: commandId,
      phase: phase.wire,
      stepK: stepK,
      stepN: stepN,
      // errorCode 는 표준 7종(§6-7)만 — StatusErrorCode enum 이 그 집합을 강제.
      errorCode: errorCode,
      requestId: _requestIdGen(),
      traceId: traceId,
      updatedAt: _nowIso(),
    );
  }

  /// 동일 (phase, stepK) 재보고 여부(멱등 억제 판별용).
  bool wouldBeDuplicate(DispensePhase phase, int stepK) =>
      _lastPhase == phase && _lastStepK == stepK;

  /// phase → WireStatus(§4-5) — 서버 CAS 가 최종 게이트지만 pi 로컬 선검사에도 사용.
  WireStatus wireStatusFor(DispensePhase phase) => phaseToWireStatus(phase);
}
