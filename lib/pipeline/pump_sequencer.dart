/// Pump Sequencer — steps 직렬 토출 + 진행보고 + 안전정지 + 동시1제조 + graceful — SoT §4-5 / §9-2.
///
/// 책임(질의서 PS-*·SR-*·CR-*):
///   - steps **직렬** 토출(idx 오름차순) — ResolvedRecipe.steps 순서대로 EngineExecutor.runStep.
///   - 각 스텝 후 **진행보고**(PROGRESS stepK/N) via StatusReporter.
///   - 중간 **영구오류 안전정지**(PS): permanent 발생 시 즉시 중단 → PARTIAL FAILED(stepK/N·ENGINE_ERROR_PERMANENT).
///   - transient 소진 실패도 중단 → PARTIAL FAILED(ENGINE_ERROR_TRANSIENT/TIMEOUT).
///   - **동시 1제조 큐잉**: 한 번에 하나만 제조. 진행 중 새 명령은 큐(대기)로(레이스·이중전진 방지).
///   - **graceful(SIGTERM)**: 요청 시 **현재 step 완주·다음 step 미시작**(PS-06) → 남으면 PARTIAL FAILED.
///   - **무응답 silent-success 0**(EP-03): EngineExecutor 가 empty=실패로 처리하므로 0step 성공 불가.
///
/// 멱등 통합: run 진입 전 Ledger.checkAndClaim(IL-02). RUNNING 마킹 후 토출, 종결 시 markSettled.
library;

import 'dart:async';
import 'dart:collection';

import '../core/order_status.dart';
import '../core/pump_guard.dart';
import '../core/wire_messages.dart' show RecipeStep;
import '../persistence/file_idempotency_ledger.dart';
import '../persistence/idempotency_ledger.dart';
import '../ports/engine_port.dart';
import 'engine_executor.dart';
import 'recipe_resolver.dart';
import 'status_reporter.dart';

/// 한 제조 job 의 최종 결과.
enum JobOutcome {
  /// 전 스텝 성공 완주 → COMPLETED.
  completed,

  /// 중간 실패(permanent/transient 소진) → PARTIAL FAILED.
  partialFailed,

  /// 검증 실패(빈/음수/상한/미매핑) → CMD_VALIDATION_FAILED drop(토출 0).
  validationFailed,

  /// 멱등 DROP(이미 본 합성키) → no-op(토출 0·IL-02).
  duplicateDropped,

  /// graceful 종료로 남은 스텝 미시작 → PARTIAL FAILED(INTERRUPTED 아님·정상 정지).
  gracefulPartial,
}

/// 제조 실행 리포트(관찰·테스트 판정).
class JobReport {
  const JobReport({
    required this.commandId,
    required this.outcome,
    required this.stepsDone,
    required this.stepN,
    this.errorCode,
  });

  final String commandId;
  final JobOutcome outcome;

  /// 완주한 스텝 수(stepK).
  final int stepsDone;

  /// 총 스텝 수(stepN). 검증/멱등 실패 시 0 가능.
  final int stepN;
  final StatusErrorCode? errorCode;

  bool get isSuccess => outcome == JobOutcome.completed;
}

/// StatusReport 를 sink 로 흘리는 콜백(제조를 막지 않게 best-effort — OQ 로 흡수).
typedef ProgressPublisher = FutureOr<void> Function(DispensePhase phase, int stepK, int stepN,
    StatusErrorCode? errorCode, String commandId, String traceId);

/// Pump Sequencer — 동시 1제조 큐잉 오케스트레이터.
class PumpSequencer {
  PumpSequencer({
    required this.ledger,
    required EnginePort engine,
    required this.resolver,
    required this.requestIdGen,
    this.publisher,
    int maxRetries = 3,
    String Function()? nowIso,
  })  : _executor = EngineExecutor(engine, maxRetries: maxRetries),
        _nowIso = nowIso ?? (() => DateTime.now().toUtc().toIso8601String());

  final FileIdempotencyLedger ledger;
  final RecipeResolver resolver;
  final EngineExecutor _executor;
  final RequestIdGen requestIdGen;
  final ProgressPublisher? publisher;
  final String Function() _nowIso;

  /// 동시 1제조 강제 — 진행 중이면 새 job 은 큐 대기(FIFO).
  bool _busy = false;
  final Queue<_PendingJob> _pending = Queue<_PendingJob>();

  /// graceful 종료 플래그 — set 후 현재 step 완주·다음 step 미시작.
  bool _draining = false;

  /// 현재 진행 중인지(관찰).
  bool get isBusy => _busy;

  /// 대기 큐 깊이(heartbeat queueDepth 파생).
  int get queueDepth => _pending.length + (_busy ? 1 : 0);

  /// graceful 종료 요청(SIGTERM). 현재 step 은 완주, 이후 미시작. 대기 job 은 실행하지 않음.
  void requestDrain() {
    _draining = true;
  }

  /// 제조 요청. 동시 1제조 — 진행 중이면 큐에 넣고 순차 실행.
  ///
  /// 반환 = 이 job 의 완료 리포트(큐 대기 시 자기 차례가 와서 완료될 때 resolve).
  Future<JobReport> submit({
    required String commandId,
    required String traceId,
    required List<RecipeStep> steps,
  }) {
    final completer = Completer<JobReport>();
    _pending.addLast(_PendingJob(
      commandId: commandId,
      traceId: traceId,
      steps: steps,
      completer: completer,
    ));
    _drainPending();
    return completer.future;
  }

  void _drainPending() {
    if (_busy) return;
    if (_pending.isEmpty) return;
    final job = _pending.removeFirst();
    _busy = true;
    // graceful 종료 중이면 대기 job 은 실행하지 않고 gracefulPartial 로 종결.
    if (_draining) {
      _busy = false;
      job.completer.complete(JobReport(
        commandId: job.commandId,
        outcome: JobOutcome.gracefulPartial,
        stepsDone: 0,
        stepN: 0,
        errorCode: StatusErrorCode.partialDispense,
      ));
      _drainPending();
      return;
    }
    _runJob(job).then((report) {
      _busy = false;
      job.completer.complete(report);
      _drainPending();
    }).catchError((Object e, StackTrace st) {
      _busy = false;
      job.completer.completeError(e, st);
      _drainPending();
    });
  }

  Future<JobReport> _runJob(_PendingJob job) async {
    // ── IL-02: 멱등 게이트 — 이미 본 합성키(4상태 전부)면 DROP(토출 0). ──
    final verdict = await ledger.checkAndClaim(job.commandId);
    if (verdict == LedgerVerdict.duplicate) {
      await _publish(DispensePhase.failed, 0, 0, StatusErrorCode.duplicateDropped,
          job.commandId, job.traceId);
      return JobReport(
        commandId: job.commandId,
        outcome: JobOutcome.duplicateDropped,
        stepsDone: 0,
        stepN: 0,
        errorCode: StatusErrorCode.duplicateDropped,
      );
    }

    // ── 검증(RR): 빈/음수/상한/미매핑 → CMD_VALIDATION_FAILED drop(토출 0). ──
    final ResolvedRecipe resolved;
    try {
      resolved = resolver.resolve(job.steps);
    } on RecipeValidationError catch (e) {
      await ledger.markSettled(job.commandId, success: false);
      await _publish(DispensePhase.failed, 0, 0, e.errorCode, job.commandId, job.traceId);
      return JobReport(
        commandId: job.commandId,
        outcome: JobOutcome.validationFailed,
        stepsDone: 0,
        stepN: 0,
        errorCode: e.errorCode,
      );
    }

    final stepN = resolved.stepN;

    // RUNNING 마킹(재부팅 시 INTERRUPTED 판정 근거·CR-01).
    await ledger.markRunning(job.commandId);

    final reporter = StatusReporter(
      commandId: job.commandId,
      traceId: job.traceId,
      requestIdGen: requestIdGen,
      nowIso: _nowIso,
    );
    // ACCEPTED 보고(제조 시작).
    await _publishVia(reporter, DispensePhase.accepted, 0, stepN, null);

    int stepsDone = 0;
    for (final step in resolved.steps) {
      // ── graceful: 다음 step 미시작(현재까지 완주분으로 PARTIAL). ──
      if (_draining) {
        await ledger.markSettled(job.commandId, success: false);
        await _publishVia(reporter, DispensePhase.failed, stepsDone, stepN,
            StatusErrorCode.partialDispense);
        return JobReport(
          commandId: job.commandId,
          outcome: JobOutcome.gracefulPartial,
          stepsDone: stepsDone,
          stepN: stepN,
          errorCode: StatusErrorCode.partialDispense,
        );
      }

      final cmd = EngineDispenseCommand(
        pumpAddr: step.pumpAddr,
        volumeUl: step.volumeUl,
        steps: step.steps,
        spec: step.spec,
      );
      final res = await _executor.runStep(cmd);

      if (!res.isSuccess) {
        // ── 중간 실패 안전정지(PS): permanent 즉시중단 / transient 소진 → PARTIAL FAILED. ──
        await ledger.markSettled(job.commandId, success: false);
        await _publishVia(reporter, DispensePhase.failed, stepsDone, stepN,
            res.errorCode ?? StatusErrorCode.partialDispense);
        return JobReport(
          commandId: job.commandId,
          outcome: JobOutcome.partialFailed,
          stepsDone: stepsDone,
          stepN: stepN,
          errorCode: res.errorCode,
        );
      }

      stepsDone++;
      // 진행보고(PROGRESS stepK/N). 종결 아니므로 phase=progress.
      if (stepsDone < stepN) {
        await _publishVia(reporter, DispensePhase.progress, stepsDone, stepN, null);
      }
    }

    // 전 스텝 성공 완주 → COMPLETED.
    await ledger.markSettled(job.commandId, success: true);
    await _publishVia(reporter, DispensePhase.completed, stepsDone, stepN, null);
    return JobReport(
      commandId: job.commandId,
      outcome: JobOutcome.completed,
      stepsDone: stepsDone,
      stepN: stepN,
    );
  }

  Future<void> _publishVia(StatusReporter reporter, DispensePhase phase, int stepK, int stepN,
      StatusErrorCode? errorCode) async {
    // reporter 로 단조성 강제(역행 시 throw) — 조립만; 실제 전송은 publisher(OQ/best-effort).
    reporter.report(phase: phase, stepK: stepK, stepN: stepN, errorCode: errorCode);
    await _publish(phase, stepK, stepN, errorCode, reporter.commandId, reporter.traceId);
  }

  Future<void> _publish(DispensePhase phase, int stepK, int stepN, StatusErrorCode? errorCode,
      String commandId, String traceId) async {
    final p = publisher;
    if (p == null) return;
    // best-effort — 관측이 제조를 막지 않는다(§10-6). 예외는 삼킨다(OQ 가 흡수).
    try {
      await p(phase, stepK, stepN, errorCode, commandId, traceId);
    } catch (_) {
      // swallow — OQ/재전송 책임.
    }
  }
}

class _PendingJob {
  _PendingJob({
    required this.commandId,
    required this.traceId,
    required this.steps,
    required this.completer,
  });

  final String commandId;
  final String traceId;
  final List<RecipeStep> steps;
  final Completer<JobReport> completer;
}
