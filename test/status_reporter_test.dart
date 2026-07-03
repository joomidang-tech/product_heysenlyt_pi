/// StatusReporter 테스트 — SoT §9-2 / §4-5.
///
/// phase 단조·역행거부·멱등판별·errorCode 표준 7종·PII 미포함.
library;

import 'package:heysenlyt_pi/heysenlyt_pi.dart';
import 'package:test/test.dart';

void main() {
  int seq = 0;
  StatusReporter make() {
    seq = 0;
    return StatusReporter(
      commandId: 'o:1',
      traceId: 'trace-uuid',
      requestIdGen: () => 'req-${seq++}',
      nowIso: () => '2026-07-03T00:00:00.000Z',
    );
  }

  test('정상 단조 진행 ACCEPTED→PROGRESS→COMPLETED', () {
    final r = make();
    final a = r.report(phase: DispensePhase.accepted, stepK: 0, stepN: 2);
    expect(a.phase, 'ACCEPTED');
    expect(a.id, 'o:1');
    expect(a.traceId, 'trace-uuid');
    expect(r.report(phase: DispensePhase.progress, stepK: 1, stepN: 2).phase, 'PROGRESS');
    expect(r.report(phase: DispensePhase.completed, stepK: 2, stepN: 2).phase, 'COMPLETED');
  });

  test('역행 거부 — PROGRESS 후 ACCEPTED 는 PhaseRegressionError', () {
    final r = make();
    r.report(phase: DispensePhase.accepted, stepK: 0, stepN: 2);
    r.report(phase: DispensePhase.progress, stepK: 1, stepN: 2);
    expect(() => r.report(phase: DispensePhase.accepted, stepK: 0, stepN: 2),
        throwsA(isA<PhaseRegressionError>()));
  });

  test('종결 후 추가 보고 거부(멱등·단조)', () {
    final r = make();
    r.report(phase: DispensePhase.accepted, stepK: 0, stepN: 1);
    r.report(phase: DispensePhase.completed, stepK: 1, stepN: 1);
    expect(() => r.report(phase: DispensePhase.progress, stepK: 1, stepN: 1),
        throwsA(isA<PhaseRegressionError>()));
    expect(() => r.report(phase: DispensePhase.completed, stepK: 1, stepN: 1),
        throwsA(isA<PhaseRegressionError>()));
  });

  test('errorCode 표준 7종 방출(FAILED)', () {
    final r = make();
    r.report(phase: DispensePhase.accepted, stepK: 0, stepN: 2);
    final f = r.report(
        phase: DispensePhase.failed,
        stepK: 1,
        stepN: 2,
        errorCode: StatusErrorCode.partialDispense);
    expect(f.errorCode, StatusErrorCode.partialDispense);
    expect(f.toJson()['errorCode'], 'PARTIAL_DISPENSE');
  });

  test('PII 미포함 — StatusReport JSON 에 uid/userName/연락처 키 없음', () {
    final r = make();
    final json = r.report(phase: DispensePhase.accepted, stepK: 0, stepN: 1).toJson();
    for (final banned in ['uid', 'userName', 'phone', 'email', 'ip', 'sessionId']) {
      expect(json.containsKey(banned), isFalse, reason: '$banned 누출 금지');
    }
    // 허용 키만.
    expect(json.keys.toSet(),
        {'id', 'phase', 'stepK', 'stepN', 'errorCode', 'requestId', 'traceId', 'updatedAt'});
  });

  test('멱등 판별 — 동일 (phase, stepK) 재보고 wouldBeDuplicate', () {
    final r = make();
    r.report(phase: DispensePhase.progress, stepK: 1, stepN: 3);
    expect(r.wouldBeDuplicate(DispensePhase.progress, 1), isTrue);
    expect(r.wouldBeDuplicate(DispensePhase.progress, 2), isFalse);
  });

  test('requestId 매 보고 새로 발급(재사용 금지·O-3)', () {
    final r = make();
    final a = r.report(phase: DispensePhase.accepted, stepK: 0, stepN: 2);
    final b = r.report(phase: DispensePhase.progress, stepK: 1, stepN: 2);
    expect(a.requestId, isNot(b.requestId));
  });
}
