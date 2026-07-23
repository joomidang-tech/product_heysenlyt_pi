/// PumpSequencer 테스트 — SoT §4-5 / §9-2 / 질의서 PS-*·IL-02.
///
/// **PASS 게이트 IL-02(중복토출0)** = dispense 카운터로 객관 검증.
/// 직렬 토출·진행보고·중간 영구오류 안전정지(PARTIAL)·동시1제조 큐잉·graceful.
library;

import 'dart:io';

import 'package:heysenlyt_pi/heysenlyt_pi.dart';
import 'package:test/test.dart';

import 'support/fake_engine_port.dart';

void main() {
  late Directory tmp;
  late FileIdempotencyLedger ledger;
  late FakeEnginePort fake;
  late RecipeResolver resolver;
  final spec = SyringeSpec(pumpFullStroke: 12000, syringeCapacityMl: 1.25);

  int reqSeq = 0;

  setUp(() async {
    tmp = await Directory.systemTemp.createTemp('seq_test_');
    ledger = await FileIdempotencyLedger.open('${tmp.path}/l.log');
    fake = FakeEnginePort()..scriptAll(FakeEngineOutcome.ack);
    resolver = RecipeResolver({1: spec, 2: spec, 3: spec});
    reqSeq = 0;
  });

  tearDown(() async {
    await ledger.close();
    if (await tmp.exists()) await tmp.delete(recursive: true);
  });

  PumpSequencer seq({ProgressPublisher? publisher, int maxRetries = 3}) => PumpSequencer(
        ledger: ledger,
        engine: fake,
        resolver: resolver,
        requestIdGen: () => 'req-${reqSeq++}',
        publisher: publisher,
        maxRetries: maxRetries,
        nowIso: () => '2026-07-03T00:00:00.000Z',
      );

  RecipeStep step(int idx, int addr, num vol) =>
      RecipeStep(idx: idx, pumpAddr: addr, flavor: 'f', volume: vol);

  test('직렬 토출 성공 → COMPLETED, dispense = stepN', () async {
    final r = await seq().submit(
      commandId: 'o:1',
      traceId: 't',
      steps: [step(0, 1, 100), step(1, 2, 100), step(2, 3, 100)],
    );
    expect(r.outcome, JobOutcome.completed);
    expect(r.stepsDone, 3);
    expect(fake.dispenseCount, 3);
  });

  test('IL-02 게이트: 중복 command.id → DROP, dispense 0 (재토출 없음)', () async {
    final s = seq();
    await s.submit(commandId: 'o:1', traceId: 't', steps: [step(0, 1, 100)]);
    expect(fake.dispenseCount, 1);
    // 동일 합성키 재제출 — DROP.
    final dup = await s.submit(commandId: 'o:1', traceId: 't', steps: [step(0, 1, 100)]);
    expect(dup.outcome, JobOutcome.duplicateDropped);
    expect(dup.errorCode, StatusErrorCode.duplicateDropped);
    expect(fake.dispenseCount, 1, reason: '중복은 추가 토출 0(IL-02)');
  });

  test('IL-02: 실패한 command.id 재제출도 DROP (Q1·FAILED 포함)', () async {
    fake.scriptAll(FakeEngineOutcome.permanent);
    final s = seq();
    final first = await s.submit(commandId: 'o:9', traceId: 't', steps: [step(0, 1, 100)]);
    expect(first.outcome, JobOutcome.partialFailed);
    expect(fake.dispenseCount, 1);
    // 실패했어도 같은 합성키 재제출은 DROP(재토출 없음).
    final dup = await s.submit(commandId: 'o:9', traceId: 't', steps: [step(0, 1, 100)]);
    expect(dup.outcome, JobOutcome.duplicateDropped);
    expect(fake.dispenseCount, 1, reason: 'FAILED 도 DROP 집합(Q1)');
  });

  test('중간 영구오류 안전정지 → PARTIAL FAILED(stepK/N)', () async {
    // step0 ok, step1 permanent → step2 미시작.
    fake.scriptFor(1, [FakeEngineOutcome.ack]);
    fake.scriptFor(2, [FakeEngineOutcome.permanent]);
    fake.defaultOutcome = FakeEngineOutcome.ack;
    final r = await seq().submit(
      commandId: 'o:1',
      traceId: 't',
      steps: [step(0, 1, 100), step(1, 2, 100), step(2, 3, 100)],
    );
    expect(r.outcome, JobOutcome.partialFailed);
    expect(r.stepsDone, 1); // step0 만 완주.
    expect(r.stepN, 3);
    expect(r.errorCode, StatusErrorCode.engineErrorPermanent);
    // step0(dispense 1) + step1(permanent, dispense 1) = 2. step2 미시작.
    expect(fake.dispenseCountFor(3), 0, reason: 'step2 미시작(안전정지)');
    expect(fake.dispenseCount, 2);
  });

  test('빈 레시피 → CMD_VALIDATION_FAILED, dispense 0', () async {
    final r = await seq().submit(commandId: 'o:1', traceId: 't', steps: []);
    expect(r.outcome, JobOutcome.validationFailed);
    expect(r.errorCode, StatusErrorCode.cmdValidationFailed);
    expect(fake.dispenseCount, 0);
  });

  test('무응답 silent-success 0 — empty steps → PARTIAL FAILED, COMPLETED 아님', () async {
    fake.scriptAll(FakeEngineOutcome.empty);
    final r = await seq(maxRetries: 1).submit(
      commandId: 'o:1',
      traceId: 't',
      steps: [step(0, 1, 100)],
    );
    expect(r.outcome, JobOutcome.partialFailed);
    expect(r.outcome, isNot(JobOutcome.completed), reason: 'silent-success 금지(EP-03)');
    expect(r.stepsDone, 0);
  });

  test('진행보고 phase 시퀀스 — ACCEPTED, PROGRESS×(N-1), COMPLETED', () async {
    final phases = <String>[];
    final r = await seq(publisher: (phase, k, n, ec, cid, tid) {
      phases.add(phase.wire);
    }).submit(
      commandId: 'o:1',
      traceId: 't',
      steps: [step(0, 1, 100), step(1, 2, 100)],
    );
    expect(r.outcome, JobOutcome.completed);
    expect(phases, ['ACCEPTED', 'PROGRESS', 'COMPLETED']);
  });

  test('동시 1제조 큐잉 — 두 job 순차 실행(직렬)', () async {
    final s = seq();
    final f1 = s.submit(commandId: 'o:1', traceId: 't', steps: [step(0, 1, 100)]);
    final f2 = s.submit(commandId: 'o:2', traceId: 't', steps: [step(0, 1, 100)]);
    final r1 = await f1;
    final r2 = await f2;
    expect(r1.outcome, JobOutcome.completed);
    expect(r2.outcome, JobOutcome.completed);
    expect(fake.dispenseCount, 2);
    expect(s.isBusy, isFalse);
    expect(s.queueDepth, 0);
  });

  test('graceful(SIGTERM) — 현재 step 완주·다음 미시작 → gracefulPartial', () async {
    // publisher 안에서 첫 step 후 drain 요청 → 다음 step 미시작.
    late PumpSequencer s;
    s = seq(publisher: (phase, k, n, ec, cid, tid) {
      if (phase == DispensePhase.progress && k == 1) {
        s.requestDrain();
      }
    });
    final r = await s.submit(
      commandId: 'o:1',
      traceId: 't',
      steps: [step(0, 1, 100), step(1, 2, 100), step(2, 3, 100)],
    );
    expect(r.outcome, JobOutcome.gracefulPartial);
    expect(r.stepsDone, 1, reason: 'step0 완주 후 drain → step1 미시작');
    expect(fake.dispenseCount, 1);
  });

  test('graceful 중 대기 job 은 미실행(gracefulPartial)', () async {
    final s = seq();
    s.requestDrain();
    final r = await s.submit(commandId: 'o:1', traceId: 't', steps: [step(0, 1, 100)]);
    expect(r.outcome, JobOutcome.gracefulPartial);
    expect(fake.dispenseCount, 0);
  });

  test('상한초과 volume → CMD_VALIDATION_FAILED, dispense 0 (Code 11 방지)', () async {
    final r = await seq().submit(
      commandId: 'o:1',
      traceId: 't',
      steps: [step(0, 1, 5000)], // maxVolumeUl=1250 초과.
    );
    expect(r.outcome, JobOutcome.validationFailed);
    expect(fake.dispenseCount, 0);
  });
}
