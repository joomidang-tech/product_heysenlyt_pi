/// 적대적 게이트 독립검증(G≠E) — IL-02 / CR-01 / EP-03 의 카운터 급소를 직접 공격.
///
/// 기존 테스트가 단일스텝·낙관경로 위주라 아래 급소를 추가 커버:
///   - IL-02: **멀티스텝** 중복 dispatch → dispense 카운터 = stepN 정확히 1회분(2배 아님).
///   - IL-02: **FAILED 후 동일 commandId 재제출** → DROP(추가 토출 0). attempt++ 만이 재제조 경로.
///   - CR-01: **RUNNING crash → 재기동 → 동일 commandId 재제출**(전체 파이프라인) → dispense 0.
///   - EP-03: **중간 스텝 empty** / **rawCode!=0 & detail==''** → silent-success 0, PARTIAL FAILED.
library;

import 'dart:io';

import 'package:heysenlyt_pi/heysenlyt_pi.dart';
import 'package:test/test.dart';

import 'support/fake_engine_port.dart';

void main() {
  late Directory tmp;
  late FileIdempotencyLedger ledger;
  late FakeEnginePort fake;
  int reqSeq = 0;

  final spec = SyringeSpec(pumpFullStroke: 12000, syringeCapacityMl: 1.25);

  setUp(() async {
    tmp = await Directory.systemTemp.createTemp('adv_gate_');
    ledger = await FileIdempotencyLedger.open('${tmp.path}/l.log');
    fake = FakeEnginePort()..scriptAll(FakeEngineOutcome.ack);
    reqSeq = 0;
  });
  tearDown(() async {
    try {
      await ledger.close();
    } on FileSystemException {
      // 일부 테스트가 ledger 를 수동 close/재open 하므로 이중 close 는 무시.
    }
    if (await tmp.exists()) await tmp.delete(recursive: true);
  });

  PumpSequencer buildSeq({FakeEnginePort? engine, FileIdempotencyLedger? l}) {
    final resolver = RecipeResolver({1: spec, 2: spec, 3: spec});
    return PumpSequencer(
      ledger: l ?? ledger,
      engine: engine ?? fake,
      resolver: resolver,
      requestIdGen: () => 'req-${reqSeq++}',
      nowIso: () => '2026-07-03T00:00:00.000Z',
    );
  }

  RecipeStep step(int idx, int addr, num vol) =>
      RecipeStep(idx: idx, pumpAddr: addr, flavor: 'f', volume: vol);

  // ── IL-02: 멀티스텝 중복 → dispense = stepN 정확히 1회분 ──
  test('IL-02 멀티스텝 중복 dispatch — dispense 카운터 = 3(정확히 1회분·2배 아님)', () async {
    final seq = buildSeq();
    final steps = [step(0, 1, 100), step(1, 2, 100), step(2, 3, 100)];

    final r1 = await seq.submit(commandId: 'o:1', traceId: 't', steps: steps);
    expect(r1.outcome, JobOutcome.completed);
    expect(fake.dispenseCount, 3, reason: '3스텝 1회 제조 = 3 dispense');

    // 동일 합성키 재제출(중복).
    final r2 = await seq.submit(commandId: 'o:1', traceId: 't', steps: steps);
    expect(r2.outcome, JobOutcome.duplicateDropped);
    expect(fake.dispenseCount, 3,
        reason: '중복 → 추가 토출 0. 총 dispense 정확히 3(6 아님)');
  });

  // ── IL-02: FAILED 후 동일 commandId 재제출도 DROP ──
  test('IL-02 FAILED 종결 후 동일 commandId 재제출 → DROP(추가 토출 0)', () async {
    // 첫 스텝 permanent → PARTIAL FAILED(dispense 1회 발생).
    fake.scriptFor(1, [FakeEngineOutcome.permanent]);
    final seq = buildSeq();
    final steps = [step(0, 1, 100), step(1, 2, 100)];

    final r1 = await seq.submit(commandId: 'o:1', traceId: 't', steps: steps);
    expect(r1.outcome, JobOutcome.partialFailed);
    final afterFail = fake.dispenseCount; // permanent 스텝 1회.

    // 동일 합성키 재제출 — FAILED 도 DROP 집합.
    final r2 = await seq.submit(commandId: 'o:1', traceId: 't', steps: steps);
    expect(r2.outcome, JobOutcome.duplicateDropped);
    expect(fake.dispenseCount, afterFail,
        reason: 'FAILED 합성키 재제출 = 추가 토출 0(재제조는 attempt++ 만)');
  });

  // ── IL-02: attempt++ 새 합성키만 fresh(재제조 성립) ──
  test('IL-02 attempt++ 새 합성키(o:2)는 fresh — 재제조 성립', () async {
    fake.scriptFor(1, [FakeEngineOutcome.permanent]);
    final seq = buildSeq();
    final steps = [step(0, 1, 100)];

    final r1 = await seq.submit(commandId: 'o:1', traceId: 't', steps: steps);
    expect(r1.outcome, JobOutcome.partialFailed);
    final afterFail = fake.dispenseCount;

    // 새 attempt = 새 합성키 → fresh(재제조). 이번엔 ack.
    fake.scriptFor(1, [FakeEngineOutcome.ack]);
    final r2 = await seq.submit(commandId: 'o:2', traceId: 't', steps: steps);
    expect(r2.outcome, JobOutcome.completed);
    expect(fake.dispenseCount, afterFail + 1,
        reason: 'attempt++ 는 fresh → 정확히 1회 추가 토출');
  });

  // ── CR-01: RUNNING crash → 재기동 → 동일 commandId 재제출 → dispense 0(전체 파이프라인) ──
  test('CR-01 RUNNING crash 후 재기동 — 동일 commandId 재제출 시 dispense 증가 0', () async {
    // 1) claim + RUNNING 마킹(제조 시작했으나 완료 전 crash 시뮬).
    await ledger.checkAndClaim('run:1');
    await ledger.markRunning('run:1');
    await ledger.close();

    // 2) 재기동 — 재open(replay 로 RUNNING 복원).
    final l2 = await FileIdempotencyLedger.open('${tmp.path}/l.log');
    // BootRecovery: RUNNING → INTERRUPTED 결정(자동재실행 금지·dispense 미호출).
    final decisions = BootRecovery(l2).plan();
    expect(decisions.single.action, RecoveryAction.reportInterrupted);
    expect(decisions.single.fromState, LedgerEntryState.running);

    // 3) status → INTERRUPTED 인가(RUNNING 자동재실행 아님).
    //    RecoveryAction.reportInterrupted = phase FAILED + errorCode INTERRUPTED(§6-7) 근거.
    expect(decisions.single.action, RecoveryAction.reportInterrupted,
        reason: 'RUNNING 은 INTERRUPTED 보고 대상(자동재실행 아님)');

    // 4) 설령 동일 commandId 가 다시 파이프라인에 들어와도 Ledger DROP → dispense 0.
    final freshFake = FakeEnginePort()..scriptAll(FakeEngineOutcome.ack);
    final seq2 = buildSeq(engine: freshFake, l: l2);
    final r = await seq2.submit(
        commandId: 'run:1', traceId: 't', steps: [step(0, 1, 100)]);
    expect(r.outcome, JobOutcome.duplicateDropped);
    expect(freshFake.dispenseCount, 0,
        reason: 'CR-01: 재기동 후 동일 합성키 자동 재토출 절대 0');
    ledger = l2; // teardown 이 live 핸들을 닫도록.
  });

  // ── CR-01: dispense 카운터 증가 0 — RUNNING 재기동 후 BootRecovery 만으로는 절대 토출 안 함 ──
  test('CR-01 재기동 dispense 증가 0 — BootRecovery 는 엔진 미주입(구조적)', () async {
    await ledger.checkAndClaim('run:1');
    await ledger.markRunning('run:1');
    await ledger.close();

    final probe = FakeEnginePort();
    final l2 = await FileIdempotencyLedger.open('${tmp.path}/l.log');
    BootRecovery(l2).plan();
    expect(probe.dispenseCount, 0);
    // RUNNING 재기동 후에도 여전히 RUNNING(자동으로 DONE 승격 안 함) — 재실행 판단 근거 보존.
    expect(l2.stateOf('run:1'), LedgerEntryState.running);
    ledger = l2; // teardown 이 live 핸들을 닫도록.
  });

  // ── EP-03: 중간(2번째) 스텝 empty → silent-success 0(PARTIAL FAILED) ──
  test('EP-03 중간 스텝 empty — COMPLETED 오판 0, PARTIAL FAILED', () async {
    // step0 ack, step1(addr2) empty(무응답) → 재시도 소진 후 실패.
    fake.scriptFor(1, [FakeEngineOutcome.ack]);
    fake.scriptFor(2, [
      FakeEngineOutcome.empty,
      FakeEngineOutcome.empty,
      FakeEngineOutcome.empty,
      FakeEngineOutcome.empty,
    ]);
    final seq = buildSeq();
    final r = await seq.submit(
      commandId: 'o:1',
      traceId: 't',
      steps: [step(0, 1, 100), step(1, 2, 100)],
    );
    expect(r.outcome, isNot(JobOutcome.completed),
        reason: 'empty 무응답을 성공으로 오판하면 안 됨');
    expect(r.outcome, JobOutcome.partialFailed);
    expect(r.stepsDone, 1, reason: '1스텝만 성공, 2번째 empty 실패');
    expect(r.errorCode, StatusErrorCode.engineErrorTransient);
  });

  // ── EP-03: 모든 스텝 empty → dispense 는 발생하나 절대 COMPLETED 아님 ──
  test('EP-03 전 스텝 empty — 카운터는 늘지만 성공 판정 0(실패)', () async {
    fake.scriptAll(FakeEngineOutcome.empty);
    final seq = buildSeq();
    final r = await seq.submit(
        commandId: 'o:1', traceId: 't', steps: [step(0, 1, 100)]);
    expect(r.isSuccess, isFalse, reason: 'silent-success 0');
    expect(r.outcome, JobOutcome.partialFailed);
    // Ledger 도 FAILED 로 종결(DONE 아님) — 재기동 시 재실행 안 함.
    expect(await ledger.isSettled('o:1'), isTrue);
    expect(ledger.stateOf('o:1'), LedgerEntryState.failed);
  });

  // ── EP-03: rawCode!=0 & detail=='' 도 실패(성공 조건은 rawCode==0 뿐) ──
  test('EP-03 rawCode 비0 & 빈 detail → 성공 아님(성공은 rawCode==0 유일)', () async {
    // busy(rawCode 1) 를 4회(첫+재시도3) → transient 소진 실패.
    fake.scriptFor(1, [
      FakeEngineOutcome.busy,
      FakeEngineOutcome.busy,
      FakeEngineOutcome.busy,
      FakeEngineOutcome.busy,
    ]);
    final seq = buildSeq();
    final r = await seq.submit(
        commandId: 'o:1', traceId: 't', steps: [step(0, 1, 100)]);
    expect(r.isSuccess, isFalse);
    expect(r.outcome, JobOutcome.partialFailed);
    // 물리 시도는 4회(첫+재시도3) 발생했으나 성공 종결 0.
    expect(fake.dispenseCount, 4, reason: 'R=3 → 첫+재시도3 = 4 물리 시도');
  });
}
