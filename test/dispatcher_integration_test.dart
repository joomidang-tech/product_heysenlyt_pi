/// Dispatcher 통합 테스트 — CS(Fake)→IL→RR→PS→EP→SR 봉합 — SoT §1-1 / §9 / 질의서 §0.
///
/// deviceId 필터(CS-08)·recipe==null 폴백 해석·fragrance mL→µL 정규화·end-to-end 봉합을
/// dispense 카운터로 검증. 3개 PASS 게이트(IL-02·CR-01·EP-03)가 통합 경로에서도 성립함을 확인.
library;

import 'dart:io';

import 'package:heysenlyt_pi/heysenlyt_pi.dart';
import 'package:test/test.dart';

import 'support/fake_command_source.dart';
import 'support/fake_engine_port.dart';

void main() {
  late Directory tmp;
  late FileIdempotencyLedger ledger;
  late FakeEnginePort fake;
  late FakeCommandSource source;
  late Dispatcher dispatcher;
  final spec = SyringeSpec(pumpFullStroke: 12000, syringeCapacityMl: 1.25);
  final fragSpec = SyringeSpec(pumpFullStroke: 12000, syringeCapacityMl: 0.5);
  int reqSeq = 0;

  setUp(() async {
    tmp = await Directory.systemTemp.createTemp('dispatch_test_');
    ledger = await FileIdempotencyLedger.open('${tmp.path}/l.log');
    fake = FakeEnginePort()..scriptAll(FakeEngineOutcome.ack);
    source = FakeCommandSource();
    reqSeq = 0;
  });

  tearDown(() async {
    await dispatcher.stop();
    await source.close();
    await ledger.close();
    if (await tmp.exists()) await tmp.delete(recursive: true);
  });

  Dispatcher build({RecipeInterpreter? interpret}) {
    final resolver = RecipeResolver({1: spec, 2: spec, 5: fragSpec});
    final sequencer = PumpSequencer(
      ledger: ledger,
      engine: fake,
      resolver: resolver,
      requestIdGen: () => 'req-${reqSeq++}',
      nowIso: () => '2026-07-03T00:00:00.000Z',
    );
    return Dispatcher(
      deviceId: 'dev-A',
      commandSource: source,
      sequencer: sequencer,
      interpret: interpret ?? (c) => c.recipe ?? [],
    );
  }

  Command cmd(String id, String deviceId, {List<RecipeStep>? recipe}) => Command(
        id: id,
        orderId: id.split(':').first,
        attempt: int.parse(id.split(':').last),
        deviceId: deviceId,
        recipe: recipe,
        traceId: 'trace-$id',
        createdAt: '2026-07-03T00:00:00.000Z',
      );

  RecipeStep step(int idx, int addr, num vol) =>
      RecipeStep(idx: idx, pumpAddr: addr, flavor: 'f', volume: vol);

  test('end-to-end 봉합 — 명령 → COMPLETED (dispatchOnce)', () async {
    dispatcher = build();
    final r = await dispatcher.dispatchOnce(
      cmd('o:1', 'dev-A', recipe: [step(0, 1, 100), step(1, 2, 100)]),
    );
    expect(r.outcome, JobOutcome.completed);
    expect(fake.dispenseCount, 2);
  });

  test('CS-08 deviceId 필터 — 타 매장 명령 무시(dispense 0)', () async {
    dispatcher = build();
    final reports = <JobReport>[];
    dispatcher.reports.listen(reports.add);
    dispatcher.start();

    source.push(cmd('o:1', 'dev-B', recipe: [step(0, 1, 100)])); // 타 매장.
    await Future<void>.delayed(const Duration(milliseconds: 20));
    expect(fake.dispenseCount, 0, reason: 'deviceId 불일치 → 미소비');

    source.push(cmd('o:2', 'dev-A', recipe: [step(0, 1, 100)])); // 내 매장.
    await Future<void>.delayed(const Duration(milliseconds: 20));
    expect(fake.dispenseCount, 1);
  });

  test('recipe==null 폴백 해석 — recipeId/fragranceResult → steps', () async {
    // fragrance notes → mL→µL 정규화 해석기 주입.
    dispatcher = build(interpret: (c) {
      // amountMl 0.3 → 300µL.
      return fragranceNotesToSteps(
        [
          {'name': 'rose', 'amountMl': 0.3},
        ],
        pumpAddrOf: (_) => 5, // fragrance 펌프 addr.
      );
    });
    final r = await dispatcher.dispatchOnce(cmd('o:1', 'dev-A', recipe: null));
    expect(r.outcome, JobOutcome.completed);
    // 300µL / 0.5mL(fragSpec) → 12000 × 300 ÷ 500 = 7200 steps. dispense 1회.
    expect(fake.dispenseCount, 1);
    expect(fake.dispenseCalls.single.volumeUl, 300); // mL→µL 정규화 확인(§6-6).
    expect(fake.dispenseCalls.single.steps, 7200);
  });

  test('통합 IL-02 — 스트림 중복 command → DROP(추가 토출 0)', () async {
    dispatcher = build();
    dispatcher.start();
    source.push(cmd('o:1', 'dev-A', recipe: [step(0, 1, 100)]));
    await Future<void>.delayed(const Duration(milliseconds: 20));
    source.push(cmd('o:1', 'dev-A', recipe: [step(0, 1, 100)])); // 중복.
    await Future<void>.delayed(const Duration(milliseconds: 20));
    expect(fake.dispenseCount, 1, reason: '중복 command.id → 추가 토출 0(IL-02)');
  });

  test('통합 EP-03 — empty 명령 → COMPLETED 아님(silent-success 0)', () async {
    fake.scriptAll(FakeEngineOutcome.empty);
    dispatcher = build();
    final r = await dispatcher.dispatchOnce(
      cmd('o:1', 'dev-A', recipe: [step(0, 1, 100)]),
    );
    expect(r.outcome, isNot(JobOutcome.completed));
    expect(r.outcome, JobOutcome.partialFailed);
  });
}
