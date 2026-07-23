/// RecipeResolver 테스트 — SoT §6-4 / §9-1 / 질의서 Q2(RR-05)·Q3(RR-07).
///
/// 정렬(idx 오름차순)·검증 게이트(음수·0·상한초과·미매핑·빈레시피 → CMD_VALIDATION_FAILED)·
/// steps 파생(하드코딩 금지·§6-4 검산).
library;

import 'package:heysenlyt_pi/heysenlyt_pi.dart';
import 'package:test/test.dart';

void main() {
  // flavor 1.25mL(fullStroke 12000) → maxVolumeUl=1250, stepsPerMl=9600.
  final flavorSpec = SyringeSpec(pumpFullStroke: 12000, syringeCapacityMl: 1.25);
  // fragrance 0.5mL → maxVolumeUl=500, stepsPerMl=24000.
  final fragSpec = SyringeSpec(pumpFullStroke: 12000, syringeCapacityMl: 0.5);

  final resolver = RecipeResolver({1: flavorSpec, 2: flavorSpec, 5: fragSpec});

  RecipeStep step(int idx, int addr, num vol) =>
      RecipeStep(idx: idx, pumpAddr: addr, flavor: 'f$addr', volume: vol);

  test('idx 오름차순 직렬 정렬(§9-1)', () {
    final r = resolver.resolve([step(2, 1, 100), step(0, 1, 100), step(1, 2, 100)]);
    expect(r.steps.map((s) => s.idx).toList(), [0, 1, 2]);
  });

  test('steps 파생 검산 — 100µL/1.25mL = 960 steps(§6-4)', () {
    final r = resolver.resolve([step(0, 1, 100)]);
    expect(r.steps.single.steps, 960); // 12000 × 100 ÷ 1250 = 960.
  });

  test('RR-07(Q3): 빈 레시피 → CMD_VALIDATION_FAILED (0step COMPLETED 금지)', () {
    expect(
      () => resolver.resolve([]),
      throwsA(isA<RecipeValidationError>()
          .having((e) => e.reason, 'reason', 'empty_recipe')
          .having((e) => e.errorCode, 'errorCode', StatusErrorCode.cmdValidationFailed)),
    );
  });

  test('RR-05(Q2): 0/음수 volume → drop', () {
    expect(() => resolver.resolve([step(0, 1, 0)]),
        throwsA(isA<RecipeValidationError>().having((e) => e.reason, 'r', 'non_positive_volume')));
    expect(() => resolver.resolve([step(0, 1, -5)]),
        throwsA(isA<RecipeValidationError>().having((e) => e.reason, 'r', 'non_positive_volume')));
  });

  test('RR-05(Q2): 상한초과 volume → drop (maxVolumeUl=1250)', () {
    expect(() => resolver.resolve([step(0, 1, 1251)]),
        throwsA(isA<RecipeValidationError>().having((e) => e.reason, 'r', 'volume_over_max')));
    // 경계값 1250 은 통과(≤).
    expect(resolver.resolve([step(0, 1, 1250)]).steps.single.steps, 12000);
  });

  test('미매핑 pumpAddr → drop', () {
    expect(() => resolver.resolve([step(0, 99, 100)]),
        throwsA(isA<RecipeValidationError>().having((e) => e.reason, 'r', 'unmapped_pump_addr')));
  });

  test('fragrance 0.5mL 펌프 — maxVolumeUl=500, 500µL 경계 통과', () {
    final r = resolver.resolve([step(0, 5, 500)]);
    expect(r.steps.single.steps, 12000); // 12000 × 500 ÷ 500 = 12000.
    expect(() => resolver.resolve([step(0, 5, 501)]),
        throwsA(isA<RecipeValidationError>().having((e) => e.reason, 'r', 'volume_over_max')));
  });

  test('여러 스텝 stepN 반영', () {
    final r = resolver.resolve([step(0, 1, 100), step(1, 2, 200)]);
    expect(r.stepN, 2);
    expect(r.steps[1].steps, 1920); // 12000 × 200 ÷ 1250.
  });
}
