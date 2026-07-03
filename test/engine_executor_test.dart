/// EngineExecutor 테스트 — SoT §6-7 / 질의서 Q8(EP-03·EP-09).
///
/// **EP-03 게이트(빈응답=실패·silent-success 금지)** = 이 파일의 핵심.
/// 재시도(transient R=3)·즉시중단(permanent)·timeout·empty 실패 분류를 dispense 카운터로 검증.
library;

import 'package:heysenlyt_pi/heysenlyt_pi.dart';
import 'package:test/test.dart';

import 'support/fake_engine_port.dart';

void main() {
  final spec = SyringeSpec(pumpFullStroke: 12000, syringeCapacityMl: 1.25);
  EngineDispenseCommand cmd(int addr) =>
      EngineDispenseCommand(pumpAddr: addr, volumeUl: 100, steps: 960, spec: spec);

  test('정상 ack → success, 1회 dispense', () async {
    final fake = FakeEnginePort()..scriptAll(FakeEngineOutcome.ack);
    final ex = EngineExecutor(fake);
    final res = await ex.runStep(cmd(1));
    expect(res.isSuccess, isTrue);
    expect(res.attempts, 1);
    expect(fake.dispenseCount, 1);
  });

  test('EP-03: empty(무응답) = 실패 — silent-success 0, R 소진', () async {
    final fake = FakeEnginePort()..scriptAll(FakeEngineOutcome.empty);
    final ex = EngineExecutor(fake, maxRetries: 3);
    final res = await ex.runStep(cmd(1));
    expect(res.isSuccess, isFalse, reason: '빈응답은 절대 성공 아님(EP-03)');
    expect(res.status, EngineStepStatus.transientExhausted);
    // 첫 시도 + 3 재시도 = 4 물리 dispense(재시도했으나 전부 empty).
    expect(fake.dispenseCount, 4);
    expect(res.errorCode, StatusErrorCode.engineErrorTransient);
  });

  test('permanent → 즉시중단(재시도 없음), 1회 dispense', () async {
    final fake = FakeEnginePort()..scriptAll(FakeEngineOutcome.permanent);
    final ex = EngineExecutor(fake, maxRetries: 3);
    final res = await ex.runStep(cmd(1));
    expect(res.status, EngineStepStatus.permanent);
    expect(res.attempts, 1, reason: 'permanent 는 재시도 안 함');
    expect(fake.dispenseCount, 1);
    expect(res.errorCode, StatusErrorCode.engineErrorPermanent);
  });

  test('busy(transient) 후 ack → 재시도 성공', () async {
    final fake = FakeEnginePort()
      ..scriptFor(1, [FakeEngineOutcome.busy, FakeEngineOutcome.ack]);
    final ex = EngineExecutor(fake, maxRetries: 3);
    final res = await ex.runStep(cmd(1));
    expect(res.isSuccess, isTrue);
    expect(res.attempts, 2);
    expect(fake.dispenseCount, 2);
  });

  test('timeout → ENGINE_TIMEOUT 분류, R 소진 실패', () async {
    final fake = FakeEnginePort()..scriptAll(FakeEngineOutcome.timeout);
    final ex = EngineExecutor(fake, maxRetries: 2);
    final res = await ex.runStep(cmd(1));
    expect(res.isSuccess, isFalse);
    expect(res.errorCode, StatusErrorCode.engineTimeout);
    expect(fake.dispenseCount, 3); // 1 + 2 재시도.
  });

  test('transient 재시도 소진(전부 busy) → transientExhausted', () async {
    final fake = FakeEnginePort()..scriptAll(FakeEngineOutcome.busy);
    final ex = EngineExecutor(fake, maxRetries: 3);
    final res = await ex.runStep(cmd(1));
    expect(res.status, EngineStepStatus.transientExhausted);
    expect(fake.dispenseCount, 4);
  });
}
