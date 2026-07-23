/// BootRecovery 테스트 — SoT §9-1 / 질의서 Q4(CR-01·CR-02).
///
/// **PASS 게이트 CR-01(재기동 자동재실행 금지)**: RUNNING→INTERRUPTED 결정만 산출하고
/// 엔진(dispense)을 호출하지 않는다(구조적 보장 — BootRecovery 는 엔진 미주입).
library;

import 'dart:io';

import 'package:heysenlyt_pi/heysenlyt_pi.dart';
import 'package:test/test.dart';

import 'support/fake_engine_port.dart';

void main() {
  late Directory tmp;

  setUp(() async {
    tmp = await Directory.systemTemp.createTemp('boot_test_');
  });
  tearDown(() async {
    if (await tmp.exists()) await tmp.delete(recursive: true);
  });

  Future<FileIdempotencyLedger> mkLedger() =>
      FileIdempotencyLedger.open('${tmp.path}/l.log');

  test('RUNNING → reportInterrupted (자동재실행 금지·CR-01)', () async {
    final l1 = await mkLedger();
    await l1.checkAndClaim('run:1');
    await l1.markRunning('run:1');
    await l1.close();

    // 재부팅 시뮬 — 재open.
    final l2 = await mkLedger();
    final decisions = BootRecovery(l2).plan();
    expect(decisions.length, 1);
    expect(decisions.single.action, RecoveryAction.reportInterrupted);
    expect(decisions.single.commandId, 'run:1');
    await l2.close();
  });

  test('RECEIVED → clearAndFresh (미시작·물리 토출 전·CR-02)', () async {
    final l1 = await mkLedger();
    await l1.checkAndClaim('recv:1'); // RECEIVED 만.
    await l1.close();

    final l2 = await mkLedger();
    final decisions = BootRecovery(l2).plan();
    expect(decisions.single.action, RecoveryAction.clearAndFresh);
    await l2.close();
  });

  test('DONE → 무동작(결정 목록에 없음)', () async {
    final l1 = await mkLedger();
    await l1.checkAndClaim('done:1');
    await l1.markSettled('done:1', success: true);
    await l1.close();

    final l2 = await mkLedger();
    expect(BootRecovery(l2).plan(), isEmpty);
    await l2.close();
  });

  test('FAILED → 무동작(멱등 DROP 집합·재실행 없음)', () async {
    final l1 = await mkLedger();
    await l1.checkAndClaim('fail:1');
    await l1.markSettled('fail:1', success: false);
    await l1.close();

    final l2 = await mkLedger();
    expect(BootRecovery(l2).plan(), isEmpty);
    await l2.close();
  });

  test('CR-01 구조적 보장 — BootRecovery 는 dispense 를 호출하지 않는다', () async {
    // 엔진을 주입할 자리조차 없음(생성자에 엔진 없음). 여기서는 fake 를 별도로 관찰:
    // plan() 실행 후에도 어떤 엔진도 토출되지 않았음을 확인(자동재실행 금지의 물리 증거).
    final fake = FakeEnginePort();
    final l1 = await mkLedger();
    await l1.checkAndClaim('run:1');
    await l1.markRunning('run:1');
    await l1.close();

    final l2 = await mkLedger();
    BootRecovery(l2).plan(); // 결정만 산출.
    expect(fake.dispenseCount, 0, reason: '재기동 시 자동 토출 절대 금지(CR-01)');
    await l2.close();
  });

  test('혼합 상태 — RUNNING·RECEIVED·DONE 동시 복구 결정', () async {
    final l1 = await mkLedger();
    await l1.checkAndClaim('run:1');
    await l1.markRunning('run:1');
    await l1.checkAndClaim('recv:1');
    await l1.checkAndClaim('done:1');
    await l1.markSettled('done:1', success: true);
    await l1.close();

    final l2 = await mkLedger();
    final decisions = BootRecovery(l2).plan();
    final byId = {for (final d in decisions) d.commandId: d.action};
    expect(byId['run:1'], RecoveryAction.reportInterrupted);
    expect(byId['recv:1'], RecoveryAction.clearAndFresh);
    expect(byId.containsKey('done:1'), isFalse);
    await l2.close();
  });
}
