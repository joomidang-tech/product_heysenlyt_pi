/// FileIdempotencyLedger 테스트 — SoT §4-6 / 질의서 Q1(IL-04·CR-06) / 부록A P-2.
///
/// IL-02 게이트 근거: 합성키 4상태 전부 DROP·fsync 원자 영속·재부팅 replay 복원.
library;

import 'dart:io';

import 'package:heysenlyt_pi/heysenlyt_pi.dart';
import 'package:test/test.dart';

void main() {
  late Directory tmp;

  setUp(() async {
    tmp = await Directory.systemTemp.createTemp('ledger_test_');
  });

  tearDown(() async {
    if (await tmp.exists()) await tmp.delete(recursive: true);
  });

  String ledgerPath() => '${tmp.path}${Platform.pathSeparator}ledger.log';

  test('처음 본 합성키 = fresh, 재관찰 = duplicate', () async {
    final ledger = await FileIdempotencyLedger.open(ledgerPath());
    expect(await ledger.checkAndClaim('order-1:1'), LedgerVerdict.fresh);
    expect(await ledger.checkAndClaim('order-1:1'), LedgerVerdict.duplicate);
    await ledger.close();
  });

  test('4상태 전부 DROP — RECEIVED/RUNNING/DONE/FAILED 모두 재claim=duplicate (Q1)', () async {
    for (final settle in [null, 'running', 'done', 'failed']) {
      final ledger = await FileIdempotencyLedger.open(ledgerPath());
      const cid = 'o:1';
      expect(await ledger.checkAndClaim(cid), LedgerVerdict.fresh);
      if (settle == 'running') await ledger.markRunning(cid);
      if (settle == 'done') await ledger.markSettled(cid, success: true);
      if (settle == 'failed') await ledger.markSettled(cid, success: false);
      // 어떤 상태든 재claim 은 duplicate.
      expect(await ledger.checkAndClaim(cid), LedgerVerdict.duplicate,
          reason: 'state=$settle 도 DROP 이어야');
      await ledger.close();
      await File(ledgerPath()).delete();
    }
  });

  test('attempt 증가 = 새 합성키 = fresh (재제조 성립·§4-4)', () async {
    final ledger = await FileIdempotencyLedger.open(ledgerPath());
    expect(await ledger.checkAndClaim('order-9:1'), LedgerVerdict.fresh);
    await ledger.markSettled('order-9:1', success: false);
    // 같은 attempt 재시도는 DROP.
    expect(await ledger.checkAndClaim('order-9:1'), LedgerVerdict.duplicate);
    // attempt++ = 새 합성키 = fresh.
    expect(await ledger.checkAndClaim('order-9:2'), LedgerVerdict.fresh);
    await ledger.close();
  });

  test('fsync 영속 — 재open 후 duplicate 판정 유지(crash-safe)', () async {
    final l1 = await FileIdempotencyLedger.open(ledgerPath());
    await l1.checkAndClaim('o:1');
    await l1.markRunning('o:1');
    await l1.close();

    // 재open(재부팅 시뮬).
    final l2 = await FileIdempotencyLedger.open(ledgerPath());
    expect(await l2.checkAndClaim('o:1'), LedgerVerdict.duplicate);
    expect(l2.stateOf('o:1'), LedgerEntryState.running);
    await l2.close();
  });

  test('replay — RUNNING/RECEIVED 스캔(재부팅 복구 근거·CR-01/CR-02)', () async {
    final l1 = await FileIdempotencyLedger.open(ledgerPath());
    await l1.checkAndClaim('run:1');
    await l1.markRunning('run:1');
    await l1.checkAndClaim('recv:1'); // RECEIVED(미시작).
    await l1.checkAndClaim('done:1');
    await l1.markSettled('done:1', success: true);
    await l1.close();

    final l2 = await FileIdempotencyLedger.open(ledgerPath());
    expect(l2.runningCommands(), ['run:1']);
    expect(l2.receivedCommands(), ['recv:1']);
    expect(await l2.isSettled('done:1'), isTrue);
    await l2.close();
  });

  test('부분 프레임(잘린 마지막 라인) 무시 — crash-safe', () async {
    final l1 = await FileIdempotencyLedger.open(ledgerPath());
    await l1.checkAndClaim('o:1');
    await l1.close();
    // 전원 단절로 잘린 라인 append(불완전 JSON).
    await File(ledgerPath()).writeAsString('{"commandId":"o:2","stat', mode: FileMode.append);

    final l2 = await FileIdempotencyLedger.open(ledgerPath());
    expect(await l2.checkAndClaim('o:1'), LedgerVerdict.duplicate); // 온전한 레코드는 유지.
    expect(await l2.checkAndClaim('o:2'), LedgerVerdict.fresh); // 잘린 레코드는 무시.
    await l2.close();
  });

  test('compact — 최신 상태만 남기고 atomic swap 후 판정 유지', () async {
    final l1 = await FileIdempotencyLedger.open(ledgerPath());
    await l1.checkAndClaim('o:1');
    await l1.markRunning('o:1');
    await l1.markSettled('o:1', success: true);
    await l1.compact();
    expect(await l1.checkAndClaim('o:1'), LedgerVerdict.duplicate);
    await l1.close();

    final l2 = await FileIdempotencyLedger.open(ledgerPath());
    expect(l2.stateOf('o:1'), LedgerEntryState.done);
    await l2.close();
  });
}
