/// OfflineQueue 테스트 — SoT §4-6 / 질의서 Q5(OQ-04·CS-07).
///
/// 단절 중 적재·재연결 FIFO flush·멱등(at-least-once·서버 dedup)·fetchSince cursor 누락보정.
library;

import 'package:heysenlyt_pi/heysenlyt_pi.dart';
import 'package:test/test.dart';

void main() {
  StatusReport rep(String id, String phase, int k) => StatusReport(
        id: id,
        phase: phase,
        stepK: k,
        stepN: 3,
        errorCode: null,
        requestId: 'req-$id-$phase-$k',
        traceId: 't',
        updatedAt: '2026-07-03T00:00:0$k.000Z',
      );

  test('단절 중 적재 — flush 는 online 일 때만 전송', () async {
    final oq = OfflineQueue()..disconnect();
    oq.enqueue(rep('o:1', 'PROGRESS', 1));
    oq.enqueue(rep('o:1', 'PROGRESS', 2));
    expect(oq.depth, 2);

    final sent = <String>[];
    // 단절 중 flush → 0.
    expect(await oq.flush((r) async {
      sent.add(r.phase);
      return true;
    }), 0);
    expect(oq.depth, 2);

    // 재연결 후 flush → FIFO 순서.
    oq.reconnect();
    expect(await oq.flush((r) async {
      sent.add('${r.phase}-${r.stepK}');
      return true;
    }), 2);
    expect(sent, ['PROGRESS-1', 'PROGRESS-2']);
    expect(oq.depth, 0);
  });

  test('멱등 — 이미 성공 전송된 서명 재적재 안 함', () async {
    final oq = OfflineQueue();
    oq.enqueue(rep('o:1', 'PROGRESS', 1));
    await oq.flush((r) async => true);
    // 동일 (id, phase, stepK) 재적재 → 무시.
    oq.enqueue(rep('o:1', 'PROGRESS', 1));
    expect(oq.depth, 0);
  });

  test('flush 실패 시 순서 보존 — 실패 지점에서 멈추고 재시도', () async {
    final oq = OfflineQueue();
    oq.enqueue(rep('o:1', 'ACCEPTED', 0));
    oq.enqueue(rep('o:1', 'PROGRESS', 1));
    oq.enqueue(rep('o:1', 'PROGRESS', 2));

    int calls = 0;
    // 두 번째 전송 실패.
    final sent1 = await oq.flush((r) async {
      calls++;
      if (r.stepK == 1) return false;
      return true;
    });
    expect(sent1, 1); // ACCEPTED 만 성공.
    expect(oq.depth, 2); // PROGRESS 1,2 남음.

    // 재시도 — 이번엔 전부 성공.
    final sent2 = await oq.flush((r) async => true);
    expect(sent2, 2);
    expect(oq.depth, 0);
  });

  test('at-least-once 안전 — send throw 시 큐 유지(멱등 재전송)', () async {
    final oq = OfflineQueue();
    oq.enqueue(rep('o:1', 'PROGRESS', 1));
    final sent = await oq.flush((r) async => throw StateError('network'));
    expect(sent, 0);
    expect(oq.depth, 1);
  });

  test('fetchSince cursor — createdAt > cursor 만 fresh(OQ-04 누락보정)', () {
    final oq = OfflineQueue();
    expect(oq.isAfterCursor('2026-07-03T00:00:01.000Z'), isTrue); // cursor 없음 → 전부 fresh.
    oq.advanceCursor('2026-07-03T00:00:05.000Z');
    expect(oq.isAfterCursor('2026-07-03T00:00:04.000Z'), isFalse); // 이미 처리분.
    expect(oq.isAfterCursor('2026-07-03T00:00:06.000Z'), isTrue); // 놓친 후속.
  });

  test('cursor 단조 전진 — 역행 무시', () {
    final oq = OfflineQueue();
    oq.advanceCursor('2026-07-03T00:00:05.000Z');
    oq.advanceCursor('2026-07-03T00:00:03.000Z'); // 더 이른 값 무시.
    expect(oq.cursor, '2026-07-03T00:00:05.000Z');
  });

  test('maxDepth 초과 시 FIFO 앞부분 드롭(폭주 방어)', () {
    final oq = OfflineQueue(maxDepth: 2);
    oq.enqueue(rep('o:1', 'PROGRESS', 1));
    oq.enqueue(rep('o:1', 'PROGRESS', 2));
    oq.enqueue(rep('o:1', 'PROGRESS', 3));
    expect(oq.depth, 2); // 가장 오래된 것 드롭.
  });
}
