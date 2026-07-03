/// Offline Queue (OQ) + resync — SoT §4-6 / 질의서 Q5(OQ-04·CS-07) / §8-3.
///
/// **단절 중 적재·진행 계속**: 네트워크 단절 시 status 역보고를 로컬 큐에 FIFO 적재하고 제조는
/// 계속한다(관측이 제조를 막지 않는다·§10-6). 재연결 시 **FIFO flush**.
///
/// **멱등 flush(at-least-once)**: flush 는 재전송해도 무해해야 한다 — 서버가 requestId 로 dedup(§4-6).
///   OQ 재시도는 requestId 만 싣고 expectedFrom 미포함 → 서버 CAS 스킵(§4-3). 동일 (id, phase) 1회 보장.
///
/// **fetchSince 누락보정(Q5·OQ-04)**: server-mediated uplink 로 A/B(MQTT/Firestore) 선택을 흡수.
///   재연결 후 `fetchSince(createdAt > cursor)` 로 단절 중 놓친 command 를 결정적으로 복원(누락 0).
///
/// 이 큐는 순수 인메모리+영속 훅(선택). 실 http flush 는 StatusSinkPort 어댑터가 담당하고,
/// 이 클래스는 큐잉·FIFO·dedup 키 관리·cursor 만 책임(테스트로 완전 검증 가능).
library;

import 'dart:collection';

import '../core/wire_messages.dart' show StatusReport;

/// flush 시 각 항목을 실제 전송하는 콜백(성공 시 true → 큐에서 제거).
///
/// at-least-once: false/throw 면 항목을 큐에 남겨 다음 flush 재시도(멱등이라 안전).
typedef StatusSender = Future<bool> Function(StatusReport report);

/// Offline Queue.
class OfflineQueue {
  OfflineQueue({this.maxDepth = 1000});

  /// 큐 상한(폭주 방어). 초과 시 가장 오래된 항목부터 드롭(진행보고 최신성 우선).
  final int maxDepth;

  final Queue<StatusReport> _queue = Queue<StatusReport>();

  /// 이미 flush 성공한 (id, phase, stepK) 서명 — 로컬 중복 방출 억제(서버 dedup 이중화).
  final Set<String> _sentSignatures = <String>{};

  /// resync cursor — 마지막으로 성공 소비한 command.createdAt(ISO8601). fetchSince 기준.
  String? _cursor;

  /// 온라인 여부(단절 시뮬레이션).
  bool online = true;

  int get depth => _queue.length;
  bool get isEmpty => _queue.isEmpty;

  String? get cursor => _cursor;

  String _sig(StatusReport r) => '${r.id}|${r.phase}|${r.stepK}';

  /// status 보고를 적재(단절 여부와 무관 — flush 시 online 판정).
  ///
  /// 로컬 dedup: 이미 성공 전송된 서명은 재적재하지 않는다(멱등·OQ 폭주 완화).
  void enqueue(StatusReport report) {
    if (_sentSignatures.contains(_sig(report))) return;
    _queue.addLast(report);
    // 상한 초과 시 FIFO 앞부분 드롭(최신 진행 우선).
    while (_queue.length > maxDepth) {
      _queue.removeFirst();
    }
  }

  /// 재연결 flush — FIFO 순서로 [send] 호출. 멱등(서버 dedup)·at-least-once.
  ///
  /// online=false 면 아무 것도 보내지 않고 그대로 유지(단절 중 진행 계속).
  /// 반환 = 이번 flush 로 성공 전송된 항목 수.
  Future<int> flush(StatusSender send) async {
    if (!online) return 0;
    int sent = 0;
    // FIFO — 앞에서부터. 실패 항목은 남기고 그 뒤도 계속 시도하지 않는다(순서 보존·재시도).
    while (_queue.isNotEmpty) {
      final head = _queue.first;
      final sig = _sig(head);
      if (_sentSignatures.contains(sig)) {
        // 이미 성공(재적재 방어망) — 조용히 제거.
        _queue.removeFirst();
        continue;
      }
      bool ok;
      try {
        ok = await send(head);
      } catch (_) {
        ok = false;
      }
      if (!ok) break; // 순서 보존 — 실패 지점에서 멈추고 다음 flush 재시도.
      _queue.removeFirst();
      _sentSignatures.add(sig);
      sent++;
    }
    return sent;
  }

  /// 성공 소비한 command 의 createdAt 로 cursor 전진(resync 기준·§9-1 createdAt).
  ///
  /// 단조 전진만(더 이른 createdAt 은 무시) — 재연결 중복 수신 시 cursor 역행 방지.
  void advanceCursor(String createdAtIso) {
    final cur = _cursor;
    if (cur == null || createdAtIso.compareTo(cur) > 0) {
      _cursor = createdAtIso;
    }
  }

  /// fetchSince 대상 판별(Q5·OQ-04): createdAt > cursor 인 command 만 재처리 대상.
  ///
  /// ISO8601 밀리초 Z 고정(§5-3·부록A P-3) → 문자열 비교가 시간 비교와 동치.
  bool isAfterCursor(String createdAtIso) {
    final cur = _cursor;
    if (cur == null) return true; // cursor 없으면 전부 fresh.
    return createdAtIso.compareTo(cur) > 0;
  }

  /// 재연결 신호.
  void reconnect() {
    online = true;
  }

  /// 단절 신호(테스트/실제).
  void disconnect() {
    online = false;
  }
}
