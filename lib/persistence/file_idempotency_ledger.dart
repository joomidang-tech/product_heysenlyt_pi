/// 파일 영속 멱등 Ledger — SoT §4-6(pi측 dedup) / 부록A P-2 / 질의서 Q1(IL-04·CR-06).
///
/// **IL-02 게이트(중복토출0)의 물리 보증**: 합성키 `{orderId}:{attempt}` 를 기준으로
/// [LedgerEntryState] 4상태 **전부**(RECEIVED·RUNNING·DONE·FAILED)를 **한번 본 id = DROP**.
///   - Q1(계승): 멱등 DROP 집합에 **FAILED 포함**. 재주문은 attempt 증가로 새 command.id 를 만들어
///     fresh 판정을 받는다(status-only 되돌림 금지·§4-4). 같은 합성키는 실패했어도 재토출 안 함.
///
/// **crash-safe 영속(부록A·CR-01)**: append-only 로그를 매 write 마다 fsync(flush)로 원자 영속한다.
///   - 각 라인 = 1 JSON 레코드(개행 구분). temp 파일 rename atomic swap 은 컴팩션(재기동 로드) 시 사용.
///   - 재부팅 시 로그를 재생(replay)해 마지막 상태를 복원 → on_boot recovery(§9-1) 판단 근거.
///
/// 순수 dart:io(외부 의존 0). SQLite 대신 append+fsync 로그 — 단일 라이터(pi 데몬) 전제.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'idempotency_ledger.dart';

/// Ledger 4상태 — SoT §4-6 / 질의서 Q1. **전부 DROP 집합**(fresh 는 미기록 키에만).
enum LedgerEntryState {
  /// 명령 수신·예약(claim). 아직 제조 시작 전.
  received('RECEIVED'),

  /// 제조 진행 중. 재부팅 시 INTERRUPTED 판정 대상(CR-01).
  running('RUNNING'),

  /// 제조 완료(성공 종결).
  done('DONE'),

  /// 제조 실패 종결. **DROP 집합 포함**(재주문은 새 attempt).
  failed('FAILED');

  const LedgerEntryState(this.wire);
  final String wire;

  static LedgerEntryState? fromWire(Object? v) {
    if (v is! String) return null;
    for (final s in LedgerEntryState.values) {
      if (s.wire == v) return s;
    }
    return null;
  }
}

/// Ledger 레코드(replay 로 최종 상태 복원).
class LedgerRecord {
  const LedgerRecord({required this.commandId, required this.state, required this.ts});

  final String commandId;
  final LedgerEntryState state;
  final String ts; // ISO8601 (관찰/디버그용, 판정 무관)

  Map<String, Object?> toJson() => {
        'commandId': commandId,
        'state': state.wire,
        'ts': ts,
      };
}

/// 파일 append+fsync 영속 Ledger.
///
/// checkAndClaim: 미기록 키 → fresh 로 RECEIVED 예약(원자 append+flush). 기록된 키(4상태 전부) → duplicate.
/// markRunning/markSettled: 상태 전이 append. isSettled/stateOf: replay 된 인메모리 인덱스 조회.
class FileIdempotencyLedger implements IdempotencyLedger {
  FileIdempotencyLedger._(this._file, this._raf, this._index);

  final File _file;
  RandomAccessFile _raf;

  /// commandId → 현재(최신) 상태. replay 로 구성·write 마다 갱신.
  final Map<String, LedgerEntryState> _index;

  /// 시계 주입(테스트 결정성). 기본 = DateTime.now().toUtc().
  String Function() nowIso = () => DateTime.now().toUtc().toIso8601String();

  /// 로그 파일을 열고(없으면 생성) replay 하여 인메모리 인덱스를 복원한다.
  static Future<FileIdempotencyLedger> open(String path) async {
    final file = File(path);
    await file.parent.create(recursive: true);
    final index = <String, LedgerEntryState>{};

    if (await file.exists()) {
      final lines = await file.readAsLines();
      for (final line in lines) {
        final trimmed = line.trim();
        if (trimmed.isEmpty) continue;
        Object? parsed;
        try {
          parsed = jsonDecode(trimmed);
        } on FormatException {
          // 부분 프레임(전원 단절 중 잘린 마지막 라인) — 무시하고 계속(crash-safe).
          continue;
        }
        if (parsed is! Map) continue;
        final cid = parsed['commandId'];
        final state = LedgerEntryState.fromWire(parsed['state']);
        if (cid is String && state != null) {
          index[cid] = state; // 마지막 승자(append 순서 = 시간 순서).
        }
      }
    }

    // append 모드로 열되, replay 후 이어쓰기.
    final raf = await file.open(mode: FileMode.append);
    return FileIdempotencyLedger._(file, raf, index);
  }

  Future<void> _append(LedgerRecord rec) async {
    final line = '${jsonEncode(rec.toJson())}\n';
    await _raf.writeString(line);
    await _raf.flush(); // fsync 등가 — 원자 영속(crash-safe).
    _index[rec.commandId] = rec.state;
  }

  @override
  Future<LedgerVerdict> checkAndClaim(String commandId) async {
    // 4상태 전부 DROP — 한번 본 합성키면 fresh 아님(Q1·IL-02).
    if (_index.containsKey(commandId)) return LedgerVerdict.duplicate;
    await _append(LedgerRecord(
      commandId: commandId,
      state: LedgerEntryState.received,
      ts: nowIso(),
    ));
    return LedgerVerdict.fresh;
  }

  /// RECEIVED → RUNNING 전이(제조 시작). 재부팅 시 INTERRUPTED 판정 근거(CR-01).
  Future<void> markRunning(String commandId) async {
    await _append(LedgerRecord(
      commandId: commandId,
      state: LedgerEntryState.running,
      ts: nowIso(),
    ));
  }

  @override
  Future<void> markSettled(String commandId, {required bool success}) async {
    await _append(LedgerRecord(
      commandId: commandId,
      state: success ? LedgerEntryState.done : LedgerEntryState.failed,
      ts: nowIso(),
    ));
  }

  @override
  Future<bool> isSettled(String commandId) async {
    final s = _index[commandId];
    return s == LedgerEntryState.done || s == LedgerEntryState.failed;
  }

  /// 현재 상태 조회(on_boot recovery 판단용·§9-1). 미기록 = null.
  LedgerEntryState? stateOf(String commandId) => _index[commandId];

  /// 특정 상태의 모든 commandId(재부팅 복구 스캔용).
  List<String> commandsInState(LedgerEntryState state) =>
      _index.entries.where((e) => e.value == state).map((e) => e.key).toList();

  /// 진행 중(RUNNING) 합성키 목록 — CR-01: RUNNING→INTERRUPTED 대상.
  List<String> runningCommands() => commandsInState(LedgerEntryState.running);

  /// RECEIVED(수신했으나 미시작) 목록 — CR: 클리어 후 fresh 재실행 대상.
  List<String> receivedCommands() => commandsInState(LedgerEntryState.received);

  Future<void> close() async {
    await _raf.close();
  }

  /// 로그 컴팩션(선택) — 최신 상태만 남겨 temp 로 쓰고 atomic rename swap.
  ///
  /// crash-safe: temp 완성·fsync 후에만 rename. 실패 시 원본 유지.
  Future<void> compact() async {
    final tmp = File('${_file.path}.tmp');
    final sink = tmp.openSync(mode: FileMode.write);
    try {
      for (final entry in _index.entries) {
        final rec = LedgerRecord(commandId: entry.key, state: entry.value, ts: nowIso());
        sink.writeStringSync('${jsonEncode(rec.toJson())}\n');
      }
      sink.flushSync();
    } finally {
      sink.closeSync();
    }
    await _raf.close();
    await tmp.rename(_file.path); // atomic swap.
    _raf = await _file.open(mode: FileMode.append);
  }
}
