/// 와이어 메시지 (command / status / heartbeat) — SoT §9. 세 와이어 모두 PII 미포함.
///
/// **양 언어 바이트 동일**(TS interface ↔ Dart 순수 클래스). freezed 대신 순수 클래스이나
/// includeIfNull:false 규칙(부록A P-4)을 `putIfPresent` 로 지킨다.
///
/// 합성 멱등키 규약(부록A P-2): `command.id` = `status.id` = `{orderId}:{attempt}`
///   — 콜론 구분·attempt 십진(zero-pad 금지). order.id 와 다르다.
library;

import 'pump_guard.dart' show StatusErrorCode;
import 'wire_json.dart';

/// 합성 멱등키 조립 — SoT §5-6 / 부록A P-2. `{orderId}:{attempt}` (콜론·zero-pad 없음).
String buildCommandId(String orderId, int attempt) => '$orderId:$attempt';

// ─────────────────────────────────────────────────────────────────────────────
// §9-1  command (서버 → pi · SSE snapshot 파생)
// ─────────────────────────────────────────────────────────────────────────────

/// recipe 스텝 — SoT §9-1. idx 0부터 오름차순 직렬.
class RecipeStep {
  const RecipeStep({
    required this.idx,
    required this.pumpAddr,
    required this.flavor,
    required this.volume,
  });

  final int idx;
  final int pumpAddr;
  final String flavor;

  /// µL.
  final num volume;

  factory RecipeStep.fromJson(Map<String, Object?> j) => RecipeStep(
        idx: (j['idx'] as num).toInt(),
        pumpAddr: (j['pumpAddr'] as num).toInt(),
        flavor: j['flavor'] as String,
        volume: j['volume'] as num,
      );

  Map<String, Object?> toJson() => {
        'idx': idx,
        'pumpAddr': pumpAddr,
        'flavor': flavor,
        'volume': volume,
      };
}

/// command — SoT §9-1.
///
/// `recipe == null` 이면 pi 가 recipeId(flavor)/fragranceResult(fragrance)로 해석(§9-1).
class Command {
  const Command({
    required this.id,
    required this.orderId,
    required this.attempt,
    required this.deviceId,
    required this.recipe,
    required this.traceId,
    required this.createdAt,
  });

  /// `{orderId}:{attempt}` — 합성 멱등키(order.id 아님·부록A P-2).
  final String id;
  final String orderId;

  /// int·최초 1·재시도마다 +1.
  final int attempt;

  /// 라우팅·pi 자기것만 소비(CS-08).
  final String deviceId;

  /// recipe steps | null.
  final List<RecipeStep>? recipe;
  final String traceId;

  /// ISO8601 (resync 기준·재포맷 금지·부록A P-3).
  final String createdAt;

  factory Command.fromJson(Map<String, Object?> j) {
    final Object? rawRecipe = j['recipe'];
    return Command(
      id: j['id'] as String,
      orderId: j['orderId'] as String,
      attempt: (j['attempt'] as num).toInt(),
      deviceId: j['deviceId'] as String,
      recipe: rawRecipe == null
          ? null
          : [
              for (final s in (rawRecipe as List))
                RecipeStep.fromJson((s as Map).cast<String, Object?>()),
            ],
      traceId: j['traceId'] as String,
      createdAt: j['createdAt'] as String,
    );
  }

  Map<String, Object?> toJson() => {
        'id': id,
        'orderId': orderId,
        'attempt': attempt,
        'deviceId': deviceId,
        // recipe 는 null 도 의미가 있으므로(§9-1 폴백 신호) 명시적으로 방출.
        'recipe': recipe?.map((s) => s.toJson()).toList(),
        'traceId': traceId,
        'createdAt': createdAt,
      };
}

// ─────────────────────────────────────────────────────────────────────────────
// §9-2  status (pi → 서버 · PATCH /api/dispenser/orders/[id] body)
// ─────────────────────────────────────────────────────────────────────────────

/// status report — SoT §9-2. phase→WireStatus 는 order_status.dart `phaseToWireStatus`.
class StatusReport {
  const StatusReport({
    required this.id,
    required this.phase,
    required this.stepK,
    required this.stepN,
    required this.errorCode,
    required this.requestId,
    required this.traceId,
    required this.updatedAt,
  });

  /// `{orderId}:{attempt}` (= command.id).
  final String id;

  /// "ACCEPTED" | "PROGRESS" | "COMPLETED" | "FAILED" — 단조·역행 금지.
  final String phase;
  final int stepK;
  final int stepN;

  /// 7종 enum | null.
  final StatusErrorCode? errorCode;

  /// uuid — 서버 dedup(OQ flush at-least-once).
  final String requestId;
  final String traceId;

  /// ISO8601 (재포맷 금지·부록A P-3).
  final String updatedAt;

  Map<String, Object?> toJson() {
    final map = <String, Object?>{
      'id': id,
      'phase': phase,
      'stepK': stepK,
      'stepN': stepN,
      // errorCode 는 null 도 의미(정상)이므로 명시 방출 — 서버 계약이 `ErrorCode | null`.
      'errorCode': errorCode?.wire,
      'requestId': requestId,
      'traceId': traceId,
      'updatedAt': updatedAt,
    };
    return map;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// §9-3  heartbeat (pi → 서버 · PATCH /api/dispenser/heartbeat)
// ─────────────────────────────────────────────────────────────────────────────

/// heartbeat request — SoT §9-3. ⚠️ traceId 없음(주문 무관·deviceId 상관).
///
/// 주기 30s(±jitter). online 판정 = 최근 3주기(90s) 내(서버 판정·pi 시계 미신뢰).
class Heartbeat {
  const Heartbeat({
    required this.deviceId,
    required this.queueDepth,
    this.engine,
    this.lastError,
  });

  final String deviceId;

  /// int·유휴=0.
  final int queueDepth;

  /// "sy01b" | null.
  final String? engine;

  /// 7종 | null.
  final StatusErrorCode? lastError;

  /// includeIfNull:false — engine/lastError 는 부재 시 키 방출 안 함(부록A P-4).
  Map<String, Object?> toJson() {
    final map = <String, Object?>{
      'deviceId': deviceId,
      'queueDepth': queueDepth,
    };
    putIfPresent(map, 'engine', engine);
    putIfPresent(map, 'lastError', lastError?.wire);
    return map;
  }
}
