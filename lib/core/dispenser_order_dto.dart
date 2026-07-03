/// DispenserOrder DTO (수신측 미러) — SoT §5.
///
/// **정본 = 서버 `dispenserOrder.ts` `toDispenserOrderDTO`.** 서버가 이 형상으로 투영해
/// SSE snapshot 으로 보내면 pi 가 이 클래스로 파싱한다. 양 언어 바이트 동일.
///
/// pi 는 **소비자**(투영을 만들지 않는다) — PII 봉인은 서버가 담당하고(§5-2), pi 는 구조적으로
/// uid/userName/연락처/IP/sessionId 를 볼 수 없다. 이 클래스에는 그 필드가 존재하지 않는다.
///
/// includeIfNull:false(부록A P-4): 옵셔널 부재 = null 로 수용, 직렬화 시 키 재부재로 재현.
/// createdAt(부록A P-3): 서버가 준 ISO8601 string 을 **재포맷 없이 보존**.
library;

import 'order_status.dart' show WireStatus;
import 'wire_json.dart';

/// net-new 3필드 마이그레이션 폴백 — SoT §5-4.
///
/// v1.1.0 계승 주문(deviceId/attempt/traceId 부재)의 non-null 파싱 보호.
/// 실제 값은 서버 투영이 채우는 것이 정상 경로이며, 이 상수는 방어선일 뿐이다.
const String kDefaultDeviceId = 'default';

/// flavor 서브객체 — SoT §5-1. content 는 형상 보존만(pi 는 평탄미러 우선 읽기·O-8).
class FlavorSub {
  const FlavorSub({required this.recipeId, this.flavorContent, this.vapiResult});

  final String recipeId; // 없으면 ""
  final Map<String, Object?>? flavorContent; // LocalizedContent<FlavorContent>
  final Map<String, Object?>? vapiResult; // FlavorVapiResult

  factory FlavorSub.fromJson(Map<String, Object?> j) => FlavorSub(
        recipeId: (j['recipeId'] as String?) ?? '',
        flavorContent: _asMap(j['flavorContent']),
        vapiResult: _asMap(j['vapiResult']),
      );

  Map<String, Object?> toJson() {
    final map = <String, Object?>{'recipeId': recipeId};
    putIfPresent(map, 'flavorContent', flavorContent);
    putIfPresent(map, 'vapiResult', vapiResult);
    return map;
  }
}

/// fragrance 서브객체 — SoT §5-1 (@deprecated 평탄미러 포함·Flutter 호환).
class FragranceSub {
  const FragranceSub({
    this.fragranceContent,
    this.fragranceResult,
    this.name,
    this.nameKo,
    this.story,
    this.storyKo,
    this.description,
  });

  final Map<String, Object?>? fragranceContent;
  final Map<String, Object?>? fragranceResult;
  final String? name;
  final String? nameKo;
  final String? story;
  final String? storyKo;
  final String? description;

  factory FragranceSub.fromJson(Map<String, Object?> j) => FragranceSub(
        fragranceContent: _asMap(j['fragranceContent']),
        fragranceResult: _asMap(j['fragranceResult']),
        name: j['name'] as String?,
        nameKo: j['nameKo'] as String?,
        story: j['story'] as String?,
        storyKo: j['storyKo'] as String?,
        description: j['description'] as String?,
      );

  Map<String, Object?> toJson() {
    final map = <String, Object?>{};
    putIfPresent(map, 'fragranceContent', fragranceContent);
    putIfPresent(map, 'fragranceResult', fragranceResult);
    putIfPresent(map, 'name', name);
    putIfPresent(map, 'nameKo', nameKo);
    putIfPresent(map, 'story', story);
    putIfPresent(map, 'storyKo', storyKo);
    putIfPresent(map, 'description', description);
    return map;
  }
}

/// DispenserOrderDTO — SoT §5-5 타입 시그니처 바이트 동일.
class DispenserOrderDto {
  const DispenserOrderDto({
    required this.id,
    required this.mode,
    required this.status,
    required this.orderNumber,
    required this.language,
    required this.createdAt,
    required this.isDeleted,
    required this.isDemo,
    required this.deviceId,
    required this.attempt,
    required this.traceId,
    this.userAge,
    this.userGender,
    this.flavor,
    this.fragrance,
  });

  final String id;
  final String mode; // "flavor" | "fragrance"
  final WireStatus status;
  final int orderNumber;
  final String language; // "ko" | "en" | "ja" | "vi"
  final String createdAt; // ISO8601, 항상 (재포맷 금지)
  final bool isDeleted;
  final bool isDemo;

  // net-new (필수·§5-1·O-5) — 마이그레이션 폴백(§5-4)으로 non-null 보장.
  final String deviceId;
  final int attempt;
  final String traceId;

  // 비식별(옵셔널) — 값 있을 때만 키 존재.
  final int? userAge;
  final String? userGender; // "male" | "female"

  final FlavorSub? flavor; // mode == "flavor" 일 때만
  final FragranceSub? fragrance; // mode == "fragrance" 일 때만

  /// 서버 투영 JSON 파싱 — 마이그레이션 폴백(§5-4) 적용(구버전 문서 non-null 보호).
  factory DispenserOrderDto.fromJson(Map<String, Object?> j) {
    final String mode = j['mode'] as String;
    return DispenserOrderDto(
      id: j['id'] as String,
      mode: mode,
      // status 는 와이어 문자열 → enum. 4종 외면 방어적으로 FAILED coercion 하지 않고 파싱 실패
      // 대신 서버가 항상 유효값을 보낸다는 계약을 신뢰하되, 알 수 없으면 pending 으로 두지 않고
      // 명시적으로 예외를 던져 오염을 조기에 드러낸다.
      status: WireStatus.fromWire(j['status']) ??
          (throw FormatException('unknown status: ${j['status']}')),
      orderNumber: (j['orderNumber'] as num).toInt(),
      language: j['language'] as String,
      createdAt: j['createdAt'] as String,
      isDeleted: j['isDeleted'] == true,
      isDemo: j['isDemo'] == true,
      // §5-4 마이그레이션 폴백: 구버전 문서에 net-new 3필드 부재 시 기본값.
      deviceId: (j['deviceId'] as String?) ?? kDefaultDeviceId,
      attempt: (j['attempt'] as num?)?.toInt() ?? 1,
      traceId: (j['traceId'] as String?) ?? '',
      userAge: (j['userAge'] as num?)?.toInt(),
      userGender: j['userGender'] as String?,
      flavor: mode == 'flavor' && j['flavor'] is Map
          ? FlavorSub.fromJson((j['flavor'] as Map).cast<String, Object?>())
          : null,
      fragrance: mode == 'fragrance' && j['fragrance'] is Map
          ? FragranceSub.fromJson((j['fragrance'] as Map).cast<String, Object?>())
          : null,
    );
  }

  /// 직렬화 — 서버 DTO 형상 재현(§5-3): 옵셔널 부재 시 키 재부재(includeIfNull:false).
  Map<String, Object?> toJson() {
    final map = <String, Object?>{
      'id': id,
      'mode': mode,
      'status': status.wire,
      'orderNumber': orderNumber,
      'language': language,
      'createdAt': createdAt,
      'isDeleted': isDeleted,
      'isDemo': isDemo,
      'deviceId': deviceId,
      'attempt': attempt,
      'traceId': traceId,
    };
    putIfPresent(map, 'userAge', userAge);
    putIfPresent(map, 'userGender', userGender);
    putIfPresent(map, 'flavor', flavor?.toJson());
    putIfPresent(map, 'fragrance', fragrance?.toJson());
    return map;
  }

  /// command 파생 키 — SoT §5-6. `{id}:{attempt}`.
  String get commandId => '$id:$attempt';
}

Map<String, Object?>? _asMap(Object? v) =>
    v is Map ? v.cast<String, Object?>() : null;
