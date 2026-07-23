/// 와이어 JSON 직렬화 헬퍼 — SoT 투영 불변식(§5-3) / includeIfNull:false 규칙(부록A P-4).
///
/// 순수 클래스 기반(freezed 미사용). freezed 없이도 계약을 지키는 두 규칙:
///   (P-4) 옵셔널 필드가 null 이면 **키 자체를 방출하지 않는다**(JSON 에 null 금지).
///   (P-3) createdAt/updatedAt/ts 는 서버가 준 ISO8601 string 을 **재포맷 없이 그대로 보존**한다.
library;

/// null 이 아닐 때만 키를 넣는다 — `includeIfNull:false` 등가(부록A P-4).
void putIfPresent(Map<String, Object?> map, String key, Object? value) {
  if (value != null) map[key] = value;
}
