/// 디스펜서 Bearer 세션 토큰 (수신·검증측 미러) — SoT §7-4 (축 B).
///
/// **정본 = 서버 `dispenserSession.ts`.** pi 는 POST /api/dispenser/login 으로 토큰을
/// **발급받아 저장·전송**만 한다(서명·검증은 서버). 따라서 이 파일은:
///   - 토큰 payload 구조/키 순서(sub,role,iat,exp·부록A P-5)를 **알기만** 하고,
///   - 서명은 pi 가 직접 만들지 않는다(토큰은 opaque). getSessionSecret 이 pi 에 없음(§7-8).
///
/// ⚠️ 부록A P-5: 토큰은 pi 에서 **opaque 로만** 다룬다 — payload 를 재직렬화하려 하지 말 것
///    (JSON 키 순서 한 글자만 달라도 서명 깨짐). 만료(exp) 판단을 위해 **파싱만** 허용.
///
/// crypto(HMAC 검증)는 pi 범위 밖(서버 책임) — 이번 웨이브는 payload 파싱·만료 판단만.
library;

import 'dart:convert';

/// 디스펜서 role 상수 — SoT §7-4.
const String dispenserRole = 'dispenser';

/// 세션 TTL(초) — 12h (§7-2). 참고용(발급은 서버).
const int dispenserSessionTtlSeconds = 60 * 60 * 12;

/// 서명 도메인 prefix — SoT §7-4 / 부록A P-6. **참고 상수**(pi 는 서명 안 함).
/// 교차 재사용 차단의 근거 — 축 A(prefix 없음)·축 B(dispenser)·축 C(operator).
const String dispenserSigDomain = 'dispenser-session:v1:';

/// 토큰 payload(읽기 전용) — 키 순서 sub,role,iat,exp (부록A P-5, opaque 원칙).
class DispenserTokenPayload {
  const DispenserTokenPayload({
    required this.sub,
    required this.role,
    required this.iat,
    required this.exp,
  });

  final String sub;
  final String role;
  final int iat;
  final int exp;
}

/// 로컬 만료 사전판단(선택) — opaque 토큰의 exp 만 base64url payload 에서 읽는다.
///
/// ⚠️ 이것은 **서버 검증의 대체가 아니다**. 서명 검증은 서버가 수행한다.
/// pi 는 만료 임박 시 재로그인 트리거를 위해 exp 만 참조한다(네트워크 절약).
/// 형식·role 이 어긋나면 null(방어).
DispenserTokenPayload? peekTokenPayload(String? token) {
  if (token == null || token.isEmpty) return null;
  final int dot = token.lastIndexOf('.');
  if (dot <= 0) return null;
  final String payloadB64 = token.substring(0, dot);
  try {
    final String jsonStr = utf8.decode(base64Url.decode(_pad(payloadB64)));
    final Object? decoded = jsonDecode(jsonStr);
    if (decoded is! Map) return null;
    final Object? sub = decoded['sub'];
    final Object? role = decoded['role'];
    final Object? exp = decoded['exp'];
    final Object? iat = decoded['iat'];
    if (sub is! String || sub.isEmpty) return null;
    if (role is! String || role != dispenserRole) return null;
    if (exp is! int) return null;
    return DispenserTokenPayload(
      sub: sub,
      role: role,
      iat: iat is int ? iat : 0,
      exp: exp,
    );
  } catch (_) {
    return null;
  }
}

/// exp(epoch sec) 가 now 이하이면 만료 — SoT §7-4 (strict, =이면 만료).
bool isTokenExpired(DispenserTokenPayload payload, {int? nowSeconds}) {
  final int now = nowSeconds ?? DateTime.now().millisecondsSinceEpoch ~/ 1000;
  return payload.exp <= now;
}

/// base64url 패딩 복원(Dart base64Url.decode 는 패딩 필요).
String _pad(String b64url) {
  final int mod = b64url.length % 4;
  if (mod == 0) return b64url;
  return b64url + '=' * (4 - mod);
}
