/// DispenserOrderDTO 미러 회귀 — SoT §5 (부록A P-2/P-3/P-4).
///
/// includeIfNull:false · createdAt 재포맷 금지 · 마이그레이션 폴백 · command 파생 키.
library;

import 'package:heysenlyt_pi/core/dispenser_order_dto.dart';
import 'package:heysenlyt_pi/core/order_status.dart';
import 'package:test/test.dart';

void main() {
  group('fromJson/toJson roundtrip — flavor', () {
    test('필수 + net-new 3필드 파싱', () {
      final dto = DispenserOrderDto.fromJson({
        'id': 'ord123',
        'mode': 'flavor',
        'status': 'PENDING',
        'orderNumber': 42,
        'language': 'ko',
        'createdAt': '2026-07-03T12:34:56.789Z',
        'isDeleted': false,
        'isDemo': false,
        'deviceId': 'store-A',
        'attempt': 2,
        'traceId': 'trace-uuid',
        'flavor': {'recipeId': '럭퓨-01'},
      });
      expect(dto.status, WireStatus.pending);
      expect(dto.deviceId, 'store-A');
      expect(dto.attempt, 2);
      expect(dto.flavor!.recipeId, '럭퓨-01');
      expect(dto.fragrance, isNull);
    });

    test('createdAt 재포맷 금지(부록A P-3) — 밀리초·Z 그대로 보존', () {
      const iso = '2026-07-03T12:34:56.789Z';
      final dto = _minDto(createdAt: iso);
      expect(dto.createdAt, iso);
      expect(dto.toJson()['createdAt'], iso);
    });
  });

  group('includeIfNull:false(부록A P-4)', () {
    test('옵셔널 부재 시 키 자체 미방출', () {
      final json = _minDto().toJson();
      expect(json.containsKey('userAge'), isFalse);
      expect(json.containsKey('userGender'), isFalse);
      // 필수 net-new 는 항상 존재.
      expect(json.containsKey('deviceId'), isTrue);
      expect(json.containsKey('attempt'), isTrue);
      expect(json.containsKey('traceId'), isTrue);
    });

    test('옵셔널 존재 시 키 방출', () {
      final json = DispenserOrderDto.fromJson({
        'id': 'o',
        'mode': 'flavor',
        'status': 'PENDING',
        'orderNumber': 1,
        'language': 'ko',
        'createdAt': '2026-07-03T00:00:00.000Z',
        'isDeleted': false,
        'isDemo': false,
        'deviceId': 'd',
        'attempt': 1,
        'traceId': 't',
        'userAge': 30,
        'userGender': 'male',
        'flavor': {'recipeId': 'r'},
      }).toJson();
      expect(json['userAge'], 30);
      expect(json['userGender'], 'male');
    });
  });

  group('마이그레이션 폴백(§5-4)', () {
    test('net-new 3필드 부재 구버전 문서 — non-null 보호', () {
      final dto = DispenserOrderDto.fromJson({
        'id': 'legacy',
        'mode': 'fragrance',
        'status': 'COMPLETED',
        'orderNumber': 7,
        'language': 'en',
        'createdAt': '2026-01-01T00:00:00.000Z',
        'isDeleted': false,
        'isDemo': false,
        // deviceId/attempt/traceId 부재.
        'fragrance': {'name': 'Rose'},
      });
      expect(dto.deviceId, kDefaultDeviceId);
      expect(dto.attempt, 1);
      expect(dto.traceId, '');
      expect(dto.fragrance!.name, 'Rose');
    });
  });

  group('isDeleted/isDemo === true 강제(§5-3.5)', () {
    test('truthy 아닌 값 → false', () {
      final dto = DispenserOrderDto.fromJson({
        'id': 'o',
        'mode': 'flavor',
        'status': 'PENDING',
        'orderNumber': 1,
        'language': 'ko',
        'createdAt': '2026-07-03T00:00:00.000Z',
        // isDeleted/isDemo 부재 → false.
        'deviceId': 'd',
        'attempt': 1,
        'traceId': 't',
        'flavor': {'recipeId': 'r'},
      });
      expect(dto.isDeleted, isFalse);
      expect(dto.isDemo, isFalse);
    });
  });

  group('command 파생 키(§5-6·부록A P-2)', () {
    test('commandId = {id}:{attempt} — 콜론·zero-pad 없음', () {
      final dto = _minDto();
      expect(dto.commandId, 'ord:1');
      final dto2 = DispenserOrderDto.fromJson({
        'id': 'ord',
        'mode': 'flavor',
        'status': 'PENDING',
        'orderNumber': 1,
        'language': 'ko',
        'createdAt': '2026-07-03T00:00:00.000Z',
        'isDeleted': false,
        'isDemo': false,
        'deviceId': 'd',
        'attempt': 12,
        'traceId': 't',
        'flavor': {'recipeId': 'r'},
      });
      expect(dto2.commandId, 'ord:12');
    });
  });

  group('PII 봉인(§5-2) — 구조적 부재', () {
    test('uid/userName 키가 있어도 파싱·직렬화에 흡수되지 않음', () {
      final dto = DispenserOrderDto.fromJson({
        'id': 'o',
        'mode': 'flavor',
        'status': 'PENDING',
        'orderNumber': 1,
        'language': 'ko',
        'createdAt': '2026-07-03T00:00:00.000Z',
        'isDeleted': false,
        'isDemo': false,
        'deviceId': 'd',
        'attempt': 1,
        'traceId': 't',
        'uid': 'SECRET-UID',
        'userName': '홍길동',
        'flavor': {'recipeId': 'r'},
      });
      final json = dto.toJson();
      expect(json.containsKey('uid'), isFalse);
      expect(json.containsKey('userName'), isFalse);
    });
  });
}

DispenserOrderDto _minDto({String createdAt = '2026-07-03T00:00:00.000Z'}) =>
    DispenserOrderDto.fromJson({
      'id': 'ord',
      'mode': 'flavor',
      'status': 'PENDING',
      'orderNumber': 1,
      'language': 'ko',
      'createdAt': createdAt,
      'isDeleted': false,
      'isDemo': false,
      'deviceId': 'd',
      'attempt': 1,
      'traceId': 't',
      'flavor': {'recipeId': 'r'},
    });
