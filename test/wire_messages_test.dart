/// 와이어 메시지 회귀 — SoT §9 (부록A P-2/P-4).
///
/// 합성 멱등키 조립 · command recipe null 폴백 신호 · heartbeat includeIfNull:false.
library;

import 'package:heysenlyt_pi/core/pump_guard.dart' show StatusErrorCode;
import 'package:heysenlyt_pi/core/wire_messages.dart';
import 'package:test/test.dart';

void main() {
  group('buildCommandId(부록A P-2)', () {
    test('{orderId}:{attempt} — 콜론·zero-pad 없음', () {
      expect(buildCommandId('ord', 1), 'ord:1');
      expect(buildCommandId('ord', 12), 'ord:12');
      expect(buildCommandId('a:b', 3), 'a:b:3'); // orderId 에 콜론 있어도 lastIndexOf 규칙은 서버측
    });
  });

  group('Command roundtrip(§9-1)', () {
    test('recipe steps 파싱·직렬화', () {
      final c = Command.fromJson({
        'id': 'ord:1',
        'orderId': 'ord',
        'attempt': 1,
        'deviceId': 'store-A',
        'recipe': [
          {'idx': 0, 'pumpAddr': 1, 'flavor': 'rose', 'volume': 100},
          {'idx': 1, 'pumpAddr': 2, 'flavor': 'musk', 'volume': 50},
        ],
        'traceId': 't',
        'createdAt': '2026-07-03T00:00:00.000Z',
      });
      expect(c.recipe, isNotNull);
      expect(c.recipe!.length, 2);
      expect(c.recipe![0].idx, 0);
      expect(c.recipe![1].volume, 50);
      final json = c.toJson();
      expect((json['recipe'] as List).length, 2);
    });

    test('recipe = null 폴백 신호 보존(§9-1)', () {
      final c = Command.fromJson({
        'id': 'ord:1',
        'orderId': 'ord',
        'attempt': 1,
        'deviceId': 'd',
        'recipe': null,
        'traceId': 't',
        'createdAt': '2026-07-03T00:00:00.000Z',
      });
      expect(c.recipe, isNull);
      // recipe null 은 의미가 있으므로 키가 남아야 한다(pi 가 recipeId/fragranceResult 로 해석).
      expect(c.toJson().containsKey('recipe'), isTrue);
      expect(c.toJson()['recipe'], isNull);
    });
  });

  group('Heartbeat includeIfNull:false(§9-3·부록A P-4)', () {
    test('engine/lastError 부재 시 키 미방출', () {
      final hb = Heartbeat(deviceId: 'd', queueDepth: 0).toJson();
      expect(hb['deviceId'], 'd');
      expect(hb['queueDepth'], 0);
      expect(hb.containsKey('engine'), isFalse);
      expect(hb.containsKey('lastError'), isFalse);
    });

    test('engine/lastError 존재 시 키 방출', () {
      final hb = Heartbeat(
        deviceId: 'd',
        queueDepth: 2,
        engine: 'sy01b',
        lastError: StatusErrorCode.engineTimeout,
      ).toJson();
      expect(hb['engine'], 'sy01b');
      expect(hb['lastError'], 'ENGINE_TIMEOUT');
    });
  });

  group('StatusReport(§9-2)', () {
    test('errorCode null 도 명시 방출(계약 ErrorCode|null)', () {
      final r = StatusReport(
        id: 'ord:1',
        phase: 'PROGRESS',
        stepK: 3,
        stepN: 10,
        errorCode: null,
        requestId: 'req',
        traceId: 't',
        updatedAt: '2026-07-03T00:00:00.000Z',
      ).toJson();
      expect(r.containsKey('errorCode'), isTrue);
      expect(r['errorCode'], isNull);
      expect(r['phase'], 'PROGRESS');
    });

    test('errorCode 7종 wire 문자열', () {
      final r = StatusReport(
        id: 'ord:1',
        phase: 'FAILED',
        stepK: 3,
        stepN: 10,
        errorCode: StatusErrorCode.partialDispense,
        requestId: 'req',
        traceId: 't',
        updatedAt: '2026-07-03T00:00:00.000Z',
      ).toJson();
      expect(r['errorCode'], 'PARTIAL_DISPENSE');
    });
  });
}
