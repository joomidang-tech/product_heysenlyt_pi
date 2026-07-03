/// 전이표 케이스 매트릭스 — SoT §4-2 그대로. 서버 `orderStatus.test.ts` 와 동일 통과가 목표.
///
/// 부록A P-1 게이트: 이 8셀 매트릭스가 TS 와 바이트 동일해야 계약 성립.
library;

import 'package:heysenlyt_pi/core/order_status.dart';
import 'package:test/test.dart';

void main() {
  group('evaluateTransition', () {
    test('from == to 는 noop(멱등)', () {
      for (final s in WireStatus.values) {
        expect(evaluateTransition(s, s), TransitionVerdict.noop);
      }
    });

    test('PENDING 전진은 모두 apply (COMPLETED 직행 허용 — F2)', () {
      expect(evaluateTransition(WireStatus.pending, WireStatus.processing),
          TransitionVerdict.apply);
      expect(evaluateTransition(WireStatus.pending, WireStatus.completed),
          TransitionVerdict.apply);
      expect(evaluateTransition(WireStatus.pending, WireStatus.failed),
          TransitionVerdict.apply);
    });

    test('PROCESSING → COMPLETED/FAILED 는 apply', () {
      expect(evaluateTransition(WireStatus.processing, WireStatus.completed),
          TransitionVerdict.apply);
      expect(evaluateTransition(WireStatus.processing, WireStatus.failed),
          TransitionVerdict.apply);
    });

    test('COMPLETED 는 terminal — 어떤 전진도 illegal (un-complete 금지)', () {
      expect(evaluateTransition(WireStatus.completed, WireStatus.pending),
          TransitionVerdict.illegal);
      expect(evaluateTransition(WireStatus.completed, WireStatus.processing),
          TransitionVerdict.illegal);
      expect(evaluateTransition(WireStatus.completed, WireStatus.failed),
          TransitionVerdict.illegal);
    });

    test('FAILED → PENDING 만 허용(운영자 재시도), 그 외 illegal', () {
      expect(evaluateTransition(WireStatus.failed, WireStatus.pending),
          TransitionVerdict.apply);
      expect(evaluateTransition(WireStatus.failed, WireStatus.processing),
          TransitionVerdict.illegal);
      expect(evaluateTransition(WireStatus.failed, WireStatus.completed),
          TransitionVerdict.illegal);
    });

    test('PROCESSING → PENDING(역행)은 illegal', () {
      expect(evaluateTransition(WireStatus.processing, WireStatus.pending),
          TransitionVerdict.illegal);
    });

    test('전체 4x4 매트릭스 — SoT §4-2 표 그대로', () {
      // 행=from, 열=to. (from, to) → 기대 verdict.
      const noop = TransitionVerdict.noop;
      const apply = TransitionVerdict.apply;
      const illegal = TransitionVerdict.illegal;
      final expected = <WireStatus, Map<WireStatus, TransitionVerdict>>{
        WireStatus.pending: {
          WireStatus.pending: noop,
          WireStatus.processing: apply,
          WireStatus.completed: apply,
          WireStatus.failed: apply,
        },
        WireStatus.processing: {
          WireStatus.pending: illegal,
          WireStatus.processing: noop,
          WireStatus.completed: apply,
          WireStatus.failed: apply,
        },
        WireStatus.completed: {
          WireStatus.pending: illegal,
          WireStatus.processing: illegal,
          WireStatus.completed: noop,
          WireStatus.failed: illegal,
        },
        WireStatus.failed: {
          WireStatus.pending: apply,
          WireStatus.processing: illegal,
          WireStatus.completed: illegal,
          WireStatus.failed: noop,
        },
      };
      for (final from in WireStatus.values) {
        for (final to in WireStatus.values) {
          expect(evaluateTransition(from, to), expected[from]![to],
              reason: '${from.wire} -> ${to.wire}');
        }
      }
    });
  });

  group('isWireStatus / fromWire', () {
    test('알려진 상태만 true', () {
      expect(isWireStatus('PENDING'), isTrue);
      expect(isWireStatus('COMPLETED'), isTrue);
      expect(isWireStatus('ERROR'), isFalse);
      expect(isWireStatus(null), isFalse);
      expect(isWireStatus(123), isFalse);
    });
  });

  group('phaseToWireStatus — SoT §4-5 / §9-2', () {
    test('ACCEPTED/PROGRESS → PROCESSING · COMPLETED → COMPLETED · FAILED → FAILED', () {
      expect(phaseToWireStatus(DispensePhase.accepted), WireStatus.processing);
      expect(phaseToWireStatus(DispensePhase.progress), WireStatus.processing);
      expect(phaseToWireStatus(DispensePhase.completed), WireStatus.completed);
      expect(phaseToWireStatus(DispensePhase.failed), WireStatus.failed);
    });
  });
}
