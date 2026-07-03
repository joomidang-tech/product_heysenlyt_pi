/// PumpGuard 회귀 — SoT §6 (byte-parity 안전 급소·부록A P-7/P-8).
///
/// builtin 수치표 강제 · custom 단조성 2줄 순서 · steps=round half-up · fragrance ×1000 · 검산값.
library;

import 'package:heysenlyt_pi/core/pump_guard.dart';
import 'package:test/test.dart';

void main() {
  group('clampPumpPreset — builtin 정식 수치 강제(§6-2)', () {
    test('sy01b — 입력 수치 무시하고 표 그대로', () {
      final p = clampPumpPreset({
        'pumpPresetId': 'sy01b',
        // 아래 입력은 전부 무시되어야 한다(builtin 강제).
        'pumpFullStroke': 99999,
        'pumpMaxStartSpeedHz': 99999,
      });
      expect(p.pumpFullStroke, 12000);
      expect(p.pumpMaxStartSpeedHz, 1000);
      expect(p.pumpMaxTopSpeedHz, 6000);
      expect(p.pumpMaxCutoffSpeedHz, 5400);
      expect(p.pumpMaxSlope, 20);
      expect(p.pumpSyringeTypeCode, 200);
    });

    test('cavro_xlp6000 — 표 그대로', () {
      final p = clampPumpPreset({'pumpPresetId': 'cavro_xlp6000'});
      expect(p.pumpFullStroke, 6000);
      expect(p.pumpMaxStartSpeedHz, 8000);
      expect(p.pumpMaxTopSpeedHz, 48000);
      expect(p.pumpMaxCutoffSpeedHz, 21600);
    });

    test('cavro_xcalibur — v/V/c 미확정은 SY-01B 하한(§6-2 * / O-12)', () {
      final p = clampPumpPreset({'pumpPresetId': 'cavro_xcalibur'});
      expect(p.pumpFullStroke, 3000);
      expect(p.pumpMaxStartSpeedHz, 1000); // SY-01B 하한
      expect(p.pumpMaxTopSpeedHz, 6000);
      expect(p.pumpMaxCutoffSpeedHz, 5400);
    });

    test('unknown id → sy01b 폴백(§6-3.3)', () {
      final p = clampPumpPreset({'pumpPresetId': 'nonsense_pump'});
      expect(p.pumpPresetId, 'sy01b');
      expect(p.pumpFullStroke, 12000);
    });

    test('null/누락 문서 → sy01b(§6-3.4)', () {
      final p = clampPumpPreset(null);
      expect(p.pumpPresetId, 'sy01b');
    });
  });

  group('clampPumpPreset — custom 절대상한 + 단조성(§6-3.2·부록A P-7)', () {
    test('절대상한 clamp', () {
      final p = clampPumpPreset({
        'pumpPresetId': 'custom',
        'pumpFullStroke': 999999, // → 96000
        'pumpMaxStartSpeedHz': 0, // → 1
        'pumpMaxTopSpeedHz': 999999, // → 48000
        'pumpMaxCutoffSpeedHz': 999999, // → 48000
        'pumpMaxSlope': 999, // → 40
        'pumpSyringeTypeCode': 9999, // → 999
      });
      expect(p.pumpFullStroke, 96000);
      expect(p.pumpMaxTopSpeedHz, 48000);
      expect(p.pumpMaxSlope, 40);
      expect(p.pumpSyringeTypeCode, 999);
      // 단조성: v ≤ c ≤ V. v 입력 1 → clamp 후 v≤c 유지.
      expect(p.pumpMaxStartSpeedHz <= p.pumpMaxCutoffSpeedHz, isTrue);
      expect(p.pumpMaxCutoffSpeedHz <= p.pumpMaxTopSpeedHz, isTrue);
    });

    test('단조성 2줄 순서 — c=min(max(c,v),V); v=min(v,c) 경계 결과(부록A P-7)', () {
      // v=5000, V=1000, c=100 (역전 입력) → c=min(max(100,5000),1000)=1000; v=min(5000,1000)=1000.
      final p = clampPumpPreset({
        'pumpPresetId': 'custom',
        'pumpFullStroke': 12000,
        'pumpMaxStartSpeedHz': 5000,
        'pumpMaxTopSpeedHz': 1000,
        'pumpMaxCutoffSpeedHz': 100,
        'pumpMaxSlope': 20,
        'pumpSyringeTypeCode': 200,
      });
      expect(p.pumpMaxCutoffSpeedHz, 1000, reason: 'c = min(max(c,v),V)');
      expect(p.pumpMaxStartSpeedHz, 1000, reason: 'v = min(v,c)');
      expect(p.pumpMaxTopSpeedHz, 1000);
    });
  });

  group('SyringeSpec 파생 — 검산(§6-4)', () {
    test('12000 × 100 ÷ 1250 = 960 steps', () {
      final spec = SyringeSpec(pumpFullStroke: 12000, syringeCapacityMl: 1.25);
      expect(spec.stepsForVolumeUl(100), 960);
    });

    test('식향 1.25mL stepsPerMl = 9600', () {
      final spec = SyringeSpec(pumpFullStroke: 12000, syringeCapacityMl: 1.25);
      expect(spec.stepsPerMl, 9600);
      expect(spec.maxVolumeUl, 1250);
    });

    test('향장향 0.5mL stepsPerMl = 24000', () {
      // fullStroke 12000, capacity 0.5mL → 12000/0.5 = 24000 (Code 11 방지 검산값).
      final spec = SyringeSpec(pumpFullStroke: 12000, syringeCapacityMl: 0.5);
      expect(spec.stepsPerMl, 24000);
      expect(spec.maxVolumeUl, 500);
    });

    test('round half-up 고정(부록A P-8)', () {
      // fullStroke 12000, cap 1.25mL: steps = 12000*vol/1250.
      // vol 이 .5 스텝 경계를 만들도록: 12000*V/1250 = k.5 → V = 1250*(k+0.5)/12000.
      final spec = SyringeSpec(pumpFullStroke: 12000, syringeCapacityMl: 1.25);
      // 62.5µL → 12000*62.5/1250 = 600.0 정수. 대신 소수 경계 확인:
      // 6.510416...µL → 12000*x/1250 = 62.5 → x = 62.5*1250/12000 = 6.510416...
      final v = 62.5 * 1250 / 12000; // steps 이론상 62.5 → half-up 63
      expect(spec.stepsForVolumeUl(v), 63);
    });
  });

  group('fragrance 단위 정규화(§6-6)', () {
    test('amountMl × 1000 = volumeUl', () {
      expect(fragranceMlToUl(0.1), 100);
      expect(fragranceMlToUl(0.5), 500);
    });
  });

  group('resolveSyringeCapacityMl — 이산값 폴백(§6-1/O-15)', () {
    test('유효집합 밖 → 모드 기본', () {
      expect(resolveSyringeCapacityMl(1.25, isFlavor: true), 1.25);
      expect(resolveSyringeCapacityMl(0.5, isFlavor: false), 0.5);
      expect(resolveSyringeCapacityMl(0.99, isFlavor: true), 1.25); // 폴백
      expect(resolveSyringeCapacityMl(0.99, isFlavor: false), 0.5); // 폴백
      expect(resolveSyringeCapacityMl(null, isFlavor: true), 1.25);
    });
  });

  group('안전 게이트(§6-4)', () {
    test('0 < volumeUl ≤ maxVolumeUl', () {
      final spec = SyringeSpec(pumpFullStroke: 12000, syringeCapacityMl: 1.25);
      expect(isVolumeWithinGate(100, spec), isTrue);
      expect(isVolumeWithinGate(1250, spec), isTrue);
      expect(isVolumeWithinGate(0, spec), isFalse);
      expect(isVolumeWithinGate(-1, spec), isFalse);
      expect(isVolumeWithinGate(1251, spec), isFalse); // Code 11 방지
    });
  });

  group('classifyEngineErrorCode(§6-7)', () {
    test('0=normal · 1/7/11/15=transient · 2/3/9/10=permanent', () {
      expect(classifyEngineErrorCode(0), EngineErrorClass.normal);
      for (final c in [1, 7, 11, 15]) {
        expect(classifyEngineErrorCode(c), EngineErrorClass.transient);
      }
      for (final c in [2, 3, 9, 10]) {
        expect(classifyEngineErrorCode(c), EngineErrorClass.permanent);
      }
    });
  });
}
