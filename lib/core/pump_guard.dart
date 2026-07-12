/// 펌프 프리셋 clamp + 부피→스텝 파생 — SoT §6 (byte-parity 안전 급소).
///
/// **바이트 동일 축 = 서버 `settingsClamp.ts`(TS·PumpGuard 부분) ↔ 이 파일(Dart).**
/// P0 HW 안전 — 식향 Code 11(플런저 오버로드·과다흡입) 재발 방지. 근본원인은
/// "하드코딩 24000 vs 파생 9600"의 2.5배 불일치였다(SoT 서두).
///
/// ⚠️ SoT §6 서두: 현재 web 에 이 축의 서버쪽 절반이 빠져 있다. 이 Dart 포트는 **SoT §6 수치표·
///   알고리즘·라운딩·단조성 보정 순서**를 정본으로 삼는다. 서버 복원 시 이 상수·순서에 맞춘다.
///   (부록A P-7: clampPumpPreset custom 단조성 2줄 순서 / P-8: steps=round half-up·fragrance ×1000)
library;

/// PumpPreset 7필드 — SoT §6-1 (고정 필드명·타입·순서).
class PumpPreset {
  const PumpPreset({
    required this.pumpPresetId,
    required this.pumpFullStroke,
    required this.pumpMaxStartSpeedHz,
    required this.pumpMaxTopSpeedHz,
    required this.pumpMaxCutoffSpeedHz,
    required this.pumpMaxSlope,
    required this.pumpSyringeTypeCode,
  });

  /// ∈ {sy01b, cavro_xlp6000, cavro_xcalibur, custom}.
  final String pumpPresetId;

  /// 풀스트로크.
  final int pumpFullStroke;

  /// v 상한(start speed).
  final int pumpMaxStartSpeedHz;

  /// V 상한(top speed).
  final int pumpMaxTopSpeedHz;

  /// c 상한(cutoff speed).
  final int pumpMaxCutoffSpeedHz;

  /// L 상한(slope).
  final int pumpMaxSlope;

  /// 스톨 서브코드 U<code>.
  final int pumpSyringeTypeCode;
}

/// 빌트인 프리셋 정식 수치표 — SoT §6-2 (입력 무시·강제 · 바이트 동일 SoT).
///
/// XCalibur 의 v/V/c 는 미확정(§6-2 `*`, O-12) → **보수적 SY-01B 하한**(1000/6000/5400) 채택.
/// 속도 clamp 는 낮을수록 안전.
const Map<String, PumpPreset> pumpPresets = {
  'sy01b': PumpPreset(
    pumpPresetId: 'sy01b',
    pumpFullStroke: 12000,
    pumpMaxStartSpeedHz: 1000,
    pumpMaxTopSpeedHz: 6000,
    pumpMaxCutoffSpeedHz: 5400,
    pumpMaxSlope: 20,
    pumpSyringeTypeCode: 200,
  ),
  'cavro_xlp6000': PumpPreset(
    pumpPresetId: 'cavro_xlp6000',
    pumpFullStroke: 6000,
    pumpMaxStartSpeedHz: 8000,
    pumpMaxTopSpeedHz: 48000,
    pumpMaxCutoffSpeedHz: 21600,
    pumpMaxSlope: 20,
    pumpSyringeTypeCode: 200, // 서버 pumpGuard.ts U200 (v1.1.0 확정 — 구 이관본 0은 stale 버그, 2026-07-12 정정)
  ),
  'cavro_xcalibur': PumpPreset(
    pumpPresetId: 'cavro_xcalibur',
    pumpFullStroke: 3000,
    pumpMaxStartSpeedHz: 1000, // * SY-01B 하한(미확정·보수적)
    pumpMaxTopSpeedHz: 6000, // * SY-01B 하한
    pumpMaxCutoffSpeedHz: 5400, // * SY-01B 하한
    pumpMaxSlope: 20,
    pumpSyringeTypeCode: 200, // 서버 pumpGuard.ts U200 (v1.1.0 확정 — 구 이관본 0은 stale 버그, 2026-07-12 정정)
  ),
};

/// custom 절대상한 — SoT §6-3.
const int _customStrokeMin = 100;
const int _customStrokeMax = 96000;
const int _customSpeedMin = 1;
const int _customSpeedMax = 48000;
const int _customSlopeMin = 1;
const int _customSlopeMax = 40;
const int _customTypeMin = 0;
const int _customTypeMax = 999;

/// 유효 syringe 용량 이산값(mL) — v1.1.0 서버 pumpGuard.ts VALID_SYRINGE_ML 정본 **9종**.
///   (구 이관본 4종은 stale — 2026-07-12 서버 SoT에 맞춰 정정. Python 배포본과 동일.)
///
/// (double 은 primitive equality 가 없어 const Set 불가 — final 런타임 집합.)
final Set<double> validSyringeCapacitiesMl = <double>{
  0.025,
  0.05,
  0.1,
  0.25,
  0.5,
  1.0,
  1.25,
  2.5,
  5.0,
};

/// 정수 clamp(round 후 [min,max]) — TS `clampInt`. NaN/누락 → fallback.
///
/// round = half-up(양수 도메인 JS `Math.round` ↔ Dart `round()` 실질 동일·SoT §6-4/O-14).
int _clampInt(Object? v, int min, int max, int fallback) {
  final num? n = v is num ? v : num.tryParse('$v');
  if (n == null || !n.isFinite) return fallback;
  final int r = n.round();
  return r < min ? min : (r > max ? max : r);
}

/// clampPumpPreset(cfg) — SoT §6-3 (서버 ↔ pi 동일 알고리즘).
///
/// 1) builtin(sy01b|cavro_xlp6000|cavro_xcalibur) → 표의 정식 수치 그대로(입력 전부 무시).
/// 2) custom → 절대상한 clamp + 속도 단조성 강제(⚠️ 2줄 순서 고정·부록A P-7).
/// 3) unknown id → sy01b 폴백.
/// 4) 미설정/누락 → sy01b 프리셋(호출 측이 null 전달).
PumpPreset clampPumpPreset(Map<String, Object?>? cfg) {
  final Object? rawId = cfg?['pumpPresetId'];
  final String id = rawId is String ? rawId : 'sy01b';

  // 1) builtin — 정식 수치 강제(입력 수치 전부 무시).
  final PumpPreset? builtin = pumpPresets[id];
  if (builtin != null && id != 'custom') return builtin;

  // 3) unknown id(그리고 non-custom 이 아닌 알 수 없는 값) → sy01b 폴백.
  if (id != 'custom') return pumpPresets['sy01b']!;

  // 2) custom → 절대상한 clamp.
  final int stroke =
      _clampInt(cfg?['pumpFullStroke'], _customStrokeMin, _customStrokeMax, 12000);
  int v = _clampInt(cfg?['pumpMaxStartSpeedHz'], _customSpeedMin, _customSpeedMax, 1000);
  final int bigV = _clampInt(cfg?['pumpMaxTopSpeedHz'], _customSpeedMin, _customSpeedMax, 6000);
  int c = _clampInt(cfg?['pumpMaxCutoffSpeedHz'], _customSpeedMin, _customSpeedMax, 5400);
  final int slope = _clampInt(cfg?['pumpMaxSlope'], _customSlopeMin, _customSlopeMax, 20);
  final int typeCode = _clampInt(cfg?['pumpSyringeTypeCode'], _customTypeMin, _customTypeMax, 200);

  // 속도 단조성 강제 — ⚠️ 순서 고정(부록A P-7: 2줄 순서가 바이트 동일이어야 경계 입력 결과 일치).
  //   (SY-01B 제약 v ≤ c ≤ V)
  c = _min(_max(c, v), bigV);
  v = _min(v, c);

  return PumpPreset(
    pumpPresetId: 'custom',
    pumpFullStroke: stroke,
    pumpMaxStartSpeedHz: v,
    pumpMaxTopSpeedHz: bigV,
    pumpMaxCutoffSpeedHz: c,
    pumpMaxSlope: slope,
    pumpSyringeTypeCode: typeCode,
  );
}

int _min(int a, int b) => a < b ? a : b;
int _max(int a, int b) => a > b ? a : b;

/// syringeCapacityMl 이산값 검증 — SoT §6-1 / O-15.
///
/// 유효집합(1.25/0.5/2.5/5) 밖이면 **모드 기본값 폴백**(스냅 아님). flavor 1.25 / fragrance 0.5.
double resolveSyringeCapacityMl(Object? raw, {required bool isFlavor}) {
  final double fallback = isFlavor ? 1.25 : 0.5;
  if (raw is! num) return fallback;
  final double d = raw.toDouble();
  return validSyringeCapacitiesMl.contains(d) ? d : fallback;
}

/// 부피→스텝 파생(SyringeSpec) — SoT §6-4 (하드코딩 금지·파생이 SoT).
///
///   steps         = round( pumpFullStroke × volumeUl ÷ (syringeCapacityMl × 1000) )
///   stepsPerMl    = pumpFullStroke ÷ syringeCapacityMl
///   maxVolumeUl   = syringeCapacityMl × 1000   (per-pump 안전 게이트 상한)
///
/// 검산(§6-4): 12000 × 100 ÷ 1250 = 960 steps / 식향 1.25mL stepsPerMl=9600 /
///            향장향 0.5mL stepsPerMl=24000.
class SyringeSpec {
  SyringeSpec({required this.pumpFullStroke, required this.syringeCapacityMl});

  final int pumpFullStroke;
  final double syringeCapacityMl;

  /// per-pump 안전 게이트 상한(µL).
  double get maxVolumeUl => syringeCapacityMl * 1000;

  /// mL 당 스텝수.
  double get stepsPerMl => pumpFullStroke / syringeCapacityMl;

  /// 부피(µL) → 스텝수. round = half-up(양수 도메인·부록A P-8).
  int stepsForVolumeUl(double volumeUl) =>
      (pumpFullStroke * volumeUl / (syringeCapacityMl * 1000)).round();
}

/// fragrance 단위 정규화 — SoT §6-6. amountMl → volumeUl(µL). 미스매치 = Code 11.
/// (flavor volume 은 이미 µL 이므로 정규화 불필요.)
double fragranceMlToUl(double amountMl) => amountMl * 1000;

/// recipe 스텝 검증 게이트 — SoT §6-4 / §9-1.
///   0 < volumeUl ≤ maxVolumeUl · steps.length(=steps) ≥ 1. 위반 → CMD_VALIDATION_FAILED(drop).
bool isVolumeWithinGate(double volumeUl, SyringeSpec spec) =>
    volumeUl > 0 && volumeUl <= spec.maxVolumeUl;

/// EnginePort 에러코드 분류 — SoT §6-7.
enum EngineErrorClass { normal, transient, permanent }

/// 엔진 raw errorCode(정수) → 분류 — SoT §6-7.
///   0 = 정상 / 1·7·11·15·timeout = transient(R=3 재시도) / 2·3·9·10 = permanent(즉시중단 FAILED).
EngineErrorClass classifyEngineErrorCode(int code) {
  if (code == 0) return EngineErrorClass.normal;
  if (code == 1 || code == 7 || code == 11 || code == 15) return EngineErrorClass.transient;
  if (code == 2 || code == 3 || code == 9 || code == 10) return EngineErrorClass.permanent;
  // 미분류 코드는 보수적으로 permanent(안전측·즉시중단).
  return EngineErrorClass.permanent;
}

/// status.errorCode 7종 — SoT §6-7 / §9-2.
enum StatusErrorCode {
  cmdValidationFailed('CMD_VALIDATION_FAILED'),
  duplicateDropped('DUPLICATE_DROPPED'),
  engineTimeout('ENGINE_TIMEOUT'),
  engineErrorTransient('ENGINE_ERROR_TRANSIENT'),
  engineErrorPermanent('ENGINE_ERROR_PERMANENT'),
  partialDispense('PARTIAL_DISPENSE'),
  interrupted('INTERRUPTED');

  const StatusErrorCode(this.wire);
  final String wire;

  static StatusErrorCode? fromWire(Object? v) {
    if (v is! String) return null;
    for (final e in StatusErrorCode.values) {
      if (e.wire == v) return e;
    }
    return null;
  }
}
