/// EngineExecutor — EnginePort 재시도/오류분류 층 — SoT §6-7 / 질의서 Q8(EP-03·EP-09).
///
/// **EP-03 게이트(빈응답=실패·silent-success 금지)**: 빈/무응답 결과는 절대 성공으로 통과시키지
/// 않는다. rawErrorCode 0 만 성공(normal). 그 외(빈응답 sentinel·timeout·busy·permanent)는 실패.
///
/// 재시도 정책(§6-7):
///   - transient(`1·7·11·15·timeout`) → R=3 재시도(첫 시도 포함 최대 R+? — 아래 [maxRetries] 정의).
///   - permanent(`2·3·9·10`) → 즉시중단(재시도 없음) → FAILED.
///   - empty(무응답 sentinel) → **실패**(EP-03). 보수적으로 transient 로 재시도하되, R 소진 시 실패.
///
/// 이 층은 단일 스텝(dispense)의 실행+재시도만 책임진다. 스텝 직렬 진행·중간 영구오류 안전정지는
/// Pump Sequencer(pump_sequencer.dart) 책임.
library;

import '../core/pump_guard.dart';
import '../ports/engine_port.dart';
import '../test_seam/fake_engine_sentinels.dart';

/// 단일 스텝 실행 최종 결과.
enum EngineStepStatus {
  /// 정상(rawErrorCode 0).
  success,

  /// transient(빈응답 포함) 재시도 소진 실패 → ENGINE_ERROR_TRANSIENT / ENGINE_TIMEOUT.
  transientExhausted,

  /// permanent 즉시중단 → ENGINE_ERROR_PERMANENT.
  permanent,
}

/// 단일 스텝 실행 결과 + 오류코드.
class EngineStepResult {
  const EngineStepResult({
    required this.status,
    required this.attempts,
    this.errorCode,
    this.lastRawCode,
  });

  final EngineStepStatus status;

  /// 실제 물리 시도 횟수(재시도 포함).
  final int attempts;

  /// 실패 시 status.errorCode(§6-7). 성공이면 null.
  final StatusErrorCode? errorCode;

  /// 마지막 raw errorCode(관찰/디버그).
  final int? lastRawCode;

  bool get isSuccess => status == EngineStepStatus.success;
}

/// EnginePort 재시도/오류분류 실행기.
///
/// [maxRetries] = R (SoT §6-7 = 3). 첫 시도 + 최대 R 회 재시도 → 총 최대 (R+1) 물리 시도.
class EngineExecutor {
  EngineExecutor(this.engine, {this.maxRetries = 3});

  final EnginePort engine;

  /// R — transient 재시도 횟수(SoT §6-7 = 3).
  final int maxRetries;

  /// 단일 스텝(dispense)을 재시도 정책과 함께 실행.
  ///
  /// 빈응답(무응답) = 실패(EP-03). silent-success 0 — rawErrorCode 0 만 success.
  Future<EngineStepResult> runStep(EngineDispenseCommand cmd) async {
    int attempts = 0;
    int? lastRaw;
    StatusErrorCode lastErrorCode = StatusErrorCode.engineErrorTransient;

    // 첫 시도 + 최대 maxRetries 재시도.
    for (int i = 0; i <= maxRetries; i++) {
      attempts++;
      final EngineResult res = await engine.dispense(cmd);
      lastRaw = res.rawErrorCode;

      // ── EP-03: 빈/무응답 판정을 성공보다 먼저 — silent-success 구조적 차단. ──
      if (res.rawErrorCode == kFakeEmptyRawCode || (res.detail == '' && res.rawErrorCode != 0)) {
        // empty = 실패. 보수적으로 transient 재시도(무응답은 일시 통신 문제일 수 있음).
        lastErrorCode = StatusErrorCode.engineErrorTransient;
        continue;
      }

      // timeout sentinel → transient(ENGINE_TIMEOUT).
      if (res.rawErrorCode == kFakeTimeoutRawCode) {
        lastErrorCode = StatusErrorCode.engineTimeout;
        continue;
      }

      final EngineErrorClass cls = classifyEngineErrorCode(res.rawErrorCode);
      switch (cls) {
        case EngineErrorClass.normal:
          return EngineStepResult(
            status: EngineStepStatus.success,
            attempts: attempts,
            lastRawCode: lastRaw,
          );
        case EngineErrorClass.transient:
          lastErrorCode = StatusErrorCode.engineErrorTransient;
          continue; // 재시도.
        case EngineErrorClass.permanent:
          // 즉시중단 — 재시도 없음.
          return EngineStepResult(
            status: EngineStepStatus.permanent,
            attempts: attempts,
            errorCode: StatusErrorCode.engineErrorPermanent,
            lastRawCode: lastRaw,
          );
      }
    }

    // R 소진 — transient/timeout/empty 최종 실패.
    return EngineStepResult(
      status: EngineStepStatus.transientExhausted,
      attempts: attempts,
      errorCode: lastErrorCode,
      lastRawCode: lastRaw,
    );
  }
}
