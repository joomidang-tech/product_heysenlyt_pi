/// Recipe Resolver — 주문(command) → 정렬·검증된 실행 스텝 — SoT §6-4 / §9-1 / 질의서 Q2·Q3.
///
/// 책임:
///   1) command.recipe(steps) → idx **오름차순 직렬** 정렬(§9-1: 0부터 오름차순 직렬).
///   2) 검증 게이트(§6-4·§9-1): 음수·0·상한초과·미매핑 pumpAddr·빈 레시피 → CMD_VALIDATION_FAILED.
///      - RR-05(Q2): `0 < volumeUl ≤ maxVolumeUl`(per-pump).
///      - RR-07(Q3): 빈 레시피(steps=0) = drop(0step COMPLETED 금지).
///   3) 각 스텝을 SyringeSpec 로 steps(수) 파생(§6-4·하드코딩 금지).
///
/// recipe == null(§9-1 폴백 신호) 은 이 resolver 의 책임 밖 — 상위(dispatch)가 recipeId/
/// fragranceResult 로 해석해 RecipeStep 리스트를 만든 뒤 이 resolver 로 넘긴다.
///
/// 순수 함수(firebase/http/시리얼 무의존) — 단위테스트가 하드웨어 없이 통과.
library;

import '../core/pump_guard.dart';
import '../core/wire_messages.dart' show RecipeStep;

/// 해석된 실행 스텝(정렬·검증·파생 완료).
class ResolvedStep {
  const ResolvedStep({
    required this.idx,
    required this.pumpAddr,
    required this.flavor,
    required this.volumeUl,
    required this.steps,
    required this.spec,
  });

  final int idx;
  final int pumpAddr;
  final String flavor;

  /// per-pump 정규화된 µL(fragrance 는 상위에서 mL→µL 정규화 후 전달).
  final double volumeUl;

  /// SyringeSpec 파생 스텝수(§6-4).
  final int steps;
  final SyringeSpec spec;
}

/// Resolver 실패 사유 — SoT §6-7 status.errorCode 로 매핑.
class RecipeValidationError implements Exception {
  RecipeValidationError(this.reason, {this.idx, this.pumpAddr, this.volumeUl});

  final String reason;
  final int? idx;
  final int? pumpAddr;
  final double? volumeUl;

  /// 검증 실패는 전부 CMD_VALIDATION_FAILED 로 drop(§6-4).
  StatusErrorCode get errorCode => StatusErrorCode.cmdValidationFailed;

  @override
  String toString() =>
      'RecipeValidationError($reason${idx != null ? ' idx=$idx' : ''}'
      '${pumpAddr != null ? ' pumpAddr=$pumpAddr' : ''}'
      '${volumeUl != null ? ' volumeUl=$volumeUl' : ''})';
}

/// 성공 결과.
class ResolvedRecipe {
  const ResolvedRecipe(this.steps);

  /// idx 오름차순 직렬 정렬된 실행 스텝.
  final List<ResolvedStep> steps;

  int get stepN => steps.length;
}

/// Recipe Resolver.
///
/// [pumpMap] = pumpAddr → SyringeSpec(펌프별 프리셋·용량). 미매핑 addr → CMD_VALIDATION_FAILED.
/// [pumpMap] 이 pi settings(GET-SSE) 로 수신된 clamp 된 프리셋에서 구성된다(O-18).
class RecipeResolver {
  const RecipeResolver(this.pumpMap);

  /// pumpAddr → SyringeSpec. PUMP_MAP(§9-1) 검증에 사용.
  final Map<int, SyringeSpec> pumpMap;

  /// steps 를 정렬·검증·파생한다. 위반 시 [RecipeValidationError] throw(→ drop).
  ///
  /// [steps] 는 이미 µL 정규화 완료(fragrance mL→µL 는 상위·§6-6)를 전제한다.
  ResolvedRecipe resolve(List<RecipeStep> steps) {
    // RR-07(Q3): 빈 레시피 → drop(0step COMPLETED 금지).
    if (steps.isEmpty) {
      throw RecipeValidationError('empty_recipe');
    }

    // idx 오름차순 직렬 정렬(§9-1). 안정 정렬 — 동일 idx 는 입력 순서 보존.
    final sorted = List<RecipeStep>.from(steps)
      ..sort((a, b) => a.idx.compareTo(b.idx));

    final resolved = <ResolvedStep>[];
    for (final s in sorted) {
      final double volumeUl = s.volume.toDouble();

      // 미매핑 pumpAddr(§9-1 PUMP_MAP) → drop.
      final spec = pumpMap[s.pumpAddr];
      if (spec == null) {
        throw RecipeValidationError('unmapped_pump_addr',
            idx: s.idx, pumpAddr: s.pumpAddr);
      }

      // RR-05(Q2): 음수·0·상한초과 → drop. 게이트 = `0 < volumeUl ≤ maxVolumeUl`.
      if (!(volumeUl.isFinite) || volumeUl <= 0) {
        throw RecipeValidationError('non_positive_volume',
            idx: s.idx, pumpAddr: s.pumpAddr, volumeUl: volumeUl);
      }
      if (volumeUl > spec.maxVolumeUl) {
        throw RecipeValidationError('volume_over_max',
            idx: s.idx, pumpAddr: s.pumpAddr, volumeUl: volumeUl);
      }

      // steps 파생(§6-4·하드코딩 금지).
      final int stepCount = spec.stepsForVolumeUl(volumeUl);
      // steps.length ≥ 1(§6-4) — 파생 결과가 0 이면(극소 부피) 게이트 위반.
      if (stepCount < 1) {
        throw RecipeValidationError('derived_zero_steps',
            idx: s.idx, pumpAddr: s.pumpAddr, volumeUl: volumeUl);
      }

      resolved.add(ResolvedStep(
        idx: s.idx,
        pumpAddr: s.pumpAddr,
        flavor: s.flavor,
        volumeUl: volumeUl,
        steps: stepCount,
        spec: spec,
      ));
    }

    return ResolvedRecipe(resolved);
  }
}
