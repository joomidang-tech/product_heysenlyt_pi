/// Dispatch 통합 — CS→IL→RR→PS→EP→SR 봉합 — SoT §1-1 / §9 / 질의서 §0 하네스.
///
/// 명령 소비 파이프라인의 접합부:
///   CommandSource(SSE·Fake) → deviceId 필터(CS-08) → PumpSequencer.submit
///     └ 내부: Ledger(IL) → RecipeResolver(RR) → EngineExecutor(EP) → StatusReporter(SR)
///
/// 이 계층 고유 책임:
///   - **deviceId 필터**(CS-08): 자기 deviceId 명령만 소비(다매장 라우팅).
///   - **recipe 해석**: command.recipe==null(§9-1 폴백)이면 recipeId(flavor)/fragranceResult
///     (fragrance)로 RecipeStep 리스트를 만든다. fragrance 는 mL→µL 정규화(§6-6·Code 11 방지).
///   - **command.id 신뢰**: 합성키 `{orderId}:{attempt}` 는 서버가 조립(§5-6). pi 는 그대로 멱등키로.
///
/// Sequencer 가 동시 1제조·큐잉을 책임지므로, dispatcher 는 순수 라우팅+해석만 한다.
library;

import 'dart:async';

import '../core/pump_guard.dart' show fragranceMlToUl;
import '../core/wire_messages.dart';
import '../pipeline/pump_sequencer.dart';
import '../ports/command_source_port.dart';

/// recipe==null 폴백 해석기 — recipeId/fragranceResult → RecipeStep 리스트.
///
/// 실제 레시피 소스(flavor_recipes.json·fragranceResult.notes)는 pi settings/명령 payload 에서
/// 온다. 이 typedef 는 그 해석을 주입 가능하게 하여(테스트 결정성) dispatcher 를 순수하게 유지한다.
/// 반환 리스트는 아직 검증 전(RR 이 게이트) — 단, fragrance 는 여기서 mL→µL 정규화 완료(§6-6).
typedef RecipeInterpreter = FutureOr<List<RecipeStep>> Function(Command command);

/// Dispatcher — command 스트림을 소비해 Sequencer 로 봉합.
class Dispatcher {
  Dispatcher({
    required this.deviceId,
    required this.commandSource,
    required this.sequencer,
    required this.interpret,
  });

  final String deviceId;
  final CommandSourcePort commandSource;
  final PumpSequencer sequencer;

  /// recipe==null 폴백 해석(recipeId/fragranceResult). recipe!=null 이면 그 steps 사용.
  final RecipeInterpreter interpret;

  StreamSubscription<Command>? _sub;

  /// 완료된 job 리포트 스트림(관찰·테스트).
  final _reports = StreamController<JobReport>.broadcast();
  Stream<JobReport> get reports => _reports.stream;

  /// 명령 구독 시작 — deviceId 필터 후 Sequencer.submit.
  void start() {
    _sub = commandSource.commands(deviceId).listen(_onCommand);
  }

  Future<void> _onCommand(Command command) async {
    // ── CS-08: deviceId 불일치 명령은 무시(다매장 라우팅). ──
    if (command.deviceId != deviceId) return;

    // recipe 해석: 명시 steps 우선, null 이면 폴백 해석(recipeId/fragranceResult·mL→µL).
    final List<RecipeStep> steps =
        command.recipe ?? await interpret(command);

    final report = await sequencer.submit(
      commandId: command.id,
      traceId: command.traceId,
      steps: steps,
    );
    if (!_reports.isClosed) _reports.add(report);
  }

  /// 단발 명령 처리(테스트/재처리·resync flush). 스트림 없이 직접 봉합.
  Future<JobReport> dispatchOnce(Command command) async {
    final List<RecipeStep> steps = command.recipe ?? await interpret(command);
    return sequencer.submit(
      commandId: command.id,
      traceId: command.traceId,
      steps: steps,
    );
  }

  Future<void> stop() async {
    await _sub?.cancel();
    _sub = null;
    await _reports.close();
  }
}

/// fragrance fragranceResult.notes → RecipeStep 폴백 해석 헬퍼(§6-6 mL→µL 정규화).
///
/// notes[i] = {name, amountMl, ...}. pumpAddr 는 pumpMap(flavor→addr) 을 통해 해석해야 하나,
/// 여기서는 dispatcher 주입 interpret 가 매핑을 알고 있다고 전제하고, 단위 정규화만 제공한다.
List<RecipeStep> fragranceNotesToSteps(
  List<Map<String, Object?>> notes, {
  required int Function(String flavorName) pumpAddrOf,
}) {
  final steps = <RecipeStep>[];
  for (var i = 0; i < notes.length; i++) {
    final n = notes[i];
    final name = (n['name'] as String?) ?? (n['nameKo'] as String?) ?? '';
    final amountMl = (n['amountMl'] as num?)?.toDouble() ?? 0;
    steps.add(RecipeStep(
      idx: i,
      pumpAddr: pumpAddrOf(name),
      flavor: name,
      volume: fragranceMlToUl(amountMl), // mL→µL(§6-6·Code 11 방지).
    ));
  }
  return steps;
}
