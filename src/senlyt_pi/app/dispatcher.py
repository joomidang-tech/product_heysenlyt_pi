"""Dispatch 통합 — CS→IL→RR→PS→EP→SR 봉합 — SoT §1-1 / §9 / 질의서 §0 하네스.

Dart `lib/app/dispatcher.dart` 포팅 (동기 소비 모델 — poll 로 도착분 순차 소비).

명령 소비 파이프라인의 접합부:
  CommandSource(SSE·Fake) → deviceId 필터(CS-08) → PumpSequencer.submit
    └ 내부: Ledger(IL) → RecipeResolver(RR) → EngineExecutor(EP) → StatusReporter(SR)

이 계층 고유 책임:
  - **deviceId 필터**(CS-08): 자기 deviceId 명령만 소비(다매장 라우팅).
  - **recipe 해석**: command.recipe is None(§9-1 폴백)이면 recipeId(flavor)/fragranceResult
    (fragrance)/**flavorRecipe**(v1.2.0 식향 생성형)로 RecipeStep 리스트를 만든다.
    fragrance/flavor 는 mL→µL 정규화(§6-6·Code 11 방지) — 헬퍼는
    pipeline.recipe_resolver(flavor_recipe_to_steps·flavor_recipe_source_to_steps)와 이 모듈
    (fragrance_notes_to_steps).
  - **command.id 신뢰**: 합성키 `{orderId}:{attempt}` 는 서버가 조립(§5-6). pi 는 그대로 멱등키로.

Sequencer 가 동시 1제조·큐잉을 책임지므로, dispatcher 는 순수 라우팅+해석만 한다.

── CommandSet 봉투 축 (2026-07-09 계약 device-registration-commandset-wire) ──
  기존 Command 소비와 **병행**하는 신규 축(기존 소비자 무파괴):
  - 봉투에 steps 가 있으면 recipe_resolver 폴백 해석을 **우회**하고 스텝을 직소비 —
    단 안전게이트(RR: pumpAddr∈pump_map·0<volume≤maxVolume·빈 레시피 drop)는
    Sequencer 내부 RecipeResolver 가 그대로 통과시킨다(서버 신뢰하되 검증·이중방어).
  - steps=None(레거시 폴백 신호·manufacture 만) → 기존 interpret(recipe_resolver 강등 폴백).
  - kind=maintenance(세척·퍼지·프라임) → 동일 게이트+Sequencer 경로로 실행.
    commandSetId=`mnt-{uuid}` 도 ledger dedup 대상(at-least-once 재전달 무해).
  - CommandSet.status 전이 보고(delivered→running→done|failed)는 별도 축 —
    주문 status(PATCH orders — pi 단독 전진·D15)와 독립. best-effort(제조를 막지 않음).
"""

from __future__ import annotations

from typing import Callable, Mapping, Sequence

from ..core.command_set import CommandSet, CommandSetStatus
from ..core.pump_guard import StatusErrorCode, fragrance_ml_to_ul
from ..core.wire_messages import Command, Heartbeat, RecipeStep
from ..pipeline.pump_sequencer import JobOutcome, JobReport, PumpSequencer
from ..ports.command_source_port import CommandSourcePort
from ..ports.commandset_source_port import CommandSetSourcePort

# recipe==None 폴백 해석기 — recipeId/fragranceResult/flavorRecipe → RecipeStep 리스트.
#
# 실제 레시피 소스(flavor_recipes·flavorRecipe payload·fragranceResult.notes)는 pi settings/
# 명령 payload 에서 온다. 이 Callable 은 그 해석을 주입 가능하게 하여(테스트 결정성)
# dispatcher 를 순수하게 유지한다. 반환 리스트는 아직 검증 전(RR 이 게이트) — 단,
# fragrance/flavor 는 여기서 mL→µL 정규화 완료(§6-6).
RecipeInterpreter = Callable[[Command], Sequence[RecipeStep]]

# CommandSet 상태 전이 보고 sink — (commandSet, 새 status, errorCode|None).
# 실전송(PATCH /api/dispenser/commandsets/[id]·requestId dedup)은 어댑터 책임 —
# best-effort: 예외는 삼킨다(관측이 제조를 막지 않는다·§10-6 결).
CommandSetStatusSink = Callable[
    [CommandSet, CommandSetStatus, "StatusErrorCode | None"], None
]


class Dispatcher:
    """Dispatcher — command 스트림을 소비해 Sequencer 로 봉합."""

    def __init__(
        self,
        *,
        device_id: str,
        command_source: CommandSourcePort,
        sequencer: PumpSequencer,
        interpret: RecipeInterpreter,
        commandset_source: CommandSetSourcePort | None = None,
        commandset_sink: CommandSetStatusSink | None = None,
    ) -> None:
        self.device_id = device_id
        self.command_source = command_source
        self.sequencer = sequencer
        # recipe==None 폴백 해석(recipeId/fragranceResult/flavorRecipe). recipe 있으면 그 steps 사용.
        self.interpret = interpret
        # CommandSet 봉투 축(선택 — 미주입 시 기존 Command 축만 동작·무파괴).
        self.commandset_source = commandset_source
        self.commandset_sink = commandset_sink
        # 완료된 job 리포트(관찰·테스트).
        self.reports: list[JobReport] = []

    def poll(self) -> int:
        """현재 도착분 command 를 소비 — deviceId 필터 후 Sequencer.submit.

        (Dart stream 구독의 동기 번역 — 도착분을 순차 소비하고 처리 수를 반환.)
        """
        handled = 0
        for command in self.command_source.commands(self.device_id):
            if self._on_command(command) is not None:
                handled += 1
        return handled

    def _on_command(self, command: Command) -> JobReport | None:
        # ── CS-08: deviceId 불일치 명령은 무시(다매장 라우팅). ──
        if command.device_id != self.device_id:
            return None

        # recipe 해석: 명시 steps 우선, None 이면 폴백 해석(recipeId/fragranceResult/flavor·mL→µL).
        steps: Sequence[RecipeStep] = (
            command.recipe if command.recipe is not None else self.interpret(command)
        )

        report = self.sequencer.submit(
            command_id=command.id,
            trace_id=command.trace_id,
            steps=steps,
        )
        self.reports.append(report)
        return report

    def dispatch_once(self, command: Command) -> JobReport:
        """단발 명령 처리(테스트/재처리·resync flush). 스트림 없이 직접 봉합."""
        steps: Sequence[RecipeStep] = (
            command.recipe if command.recipe is not None else self.interpret(command)
        )
        return self.sequencer.submit(
            command_id=command.id,
            trace_id=command.trace_id,
            steps=steps,
        )

    # ─────────────────────────────────────────────────────────────────────
    # CommandSet 봉투 축 (2026-07-09) — 기존 Command 축과 병행·무파괴.
    # ─────────────────────────────────────────────────────────────────────

    def poll_commandsets(self) -> int:
        """도착분 CommandSet 봉투를 소비 — deviceId 필터 후 dispatch_commandset."""
        if self.commandset_source is None:
            return 0
        handled = 0
        for cs in self.commandset_source.command_sets(self.device_id):
            if self.dispatch_commandset(cs) is not None:
                handled += 1
        return handled

    def dispatch_commandset(self, cs: CommandSet) -> JobReport | None:
        """봉투 1건 처리 — 직소비(steps) vs 레거시 폴백(steps=None) 분기 + 전이 보고.

        - deviceId 불일치 → 무시(None·CS-08 동형).
        - steps 있음 → recipe_resolver 폴백 해석 **우회**·스텝 직소비. 안전게이트
          (µL 상한 등)는 Sequencer 내부 RecipeResolver 가 그대로 적용(이중방어).
        - steps=None → manufacture 레거시 폴백: 봉투에서 합성 Command 를 재구성해
          기존 interpret(recipe_resolver 강등)로 해석. maintenance 의 steps=None 은
          계약 위반 — 토출 0 으로 failed(CMD_VALIDATION_FAILED).
        - 전이 보고: delivered → running → done|failed (best-effort·단조 전진.
          중복 재전달의 늦은 보고는 서버 게이트가 noop/422 로 흡수).
        - **중복 재전달 조용한 no-op**(2026-07-10): command_set_id 가 ledger 상 이미
          terminal(DONE/FAILED)이면 **맨 앞에서 즉시 return None** — DELIVERED/RUNNING
          전이 보고도, 제조 실행도, trace span 도 일절 없다. at-least-once 재전달로 성공
          주문 봉투가 한 번 더 도착해도(pi 제조 중 서버가 아직 delivered 라 push 가 한 번
          더 오는 창) 성공 트레이스를 오염(422 backward·dispense.failed 가짜 실패)시키지
          않는다. ledger 이중토출 차단(재토출 0)은 check_and_claim 이 그대로 유지.
        - DUPLICATE_DROPPED(선조회로 못 걸러진 잔여 케이스·비-terminal 중복)는 terminal
          보고 생략 — 원판 실행이 이미 terminal 을 보고했(거나 곧 한)다. sequencer 도
          이 경로에서 FAILED status/span 을 내지 않는다(무해 no-op).
        """
        # ── CS-08 동형: 자기 deviceId 봉투만 소비(다매장 라우팅). ──
        if cs.device_id != self.device_id:
            return None

        # ── 중복 재전달 선조회(2026-07-10): 이미 terminal(DONE/FAILED) 소유 봉투는
        #    완전한 조용한 no-op — 전이 보고·실행·span 없이 즉시 반환. 순수 read 라
        #    check_and_claim 의 원자성(재토출 0)은 훼손하지 않는다. ──
        if self.sequencer.ledger.is_settled(cs.command_set_id):
            return None

        self._report_commandset(cs, CommandSetStatus.DELIVERED, None)

        trace_id = cs.trace_id if cs.trace_id is not None else ""

        if cs.steps is not None:
            # 서버 두뇌(buildCommandRecipe)가 완성한 스텝 — 직소비(폴백 해석 우회).
            steps: Sequence[RecipeStep] = cs.steps
        elif cs.kind == "manufacture":
            # 레거시 폴백 신호 — 봉투 메타로 합성 Command 를 재구성해 기존 해석기로.
            legacy = Command(
                id=cs.command_set_id,
                order_id=cs.source_order_id or "",
                attempt=cs.attempt if cs.attempt is not None else 1,
                device_id=cs.device_id,
                recipe=None,
                trace_id=trace_id,
                created_at=cs.created_at,
            )
            steps = self.interpret(legacy)
        else:
            # maintenance + steps=None = 계약 위반(oneOf null 은 manufacture 전용) —
            # 토출 0 으로 즉시 failed(Sequencer 진입 없음·ledger 미점유).
            report = JobReport(
                command_id=cs.command_set_id,
                outcome=JobOutcome.VALIDATION_FAILED,
                steps_done=0,
                step_n=0,
                error_code=StatusErrorCode.CMD_VALIDATION_FAILED,
            )
            self._report_commandset(
                cs, CommandSetStatus.FAILED, StatusErrorCode.CMD_VALIDATION_FAILED
            )
            self.reports.append(report)
            return report

        self._report_commandset(cs, CommandSetStatus.RUNNING, None)

        report = self.sequencer.submit(
            command_id=cs.command_set_id,
            trace_id=trace_id,
            steps=steps,
        )
        self.reports.append(report)

        if report.outcome is JobOutcome.COMPLETED:
            self._report_commandset(cs, CommandSetStatus.DONE, None)
        elif report.outcome is JobOutcome.DUPLICATE_DROPPED:
            # 원판 실행이 terminal 을 소유 — 재전달분은 terminal 보고 생략(무해 no-op).
            pass
        else:
            self._report_commandset(cs, CommandSetStatus.FAILED, report.error_code)
        return report

    def build_heartbeat(
        self,
        *,
        engine: str | None = None,
        last_error: StatusErrorCode | None = None,
        needs_cleaning: bool | None = None,
    ) -> Heartbeat:
        """하트비트 조립(§9-3·30s 주기) — queueDepth 는 Sequencer 에서 파생(유휴=0).
        전송(PATCH /api/dispenser/heartbeat)은 StatusSinkPort 어댑터 책임."""
        return Heartbeat(
            device_id=self.device_id,
            queue_depth=self.sequencer.queue_depth,
            engine=engine,
            last_error=last_error,
            needs_cleaning=needs_cleaning,
        )

    def _report_commandset(
        self,
        cs: CommandSet,
        status: CommandSetStatus,
        error_code: StatusErrorCode | None,
    ) -> None:
        sink = self.commandset_sink
        if sink is None:
            return
        # best-effort — 관측이 제조를 막지 않는다(§10-6). 예외는 삼킨다(재전송은 어댑터/OQ 책임).
        try:
            sink(cs, status, error_code)
        except Exception:
            pass


def fragrance_notes_to_steps(
    notes: Sequence[Mapping[str, object]],
    *,
    pump_addr_of: Callable[[str], int],
) -> list[RecipeStep]:
    """fragrance fragranceResult.notes → RecipeStep 폴백 해석 헬퍼(§6-6 mL→µL 정규화).

    notes[i] = {name, amountMl, ...}. pumpAddr 는 pumpMap(flavor→addr) 을 통해 해석해야 하나,
    여기서는 dispatcher 주입 interpret 가 매핑을 알고 있다고 전제하고, 단위 정규화만 제공한다.
    """
    steps: list[RecipeStep] = []
    for i, n in enumerate(notes):
        raw_name = n.get("name") or n.get("nameKo") or ""
        name = raw_name if isinstance(raw_name, str) else ""
        raw_amount = n.get("amountMl")
        amount_ml = float(raw_amount) if isinstance(raw_amount, (int, float)) else 0.0
        steps.append(
            RecipeStep(
                idx=i,
                pump_addr=pump_addr_of(name),
                flavor=name,
                volume=fragrance_ml_to_ul(amount_ml),  # mL→µL(§6-6·Code 11 방지).
            )
        )
    return steps
