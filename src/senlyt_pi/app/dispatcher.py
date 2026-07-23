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

import time
from datetime import datetime, timezone
from typing import Callable, Mapping, Sequence

from ..core.command_set import CommandSet, CommandSetStatus
from ..core.pump_guard import StatusErrorCode, fragrance_ml_to_ul
from ..core.wire_messages import Command, Heartbeat, RecipeStep
from ..obs.log import STAGE_ERROR, STAGE_PI_RECEIVED, StructuredLogger
from ..persistence.file_idempotency_ledger import LedgerEntryState
from ..pipeline.pump_sequencer import JobOutcome, JobReport, PumpSequencer
from ..ports.command_source_port import CommandSourcePort
from ..ports.commandset_source_port import CommandSetSourcePort

# ── 정비 봉투 신선도 상한(초 · 2026-07-19 QA "흡입/배출 이슈") ─────────────────────
#   정비(운영자 버튼)는 "지금 아니면 무효"다 — 제조와 달리 큐에 묵혀서 나중에 실행하면
#   운영자가 이미 포기한 명령이 유령처럼 물리 동작한다(연타 큐 누적 → "2~3분 뒤 1·2펌프
#   동시 작동" 실기기 재현). 발행(createdAt) 후 이 시간이 지난 maintenance 봉투는 **물리
#   실행 없이** failed 로 종단한다. 90s = 정상 큐 대기(스텝 수 초 × 몇 건)는 넉넉히 통과
#   시키고, "몇 분 묵은" 유령만 자르는 값(안전 창 RECLAIM 90s 와 같은 결). manufacture
#   (주문 제조)는 대상 아님 — 주문은 묵어도 반드시 실행돼야 한다(멱등·유실 0 계약).
MAINTENANCE_STALE_S = 90.0

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
# 반환(선택·2026-07-19 하드닝): 실어댑터는 서버 판정 문자열("applied"|"noop"|"rejected"|
#   "retry")을 돌려줄 수 있다 — dispatcher 는 DELIVERED 보고의 "rejected"(422 역행/404 미존재
#   = 서버가 이미 종단·취소한 봉투)만 claim 게이트로 소비한다. None 반환(구 sink·Fake)은
#   "판정 미상"으로 취급해 기존 동작(실행 진행) 그대로다(하위호환).
CommandSetStatusSink = Callable[
    [CommandSet, CommandSetStatus, "StatusErrorCode | None"], "str | None"
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
        logger: "StructuredLogger | None" = None,
        now_s: Callable[[], float] | None = None,
    ) -> None:
        self.device_id = device_id
        self.command_source = command_source
        self.sequencer = sequencer
        # recipe==None 폴백 해석(recipeId/fragranceResult/flavorRecipe). recipe 있으면 그 steps 사용.
        self.interpret = interpret
        # CommandSet 봉투 축(선택 — 미주입 시 기존 Command 축만 동작·무파괴).
        self.commandset_source = commandset_source
        self.commandset_sink = commandset_sink
        # 신선도 게이트 관측 로그(선택) + 시계 seam(테스트 결정성 — 기본 wall clock).
        self._log = logger
        self._now_s = now_s if now_s is not None else time.time
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

    def poll_stream(self) -> int:
        """**단일 스트림** 소비(2026-07-19 귀머거리 창 봉합) — 봉투+command 두 축을 한 스트림에서.

        기존 `poll_commandsets()` → `poll()` 순차 호출은 각자 무한 스트림을 번갈아 블로킹 소비해,
        한 축을 듣는 동안(스트림 수명 내내) 다른 축 발행분이 **최대 스트림 수명만큼 지연**됐다
        (실기기 191s·293s → 신선도 게이트 익사). 소스가 `poll_batches`(단일 스트림·snapshot 당
        두 축 동시)를 제공하면 그걸 쓰고, 아니면(테스트 Fake 등) 기존 순차 소비로 폴백한다.

        - 봉투 우선 처리(기존 poll_once 순서 유지 — 봉투 축이 먼저 claim).
        - **항목 단위 예외 격리**: 봉투/command 1건의 dispatch 예외가 스트림 소비를 죽여
          제너레이터가 버려지면(=`with` 미정리) 스트림이 누수돼 중복 연결이 쌓인다(06:32 실측
          난사). 항목 예외는 삼키고 로그만 — 스트림 계층 오류만 위로 전파(재연결 백오프 대상).
        - finally 에서 **제너레이터 명시 close** — 어떤 종료 경로에서도 스트림 정리.
        """
        src = self.commandset_source if self.commandset_source is not None else self.command_source
        batches = getattr(src, "poll_batches", None)
        if not callable(batches):
            handled = self.poll_commandsets()
            handled += self.poll()
            return handled
        handled = 0
        gen = batches(self.device_id)
        try:
            for sets, cmds in gen:
                for cs in sets:
                    try:
                        if self.dispatch_commandset(cs) is not None:
                            handled += 1
                    except Exception as e:  # noqa: BLE001 — 봉투 1건 실패가 스트림을 안 죽인다.
                        self._warn_item_error("commandset", cs.command_set_id, e)
                for command in cmds:
                    try:
                        if self._on_command(command) is not None:
                            handled += 1
                    except Exception as e:  # noqa: BLE001 — command 1건 실패 격리(동일 원칙).
                        self._warn_item_error("command", command.id, e)
        finally:
            gen.close()  # 스트림 자원 정리 강제(누수 방지 — 예외·중단 공통).
        return handled

    def _warn_item_error(self, kind: str, item_id: str, e: Exception) -> None:
        """항목 단위 dispatch 예외 관측 — 스트림은 계속(격리), 원인은 로그로 남긴다."""
        if self._log is not None:
            self._log.warn(
                f"{kind} 1건 처리 예외 — 격리하고 스트림 소비 지속",
                stage=STAGE_ERROR,
                device_id=self.device_id,
                item_id=item_id,
                error=str(e),
            )

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
        - **중복 재전달 = terminal 재보고 후 no-op**(2026-07-10 → 2026-07-19 개정):
          command_set_id 가 ledger 상 이미 terminal(DONE/FAILED)이면 실행·span 없이
          **ledger 의 terminal 상태만 status PATCH 로 재보고**하고 return None.
          2026-07-10 의 "완전한 조용한 no-op" 은 terminal 전이 PATCH 유실(fire-once) 시
          서버 봉투가 delivered 로 영구 잔류 → head 무한 재전달인데 종단할 주체가 없어
          **그 기기 큐가 영구 교착**했다(P0·2026-07-19). 재보고는 같은 terminal 값이라
          서버 게이트가 noop(applied:false)로 멱등 흡수하고, DELIVERED/RUNNING 역행
          보고·dispense.failed 가짜 span 은 여전히 내지 않는다(트레이스 오염 재발 없음).
          ledger 이중토출 차단(재토출 0)은 check_and_claim 이 그대로 유지.
        - **DELIVERED claim 게이트**(2026-07-19): DELIVERED 보고에 서버가 "rejected"
          (422 역행 = 이미 종단·estop 취소 / 404 미존재)를 답하면 **실행하지 않고** None —
          취소된 봉투의 유령 물리 실행(estop 큐 취소 후 배치 잔여분 실행) 차단.
          판정 미상(구 sink·네트워크 오류)은 기존대로 진행(가용성 우선).
        - **delivered 후 예외 = 반드시 terminal 종단**(2026-07-19): steps 해석(interpret
          폴백 등)·submit 이 예외로 죽어도 FAILED(CMD_VALIDATION_FAILED)로 종단 보고한다.
          안 하면 봉투가 delivered 로 잔류(reclaim 대상 아님) → 재전달마다 같은 예외 반복
          + 큐 영구 교착(P0). poll_stream 의 항목 격리는 최후 방어로만 남는다.
        - DUPLICATE_DROPPED(선조회로 못 걸러진 잔여 케이스·비-terminal 중복)는 terminal
          보고 생략 — 원판 실행이 이미 terminal 을 보고했(거나 곧 한)다. sequencer 도
          이 경로에서 FAILED status/span 을 내지 않는다(무해 no-op).
        """
        # ── CS-08 동형: 자기 deviceId 봉투만 소비(다매장 라우팅). ──
        if cs.device_id != self.device_id:
            return None

        # ── 중복 재전달 선조회(2026-07-10·개정 2026-07-19): 이미 terminal(DONE/FAILED)
        #    소유 봉투는 실행·span 없이 ledger 의 terminal 만 재보고(교착 자가치유) 후 반환.
        #    순수 read 라 check_and_claim 의 원자성(재토출 0)은 훼손하지 않는다. ──
        if self.sequencer.ledger.is_settled(cs.command_set_id):
            self._re_report_settled_terminal(cs)
            return None

        # ── 정비 신선도 게이트(2026-07-19) — 묵은 정비는 **물리 실행 없이** 종단. ──
        #   정비=지금 아니면 무효(모듈 상수 주석). 연타로 쌓였다 몇 분 뒤 소비되는 유령
        #   실행("한참 뒤 1·2펌프 동시 작동" QA 재현)을 구조적으로 차단한다. createdAt 파싱
        #   불가(비정상 봉투)는 게이트를 통과시킨다 — 잘못된 드롭보다 기존 동작이 안전측.
        if cs.kind == "maintenance":
            age_s = self._commandset_age_s(cs.created_at)
            if age_s is not None and age_s > MAINTENANCE_STALE_S:
                if self._log is not None:
                    self._log.warn(
                        "정비 봉투 신선도 초과 — 실행 없이 종단(유령 실행 차단)",
                        stage=STAGE_PI_RECEIVED,
                        trace_id=cs.trace_id,
                        command_set_id=cs.command_set_id,
                        ageS=round(age_s, 1),
                        staleLimitS=MAINTENANCE_STALE_S,
                    )
                # CMD_STALE(2026-07-19) — "형식 오류"가 아니라 "시효 만료(다시 시도)"임을
                #   admin 이 구분 표시하도록 전용 코드로 보고(구 서버는 미지 코드도 문자열
                #   패스스루라 하위호환 무해).
                report = JobReport(
                    command_id=cs.command_set_id,
                    outcome=JobOutcome.VALIDATION_FAILED,
                    steps_done=0,
                    step_n=0,
                    error_code=StatusErrorCode.CMD_STALE,
                )
                self._report_commandset(cs, CommandSetStatus.FAILED, StatusErrorCode.CMD_STALE)
                self.reports.append(report)
                return report

        delivered_verdict = self._report_commandset(cs, CommandSetStatus.DELIVERED, None)
        if delivered_verdict == "rejected":
            # ── claim 게이트(2026-07-19): 서버가 이 봉투를 이미 종단/취소(422 역행·404) —
            #    실행하면 "긴급정지로 취소됨" 표시 뒤에서 액체가 실제 토출되는 유령 실행이 된다
            #    (estop 큐 취소 레이스). ledger 미점유·실행 0 으로 조용히 반환한다. ──
            if self._log is not None:
                self._log.warn(
                    "봉투 DELIVERED 를 서버가 거부(이미 종단/취소) — 실행 없이 skip(유령 실행 차단)",
                    stage=STAGE_PI_RECEIVED,
                    trace_id=cs.trace_id,
                    command_set_id=cs.command_set_id,
                )
            return None

        trace_id = cs.trace_id if cs.trace_id is not None else ""

        # ── delivered 보고 이후 = 종단 책임 구간(2026-07-19 P0) — 어떤 예외도 봉투를
        #    delivered 로 방치하지 않는다(방치 = reclaim 불가·재전달 예외 무한 반복·큐 교착). ──
        try:
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
        except Exception as e:  # noqa: BLE001 — 종단 불변식: delivered 봉투는 반드시 terminal 로.
            if self._log is not None:
                self._log.error(
                    "봉투 처리 예외 — FAILED 로 종단(delivered 방치 = 큐 영구 교착 방지)",
                    stage=STAGE_ERROR,
                    trace_id=cs.trace_id,
                    command_set_id=cs.command_set_id,
                    error=f"{type(e).__name__}: {e}",
                )
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
        self.reports.append(report)

        if report.outcome is JobOutcome.COMPLETED:
            self._report_commandset(cs, CommandSetStatus.DONE, None)
        elif report.outcome is JobOutcome.DUPLICATE_DROPPED:
            # 원판 실행이 terminal 을 소유 — 재전달분은 terminal 보고 생략(무해 no-op).
            pass
        else:
            self._report_commandset(cs, CommandSetStatus.FAILED, report.error_code)
        return report

    def _commandset_age_s(self, created_at: str) -> float | None:
        """봉투 나이(초) — createdAt(ISO8601·Z/오프셋) 파싱 실패 시 None(게이트 통과).

        시계 스큐(리뷰 P2-3): RTC 없는 라즈베리파이는 부팅~NTP 동기 전 시계가 **과거**로
        틀어진다(timesyncd 가 종료 시각을 복원) → age 음수 → 게이트 통과(실행) = 안전 방향.
        위험 방향(시계가 미래로 앞섬 → 신선한 정비를 stale 오판)은 무RTC 특성상 실질적으로
        드물고, 오판해도 즉시 FAILED + WARN(ageS)로 표면화돼 운영자가 재시도로 복구한다.
        """
        try:
            dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return self._now_s() - dt.timestamp()
        except (ValueError, AttributeError):
            return None

    def build_heartbeat(
        self,
        *,
        engine: str | None = None,
        last_error: StatusErrorCode | None = None,
        needs_cleaning: bool | None = None,
        pumps: "list[int] | None" = None,
        valves: "list[str] | None" = None,
        pump_health: "dict[int, str] | None" = None,
        hw_checked_at: str | None = None,
    ) -> Heartbeat:
        """하트비트 조립(§9-3·10s 주기) — queueDepth 는 Sequencer 에서 파생(유휴=0).
        전송(PATCH /api/dispenser/heartbeat)은 StatusSinkPort 어댑터 책임.
        pumps/valves = 기기 연결상태(부팅 자동인식) · pump_health/hw_checked_at = 주기 감시 실측."""
        return Heartbeat(
            device_id=self.device_id,
            queue_depth=self.sequencer.queue_depth,
            engine=engine,
            last_error=last_error,
            needs_cleaning=needs_cleaning,
            pumps=pumps,
            valves=valves,
            pump_health=pump_health,
            hw_checked_at=hw_checked_at,
        )

    def _re_report_settled_terminal(self, cs: CommandSet) -> None:
        """settled(종단) 봉투 재전달 → ledger 의 terminal 상태를 status PATCH 로 **재보고**.

        2026-07-19 P0(교착 자가치유): terminal 전이 보고가 fire-once 로 유실되면 서버 봉투가
        delivered 로 영구 잔류하고(reclaim 은 running 전용), head 재전달을 종단할 주체가
        어디에도 없어 그 기기 큐가 영구 교착했다. 여기서 재전달분을 계기로 terminal 을
        재보고해 at-least-once 를 성립시킨다.

        트레이스 오염(2026-07-10 우려) 회피: **status PATCH 만** 보낸다(dispense span 0·
        DELIVERED/RUNNING 역행 보고 0). 서버가 이미 terminal 이면 동일값 noop(applied:false)
        로 멱등 흡수된다. ledger 상태를 모르는 구/테스트 ledger(state_of 미제공)는 종전
        조용한 no-op 그대로(하위호환).
        """
        state_of = getattr(self.sequencer.ledger, "state_of", None)
        if not callable(state_of):
            return
        try:
            state = state_of(cs.command_set_id)
        except Exception:  # noqa: BLE001 — 조회 실패는 종전 no-op(관측이 소비를 막지 않는다).
            return
        if state is LedgerEntryState.DONE:
            self._report_commandset(cs, CommandSetStatus.DONE, None)
        elif state is LedgerEntryState.FAILED:
            # 원 errorCode 는 원장에 없다 — 코드 없는 failed 로 종단만 성립시킨다
            # (서버가 이미 failed 면 noop·원판 보고가 살아 있으면 이 재보고는 도달 전 흡수).
            self._report_commandset(cs, CommandSetStatus.FAILED, None)

    def _report_commandset(
        self,
        cs: CommandSet,
        status: CommandSetStatus,
        error_code: StatusErrorCode | None,
    ) -> "str | None":
        """전이 보고 위임 — sink 의 서버 판정(문자열)을 그대로 올린다(None = 판정 미상).

        best-effort — 관측이 제조를 막지 않는다(§10-6). 예외는 삼킨다(재전송은 어댑터 책임).
        반환값은 DELIVERED claim 게이트("rejected" 시 실행 skip)에만 소비된다.
        """
        sink = self.commandset_sink
        if sink is None:
            return None
        try:
            verdict = sink(cs, status, error_code)
        except Exception:
            return None
        return verdict if isinstance(verdict, str) else None


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
