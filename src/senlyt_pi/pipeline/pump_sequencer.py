"""Pump Sequencer — steps **stage 병렬** 토출 + 진행보고 + 안전정지 + 동시1제조 + graceful — SoT §4-5 / §9-2.

§9-1 v2(2026-07-14 병렬토출 설계 — 99_daily/2026-07-14-pi데몬-병렬토출-설계): v1.1.0 의
"병렬 모션 + 직렬 버스" 패턴 이식. Dart Future.wait ↔ ThreadPoolExecutor 1:1 대응.

책임(질의서 PS-*·SR-*·CR-* + 설계 §8·§10):
  - **stage 병렬 토출**: ResolvedRecipe.stages 를 오름차순 순회(배리어) — 같은 stage 의
    스텝들은 펌프/밸브별 태스크로 **동시 실행**(ThreadPoolExecutor). 구계약(stage 부재)은
    stage=idx = 그룹당 1스텝 = **기존 완전 직렬과 동일 동작**(하위호환).
  - 각 stage 후 **진행보고**(PROGRESS stepK/N) via StatusReporter — reporter 는 메인 스레드
    에서만 호출(단조성 보장·스레드 안전은 구조로).
  - **stage 내 partial 실패**(설계 §10): 한 태스크가 실패해도 나머지 in-flight 태스크는
    **완주**(모션 중 강제 중단이 더 위험) → stage 실패 → 다음 stage 미진입 → PARTIAL FAILED.
  - **동시 1제조 큐잉(L4)**: 한 번에 하나만 제조. 대기 job 은 FIFO 로 순차 실행.
  - **graceful(SIGTERM)**: 현재 **stage 의 in-flight 태스크 완주·다음 stage 미시작**(PS-06
    재정의·설계 §10) → 남으면 PARTIAL FAILED.
  - **밸브(기주) 스텝**: ValvePort 로 실행(GPIO — RS485 버스와 독립·뮤텍스 L3). 밸브 미결선
    상태로 valve 스텝 수신 = CMD_VALIDATION_FAILED drop(토출 0·fail-closed·pre-flight).
  - **무응답 silent-success 0**(EP-03): EngineExecutor 가 empty=실패로 처리하므로 0step 성공 불가.

멱등 통합: run 진입 전 Ledger.check_and_claim(IL-02). RUNNING 마킹 후 토출, 종결 시 mark_settled.

⚠️ 뮤텍스 계층(설계 §4): 이 모듈은 L4(잡)·L2(stage 검증=구조 보장)만 안다. L1(RS485 버스 락)은
엔진 어댑터 몫(명령 송신+ACK 만 잡고 즉시 해제·폴링 백오프는 락 밖 — sy01b 실어댑터 구현 규약),
L3(밸브 상호배타)는 ValveAdapter 몫. 교차 의존 금지.

⚠️ 동기 포팅 노트: Dart 의 Future 큐잉을 동기 FIFO 루프로 번역했다. 제조 콜백(publisher)
안에서의 재진입 submit 은 미지원(RuntimeError) — request_drain 등 플래그 조작만 허용.
재진입 거부 시 해당 job 은 큐에서 **제거**된다(거부된 submit 이 나중에 실행되는 일 없음 — HW 안전).
"""

from __future__ import annotations

import enum
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Sequence

from ..core.order_status import DispensePhase
from ..core.pump_guard import StatusErrorCode
from ..core.wire_messages import RecipeStep
from ..obs.log import STAGE_STEP_EXEC, StructuredLogger
from ..persistence.file_idempotency_ledger import FileIdempotencyLedger
from ..persistence.idempotency_ledger import LedgerVerdict
from ..ports.engine_port import OP_INITIALIZE, EngineDispenseCommand, EnginePort
from ..ports.valve_port import ValvePort
from .engine_executor import EngineExecutor
from .recipe_resolver import (
    RecipeResolver,
    RecipeValidationError,
    ResolvedStep,
    ResolvedOpStep,
    ResolvedValveStep,
)
from .status_reporter import RequestIdGen, StatusReporter


class JobOutcome(enum.Enum):
    """한 제조 job 의 최종 결과."""

    # 전 스텝 성공 완주 → COMPLETED.
    COMPLETED = "completed"
    # 중간 실패(permanent/transient 소진) → PARTIAL FAILED.
    PARTIAL_FAILED = "partial_failed"
    # 검증 실패(빈/음수/상한/미매핑) → CMD_VALIDATION_FAILED drop(토출 0).
    VALIDATION_FAILED = "validation_failed"
    # 멱등 DROP(이미 본 합성키) → no-op(토출 0·IL-02).
    DUPLICATE_DROPPED = "duplicate_dropped"
    # graceful 종료로 남은 스텝 미시작 → PARTIAL FAILED(INTERRUPTED 아님·정상 정지).
    GRACEFUL_PARTIAL = "graceful_partial"
    # 긴급정지(§9-4)로 진행 중 제조를 하드 중단 → FAILED(INTERRUPTED). 물리 TR 은 어댑터가 이미 걸었다.
    ESTOP_ABORTED = "estop_aborted"


@dataclass(frozen=True, slots=True)
class JobReport:
    """제조 실행 리포트(관찰·테스트 판정)."""

    command_id: str
    outcome: JobOutcome
    # 완주한 스텝 수(stepK).
    steps_done: int
    # 총 스텝 수(stepN). 검증/멱등 실패 시 0 가능.
    step_n: int
    error_code: StatusErrorCode | None = None

    @property
    def is_success(self) -> bool:
        return self.outcome is JobOutcome.COMPLETED


# StatusReport 를 sink 로 흘리는 콜백(제조를 막지 않게 best-effort — OQ 로 흡수).
# (phase, step_k, step_n, error_code, command_id, trace_id)
ProgressPublisher = Callable[
    [DispensePhase, int, int, "StatusErrorCode | None", str, str], None
]


def _default_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class _PendingJob:
    command_id: str
    trace_id: str
    steps: tuple[RecipeStep, ...]
    report: JobReport | None = field(default=None)


class PumpSequencer:
    """Pump Sequencer — 동시 1제조 큐잉 오케스트레이터."""

    def __init__(
        self,
        *,
        ledger: FileIdempotencyLedger,
        engine: EnginePort,
        resolver: RecipeResolver,
        request_id_gen: RequestIdGen,
        publisher: ProgressPublisher | None = None,
        max_retries: int = 3,
        now_iso: Callable[[], str] | None = None,
        valve: ValvePort | None = None,
        max_parallel: int = 8,
        estop_event: "threading.Event | None" = None,
        logger: "StructuredLogger | None" = None,
    ) -> None:
        self.ledger = ledger
        self.resolver = resolver
        # 실패 진단 로그용(2026-07-18) — 스텝 실패 시 raw 엔진코드·detail 을 남긴다(없으면 미기록).
        self._log = logger
        self._executor = EngineExecutor(engine, max_retries=max_retries)
        self.request_id_gen = request_id_gen
        self.publisher = publisher
        self._now_iso = now_iso if now_iso is not None else _default_now_iso
        # 기주 밸브 포트(§9-1 v2) — None 이면 valve 스텝 수신 시 fail-closed drop(토출 0).
        self.valve = valve
        # stage 병렬 태스크 풀(설계 §5 — threads·pyserial 친화). lazy 생성·재사용.
        #   기본 8 = 한 stage 동시 실행 상한(넉넉한 여유값) — 실제 동시 스텝은 stage 내
        #   pumpAddr 유일 제약(RR L2)이라 장착 펌프 수(식향 2·향장향 3)를 넘지 않는다. 이보다
        #   작으면 큰 stage 가 조용히 파도(wave) 직렬화된다. 재시도 의미론 노트(리뷰 P2-3): 실패 job 재시도는
        #   기존과 동일하게 **레시피 전체 재실행**(per-step resume 없음) — stage partial
        #   성공분 이중토출 여부는 운영 재시도 정책(수동) 몫.
        self._max_parallel = max(1, max_parallel)
        self._pool: ThreadPoolExecutor | None = None

        # 동시 1제조 강제 — 진행 중이면 새 job 은 큐 대기(FIFO).
        self._busy = False
        self._pending: deque[_PendingJob] = deque()
        # graceful 종료 플래그 — set 후 현재 stage 완주·다음 stage 미시작.
        self._draining = False
        # 긴급정지 래치(§9-4) — 데몬 감시 스레드와 어댑터가 공유. set 되면 진행 중 제조를 **하드 중단**
        #   (in-flight 스텝은 어댑터가 _estop 로 즉시 실패시키고, 시퀀서는 다음 stage 를 시작하지 않는다).
        #   drain(우아한 종료·현재 step 완주)과 달리 estop 은 "지금 당장 멈춤"이라 별도 축이다.
        self._estop = estop_event if estop_event is not None else threading.Event()

    @property
    def is_busy(self) -> bool:
        """현재 진행 중인지(관찰)."""
        return self._busy

    @property
    def queue_depth(self) -> int:
        """대기 큐 깊이(heartbeat queueDepth 파생)."""
        return len(self._pending) + (1 if self._busy else 0)

    def request_drain(self) -> None:
        """graceful 종료 요청(SIGTERM). 현재 step 은 완주, 이후 미시작. 대기 job 은 실행하지 않음."""
        self._draining = True

    def submit(
        self, *, command_id: str, trace_id: str, steps: Sequence[RecipeStep]
    ) -> JobReport:
        """제조 요청. 동시 1제조 — FIFO 큐로 순차 실행하고 이 job 의 완료 리포트를 반환."""
        job = _PendingJob(command_id=command_id, trace_id=trace_id, steps=tuple(steps))
        self._pending.append(job)
        self._drain_pending()
        if job.report is None:
            # 제조 중 콜백에서의 재진입 submit — 동기 포팅에서는 미지원(레이스·이중전진 방지).
            # ⚠️ 거부하는 job 은 반드시 큐에서 **제거**해야 한다 — 남겨두면 현재 job 종료 후
            # 바깥 drain 루프가 '거부된' job 을 그대로 실행해 물리 토출을 일으킨다(HW 안전).
            # 이 시점에 job 은 방금 append 한 마지막 원소다(busy 라 drain 이 즉시 반환).
            if self._pending and self._pending[-1] is job:
                self._pending.pop()
            else:
                # 방어적 폴백 — 동일성(is) 기준으로 어디에 있든 제거
                # (deque.remove 는 == 매칭이라 동값의 다른 job 을 지울 수 있어 쓰지 않는다).
                self._pending = deque(j for j in self._pending if j is not job)
            raise RuntimeError(
                "[PumpSequencer.submit] 재진입 submit 미지원 — 제조 콜백에서 submit 금지"
            )
        return job.report

    def _drain_pending(self) -> None:
        if self._busy:
            return
        while self._pending:
            job = self._pending.popleft()
            self._busy = True
            try:
                # graceful 종료 중이면 대기 job 은 실행하지 않고 gracefulPartial 로 종결.
                if self._draining:
                    job.report = JobReport(
                        command_id=job.command_id,
                        outcome=JobOutcome.GRACEFUL_PARTIAL,
                        steps_done=0,
                        step_n=0,
                        error_code=StatusErrorCode.PARTIAL_DISPENSE,
                    )
                else:
                    job.report = self._run_job(job)
            finally:
                self._busy = False

    def _run_job(self, job: _PendingJob) -> JobReport:
        # ── IL-02: 멱등 게이트 — 이미 본 합성키(4상태 전부)면 DROP(토출 0). ──
        #    claim 시 원 traceId 를 함께 영속 — 재기동 복구 보고가 원 트레이스와 상관되게.
        verdict = self.ledger.check_and_claim(job.command_id, job.trace_id)
        if verdict is LedgerVerdict.DUPLICATE:
            # 무해한 중복 재전달 — status 역보고도, trace span 도 발행하지 않는다
            # (2026-07-10). 이전엔 여기서 FAILED·DUPLICATE_DROPPED 를 publish 해
            # 완료 주문 트레이스에 dispense.failed 가짜 실패 span 과 FAILED status(→422)
            # 를 남겼다. 재토출 0(check_and_claim)은 그대로, 관측만 조용한 no-op 으로.
            return JobReport(
                command_id=job.command_id,
                outcome=JobOutcome.DUPLICATE_DROPPED,
                steps_done=0,
                step_n=0,
                error_code=StatusErrorCode.DUPLICATE_DROPPED,
            )

        # ── 검증(RR): 빈/음수/상한/미매핑 → CMD_VALIDATION_FAILED drop(토출 0). ──
        try:
            resolved = self.resolver.resolve(job.steps)
        except RecipeValidationError as e:
            self.ledger.mark_settled(job.command_id, success=False)
            self._publish(
                DispensePhase.FAILED, 0, 0, e.error_code, job.command_id, job.trace_id
            )
            return JobReport(
                command_id=job.command_id,
                outcome=JobOutcome.VALIDATION_FAILED,
                steps_done=0,
                step_n=0,
                error_code=e.error_code,
            )

        # ── 밸브 pre-flight(fail-closed·설계 §8): valve 스텝이 있는데 밸브 미결선이면
        #    어떤 토출도 시작하기 **전에** drop(토출 0) — 검증 실패와 동형 처리. ──
        if resolved.has_valve and self.valve is None:
            self.ledger.mark_settled(job.command_id, success=False)
            self._publish(
                DispensePhase.FAILED, 0, 0,
                StatusErrorCode.CMD_VALIDATION_FAILED, job.command_id, job.trace_id,
            )
            return JobReport(
                command_id=job.command_id,
                outcome=JobOutcome.VALIDATION_FAILED,
                steps_done=0,
                step_n=0,
                error_code=StatusErrorCode.CMD_VALIDATION_FAILED,
            )

        step_n = resolved.step_n

        # RUNNING 마킹(재부팅 시 INTERRUPTED 판정 근거·CR-01).
        self.ledger.mark_running(job.command_id)

        reporter = StatusReporter(
            command_id=job.command_id,
            trace_id=job.trace_id,
            request_id_gen=self.request_id_gen,
            now_iso=self._now_iso,
        )
        # ACCEPTED 보고(제조 시작).
        self._publish_via(reporter, DispensePhase.ACCEPTED, 0, step_n, None)

        steps_done = 0
        try:
            return self._run_stages(job, reporter, resolved, step_n)
        except Exception:
            # 방어(리뷰 P2): 어떤 예기치 못한 예외에도 ledger 를 반드시 종결(미정착 RUNNING 금지).
            try:
                self.ledger.mark_settled(job.command_id, success=False)
            except Exception:
                pass
            self._publish(
                DispensePhase.FAILED, steps_done, step_n,
                StatusErrorCode.PARTIAL_DISPENSE, job.command_id, job.trace_id,
            )
            return JobReport(
                command_id=job.command_id,
                outcome=JobOutcome.PARTIAL_FAILED,
                steps_done=steps_done,
                step_n=step_n,
                error_code=StatusErrorCode.PARTIAL_DISPENSE,
            )

    def _run_stages(
        self,
        job: _PendingJob,
        reporter: StatusReporter,
        resolved,  # ResolvedRecipe
        step_n: int,
    ) -> JobReport:
        """stage 배리어 루프 본체 — _run_job 의 방어 try 안에서 돈다."""
        steps_done = 0
        for stage_steps in resolved.stages:
            # ── 긴급정지(§9-4): 다음 stage 미시작 + FAILED(INTERRUPTED). 물리 TR 은 어댑터가 이미
            #    걸었고(emergency_stop_all), in-flight 스텝은 어댑터 _estop 로 즉시 실패했다. 여기선
            #    "다음 stage 를 시작하지 않음"을 보장한다(drain 보다 우선 — 즉시 하드 중단). ──
            if self._estop.is_set():
                self.ledger.mark_settled(job.command_id, success=False)
                self._publish_via(
                    reporter, DispensePhase.FAILED, steps_done, step_n,
                    StatusErrorCode.INTERRUPTED,
                )
                return JobReport(
                    command_id=job.command_id,
                    outcome=JobOutcome.ESTOP_ABORTED,
                    steps_done=steps_done,
                    step_n=step_n,
                    error_code=StatusErrorCode.INTERRUPTED,
                )

            # ── graceful: 다음 stage 미시작(현재까지 완주분으로 PARTIAL·설계 §10). ──
            if self._draining:
                self.ledger.mark_settled(job.command_id, success=False)
                self._publish_via(
                    reporter, DispensePhase.FAILED, steps_done, step_n,
                    StatusErrorCode.PARTIAL_DISPENSE,
                )
                return JobReport(
                    command_id=job.command_id,
                    outcome=JobOutcome.GRACEFUL_PARTIAL,
                    steps_done=steps_done,
                    step_n=step_n,
                    error_code=StatusErrorCode.PARTIAL_DISPENSE,
                )

            # ── stage 동시 실행 — 전 태스크 **완주 후** 판정(취소 없음·모션 중 강제중단 금지). ──
            results = self._run_stage(stage_steps)
            stage_ok = sum(1 for ok, _ in results if ok)
            steps_done += stage_ok

            failures = [ec for ok, ec in results if not ok]
            if failures:
                # ── 긴급정지로 in-flight 스텝이 실패했으면 ESTOP_ABORTED(INTERRUPTED)로 분류한다
                #    (어댑터 _estop 로 즉시 실패한 케이스 — 일반 하드웨어 실패와 구분). ──
                if self._estop.is_set():
                    self.ledger.mark_settled(job.command_id, success=False)
                    self._publish_via(
                        reporter, DispensePhase.FAILED, steps_done, step_n,
                        StatusErrorCode.INTERRUPTED,
                    )
                    return JobReport(
                        command_id=job.command_id,
                        outcome=JobOutcome.ESTOP_ABORTED,
                        steps_done=steps_done,
                        step_n=step_n,
                        error_code=StatusErrorCode.INTERRUPTED,
                    )
                # ── stage 실패 안전정지(PS·설계 §10): permanent 우선 보고 → PARTIAL FAILED. ──
                error_code = next(
                    (ec for ec in failures if ec is StatusErrorCode.ENGINE_ERROR_PERMANENT),
                    next((ec for ec in failures if ec is not None), None),
                )
                self.ledger.mark_settled(job.command_id, success=False)
                self._publish_via(
                    reporter, DispensePhase.FAILED, steps_done, step_n,
                    error_code
                    if error_code is not None
                    else StatusErrorCode.PARTIAL_DISPENSE,
                )
                return JobReport(
                    command_id=job.command_id,
                    outcome=JobOutcome.PARTIAL_FAILED,
                    steps_done=steps_done,
                    step_n=step_n,
                    error_code=error_code,
                )

            # 진행보고(PROGRESS stepK/N) — stage 경계에서(reporter 는 메인 스레드 전용·단조).
            #   구계약(그룹당 1스텝)은 기존 per-step 보고와 동일 cadence(하위호환).
            if steps_done < step_n:
                self._publish_via(reporter, DispensePhase.PROGRESS, steps_done, step_n, None)

        # 전 스텝 성공 완주 → COMPLETED.
        self.ledger.mark_settled(job.command_id, success=True)
        self._publish_via(reporter, DispensePhase.COMPLETED, steps_done, step_n, None)
        return JobReport(
            command_id=job.command_id,
            outcome=JobOutcome.COMPLETED,
            steps_done=steps_done,
            step_n=step_n,
        )

    # ─────────────────────────────────────────────────────────────────────
    # stage 실행 — v1.1.0 Future.wait ↔ ThreadPoolExecutor 이식(설계 §3·§5)
    # ─────────────────────────────────────────────────────────────────────

    def _ensure_pool(self) -> ThreadPoolExecutor:
        if self._pool is None:
            self._pool = ThreadPoolExecutor(
                max_workers=self._max_parallel, thread_name_prefix="senlyt-stage"
            )
        return self._pool

    def _run_stage(
        self, stage_steps: Sequence["ResolvedStep | ResolvedValveStep | ResolvedOpStep"]
    ) -> list[tuple[bool, StatusErrorCode | None]]:
        """한 stage 의 스텝들을 동시 실행하고 (성공여부, 에러코드) 목록을 반환.

        - 1스텝 stage(구계약 전체·직렬 폴백)는 스레드 없이 인라인 실행(오버헤드 0·기존 동일).
        - 다스텝 stage 는 ThreadPoolExecutor 로 병렬 — **전 future 완주 대기**(부분 취소 없음).
          per-step 재시도(EngineExecutor R=3)는 태스크 안에서 펌프별 **독립** 진행.
        """
        if len(stage_steps) == 1:
            return [self._run_one(stage_steps[0])]
        # ── 브로드캐스트 초기화 합치기(v1.1.0 병렬 홈) ─────────────────────────────
        #   같은 stage 가 **전부 `initialize` op** 이면, per-pump 동시 명령(반이중 RS485 경합으로
        #   초기화가 깨졌던 D38 stage:0)이 아니라 **브로드캐스트 1콜**(명령 1발·경합 없음 + 최종
        #   Ready 폴로 펌프별 판정)로 처리한다. 브로드캐스트 미지원 엔진(Fake/구 어댑터)은 None →
        #   아래 일반 동시 실행으로 폴백(테스트·하위호환 무영향).
        coalesced = self._maybe_broadcast_init(stage_steps)
        if coalesced is not None:
            return coalesced
        pool = self._ensure_pool()
        # submit 자체의 예외(워커 스레드 생성 실패 등·리뷰 P2 봉합)도 가드 — 이미 제출된
        # future 는 **반드시 완주 대기**(고아 in-flight 모션 금지) 후 실패 결과로 집계한다.
        futures = []
        submit_failures = 0
        for s in stage_steps:
            try:
                futures.append(pool.submit(self._run_one, s))
            except Exception:
                submit_failures += 1
        results = [f.result() for f in futures]  # _run_one 이 예외를 흡수 — result() 는 안 던진다.
        results.extend(
            (False, StatusErrorCode.ENGINE_ERROR_PERMANENT) for _ in range(submit_failures)
        )
        return results

    def _maybe_broadcast_init(
        self, stage_steps: "Sequence[ResolvedStep | ResolvedValveStep | ResolvedOpStep]"
    ) -> "list[tuple[bool, StatusErrorCode | None]] | None":
        """stage 가 **전부 initialize op** 이고 엔진이 브로드캐스트를 지원하면 1콜로 합쳐 실행.

        반환: 해당·지원 시 per-step (ok, code) 목록 / 아니면 None(일반 동시 실행 폴백).
        브로드캐스트가 각 펌프에 닿았는지는 어댑터 `initialize_broadcast` 가 **최종 Ready 폴**로
        펌프별 판정해 {addr: code} 로 돌려주므로, 여기서 step 순서대로 매핑해 보고한다
        (연결성 = 끝 폴 결과 — 진짜 죽은 펌프만 타임아웃으로 드러난다).
        """
        if not all(
            isinstance(s, ResolvedOpStep) and s.op == OP_INITIALIZE for s in stage_steps
        ):
            return None
        init_broadcast = getattr(self._executor.engine, "initialize_broadcast", None)
        if not callable(init_broadcast):
            return None  # Fake/구 어댑터 — 일반 per-pump 동시 실행으로 폴백.
        addrs = [s.pump_addr for s in stage_steps]
        spec = stage_steps[0].spec
        try:
            results = init_broadcast(addrs, spec)
        except Exception as exc:  # noqa: BLE001 — 브로드캐스트 실패도 흡수(형제 완주 계약·다음 stage 미진입).
            if self._log is not None:
                self._log.error(
                    "브로드캐스트 초기화 실패 — 전 펌프",
                    stage=STAGE_STEP_EXEC,
                    error=f"{type(exc).__name__}: {exc}",
                )
            return [(False, StatusErrorCode.ENGINE_ERROR_PERMANENT) for _ in stage_steps]
        out: list[tuple[bool, StatusErrorCode | None]] = []
        for s in stage_steps:
            code = results.get(s.pump_addr, -1)  # 목록에 없으면(방어) 실패 처리.
            ok = code == 0
            if not ok and self._log is not None:
                self._log.error(
                    f"브로드캐스트 초기화 실패 — pump={s.pump_addr}",
                    stage=STAGE_STEP_EXEC,
                    engineCode=code,
                    pumpAddr=s.pump_addr,
                )
            out.append((ok, None if ok else StatusErrorCode.ENGINE_ERROR_PERMANENT))
        return out

    def _run_one(
        self, step: "ResolvedStep | ResolvedValveStep | ResolvedOpStep"
    ) -> tuple[bool, StatusErrorCode | None]:
        """스텝 1개 실행(태스크 본체 — 워커 스레드에서 돈다).

        ⚠️ 여기서는 publisher/reporter 를 호출하지 않는다(단조 reporter 는 메인 스레드 전용).
        ⚠️ **예외를 밖으로 던지지 않는다** — 실엔진(pyserial SerialException 등)/밸브 예외를
          실패 결과로 흡수한다. future.result() 가 재-raise 하면 형제 in-flight future 를
          대기하지 않은 채 _run_job 을 뚫고 나가 동시1제조(L4)·ledger settle 이 깨진다(리뷰 P1-1).
        """
        try:
            if isinstance(step, ResolvedOpStep):
                # 엔진 조작(정비 버튼) — 토출이 아니라 EngineExecutor 재시도층을 타지 않는다.
                #   정비는 운영자가 누른 **1회 동작**이고, 실패하면 그대로 보고해 다시 누르게
                #   한다(자동 재시도 = 의도치 않은 반복 물리 동작).
                res = self._engine_op(step)
                ok = res.raw_error_code == 0
                if not ok and self._log is not None:
                    # ⚠️ 실패 원인(raw 엔진코드·detail)을 남긴다 — 없으면 ENGINE_ERROR_PERMANENT 로만
                    #   뭉개져 "홈 실패/플런저 스톨/시리얼 오류" 구분이 로그로 불가(2026-07-18 실증 봉합).
                    self._log.error(
                        f"정비 엔진 조작 실패 — op={step.op}·pump={step.pump_addr}",
                        stage=STAGE_STEP_EXEC,
                        engineCode=res.raw_error_code,
                        error=res.detail,
                        pumpAddr=step.pump_addr,
                    )
                return (ok, None if ok else StatusErrorCode.ENGINE_ERROR_PERMANENT)

            if isinstance(step, ResolvedValveStep):
                valve = self.valve
                if valve is None:
                    # pre-flight 가 걸렀어야 하는 경로 — 방어적 fail-closed.
                    return (False, StatusErrorCode.CMD_VALIDATION_FAILED)
                res = valve.dispense_volume(step.base, step.volume_ml)
                if not res.ok and self._log is not None:
                    self._log.error(
                        f"기주 밸브 토출 실패 — base={step.base}",
                        stage=STAGE_STEP_EXEC,
                        error=res.detail,
                    )
                # 밸브 실패 = permanent(시간축 재시도는 과토출 위험 — 재시도 없음·설계 §8).
                return (res.ok, None if res.ok else StatusErrorCode.ENGINE_ERROR_PERMANENT)

            cmd = EngineDispenseCommand(
                pump_addr=step.pump_addr,
                volume_ul=step.volume_ul,
                steps=step.steps,
                spec=step.spec,
                in_port=step.in_port,
                out_port=step.out_port,
                aspirate_speed_hz=step.aspirate_speed_hz,
                dispense_speed_hz=step.dispense_speed_hz,
                slope=step.slope,
            )
            res = self._executor.run_step(cmd)
            return (res.is_success, res.error_code)
        except Exception as exc:  # noqa: BLE001 — 어댑터 예외 흡수(형제 완주·settle 보존).
            # 예외 메시지를 남긴다 — 안 남기면 ENGINE_ERROR_PERMANENT 로만 뭉개져 시리얼 오류인지
            #   무엇인지 로그로 특정 불가(어댑터 예외가 가장 큰 진단 사각이었음·2026-07-18 실증 봉합).
            if self._log is not None:
                self._log.error(
                    "스텝 실행 중 예외(어댑터) — permanent 실패로 흡수",
                    stage=STAGE_STEP_EXEC,
                    error=f"{type(exc).__name__}: {exc}",
                )
            return (False, StatusErrorCode.ENGINE_ERROR_PERMANENT)

    def _engine_op(self, step: "ResolvedOpStep"):
        """엔진 조작 위임 — `run_op` 미보유 엔진(구 어댑터)은 미지원 실패로 떨어뜨린다(fail-closed)."""
        from ..ports.engine_port import EngineOpCommand, EngineResult

        run_op = getattr(self._executor.engine, "run_op", None)
        if not callable(run_op):
            return EngineResult(raw_error_code=-1, detail="engine has no run_op")
        return run_op(EngineOpCommand(pump_addr=step.pump_addr, op=step.op, spec=step.spec))

    def shutdown(self) -> None:
        """stage 태스크 풀 정리(멱등) — daemon 우아한 종료 경로에서 호출."""
        pool = self._pool
        if pool is not None:
            self._pool = None
            pool.shutdown(wait=True)

    def _publish_via(
        self,
        reporter: StatusReporter,
        phase: DispensePhase,
        step_k: int,
        step_n: int,
        error_code: StatusErrorCode | None,
    ) -> None:
        # reporter 로 단조성 강제(역행 시 raise) — 조립만; 실제 전송은 publisher(OQ/best-effort).
        reporter.report(phase=phase, step_k=step_k, step_n=step_n, error_code=error_code)
        self._publish(phase, step_k, step_n, error_code, reporter.command_id, reporter.trace_id)

    def _publish(
        self,
        phase: DispensePhase,
        step_k: int,
        step_n: int,
        error_code: StatusErrorCode | None,
        command_id: str,
        trace_id: str,
    ) -> None:
        p = self.publisher
        if p is None:
            return
        # best-effort — 관측이 제조를 막지 않는다(§10-6). 예외는 삼킨다(OQ 가 흡수).
        try:
            p(phase, step_k, step_n, error_code, command_id, trace_id)
        except Exception:
            # swallow — OQ/재전송 책임.
            pass
