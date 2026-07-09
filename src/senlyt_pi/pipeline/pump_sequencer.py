"""Pump Sequencer — steps 직렬 토출 + 진행보고 + 안전정지 + 동시1제조 + graceful — SoT §4-5 / §9-2.

Dart `lib/pipeline/pump_sequencer.dart` 포팅 (동기 실행 모델 — 단일 라이터 pi 데몬 전제).

책임(질의서 PS-*·SR-*·CR-*):
  - steps **직렬** 토출(idx 오름차순) — ResolvedRecipe.steps 순서대로 EngineExecutor.run_step.
  - 각 스텝 후 **진행보고**(PROGRESS stepK/N) via StatusReporter.
  - 중간 **영구오류 안전정지**(PS): permanent 발생 시 즉시 중단 → PARTIAL FAILED(stepK/N·
    ENGINE_ERROR_PERMANENT). transient 소진 실패도 중단 → PARTIAL FAILED.
  - **동시 1제조 큐잉**: 한 번에 하나만 제조. 대기 job 은 FIFO 로 순차 실행(레이스·이중전진 방지).
  - **graceful(SIGTERM)**: 요청 시 **현재 step 완주·다음 step 미시작**(PS-06) → 남으면 PARTIAL FAILED.
  - **무응답 silent-success 0**(EP-03): EngineExecutor 가 empty=실패로 처리하므로 0step 성공 불가.

멱등 통합: run 진입 전 Ledger.check_and_claim(IL-02). RUNNING 마킹 후 토출, 종결 시 mark_settled.

⚠️ 동기 포팅 노트: Dart 의 Future 큐잉을 동기 FIFO 루프로 번역했다. 제조 콜백(publisher)
안에서의 재진입 submit 은 미지원(RuntimeError) — request_drain 등 플래그 조작만 허용.
재진입 거부 시 해당 job 은 큐에서 **제거**된다(거부된 submit 이 나중에 실행되는 일 없음 — HW 안전).
"""

from __future__ import annotations

import enum
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Sequence

from ..core.order_status import DispensePhase
from ..core.pump_guard import StatusErrorCode
from ..core.wire_messages import RecipeStep
from ..persistence.file_idempotency_ledger import FileIdempotencyLedger
from ..persistence.idempotency_ledger import LedgerVerdict
from ..ports.engine_port import EngineDispenseCommand, EnginePort
from .engine_executor import EngineExecutor
from .recipe_resolver import RecipeResolver, RecipeValidationError
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
    ) -> None:
        self.ledger = ledger
        self.resolver = resolver
        self._executor = EngineExecutor(engine, max_retries=max_retries)
        self.request_id_gen = request_id_gen
        self.publisher = publisher
        self._now_iso = now_iso if now_iso is not None else _default_now_iso

        # 동시 1제조 강제 — 진행 중이면 새 job 은 큐 대기(FIFO).
        self._busy = False
        self._pending: deque[_PendingJob] = deque()
        # graceful 종료 플래그 — set 후 현재 step 완주·다음 step 미시작.
        self._draining = False

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
        verdict = self.ledger.check_and_claim(job.command_id)
        if verdict is LedgerVerdict.DUPLICATE:
            self._publish(
                DispensePhase.FAILED, 0, 0, StatusErrorCode.DUPLICATE_DROPPED,
                job.command_id, job.trace_id,
            )
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
        for step in resolved.steps:
            # ── graceful: 다음 step 미시작(현재까지 완주분으로 PARTIAL). ──
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

            cmd = EngineDispenseCommand(
                pump_addr=step.pump_addr,
                volume_ul=step.volume_ul,
                steps=step.steps,
                spec=step.spec,
            )
            res = self._executor.run_step(cmd)

            if not res.is_success:
                # ── 중간 실패 안전정지(PS): permanent 즉시중단 / transient 소진 → PARTIAL FAILED. ──
                self.ledger.mark_settled(job.command_id, success=False)
                self._publish_via(
                    reporter, DispensePhase.FAILED, steps_done, step_n,
                    res.error_code
                    if res.error_code is not None
                    else StatusErrorCode.PARTIAL_DISPENSE,
                )
                return JobReport(
                    command_id=job.command_id,
                    outcome=JobOutcome.PARTIAL_FAILED,
                    steps_done=steps_done,
                    step_n=step_n,
                    error_code=res.error_code,
                )

            steps_done += 1
            # 진행보고(PROGRESS stepK/N). 종결 아니므로 phase=progress.
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
