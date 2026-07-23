"""EngineExecutor — EnginePort 재시도/오류분류 층 — SoT §6-7 / 질의서 Q8(EP-03·EP-09).

Dart `lib/pipeline/engine_executor.dart` 포팅.

**EP-03 게이트(빈응답=실패·silent-success 금지)**: 빈/무응답 결과는 절대 성공으로 통과시키지
않는다. rawErrorCode 0 만 성공(normal). 그 외(빈응답 sentinel·timeout·busy·permanent)는 실패.

재시도 정책(§6-7):
  - transient(`1·7·11·15·timeout`) → R=3 재시도.
  - permanent(`2·3·9·10`) → 즉시중단(재시도 없음) → FAILED.
  - empty(무응답 sentinel) → **실패**(EP-03). 보수적으로 transient 로 재시도하되, R 소진 시 실패.

이 층은 단일 스텝(dispense)의 실행+재시도만 책임진다. 스텝 직렬 진행·중간 영구오류 안전정지는
Pump Sequencer(pump_sequencer.py) 책임.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Callable

from ..core.pump_guard import EngineErrorClass, StatusErrorCode, classify_engine_error_code
from ..ports.engine_port import (
    EngineBatchCommand,
    EngineDispenseCommand,
    EnginePort,
    EngineResult,
)
from ..test_seam.fake_engine_sentinels import FAKE_EMPTY_RAW_CODE, FAKE_TIMEOUT_RAW_CODE


class EngineStepStatus(enum.Enum):
    """단일 스텝 실행 최종 결과."""

    # 정상(rawErrorCode 0).
    SUCCESS = "success"
    # transient(빈응답 포함) 재시도 소진 실패 → ENGINE_ERROR_TRANSIENT / ENGINE_TIMEOUT.
    TRANSIENT_EXHAUSTED = "transient_exhausted"
    # permanent 즉시중단 → ENGINE_ERROR_PERMANENT.
    PERMANENT = "permanent"


@dataclass(frozen=True, slots=True)
class EngineStepResult:
    """단일 스텝 실행 결과 + 오류코드."""

    status: EngineStepStatus
    # 실제 물리 시도 횟수(재시도 포함).
    attempts: int
    # 실패 시 status.errorCode(§6-7). 성공이면 None.
    error_code: StatusErrorCode | None = None
    # 마지막 raw errorCode(관찰/디버그).
    last_raw_code: int | None = None

    @property
    def is_success(self) -> bool:
        return self.status is EngineStepStatus.SUCCESS


class EngineExecutor:
    """EnginePort 재시도/오류분류 실행기.

    `max_retries` = R (SoT §6-7 = 3). 첫 시도 + 최대 R 회 재시도 → 총 최대 (R+1) 물리 시도.
    """

    def __init__(self, engine: EnginePort, *, max_retries: int = 3) -> None:
        self.engine = engine
        # R — transient 재시도 횟수(SoT §6-7 = 3).
        self.max_retries = max_retries

    def run_step(self, cmd: EngineDispenseCommand) -> EngineStepResult:
        """단일 스텝(dispense)을 재시도 정책과 함께 실행.

        빈응답(무응답) = 실패(EP-03). silent-success 0 — rawErrorCode 0 만 success.
        """
        return self._run_with_retry(lambda: self.engine.dispense(cmd))

    def run_batch(self, cmd: EngineBatchCommand) -> EngineStepResult:
        """배치 흡입 스텝(§9-1 v3)을 재시도 정책과 함께 실행 — `run_step` 과 **동일 정책**.

        배치 전체(여러 흡입 → 한 번 배출)가 하나의 재시도 단위다(부분 재개 없음). 재시도 시
        어댑터가 홈(0)에서 다시 누적 흡입하므로 이중토출이 되지 않는다(_ensure_ready 가 원점 복구).
        """
        return self._run_with_retry(lambda: self.engine.dispense_batch(cmd))

    def _run_with_retry(self, dispense: "Callable[[], EngineResult]") -> EngineStepResult:
        """단일/배치 공통 재시도 루프 — EP-03·timeout·transient/permanent 분류가 동일하다.

        빈응답(무응답) = 실패(EP-03). silent-success 0 — rawErrorCode 0 만 success.
        """
        attempts = 0
        last_raw: int | None = None
        last_error_code = StatusErrorCode.ENGINE_ERROR_TRANSIENT

        # 첫 시도 + 최대 max_retries 재시도.
        for _ in range(self.max_retries + 1):
            attempts += 1
            res = dispense()
            last_raw = res.raw_error_code

            # ── EP-03: 빈/무응답 판정을 성공보다 먼저 — silent-success 구조적 차단. ──
            if res.raw_error_code == FAKE_EMPTY_RAW_CODE or (
                res.detail == "" and res.raw_error_code != 0
            ):
                # empty = 실패. 보수적으로 transient 재시도(무응답은 일시 통신 문제일 수 있음).
                last_error_code = StatusErrorCode.ENGINE_ERROR_TRANSIENT
                continue

            # timeout sentinel → transient(ENGINE_TIMEOUT).
            if res.raw_error_code == FAKE_TIMEOUT_RAW_CODE:
                last_error_code = StatusErrorCode.ENGINE_TIMEOUT
                continue

            cls = classify_engine_error_code(res.raw_error_code)
            if cls is EngineErrorClass.NORMAL:
                return EngineStepResult(
                    status=EngineStepStatus.SUCCESS,
                    attempts=attempts,
                    last_raw_code=last_raw,
                )
            if cls is EngineErrorClass.TRANSIENT:
                last_error_code = StatusErrorCode.ENGINE_ERROR_TRANSIENT
                continue  # 재시도.
            # permanent — 즉시중단(재시도 없음).
            return EngineStepResult(
                status=EngineStepStatus.PERMANENT,
                attempts=attempts,
                error_code=StatusErrorCode.ENGINE_ERROR_PERMANENT,
                last_raw_code=last_raw,
            )

        # R 소진 — transient/timeout/empty 최종 실패.
        return EngineStepResult(
            status=EngineStepStatus.TRANSIENT_EXHAUSTED,
            attempts=attempts,
            error_code=last_error_code,
            last_raw_code=last_raw,
        )
