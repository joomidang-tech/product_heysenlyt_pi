"""obs — 관측성(observability) 레이어: 구조화 한글 로그 + 서버 중앙집중 trace 전송.

  log   pi 전 파이프라인의 한글 구조화 JSON 로거 (traceId·orderId·deviceId·commandSetId·stage 상관)
"""

from .log import (
    STAGES,
    STAGE_COMMANDSET_ISSUED,
    STAGE_DISPENSE_DONE,
    STAGE_ERROR,
    STAGE_ORDER_RECEIVED,
    STAGE_PI_RECEIVED,
    STAGE_QUEUE_ENQUEUE,
    STAGE_STATUS_REPORT,
    STAGE_STEP_EXEC,
    STAGE_TRANSITION_DONE,
    StructuredLogger,
)

__all__ = [
    "STAGES",
    "STAGE_COMMANDSET_ISSUED",
    "STAGE_DISPENSE_DONE",
    "STAGE_ERROR",
    "STAGE_ORDER_RECEIVED",
    "STAGE_PI_RECEIVED",
    "STAGE_QUEUE_ENQUEUE",
    "STAGE_STATUS_REPORT",
    "STAGE_STEP_EXEC",
    "STAGE_TRANSITION_DONE",
    "StructuredLogger",
]
