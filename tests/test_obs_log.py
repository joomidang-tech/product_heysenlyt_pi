"""obs.log 테스트 — 한글 구조화 로그 상관 필드·stage 어휘·severity·PII 미포함(§11)."""

from __future__ import annotations

import io
import json

from senlyt_pi.obs.log import (
    STAGES,
    STAGE_STEP_EXEC,
    StructuredLogger,
)


def _logger(stream: io.StringIO) -> StructuredLogger:
    return StructuredLogger(
        device_id="dev-A",
        stream=stream,
        now_iso=lambda: "2026-07-10T00:00:00.000Z",
    )


def test_record_has_all_correlation_fields() -> None:
    buf = io.StringIO()
    rec = _logger(buf).event(
        "스텝 실행",
        stage=STAGE_STEP_EXEC,
        trace_id="trace-1",
        order_id="o1",
        command_set_id="o1:1",
    )
    for key in ("traceId", "orderId", "deviceId", "commandSetId", "stage", "severity", "ts"):
        assert key in rec
    assert rec["traceId"] == "trace-1"
    assert rec["orderId"] == "o1"
    assert rec["deviceId"] == "dev-A"  # 바인딩된 deviceId 자동 부착.
    assert rec["commandSetId"] == "o1:1"
    assert rec["stage"] == "스텝실행"


def test_missing_correlation_defaults_to_null() -> None:
    buf = io.StringIO()
    rec = _logger(buf).info("일반 이벤트", stage=STAGE_STEP_EXEC)
    assert rec["traceId"] is None
    assert rec["orderId"] is None
    assert rec["commandSetId"] is None
    assert rec["deviceId"] == "dev-A"


def test_emits_valid_json_line_with_hangul() -> None:
    buf = io.StringIO()
    _logger(buf).warn("토출 실패 감지", stage=STAGE_STEP_EXEC, order_id="o1")
    line = buf.getvalue().strip()
    parsed = json.loads(line)  # 유효한 JSON.
    assert parsed["message"] == "토출 실패 감지"  # 한글 그대로(ensure_ascii=False).
    assert "\\u" not in line  # 유니코드 escape 없음.
    assert parsed["severity"] == "WARN"


def test_detail_kwargs_nested() -> None:
    buf = io.StringIO()
    rec = _logger(buf).event(
        "펌프 응답", stage=STAGE_STEP_EXEC, pumpAddr=1, volumeUl=100
    )
    assert rec["detail"] == {"pumpAddr": 1, "volumeUl": 100}


def test_invalid_severity_falls_back_to_info() -> None:
    buf = io.StringIO()
    rec = _logger(buf).event("x", stage=STAGE_STEP_EXEC, severity="LOUD")
    assert rec["severity"] == "INFO"


def test_sink_receives_record() -> None:
    captured: list[dict] = []
    logger = StructuredLogger(
        device_id="dev-A",
        stream=io.StringIO(),
        now_iso=lambda: "t",
        sink=captured.append,
    )
    logger.info("x", stage=STAGE_STEP_EXEC)
    assert len(captured) == 1
    assert captured[0]["message"] == "x"


def test_bind_sink_wires_after_construction() -> None:
    """sink 없이 생성 후 bind_sink 로 지연 결선 — 결선 **전** 로그도 replay 된다(RC7·2026-07-19).

    부팅(프로비저닝·펌프 자동인식) 로그가 sink 결선(daemon.boot) 전이라 서버 미도달하던 사각 봉합.
    bind_sink 가 버퍼된 pre-bind 레코드를 새 sink 로 흘린다.
    """
    captured: list[dict] = []
    logger = StructuredLogger(device_id="dev-A", stream=io.StringIO(), now_iso=lambda: "t")
    logger.warn("결선 전", stage=STAGE_STEP_EXEC)  # sink 미결선 — 버퍼링(RC7)
    assert captured == []  # 아직 sink 없음 → 즉시 수신은 없다
    logger.bind_sink(captured.append)  # 결선 → 버퍼 replay
    assert len(captured) == 1  # 결선 전 로그가 replay 됨
    assert captured[0]["message"] == "결선 전"
    logger.warn("결선 후", stage=STAGE_STEP_EXEC)  # 결선 후 — 즉시 sink
    assert len(captured) == 2
    assert captured[1]["message"] == "결선 후"
    assert captured[1]["severity"] == "WARN"


def test_nine_korean_stages_defined() -> None:
    assert STAGES == frozenset(
        {
            "주문접수",
            "CommandSet발행",
            "큐적재",
            "pi수신",
            "스텝실행",
            "토출완료",
            "상태보고",
            "전이완료",
            "오류",
        }
    )


def test_bind_device_updates_default() -> None:
    buf = io.StringIO()
    logger = StructuredLogger(stream=buf, now_iso=lambda: "t")
    rec1 = logger.info("before", stage=STAGE_STEP_EXEC)
    assert rec1["deviceId"] is None
    logger.bind_device("dev-Z")
    rec2 = logger.info("after", stage=STAGE_STEP_EXEC)
    assert rec2["deviceId"] == "dev-Z"
