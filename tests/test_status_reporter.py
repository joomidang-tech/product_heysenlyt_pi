"""StatusReporter 테스트 — SoT §9-2 / §4-5.

Dart `test/status_reporter_test.dart` 포팅.
phase 단조·역행거부·멱등판별·errorCode 표준 7종·PII 미포함.
"""

import pytest

from senlyt_pi.core.order_status import DispensePhase
from senlyt_pi.core.pump_guard import StatusErrorCode
from senlyt_pi.pipeline.status_reporter import PhaseRegressionError, StatusReporter


def make() -> StatusReporter:
    seq = iter(range(1000))
    return StatusReporter(
        command_id="o:1",
        trace_id="trace-uuid",
        request_id_gen=lambda: f"req-{next(seq)}",
        now_iso=lambda: "2026-07-03T00:00:00.000Z",
    )


def test_monotonic_progression():
    """정상 단조 진행 ACCEPTED→PROGRESS→COMPLETED."""
    r = make()
    a = r.report(phase=DispensePhase.ACCEPTED, step_k=0, step_n=2)
    assert a.phase == "ACCEPTED"
    assert a.id == "o:1"
    assert a.trace_id == "trace-uuid"
    assert r.report(phase=DispensePhase.PROGRESS, step_k=1, step_n=2).phase == "PROGRESS"
    assert r.report(phase=DispensePhase.COMPLETED, step_k=2, step_n=2).phase == "COMPLETED"


def test_regression_rejected():
    """역행 거부 — PROGRESS 후 ACCEPTED 는 PhaseRegressionError."""
    r = make()
    r.report(phase=DispensePhase.ACCEPTED, step_k=0, step_n=2)
    r.report(phase=DispensePhase.PROGRESS, step_k=1, step_n=2)
    with pytest.raises(PhaseRegressionError):
        r.report(phase=DispensePhase.ACCEPTED, step_k=0, step_n=2)


def test_no_reports_after_terminal():
    """종결 후 추가 보고 거부(멱등·단조)."""
    r = make()
    r.report(phase=DispensePhase.ACCEPTED, step_k=0, step_n=1)
    r.report(phase=DispensePhase.COMPLETED, step_k=1, step_n=1)
    with pytest.raises(PhaseRegressionError):
        r.report(phase=DispensePhase.PROGRESS, step_k=1, step_n=1)
    with pytest.raises(PhaseRegressionError):
        r.report(phase=DispensePhase.COMPLETED, step_k=1, step_n=1)


def test_error_code_standard_seven_on_failed():
    """errorCode 표준 7종 방출(FAILED)."""
    r = make()
    r.report(phase=DispensePhase.ACCEPTED, step_k=0, step_n=2)
    f = r.report(
        phase=DispensePhase.FAILED,
        step_k=1,
        step_n=2,
        error_code=StatusErrorCode.PARTIAL_DISPENSE,
    )
    assert f.error_code is StatusErrorCode.PARTIAL_DISPENSE
    assert f.to_json()["errorCode"] == "PARTIAL_DISPENSE"


def test_no_pii_in_status_report_json():
    """PII 미포함 — StatusReport JSON 에 uid/userName/연락처 키 없음."""
    r = make()
    json_map = r.report(phase=DispensePhase.ACCEPTED, step_k=0, step_n=1).to_json()
    for banned in ("uid", "userName", "phone", "email", "ip", "sessionId"):
        assert banned not in json_map, f"{banned} 누출 금지"
    # 허용 키만.
    assert set(json_map.keys()) == {
        "id", "phase", "stepK", "stepN", "errorCode", "requestId", "traceId", "updatedAt",
    }


def test_would_be_duplicate():
    """멱등 판별 — 동일 (phase, stepK) 재보고 would_be_duplicate."""
    r = make()
    r.report(phase=DispensePhase.PROGRESS, step_k=1, step_n=3)
    assert r.would_be_duplicate(DispensePhase.PROGRESS, 1)
    assert not r.would_be_duplicate(DispensePhase.PROGRESS, 2)


def test_request_id_fresh_per_report():
    """requestId 매 보고 새로 발급(재사용 금지·O-3)."""
    r = make()
    a = r.report(phase=DispensePhase.ACCEPTED, step_k=0, step_n=2)
    b = r.report(phase=DispensePhase.PROGRESS, step_k=1, step_n=2)
    assert a.request_id != b.request_id
