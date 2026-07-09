"""EngineExecutor 테스트 — SoT §6-7 / 질의서 Q8(EP-03·EP-09).

Dart `test/engine_executor_test.dart` 포팅.
**EP-03 게이트(빈응답=실패·silent-success 금지)** = 이 파일의 핵심.
재시도(transient R=3)·즉시중단(permanent)·timeout·empty 실패 분류를 dispense 카운터로 검증.
"""

from senlyt_pi.adapters.fake_engine_adapter import FakeEngineOutcome, FakeEnginePort
from senlyt_pi.core.pump_guard import StatusErrorCode, SyringeSpec
from senlyt_pi.pipeline.engine_executor import EngineExecutor, EngineStepStatus
from senlyt_pi.ports.engine_port import EngineDispenseCommand

SPEC = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=1.25)


def cmd(addr: int) -> EngineDispenseCommand:
    return EngineDispenseCommand(pump_addr=addr, volume_ul=100, steps=960, spec=SPEC)


def test_ack_success_single_dispense():
    """정상 ack → success, 1회 dispense."""
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.ACK)
    ex = EngineExecutor(fake)
    res = ex.run_step(cmd(1))
    assert res.is_success
    assert res.attempts == 1
    assert fake.dispense_count == 1


def test_ep03_empty_is_failure_after_retry_exhaustion():
    """EP-03: empty(무응답) = 실패 — silent-success 0, R 소진."""
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.EMPTY)
    ex = EngineExecutor(fake, max_retries=3)
    res = ex.run_step(cmd(1))
    assert not res.is_success, "빈응답은 절대 성공 아님(EP-03)"
    assert res.status is EngineStepStatus.TRANSIENT_EXHAUSTED
    # 첫 시도 + 3 재시도 = 4 물리 dispense(재시도했으나 전부 empty).
    assert fake.dispense_count == 4
    assert res.error_code is StatusErrorCode.ENGINE_ERROR_TRANSIENT


def test_permanent_stops_immediately():
    """permanent → 즉시중단(재시도 없음), 1회 dispense."""
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.PERMANENT)
    ex = EngineExecutor(fake, max_retries=3)
    res = ex.run_step(cmd(1))
    assert res.status is EngineStepStatus.PERMANENT
    assert res.attempts == 1, "permanent 는 재시도 안 함"
    assert fake.dispense_count == 1
    assert res.error_code is StatusErrorCode.ENGINE_ERROR_PERMANENT


def test_busy_then_ack_retry_succeeds():
    """busy(transient) 후 ack → 재시도 성공."""
    fake = FakeEnginePort()
    fake.script_for(1, [FakeEngineOutcome.BUSY, FakeEngineOutcome.ACK])
    ex = EngineExecutor(fake, max_retries=3)
    res = ex.run_step(cmd(1))
    assert res.is_success
    assert res.attempts == 2
    assert fake.dispense_count == 2


def test_timeout_classified_engine_timeout():
    """timeout → ENGINE_TIMEOUT 분류, R 소진 실패."""
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.TIMEOUT)
    ex = EngineExecutor(fake, max_retries=2)
    res = ex.run_step(cmd(1))
    assert not res.is_success
    assert res.error_code is StatusErrorCode.ENGINE_TIMEOUT
    assert fake.dispense_count == 3  # 1 + 2 재시도.


def test_transient_exhaustion_all_busy():
    """transient 재시도 소진(전부 busy) → TRANSIENT_EXHAUSTED."""
    fake = FakeEnginePort()
    fake.script_all(FakeEngineOutcome.BUSY)
    ex = EngineExecutor(fake, max_retries=3)
    res = ex.run_step(cmd(1))
    assert res.status is EngineStepStatus.TRANSIENT_EXHAUSTED
    assert fake.dispense_count == 4
