"""전이표 케이스 매트릭스 — SoT §4-2 그대로. 서버 `orderStatus.test.ts` 와 동일 통과가 목표.

Dart `test/order_status_test.dart` 포팅. 부록A P-1 게이트: 이 매트릭스가 TS 와 바이트 동일해야 계약 성립.
"""

from senlyt_pi.core.order_status import (
    DispensePhase,
    TransitionVerdict,
    WireStatus,
    evaluate_transition,
    is_wire_status,
    phase_to_wire_status,
)


class TestEvaluateTransition:
    def test_same_state_is_noop(self):
        """from == to 는 noop(멱등)."""
        for s in WireStatus:
            assert evaluate_transition(s, s) is TransitionVerdict.NOOP

    def test_pending_forward_all_apply(self):
        """PENDING 전진은 모두 apply (COMPLETED 직행 허용 — F2)."""
        assert evaluate_transition(WireStatus.PENDING, WireStatus.PROCESSING) is TransitionVerdict.APPLY
        assert evaluate_transition(WireStatus.PENDING, WireStatus.COMPLETED) is TransitionVerdict.APPLY
        assert evaluate_transition(WireStatus.PENDING, WireStatus.FAILED) is TransitionVerdict.APPLY

    def test_processing_to_completed_failed_apply(self):
        """PROCESSING → COMPLETED/FAILED 는 apply."""
        assert evaluate_transition(WireStatus.PROCESSING, WireStatus.COMPLETED) is TransitionVerdict.APPLY
        assert evaluate_transition(WireStatus.PROCESSING, WireStatus.FAILED) is TransitionVerdict.APPLY

    def test_completed_is_terminal(self):
        """COMPLETED 는 terminal — 어떤 전진도 illegal (un-complete 금지)."""
        assert evaluate_transition(WireStatus.COMPLETED, WireStatus.PENDING) is TransitionVerdict.ILLEGAL
        assert evaluate_transition(WireStatus.COMPLETED, WireStatus.PROCESSING) is TransitionVerdict.ILLEGAL
        assert evaluate_transition(WireStatus.COMPLETED, WireStatus.FAILED) is TransitionVerdict.ILLEGAL

    def test_failed_only_to_pending(self):
        """FAILED → PENDING 만 허용(운영자 재시도), 그 외 illegal."""
        assert evaluate_transition(WireStatus.FAILED, WireStatus.PENDING) is TransitionVerdict.APPLY
        assert evaluate_transition(WireStatus.FAILED, WireStatus.PROCESSING) is TransitionVerdict.ILLEGAL
        assert evaluate_transition(WireStatus.FAILED, WireStatus.COMPLETED) is TransitionVerdict.ILLEGAL

    def test_processing_to_pending_illegal(self):
        """PROCESSING → PENDING(역행)은 illegal."""
        assert evaluate_transition(WireStatus.PROCESSING, WireStatus.PENDING) is TransitionVerdict.ILLEGAL

    def test_full_4x4_matrix(self):
        """전체 4x4 매트릭스 — SoT §4-2 표 그대로."""
        noop = TransitionVerdict.NOOP
        apply = TransitionVerdict.APPLY
        illegal = TransitionVerdict.ILLEGAL
        expected = {
            WireStatus.PENDING: {
                WireStatus.PENDING: noop,
                WireStatus.PROCESSING: apply,
                WireStatus.COMPLETED: apply,
                WireStatus.FAILED: apply,
            },
            WireStatus.PROCESSING: {
                WireStatus.PENDING: illegal,
                WireStatus.PROCESSING: noop,
                WireStatus.COMPLETED: apply,
                WireStatus.FAILED: apply,
            },
            WireStatus.COMPLETED: {
                WireStatus.PENDING: illegal,
                WireStatus.PROCESSING: illegal,
                WireStatus.COMPLETED: noop,
                WireStatus.FAILED: illegal,
            },
            WireStatus.FAILED: {
                WireStatus.PENDING: apply,
                WireStatus.PROCESSING: illegal,
                WireStatus.COMPLETED: illegal,
                WireStatus.FAILED: noop,
            },
        }
        for frm in WireStatus:
            for to in WireStatus:
                assert evaluate_transition(frm, to) is expected[frm][to], (
                    f"{frm.wire} -> {to.wire}"
                )


class TestIsWireStatus:
    def test_known_statuses_only(self):
        """알려진 상태만 True."""
        assert is_wire_status("PENDING") is True
        assert is_wire_status("COMPLETED") is True
        assert is_wire_status("ERROR") is False
        assert is_wire_status(None) is False
        assert is_wire_status(123) is False


class TestPhaseToWireStatus:
    def test_mapping(self):
        """ACCEPTED/PROGRESS → PROCESSING · COMPLETED → COMPLETED · FAILED → FAILED (§4-5/§9-2)."""
        assert phase_to_wire_status(DispensePhase.ACCEPTED) is WireStatus.PROCESSING
        assert phase_to_wire_status(DispensePhase.PROGRESS) is WireStatus.PROCESSING
        assert phase_to_wire_status(DispensePhase.COMPLETED) is WireStatus.COMPLETED
        assert phase_to_wire_status(DispensePhase.FAILED) is WireStatus.FAILED
