"""Code 15(Busy NAK) = "기다려라" — 2026-07-19 15:54 실기기 회귀 앵커.

실기기 실측(기기 10000000b9166a1c · trace mnt-770bf19d): fire-and-forget 초기화 done 12초 뒤
"1펌프 전량 흡입" → `I12R` 즉답이 **유효 프레임** `/0O`(status 0x4F = err 15·Busy) — 펌프가
아직 홈 모션 중이라 명령을 NAK 한 것. v1.2.0 `_settle` 은 "명시적 에러 = 즉시 실패" 규칙에
15 를 포함시켜 **건강한 펌프를 ENGINE_ERROR_PERMANENT 로 오판**했다.

v1.1.0 의미론(미러 대상): `_validateResponse` 는 Code 15 를 통과시키고, `_pollUntilReady` 는
isBusyError → 500ms 쉬고 continue — busy 는 에러가 아니라 "기다려라"다.

v1.2.0 처리(이 파일이 앵커하는 계약):
  A. `_settle`(poll=True) 즉답 Code 15 → Ready 대기 후 **명령 1회 재전송** — NAK 는 명령
     미수행이므로 그냥 폴만 하고 진행하면 밸브 회전이 유실된 채 플런저를 민다(엉뚱한 포트
     흡입 = QA "흡입/배출 이슈"의 그 물리 위험). 재전송이 반드시 있어야 한다.
  B. 재전송마저 Busy → 15 정직 반환(무한 재귀 금지·silent-success 금지).
  C. `_poll_until_ready` 중 `?` 가 15 를 줘도 실패가 아니라 계속 폴링.
  D. run_op(plunger_full) 통합 — 초기화 직후 busy NAK 시나리오가 끝까지 성공.
  + 유령 펌프 10: `/10?` 은 "주소1+명령0?" 오독이라 스캔 상한 = 9 (pump_health).
"""

from __future__ import annotations

from senlyt_pi.pipeline.pump_health import DEFAULT_SCAN_MAX, scan_addresses
from senlyt_pi.ports.engine_port import OP_PLUNGER_FULL, EngineOpCommand

# 기존 어댑터 테스트의 시리얼 더블(SerialLike seam) 관례를 재사용한다.
from test_sy01b_engine_adapter import SPEC_05, FakeSerial, adapter_with, status_frame

BUSY_NAK = status_frame(15, ready=False)  # `/0O` 미러 — err 15·ready 없음(모션 중 NAK)
READY = status_frame(0, ready=True)
BUSY_MOTION = status_frame(0, ready=False)  # `?` 중 정상 busy(err 0·ready 없음)


def _writes(fake: FakeSerial) -> list[str]:
    return [w.rstrip("\r") for w in fake.written]


class TestSettleBusyNak:
    def test_busy_nak_waits_ready_then_resends_command(self):
        # I12R → NAK(15) → 폴(busy→ready) → I12R 재전송(ACK) → 폴(ready) → 성공(0).
        fake = FakeSerial(
            responses=[
                BUSY_NAK,  # I12R 1차 — 선행 모션 중 NAK
                BUSY_MOTION,  # ? — 아직 모션
                READY,  # ? — 선행 모션 완료
                READY,  # I12R 재전송 ACK
                READY,  # ? — 회전 완료
            ]
        )
        ad = adapter_with(fake)
        code = ad._settle(1, "I12R", 1.0, poll=True, ack_tolerant=True)
        assert code == 0
        # 핵심 계약: NAK 로 버려진 명령이 **재전송**됐다 (유실 채 진행 금지 — 물리 안전).
        assert _writes(fake).count("/1I12R") == 2

    def test_busy_nak_persisting_after_retry_reports_15(self):
        # 재전송도 NAK → 15 정직 반환(무한 재귀 금지). 재전송은 정확히 1회뿐.
        fake = FakeSerial(
            responses=[
                BUSY_NAK,  # 1차 NAK
                READY,  # ? — ready (그런데도)
                BUSY_NAK,  # 재전송도 NAK — 비정상 지속
                READY,  # ? — ready
            ]
        )
        ad = adapter_with(fake)
        code = ad._settle(1, "I12R", 1.0, poll=True, ack_tolerant=True)
        assert code == 15
        assert _writes(fake).count("/1I12R") == 2  # 1차 + 재전송 1회, 그 이상 없음

    def test_busy_nak_without_poll_still_fails(self):
        # poll=False 경로(스톨 설정 등)는 기존 계약 유지 — 15 = 실패 반환(재시도 없음).
        fake = FakeSerial(responses=[BUSY_NAK])
        ad = adapter_with(fake)
        assert ad._settle(1, "U200,5R", 1.0, poll=False) == 15


class TestPollBusyTolerance:
    def test_poll_continues_through_code_15(self):
        # `?` 가 15 를 줘도 실패가 아니라 계속 폴링(v1.1.0 isBusyError continue 미러).
        fake = FakeSerial(responses=[BUSY_NAK, BUSY_MOTION, READY])
        ad = adapter_with(fake)
        assert ad._poll_until_ready(1, 1.0) == 0

    def test_poll_code_15_does_not_invalidate_setup_cache(self):
        # 15 는 7(홈 상실)과 달리 재초기화 신호가 아니다 — 캐시 유지.
        fake = FakeSerial(responses=[BUSY_NAK, READY])
        ad = adapter_with(fake)
        ad._initialized.add(1)
        assert ad._poll_until_ready(1, 1.0) == 0
        assert 1 in ad._initialized


class TestRunOpBusyNakIntegration:
    def test_plunger_full_right_after_init_succeeds(self):
        # 실기기 시나리오 재현: 브로드캐스트 초기화가 캐시를 등록해 둔 상태(셋업 스킵)에서
        # 흡입의 밸브 회전이 NAK — Ready 대기 + 재전송으로 끝까지 성공해야 한다.
        fake = FakeSerial(
            responses=[
                BUSY_NAK,  # I12R — 홈 모션 중 NAK (실측 그 순간)
                BUSY_MOTION,  # ? — 홈 진행 중
                READY,  # ? — 홈 완료
                READY,  # I12R 재전송 ACK
                READY,  # ? — 회전 완료
                READY,  # 속도+A{full} ACK
                READY,  # ? — 이동 완료
            ]
        )
        ad = adapter_with(fake)
        ad._initialized.add(1)  # fire-and-forget 초기화의 [4/4] 캐시 등록 상태 미러
        res = ad.run_op(
            EngineOpCommand(pump_addr=1, op=OP_PLUNGER_FULL, spec=SPEC_05, valve_port=12)
        )
        assert res.raw_error_code == 0
        w = _writes(fake)
        assert w.count("/1I12R") == 2  # 회전 재전송 실증
        assert any("A" in x and x.endswith("R") and "v" in x for x in w)  # 풀스트로크 이동 수행


class TestBusyWaitBudget:
    def test_busy_wait_uses_init_timeout_floor(self):
        """busy NAK 의 Ready 대기 예산 = max(현재 명령 timeout, init_timeout)(2026-07-19 P1).

        busy 의 원인은 **선행 모션**(대개 fire-and-forget 초기화의 홈 — 실측 done 보고 12s+
        뒤에도 진행)이다. 밸브 회전 I{p}R 의 read_timeout 5s 를 그대로 쓰면 잔여 홈 >5s 에서
        _NO_RESPONSE 실패 — 정비 op 는 단일 시도라 원 사고("건강한 펌프인데 실패")가 재발한다.
        """
        fake = FakeSerial(responses=[BUSY_NAK, READY, READY, READY])
        ad = adapter_with(fake)
        ad.init_timeout_s = 33.0
        polled: list[float] = []
        orig = ad._poll_until_ready

        def record(addr, timeout_s):
            polled.append(timeout_s)
            return orig(addr, timeout_s)

        ad._poll_until_ready = record  # type: ignore[method-assign]
        code = ad._settle(1, "I12R", 1.0, poll=True, ack_tolerant=True)
        assert code == 0
        # busy 대기(첫 폴) = init_timeout 바닥값(33.0) / 재전송 후 완료 폴 = 현재 명령 timeout(1.0).
        assert polled == [33.0, 1.0]


class TestGhostPumpTenScanCap:
    def test_scan_stops_at_9(self):
        # `/10?` = "주소1 + 명령 0?" 오독 → pump1 이 대답하는 유령(2026-07-19 실측). 상한 9.
        assert DEFAULT_SCAN_MAX == 9
        assert scan_addresses() == tuple(range(1, 10))
        assert 10 not in scan_addresses(15)  # 상한을 크게 줘도 9 에서 캡
