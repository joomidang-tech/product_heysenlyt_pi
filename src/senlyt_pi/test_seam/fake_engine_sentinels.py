"""Fake 엔진 sentinel raw errorCode — SoT §6-7 경계값(EP-03).

Dart `lib/test_seam/fake_engine_sentinels.dart` 포팅.

실 하드웨어의 timeout/무응답은 정수 errorCode 도메인 밖의 사건이다. Fake 하네스와
EngineExecutor 가 이를 **공유 상수**로 인식해 실패 처리(silent-success 금지)하기 위해
src 에 두 sentinel 을 둔다. 실 sy01b 어댑터도 무응답/타임아웃 시 동일 sentinel 을 방출한다.

⚠️ 이 값들은 실 errorCode 도메인(0·1·2·…)과 겹치지 않는 음수여야 한다(classify 오판 방지).
"""

from __future__ import annotations

# timeout(무응답 프레임) — transient(ENGINE_TIMEOUT·재시도) 처리 대상.
FAKE_TIMEOUT_RAW_CODE = -1000

# empty(""·빈응답) — EP-03: **실패**(silent-success 금지). 보수적 transient 재시도.
FAKE_EMPTY_RAW_CODE = -2000
