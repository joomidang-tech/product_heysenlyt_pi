"""pi 구조화 한글 로그 — 02_infra §11 로그 표준 (서버 중앙집중·상관 필드 촘촘).

설계 정본: `developer/hey_senlyt/v1.2.0/02_infra/hey_senlyt_infra.md` §11.

목적:
  - pi 전 파이프라인(수신·검증·스텝실행·토출·보고)에 **한글 구조화 JSON** 로그를 남긴다.
  - 모든 레코드는 상관 필드(`traceId·orderId·deviceId·commandSetId·stage·severity`)를
    **항상** 보유한다(부재 시 null) → 검색·상관·일관 판단이 로그 하나로 성립.
  - `traceId` 는 **주문에서 전파**된 값(web POST /api/orders 최초 발급) 을 재발급 없이 싣는다.
  - 이 로거는 **로컬 stderr** 로 JSON 라인을 방출한다. 서버 중앙집중(POST /api/dispenser/trace →
    TraceStore)은 status_sink 어댑터의 `ship_trace` 가 담당하며, 그 span 과 이 로그는 동일 traceId
    로 상관된다(§11·tech-spec §6.6 단일 traceId 전파).

⚠️ 로그 본문은 **비식별만**(§10-3) — uid/userName/연락처/IP/sessionId 를 detail 에 넣지 않는다.
   서버가 2차 sanitize 하지만 pi 도 1차로 PII 를 싣지 않는 것이 원칙.
"""

from __future__ import annotations

import json
import sys
import threading
from datetime import datetime, timezone
from typing import Any, Callable, TextIO

# ── stage 한글 어휘 9종 (§11) — 주문 파이프라인의 단계 표식(검색·상관 키). ──
STAGE_ORDER_RECEIVED = "주문접수"
STAGE_COMMANDSET_ISSUED = "CommandSet발행"
STAGE_QUEUE_ENQUEUE = "큐적재"
STAGE_PI_RECEIVED = "pi수신"
STAGE_STEP_EXEC = "스텝실행"
STAGE_DISPENSE_DONE = "토출완료"
STAGE_STATUS_REPORT = "상태보고"
STAGE_TRANSITION_DONE = "전이완료"
STAGE_ERROR = "오류"

STAGES: frozenset[str] = frozenset(
    {
        STAGE_ORDER_RECEIVED,
        STAGE_COMMANDSET_ISSUED,
        STAGE_QUEUE_ENQUEUE,
        STAGE_PI_RECEIVED,
        STAGE_STEP_EXEC,
        STAGE_DISPENSE_DONE,
        STAGE_STATUS_REPORT,
        STAGE_TRANSITION_DONE,
        STAGE_ERROR,
    }
)

# severity 4종 — TraceSpan.level 과 동형(§10-4 DEBUG|INFO|WARN|ERROR).
_SEVERITIES: frozenset[str] = frozenset({"DEBUG", "INFO", "WARN", "ERROR"})

# 상관 필드는 항상 존재(부재 시 null) — 검색·상관 일관성(§11).
_CORRELATION_KEYS = ("traceId", "orderId", "deviceId", "commandSetId", "stage")


def _default_now_iso() -> str:
    """ISO8601 밀리초 Z — TraceSpan.ts 와 동일 포맷(부록A P-3)."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


class StructuredLogger:
    """한글 구조화 JSON 로거 — 상관 필드 항상 보유(§11).

    `device_id` 는 부팅 시 1회 바인딩(등록 후 deviceId)해 매 레코드에 자동 부착한다.
    테스트는 `sink`(레코드 콜백)·`now_iso` 주입으로 결정적 검증한다.
    """

    def __init__(
        self,
        *,
        device_id: str | None = None,
        service: str = "pi",
        stream: TextIO | None = None,
        now_iso: Callable[[], str] | None = None,
        sink: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.device_id = device_id
        self.service = service
        self._stream = stream if stream is not None else sys.stderr
        self._now_iso = now_iso if now_iso is not None else _default_now_iso
        # 테스트/서버전송 훅 — 방출된 레코드(dict)를 그대로 넘겨받는다(선택).
        self._sink = sink
        # RC7(2026-07-19) — sink 결선 **전**(부팅 프로비저닝·기기등록·펌프 자동인식) 레코드를 버퍼했다가
        #   bind_sink 시 replay 한다. 그 구간 로그가 sink=None 이라 서버 미도달하던 사각 봉합. 상한 200 으로
        #   무한성장 방지(부팅 로그는 소수).
        self._pending: "list[dict[str, Any]]" = []
        # 스텝 실행 컨텍스트(스레드로컬 · 2026-07-19 QA "흡입/배출 이슈") — 시퀀서가 스텝 실행 전
        #   traceId/orderId/commandSetId 를 바인딩하면, 그 스레드에서 나가는 **모든** 로그(어댑터
        #   시리얼 왕복 DEBUG·정비 실패 ERROR 등)에 자동 부착된다. 구조: 어댑터는 trace 를 모른다
        #   (명령에 상관 필드가 없다) → 호출 스레드의 컨텍스트로 엮는다. 명시 인자가 항상 이긴다.
        #   덕분에 admin trace 타임라인에서 "어느 명령의 어느 시리얼 왕복이 깨졌는지"가 한 줄로 보인다.
        self._step_ctx = threading.local()

    def bind_device(self, device_id: str) -> None:
        """등록 후 deviceId 바인딩 — 이후 모든 레코드에 자동 부착."""
        self.device_id = device_id

    def bind_step_context(
        self,
        *,
        trace_id: str | None = None,
        order_id: str | None = None,
        command_set_id: str | None = None,
    ) -> None:
        """이 스레드의 스텝 실행 컨텍스트 바인딩 — 이후 이 스레드 로그에 상관 필드 자동 부착.

        시퀀서가 스텝 실행 직전에 부르고, 끝나면 `clear_step_context()` 로 반드시 걷는다
        (워커 스레드는 풀에서 재사용되므로 안 걷으면 다음 잡에 전 잡의 trace 가 새어 붙는다).
        """
        self._step_ctx.trace_id = trace_id
        self._step_ctx.order_id = order_id
        self._step_ctx.command_set_id = command_set_id

    def clear_step_context(self) -> None:
        """이 스레드의 스텝 컨텍스트 해제(finally 에서 호출)."""
        self._step_ctx.trace_id = None
        self._step_ctx.order_id = None
        self._step_ctx.command_set_id = None

    def bind_sink(self, sink: Callable[[dict[str, Any]], None]) -> None:
        """서버 전송 sink 결선 — 데몬이 부팅 시 자기 `_ship_log` 를 꽂는다(생성 후 지연 결선).

        `event()` 가 매 레코드(dict)를 stderr 방출과 **함께** 이 sink 로도 넘긴다 → pi 운영 로그가
        서버 trace 로 합류(admin 관측). sink 는 best-effort — 예외는 event() 가 삼킨다(제조 무영향).
        """
        self._sink = sink
        # RC7 — 결선 전 버퍼된 부팅 로그를 새 sink 로 replay(부팅 프로비저닝·펌프인식 실패 관측 복원).
        pending, self._pending = self._pending, []
        for rec in pending:
            try:
                sink(rec)
            except Exception:  # noqa: BLE001 — sink 예외는 삼킨다(제조/부팅 무영향).
                pass

    def event(
        self,
        message: str,
        *,
        stage: str,
        severity: str = "INFO",
        trace_id: str | None = None,
        order_id: str | None = None,
        command_set_id: str | None = None,
        device_id: str | None = None,
        **detail: Any,
    ) -> dict[str, Any]:
        """구조화 레코드 1건 방출 — 상관 필드 항상 포함(부재 시 null).

        Returns: 방출한 레코드(dict) — 테스트/후속 상관에 재사용.
        """
        sev = severity if severity in _SEVERITIES else "INFO"
        # 상관 필드 — 명시 인자 > 스레드 스텝 컨텍스트 > null (어댑터 로그가 자동으로 trace 에 엮인다).
        ctx = self._step_ctx
        record: dict[str, Any] = {
            "ts": self._now_iso(),
            "service": self.service,
            "severity": sev,
            "stage": stage if stage in STAGES else stage,  # 미지 stage 도 그대로(관측 우선)
            "message": message,
            "traceId": trace_id if trace_id is not None else getattr(ctx, "trace_id", None),
            "orderId": order_id if order_id is not None else getattr(ctx, "order_id", None),
            "deviceId": device_id if device_id is not None else self.device_id,
            "commandSetId": (
                command_set_id if command_set_id is not None else getattr(ctx, "command_set_id", None)
            ),
        }
        # 상관 키가 하나라도 빠지지 않도록 보증(방어).
        for k in _CORRELATION_KEYS:
            record.setdefault(k, None)
        if detail:
            record["detail"] = detail

        line = json.dumps(record, ensure_ascii=False)  # 한글 그대로(escape 금지)
        try:
            print(line, file=self._stream, flush=True)
        except (OSError, ValueError):
            pass  # 로그 방출 실패는 제조를 막지 않는다(§10-6 best-effort).
        if self._sink is not None:
            try:
                self._sink(record)
            except Exception:
                pass
        elif len(self._pending) < 200:
            # RC7 — 아직 sink 미결선(부팅 전) → 버퍼. bind_sink 가 replay 한다(상한 초과분은 버림).
            self._pending.append(record)
        return record

    # ── severity 편의 메서드 — stage 는 필수(호출측이 파이프라인 단계를 명시). ──
    def debug(self, message: str, *, stage: str, **kw: Any) -> dict[str, Any]:
        return self.event(message, stage=stage, severity="DEBUG", **kw)

    def info(self, message: str, *, stage: str, **kw: Any) -> dict[str, Any]:
        return self.event(message, stage=stage, severity="INFO", **kw)

    def warn(self, message: str, *, stage: str, **kw: Any) -> dict[str, Any]:
        return self.event(message, stage=stage, severity="WARN", **kw)

    def error(self, message: str, *, stage: str, **kw: Any) -> dict[str, Any]:
        return self.event(message, stage=stage, severity="ERROR", **kw)
