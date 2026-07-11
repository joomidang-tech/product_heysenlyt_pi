# heysenlyt-pi

hey_senlyt **v1.2.0** 라즈베리파이4 headless 디스펜서 데몬 (Firebase 무의존).
3-서비스(**order-web** 주문 / **admin-web** 관제[heysenlyt-web 내 `/admin` 라우트·same-origin] / **pi 디스펜서**) 중 pi 트랙.

> **배포 산출물 = Python 데몬** (`src/senlyt_pi/` · 콘솔 스크립트 `senlytd` · `python:3.12-slim` + systemd).
> **Dart 구현(`lib/`·`test/`)은 포팅 오라클** — 동결 계약 SoT를 바이트 동일 포팅해 Python의 parity 기준으로만 쓴다(배포 아님).
> pyproject 헤더 정본: "Dart → Python 재작성 · 포팅 오라클 = 이 레포의 Dart 구현 · 수치 정본 = heysenlyt-web `lib/server/pumpGuard.ts`+`settingsClamp.ts`".

> **동결 계약 SoT** = `developer/hey_senlyt/v1.2.0/04_erd/hey_senlyt_erd.md`
> Python `senlyt_pi/core/*` 와 Dart `lib/core/*` 는 **둘 다** 그 SoT를 **바이트 동일** 포팅한다.
> 서버 TS(`heysenlyt-web`)와 동일 통과가 목표(부록A P-1~P-10 급소).

## 배포 데몬 (Python · `src/senlyt_pi/`)

헥사고날 구조. 콘솔 스크립트 `senlytd = senlyt_pi.app.senlytd:main`.

```
src/senlyt_pi/
├── core/          # SoT 바이트 동일 포팅(순수 도메인·firebase/http 무의존)
│                  #   order_status · pump_guard · dispenser_order_dto
│                  #   dispenser_session · wire_messages · wire_json · command_set
├── ports/         # 인터페이스(command_source·commandset_source·engine·status_sink)
├── adapters/      # 실어댑터(sse_command_source · http_status_sink · http_client
│                  #   sy01b_engine · registration · device_identity · settings · fake_engine)
├── pipeline/      # 소비 파이프라인(pump_sequencer · recipe_resolver · status_reporter
│                  #   offline_queue · boot_recovery · engine_executor)
├── persistence/   # 멱등 Ledger(idempotency · file_idempotency = crash-safe 영속)
├── config/        # server_target(서버 타겟팅)
├── obs/           # 로깅(log)
├── app/           # 데몬 조립(bootstrap · daemon · dispatcher · senlytd 진입점)
└── test_seam/     # 테스트 심(fake_engine_sentinels)
```

```bash
pip install --no-cache-dir -e .   # senlytd 콘솔 스크립트 설치
pytest                            # Python 유닛 (tests/)
senlytd                           # 데몬 (SENLYT_RUN=1 에서 소비 루프 · 무설정은 안전 종료 0)
```

> 실제 런타임/배선 상태는 코드와 `Dockerfile`(`CMD ["senlytd"]`)이 정본. 펌프 실토출·시리얼 등
> 하드웨어 접점은 안전상 게이팅되며 상태는 코드 기준으로 확인한다.

## 포팅 오라클 (Dart · `lib/`)

동결 계약 SoT를 바이트 동일 포팅한 **parity 기준**. Python 구현이 이 오라클과 같은 결과를 내는지 검증한다(배포 대상 아님).

- **SoT 바이트 동일 Dart 포팅**:
  - §4 `WireStatus`·전이표 8셀·`evaluateTransition`·`phaseToWireStatus` — `lib/core/order_status.dart`
  - §6 `PumpGuard`(`PUMP_PRESETS` 수치표·`clampPumpPreset` 단조성 2줄·`SyringeSpec` 파생·round half-up·fragrance ×1000) — `lib/core/pump_guard.dart`
  - §5 `DispenserOrderDto`(net-new 3필드·마이그레이션 폴백·includeIfNull:false·PII 봉인) — `lib/core/dispenser_order_dto.dart`
  - §9 `Command`/`StatusReport`/`Heartbeat` 와이어 모델(순수 클래스·합성키 `{orderId}:{attempt}`) — `lib/core/wire_messages.dart`
  - §7-4 디스펜서 Bearer 토큰 **수신·만료판단**(opaque 원칙·서명은 서버) — `lib/core/dispenser_session.dart`
  - §4-6 멱등 Ledger 인터페이스(합성키 dedup) — `lib/persistence/idempotency_ledger.dart`
- 포트 인터페이스: `EnginePort` · `CommandSourcePort` · `StatusSinkPort` — `lib/ports/`

```bash
dart pub get      # 의존성(test·lints만)
dart test         # 39 케이스 — SoT 파리티 게이트
```

## 불변식 (SoT 발췌 · 언어 무관)

- **Firestore 직결 0** — pi 는 서버 SSE 구독 + PATCH 역보고만(§1-2 INV-1).
- **status 전진 write = pi 단독**(§1-2 INV-2) — 유일 경로 PATCH `/api/dispenser/orders/[id]`.
- **재제조 = attempt++ 동반** — 합성키 `{orderId}:{attempt}` fresh 판정(§4-4·부록A P-2).
- **PII 봉인** — DTO에 uid/userName/연락처/IP/sessionId 구조적 부재(§5-2).
- **HMAC 서명·검증 = 서버 책임** — pi 는 토큰을 opaque 로만 다룸(부록A P-5).
