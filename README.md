# heysenlyt-pi

hey_senlyt **v1.2.0** 라즈베리파이4 headless 디스펜서 데몬 — 순수 Dart CLI(Firebase 무의존).
3-서비스(web 주문 / monitor-web 관제 / **pi 디스펜서**) 중 pi 트랙.

> **동결 계약 SoT** = `developer/hey_senlyt/v1.2.0/04_erd/hey_senlyt_erd.md`
> 이 코드베이스의 `lib/core/*` 는 그 SoT를 **바이트 동일** 포팅한다. 서버 TS(`heysenlyt-web`)와
> 동일 통과가 목표(부록A P-1~P-10 급소).

## 이번 웨이브 범위 (골격 + 코어 계약 포팅)

✅ 구현:
- 헥사고날 디렉토리 골격 (`lib/{core,ports,adapters,persistence,app}/`, `bin/senlytd.dart`, `test/`)
- **SoT 바이트 동일 Dart 포팅**:
  - §4 `WireStatus`·전이표 8셀·`evaluateTransition`·`phaseToWireStatus` — `lib/core/order_status.dart`
  - §6 `PumpGuard`(`PUMP_PRESETS` 수치표·`clampPumpPreset` 단조성 2줄·`SyringeSpec` 파생·round half-up·fragrance ×1000) — `lib/core/pump_guard.dart`
  - §5 `DispenserOrderDto`(net-new 3필드·마이그레이션 폴백·includeIfNull:false·PII 봉인) — `lib/core/dispenser_order_dto.dart`
  - §9 `Command`/`StatusReport`/`Heartbeat` 와이어 모델(순수 클래스·합성키 `{orderId}:{attempt}`) — `lib/core/wire_messages.dart`
  - §7-4 디스펜서 Bearer 토큰 **수신·만료판단**(opaque 원칙·서명은 서버) — `lib/core/dispenser_session.dart`
  - §4-6 멱등 Ledger 인터페이스(합성키 dedup) — `lib/persistence/idempotency_ledger.dart`
- 포트 인터페이스: `EnginePort` · `CommandSourcePort` · `StatusSinkPort` — `lib/ports/`
- 테스트: 전이표 매트릭스(§4-2) · clamp 회귀(§6) · DTO/와이어 회귀 — 39 케이스 전부 통과

⛔ **유보(안전상 이후 웨이브)**:
- 펌프 구동 실로직(`EnginePort` 실토출·RR 시리얼) — `lib/adapters/sy01b_engine_adapter.dart` TODO 스텁
- Sequencer(소비 루프) — `lib/app/daemon.dart` 골격만
- 실 SSE 구독 / HTTP 역보고 / 오프라인 큐 flush — `lib/adapters/*_adapter.dart` TODO 스텁
- HMAC 서명·검증(서버 책임) — pi 는 토큰을 opaque 로만 다룸(부록A P-5)

## 명령

```bash
dart pub get      # 의존성(test·lints만)
dart test         # 39 케이스 — SoT 파리티 게이트
dart run bin/senlytd.dart   # 골격 안내 후 종료(펌프 미구동)
```

## 아키텍처 (헥사고날)

```
lib/
├── core/          # SoT 바이트 동일 포팅(순수 도메인·firebase/http 무의존)
├── ports/         # 인터페이스(EnginePort·CommandSourcePort·StatusSinkPort)
├── adapters/      # 실어댑터(TODO 스텁: sy01b 시리얼·SSE·HTTP)
├── persistence/   # 멱등 Ledger(인메모리는 테스트 전용·crash-safe 영속은 이후 웨이브)
└── app/           # 데몬 조립(wiring)
bin/senlytd.dart   # 진입점
test/              # 전이표·clamp·DTO·와이어 회귀
```

## 불변식 (SoT 발췌)

- **Firestore 직결 0** — pi 는 서버 SSE 구독 + PATCH 역보고만(§1-2 INV-1).
- **status 전진 write = pi 단독**(§1-2 INV-2) — 유일 경로 PATCH `/api/dispenser/orders/[id]`.
- **재제조 = attempt++ 동반** — 합성키 `{orderId}:{attempt}` fresh 판정(§4-4·부록A P-2).
- **PII 봉인** — DTO에 uid/userName/연락처/IP/sessionId 구조적 부재(§5-2).
