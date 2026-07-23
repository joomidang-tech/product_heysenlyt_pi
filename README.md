# heysenlyt-pi

hey_senlyt **v1.2.0** 라즈베리파이4 headless 디스펜서 데몬 (Firebase 무의존).
3-서비스(**order-web** 주문 / **admin-web** 관제[heysenlyt-web 내 `/admin` 라우트·same-origin] / **pi 디스펜서**) 중 pi 트랙.

## 🚀 빠른 설치 (라즈베리파이 · 1줄)

라즈베리파이에서 **한 줄**이면 다운로드부터 systemd 등록·기동까지 됩니다. 명령어는 **하나로 고정**이고, 바꾸는 건 **맨 끝 서버 URL 하나**뿐입니다:

```bash
curl -fsSL https://raw.githubusercontent.com/joomidang-tech/product_heysenlyt_pi/main/install.sh \
  | sudo bash -s -- https://senlyt.com
```

| 환경 | 맨 끝 서버 URL만 교체 |
|------|----------------------|
| **prod** | `https://senlyt.com` |
| **dev** | `https://dev-env.senlyt.com` |
| **버전 프리뷰** | `https://v1-2-0.env.senlyt.com` |

> pi 코드는 항상 **main(승격 안정본)** 에서 받습니다 — 데몬은 서버-불가지(어느 서버를 보든 인자로 받음)라, 환경 구분은 "어느 서버 URL을 보게 하나" 하나로만 합니다. (아직 main에 안 올라간 코드를 먼저 시험할 때만 `SENLYT_INSTALL_BRANCH=dev` 로 브랜치를 덮어쓰고 raw URL 경로도 그 브랜치로 바꿔 실행.)

그 뒤 흐름 = **admin에 "승인 대기"로 뜸 → `<서버URL>/admin` 에서 "승인 + 모드 배정" → online**.

- 나머지(deviceId·mode·engine·valve)는 **런타임 자동** — deviceId=HW시리얼 자동수집 · mode=승인 시 배정 · engine/valve=부팅 자동감지(실 Pi+시리얼→sy01b·GPIO→gpio·아니면 fake).
- 등록에 **비밀키 없음(TOFU)** — 키 없이 신청하고 **운영자 승인**이 관문. 승인 전엔 "승인 대기"로 폴링만(정상).
- 재실행 안전(멱등) · 부팅 자동시작 · `Restart=always` 무인 복구. 상세 수동 설치는 아래 "실행" 절 참조.

## 🛠 운영 명령 (설치 후 관리)

설치가 끝나면 데몬은 `senlytd` 라는 **systemd 서비스**로 돕니다(부팅 자동시작). 아래 명령으로 관리합니다.

```bash
# ── 시작/정지/상태 ──
sudo systemctl stop senlytd      # 잠깐 멈춤 (즉시 정지 — 서버 등록 폴링도 멈춤)
sudo systemctl start senlytd     # 다시 시작
sudo systemctl status senlytd --no-pager   # 현재 상태 확인 (running/실패 여부)

# ── 로그 ──
journalctl -u senlytd -f         # 실시간 로그 (Ctrl+C 로 빠져나옴)
                                 #   "하드웨어 자가진단" 줄에서 engine/valve 감지 결과 확인
```

> **설치 직후 정상 출력** — install.sh 가 끝나면 아래처럼 안내합니다:
> ```
> ✅ 설치·기동 완료 — senlytd 가 부팅 자동시작으로 돕니다.
>    상태:  systemctl status senlytd --no-pager
>    로그:  journalctl -u senlytd -f     ("하드웨어 자가진단" 줄로 engine/valve 확인)
> ```

- **재설치/갱신** — 위 [빠른 설치](#-빠른-설치-라즈베리파이--1줄) 한 줄을 다시 실행하면 됩니다(멱등 — 최신 코드로 pull 후 재기동). 수동으로 멈출 필요 없음.
- **`stop` 은 "잠깐 멈춤"** — 서버로의 등록·주문 폴링이 모두 멈춥니다. 다시 받으려면 `start`. 완전 제거가 아니라 부팅 시 다시 뜹니다(`disable` 해야 자동시작 해제).

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
