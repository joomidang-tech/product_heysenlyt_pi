"""senlytd 실 어댑터 조립(bootstrap) — 환경변수 → ServerConfig → 실 어댑터 결선.

정본: 02_infra §10 통합 E2E 토폴로지.

책임(스텁 제거·사용자 원칙 2026-07-10):
  - `SENLYT_ENV`/`SENLYT_SERVER_BASE_URL` → `ServerConfig`(base URL 단일 결정·fail-fast).
  - 등록(POST /api/dispensers/register·실 HTTP) → deviceId·dispenserToken 확보(파일 영속).
  - 실 어댑터 조립: SSE command/commandSet source + HTTP status sink(orders/heartbeat/trace/봉투전이).
  - **엔진만 FakeEngineAdapter**(유일 mock·v1.1.0 HW 검증). `SENLYT_ENGINE=fake|sy01b` 로 분기,
    기본(E2E)=fake. sy01b(실 RS485)는 아직 TODO 스텁이라 명시적으로 선택할 때만 조립.

⚠️ 이 모듈은 **결선(wiring)만** 한다 — 실제 펌프 소비 루프(SSE→멱등→Sequencer→역보고 상시 구동)는
   안전상 daemon.boot 유보를 유지한다. bootstrap 은 어댑터를 실체로 만들어 DaemonDeps 로 묶는다.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from ..adapters.device_identity_store import DeviceIdentity, DeviceIdentityStore
from ..adapters.fake_engine_adapter import FakeEnginePort
from ..adapters.valve_adapter import (
    DEFAULT_FLOW_ML_PER_SEC,
    DEFAULT_MAX_OPEN_SEC,
    DEFAULT_VALVE_PINS,
    FakeValveAdapter,
)
from ..adapters.http_status_sink_adapter import HttpStatusSinkAdapter
from ..adapters.registration_client import (
    RegistrationClient,
    ensure_registered,
    make_http_register_transport,
    read_hardware_id,
)
from ..adapters.settings_source import (
    fetch_settings_once,
    full_stroke_from_settings,
    syringe_capacity_from_settings,
)
from ..adapters.sse_command_source_adapter import SseCommandSourceAdapter
from ..config.server_target import ServerConfig
from ..core.pump_guard import PUMP_PRESETS, SyringeSpec, resolve_syringe_capacity_ml
from ..obs.log import STAGE_ERROR, STAGE_PI_RECEIVED, StructuredLogger
from ..persistence.file_idempotency_ledger import FileIdempotencyLedger
from ..persistence.idempotency_ledger import IdempotencyLedger, InMemoryIdempotencyLedger
from ..pipeline.pump_health import auto_pump_map, discover_pumps
from ..pipeline.trace_spill import TraceSpill
from ..pipeline.recipe_resolver import RecipeResolver
from ..ports.engine_port import EnginePort
from ..ports.valve_port import ValvePort

# 엔진 선택 env(override) — 미지정이면 **자동감지**(실 Pi+시리얼 어댑터→sy01b·아니면 fake). 02_infra §10.
#   설치 시 안 넣어도 됨("URL만"). 명시하면 그 값 우선(fake|sy01b) — E2E/개발 고정용.
SENLYT_ENGINE_ENV = "SENLYT_ENGINE"
# pi 실행 모드(주문 큐 mode·flavor|fragrance) — 어느 컬렉션/큐를 구독·역보고할지.
#   ⚠️ TOFU 후 **서버 배정(identity.mode)이 우선** — 이 env 는 서버 미배정 시 폴백일 뿐(더 이상 필수 아님).
SENLYT_MODE_ENV = "SENLYT_MODE"
# 정체성 파일 경로 override(기본 = LOG_DIR 또는 작업 디렉터리).
SENLYT_IDENTITY_PATH_ENV = "SENLYT_IDENTITY_PATH"
# 매장 표시 이름(선택) — register name.
SENLYT_DEVICE_NAME_ENV = "SENLYT_DEVICE_NAME"
# 멱등 ledger 파일 경로 override(기본 = LOG_DIR 또는 작업 디렉터리).
SENLYT_LEDGER_PATH_ENV = "SENLYT_LEDGER_PATH"
# 관측 로그 디스크 스풀 파일 경로 override(기본 = LOG_DIR 또는 작업 디렉터리).
#   단절 중 전송 실패한 trace 배치를 보존 → 재연결 시 전량 업로드(유실 0 · 2026-07-19).
SENLYT_TRACE_SPILL_PATH_ENV = "SENLYT_TRACE_SPILL_PATH"
# 펌프 addr 배치(모드별) — 예: "aroma:1,2,3;flavor:4" (E2E 02_infra §10 pi 서비스 env).
SENLYT_PUMP_ADDRESSES_ENV = "PUMP_ADDRESSES"
# 기주 밸브 선택 env(override·§9-1 v2) — 미지정이면 **자동감지**(실 Pi→gpio·아니면 fake).
#   설치 시 안 넣어도 됨("URL만"). 명시: fake(시뮬) | gpio(실기기·명시 시 결선실패=fail-fast) | off(미결선 drop).
SENLYT_VALVE_ENV = "SENLYT_VALVE"
# 밸브 핀 매핑(BCM) — 기본 "sour:17,normal:27" (신기주=BCM17/물리핀11·베이스=BCM27/물리핀13·2026-07-17 실배선 정정).
SENLYT_VALVE_PINS_ENV = "SENLYT_VALVE_PINS"
# 밸브 유량(mL/s) — openSec = volumeMl ÷ 이 값. 벤치 캘리브레이션으로 교체(기본 10.0).
SENLYT_VALVE_FLOW_ENV = "SENLYT_VALVE_FLOW_ML_PER_SEC"
# 최대 개방 클램프(s) — 밸브 영구개방 차단(기본 15.0).
SENLYT_VALVE_MAX_OPEN_ENV = "SENLYT_VALVE_MAX_OPEN_SEC"

DEFAULT_IDENTITY_FILENAME = "device-identity.json"
DEFAULT_LEDGER_FILENAME = "idempotency-ledger.log"
DEFAULT_TRACE_SPILL_FILENAME = "trace-spill.jsonl"


class BootstrapError(Exception):
    """부팅 조립 실패 — deviceId(수집 시리얼) 부재·서버 타겟 미설정·등록 실패 등(fail-fast)."""


# 부팅 1회 settings fetch seam(주입 가능·테스트가 네트워크 없이 검증) —
#   (server_config, dispenser_token, mode) → MachineSettings|None. 기본 = 실 SSE 1회 읽기.
SettingsFetcher = Callable[[ServerConfig, str, str], "Mapping[str, Any] | None"]


@dataclass(frozen=True, slots=True)
class DaemonComponents:
    """조립된 실 어댑터 묶음 — daemon 이 소비."""

    device_id: str
    server_config: ServerConfig
    identity: DeviceIdentity
    command_source: SseCommandSourceAdapter
    status_sink: HttpStatusSinkAdapter
    engine: EnginePort
    valve: ValvePort | None
    ledger: IdempotencyLedger
    logger: StructuredLogger
    # 서버 배정/env 로 확정된 실행 모드(flavor|fragrance) — 구독·역보고 축 + settings mode 쿼리.
    mode: str
    # 부팅 1회 서버 settings 스냅샷(시린지 용량 SoT) — fetch_settings=True 일 때만 채워짐(없으면 None).
    #   RecipeResolver pump_map 의 용량/스트로크를 서버값으로 얹는다(build_resolver 소비).
    server_settings: "Mapping[str, Any] | None" = None


def _resolve_mode(environ: Mapping[str, str]) -> str:
    mode = environ.get(SENLYT_MODE_ENV, "").strip().lower()
    return "fragrance" if mode == "fragrance" else "flavor"


def _gpio_available() -> bool:
    """실 라즈베리파이 GPIO 존재 여부 — **Pi4(`/dev/gpiomem`)·Pi5(`/dev/gpiomem0`·RP1) 모두 커버**.

    **자동감지 게이트** — 비-Pi(CI·dev·docker 컨테이너)는 gpiomem 계열이 없어 False → engine/valve 가
    항상 fake 로 떨어진다(결정적). 실 Pi 에서만 실 하드웨어 자동 선택이 활성화된다.
    ⚠️ Pi5 는 RP1 칩이라 `/dev/gpiomem` 이 아니라 `/dev/gpiomem0`(뱅크별 gpiomem0..4) — glob 로 둘 다 잡는다
    (`/dev/gpiomem` 단일 경로만 보면 Pi5 에서 gpio 자동감지가 fake 로 오판·2026-07-17 실기기 발견).
    """
    from glob import glob

    return bool(glob("/dev/gpiomem*"))


def build_engine(
    environ: Mapping[str, str],
    *,
    engine: EnginePort | None = None,
    on_pi: Callable[[], bool] | None = None,
    port_lister: "Callable[[], list] | None" = None,
    estop_event: "threading.Event | None" = None,
    logger: StructuredLogger | None = None,
) -> EnginePort:
    """엔진 조립 — 주입 우선. **env 미지정이면 자동감지**(실 Pi + 시리얼 어댑터 존재 → sy01b·아니면 fake).

    설치 시 `SENLYT_ENGINE` 을 안 넣어도 된다("URL만" 목표) — 실 Pi 에 USB-RS485 펌프 어댑터가
    붙어 있으면 sy01b, 그 외(비-Pi·어댑터 미장착)는 fake 로 자동 결정한다. 명시하면 그 값이 우선.
    `on_pi`·`port_lister` 는 테스트 주입 seam(기본 = 실 판정).
    """
    if engine is not None:
        return engine
    from ..adapters.serial_port_discovery import discover_serial_port

    raw = environ.get(SENLYT_ENGINE_ENV)
    if raw is None or raw.strip() == "":
        # 자동감지 — 실 Pi(gpiomem) 이고 시리얼 어댑터가 잡히면 sy01b, 아니면 fake.
        is_pi = on_pi() if on_pi is not None else _gpio_available()
        has_port = bool(discover_serial_port(environ, port_lister=port_lister))
        choice = "sy01b" if (is_pi and has_port) else "fake"
    else:
        choice = raw.strip().lower()
    if choice == "sy01b":
        # 실 RS485 어댑터의 probe/dispense 는 hw-dev 워크오더(실 시리얼). 스텁이면 self-test 가 미준비를
        # 표면화(fail-closed) — 부팅·등록 자체는 허용(제조 트래픽만 보류).
        from ..adapters.sy01b_engine_adapter import Sy01bEngineAdapter

        port = discover_serial_port(environ, port_lister=port_lister)
        # ⚠️ estop_event 주입 = 데몬·시퀀서와 **같은 공유 래치**(§9-4). 이게 있어야 어댑터의 in-flight
        #   모션 폴이 데몬이 세운 래치를 직접 보고 즉시 bail 한다(설계 '단일 공유 _estop').
        #   port=None(미탐지) 이면 어댑터 기본값(/dev/ttyUSB0) 유지 — None 을 넘기지 않는다.
        if port:
            return Sy01bEngineAdapter(port=port, estop_event=estop_event, logger=logger)
        return Sy01bEngineAdapter(estop_event=estop_event, logger=logger)
    return FakeEnginePort(estop_event=estop_event)


def _valve_pins_from_env(raw: str | None) -> dict[str, int]:
    """`SENLYT_VALVE_PINS`("sour:17,normal:27") → base→BCM 핀 매핑. 파싱 불가 항목은 건너뜀."""
    if not raw:
        return dict(DEFAULT_VALVE_PINS)
    pins: dict[str, int] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        base, pin = part.split(":", 1)
        base = base.strip().lower()
        pin = pin.strip()
        if base and pin.isdigit():
            pins[base] = int(pin)
    return pins if pins else dict(DEFAULT_VALVE_PINS)


def _float_env(environ: Mapping[str, str], key: str, default: float) -> float:
    raw = environ.get(key, "").strip()
    if not raw:
        return default
    try:
        v = float(raw)
    except ValueError:
        return default
    return v if v > 0 else default


def build_valve(
    environ: Mapping[str, str],
    *,
    valve: "ValvePort | None" = None,
    on_pi: Callable[[], bool] | None = None,
) -> ValvePort | None:
    """기주 밸브 조립(§9-1 v2) — 주입 우선. **env 미지정이면 자동감지**(실 Pi → gpio·아니면 fake).

    설치 시 `SENLYT_VALVE` 를 안 넣어도 된다("URL만" 목표) — 실 Pi(GPIO 존재)면 gpio, 비-Pi 는 fake.
      - 자동 gpio 결선 실패(gpiozero 부재 등)는 **graceful fallback → fake**(자동 선택이라 부팅 중단 X).
      - 명시 `gpio` 는 결선 실패 시 **fail-fast**(BootstrapError) — 운영자가 콕 집었으니 조용히 넘어가지 않음.
      - `off`: None — valve 스텝 수신 시 Sequencer pre-flight 가 fail-closed drop(토출 0).
    """
    if valve is not None:
        return valve
    flow = _float_env(environ, SENLYT_VALVE_FLOW_ENV, DEFAULT_FLOW_ML_PER_SEC)
    max_open = _float_env(environ, SENLYT_VALVE_MAX_OPEN_ENV, DEFAULT_MAX_OPEN_SEC)

    def _gpio() -> "GpioValveAdapter":
        from ..adapters.valve_adapter import GpioValveAdapter

        return GpioValveAdapter(
            pins=_valve_pins_from_env(environ.get(SENLYT_VALVE_PINS_ENV)),
            flow_ml_per_sec=flow,
            max_open_sec=max_open,
        )

    raw = environ.get(SENLYT_VALVE_ENV)
    if raw is None or raw.strip() == "":
        # 자동감지 — 실 Pi 면 gpio(결선 실패 시 graceful fake), 아니면 fake.
        is_pi = on_pi() if on_pi is not None else _gpio_available()
        if is_pi:
            try:
                return _gpio()
            except Exception:  # noqa: BLE001 — 자동 선택 실패는 안전 폴백(fake), 부팅 중단 없음.
                return FakeValveAdapter(flow_ml_per_sec=flow, max_open_sec=max_open)
        return FakeValveAdapter(flow_ml_per_sec=flow, max_open_sec=max_open)

    choice = raw.strip().lower()
    if choice == "off":
        return None
    if choice == "gpio":
        try:
            return _gpio()
        except Exception as e:  # 명시 선택 — fail-fast(잘못된 핀으로 조용히 뜨는 것 방지).
            raise BootstrapError(f"GPIO 밸브 결선 실패: {e}") from e
    return FakeValveAdapter(flow_ml_per_sec=flow, max_open_sec=max_open)


def _identity_path(environ: Mapping[str, str]) -> Path:
    explicit = environ.get(SENLYT_IDENTITY_PATH_ENV, "").strip()
    if explicit:
        return Path(explicit)
    log_dir = environ.get("LOG_DIR", "").strip()
    base = Path(log_dir) if log_dir else Path.cwd()
    return base / DEFAULT_IDENTITY_FILENAME


def _ledger_path(environ: Mapping[str, str]) -> Path:
    explicit = environ.get(SENLYT_LEDGER_PATH_ENV, "").strip()
    if explicit:
        return Path(explicit)
    log_dir = environ.get("LOG_DIR", "").strip()
    base = Path(log_dir) if log_dir else Path.cwd()
    return base / DEFAULT_LEDGER_FILENAME


def _trace_spill_path(environ: Mapping[str, str]) -> Path:
    """관측 로그 스풀 경로 — ledger/identity 와 같은 우선순위(override > LOG_DIR > cwd)."""
    explicit = environ.get(SENLYT_TRACE_SPILL_PATH_ENV, "").strip()
    if explicit:
        return Path(explicit)
    log_dir = environ.get("LOG_DIR", "").strip()
    base = Path(log_dir) if log_dir else Path.cwd()
    return base / DEFAULT_TRACE_SPILL_FILENAME


def build_ledger(environ: Mapping[str, str]) -> FileIdempotencyLedger:
    """crash-safe 파일 멱등 ledger 조립 — 상시 소비 루프의 IL-02/CR-01 물리 보증.

    경로: `SENLYT_LEDGER_PATH` > `LOG_DIR`/idempotency-ledger.log > cwd/idempotency-ledger.log.
    (InMemoryIdempotencyLedger 는 mark_running/recovery 스캔 미지원 — 실 루프엔 파일 ledger.)
    """
    return FileIdempotencyLedger.open(_ledger_path(environ))


def pump_map_from_addresses_env(
    raw: str | None,
    *,
    capacity_override: float | None = None,
    full_stroke_override: int | None = None,
) -> dict[int, SyringeSpec]:
    """`PUMP_ADDRESSES`("aroma:1,2,3;flavor:4") → pumpAddr→SyringeSpec 매핑(RR pump_map).

    명시 env 로 주소를 고정하는 경로 — 용량/스트로크는 **서버 settings 오버라이드 우선**(O-18):
      - `capacity_override`(서버 settings 프리셋 용량)가 있으면 그 값, 없으면 모드 기본 0.5mL
        (양 모드 공통·2026-07-17 확정). ⚠️ 용량 오류 = Code 11 과다흡입이라 서버값이 안전 SoT.
      - `full_stroke_override`(서버 프리셋 스트로크)가 있으면 그 값, 없으면 sy01b(12000).
    누락/비정수 addr 는 건너뛴다(미매핑 addr 는 RR 게이트가 drop — silent 매핑 금지).
    """
    pump_map: dict[int, SyringeSpec] = {}
    if not raw:
        return pump_map
    stroke = (
        full_stroke_override
        if full_stroke_override is not None
        else PUMP_PRESETS["sy01b"].pump_full_stroke
    )
    for group in raw.split(";"):
        group = group.strip()
        if not group or ":" not in group:
            continue
        mode, addrs = group.split(":", 1)
        is_flavor = mode.strip().lower() == "flavor"
        capacity = (
            capacity_override
            if capacity_override is not None
            else resolve_syringe_capacity_ml(None, is_flavor=is_flavor)  # 모드 기본값 폴백.
        )
        spec = SyringeSpec(pump_full_stroke=stroke, syringe_capacity_ml=capacity)
        for a in addrs.split(","):
            a = a.strip()
            # ⛔ addr 0 = RS485 브로드캐스트 — pump_map 에 넣으면 어댑터가 `/0…`(전 펌프 동시
            #   응답·충돌)을 쏜다. 서버 게이트와 대칭으로 0 을 배제(실 주소는 1.. 뿐).
            if a.isdigit() and int(a) >= 1:
                pump_map[int(a)] = spec
    return pump_map


def build_resolver(
    environ: Mapping[str, str],
    *,
    engine: EnginePort | None = None,
    server_settings: "Mapping[str, Any] | None" = None,
    mode: str | None = None,
) -> RecipeResolver:
    """RecipeResolver 조립 — pump_map 을 **자동인식**하고, env 가 있으면 그게 이긴다.

    우선순위(주소):
      ① `PUMP_ADDRESSES` env 명시 → 그대로(고정 구성·기존 설치 호환).
      ② 미지정 → **버스 스캔 자동인식** — 단 **모드가 알려주는 예상 주소만** 프로브한다.
      ③ 둘 다 실패 → 빈 매핑(모든 스텝 unmapped drop = 토출 0·안전측).

    ⚠️ **시린지 용량/스트로크 = 서버 settings 우선(O-18·안전 급소)**. `server_settings`(부팅 1회
       GET-SSE 스냅샷)의 pumpPreset.syringeCapacityMl/pumpFullStroke 이 있으면 그 값을 pump_map
       SyringeSpec 에 얹고, 없으면 모드 기본(0.5mL/sy01b 12000)으로 폴백한다. 용량이 실 시린지와
       어긋나면 stepsPerMl 오산 → 과다흡입 → Code 11(펌프 파손)이라, 서버 SoT 값을 우선한다.
       주소 자체는 위 우선순위(명시 env > 물리 프로브)가 정한다 — settings 는 '용량'만 얹는다
       (설정상 있어야 할 펌프를 config 만 보고 매핑하지 않는다·물리 프로브가 존재 SoT).

    ⚠️ ②가 없으면 **`PUMP_ADDRESSES` 없는 기기는 전 스텝이 CMD_VALIDATION_FAILED 로 죽는다**
    — "URL만" 설치 목표(설치 시 env 안 넣어도 됨)와 정면 충돌한다. `pump_health` 는 이 스캔
    로직을 갖고 있었지만 **부팅에 배선돼 있지 않았다**(비-테스트 호출자 0건·2026-07-17 발견).

    ⚠️ **스캔 범위 = 소프트웨어 포트 매핑(모드)이 정한다**(2026-07-18). 무작정 1..10 을 훑으면
    식향(펌프 2대)에서 없는 3..10 각각에 프로브 상한(~6s)만큼 낭비해 부팅이 ~48s 늘어진다.
    모드가 펌프 수를 알려주므로(식향 2 → 주소 1,2 · 향장향 3 → 1,2,3) 그 예상 주소만 프로브한다
    — 부재 주소 낭비 0. (더 넓은 구성이 필요하면 `PUMP_ADDRESSES` 로 명시 = ①이 이긴다.)

    자동인식의 근거는 **실제 펌프 응답**이지 VID/PID 가 아니다 — 엔진 어댑터가 `probe(addr)`
    를 제공하면 그걸 쓰고(sy01b=RS485 상태쿼리), 없으면(Fake 등) 건너뛴다.
    """
    # 서버 settings 프리셋(부팅 스냅샷) → 용량/스트로크 오버라이드(없으면 None → 모드 기본 폴백).
    capacity_override = syringe_capacity_from_settings(server_settings)
    stroke_override = full_stroke_from_settings(server_settings)

    raw = environ.get(SENLYT_PUMP_ADDRESSES_ENV)
    if raw and raw.strip():
        return RecipeResolver(
            pump_map_from_addresses_env(
                raw,
                capacity_override=capacity_override,
                full_stroke_override=stroke_override,
            )
        )

    probe = getattr(engine, "probe", None) if engine is not None else None
    if callable(probe):
        # 모드 = **서버배정 mode 우선**(TOFU 후 identity.mode), env 는 폴백(2026-07-18). 'URL만' 설치는
        #   env 를 안 넣으므로, mode 를 안 받으면 식향 2펌프 기기도 향장향으로 오판해 부재 addr 3 프로브
        #   상한(~6s)을 낭비한다. 기능은 물리 프로브가 SoT 라 어느 경로든 정확하나, 부팅 지연·설계 정합.
        mode_str = (mode or environ.get(SENLYT_MODE_ENV, "") or "").strip().lower()
        is_flavor = mode_str == "flavor"
        # 모드 → 예상 펌프 주소(소프트웨어 매핑). 식향 2대(1,2) / 향장향 3대(1,2,3). 2026-07-17 확정.
        expected = [1, 2] if is_flavor else [1, 2, 3]
        found = discover_pumps(probe, expected)
        if found:
            capacity = (
                capacity_override
                if capacity_override is not None
                else resolve_syringe_capacity_ml(None, is_flavor=is_flavor)
            )
            return RecipeResolver(
                auto_pump_map(found, capacity_ml=capacity, full_stroke=stroke_override)
            )
    return RecipeResolver({})


def build_components(
    environ: Mapping[str, str],
    *,
    engine: EnginePort | None = None,
    ledger: IdempotencyLedger | None = None,
    logger: StructuredLogger | None = None,
    identity_store: DeviceIdentityStore | None = None,
    register: bool = True,
    fetch_settings: bool = False,
    settings_fetcher: SettingsFetcher | None = None,
    estop_event: "threading.Event | None" = None,
) -> DaemonComponents:
    """환경변수에서 실 어댑터 전체를 조립 — 서버 타겟 결정 + 등록 + 어댑터 결선.

    Args:
      register: True(기본)면 실 HTTP 등록을 수행. 테스트는 register=False + identity_store
                (선주입 정체성)로 네트워크 없이 조립 검증.
      engine:   주입 시 그대로(유일 mock=Fake). 미주입이면 SENLYT_ENGINE 분기.
      fetch_settings: True 면 부팅 1회 서버 settings 스냅샷을 읽어 server_settings 에 싣는다
                (시린지 용량 SoT·O-18). 실 데몬(senlytd._run)만 켠다 — 기본 False(조립 테스트는
                네트워크 없이). best-effort: 실패 시 server_settings=None(모드 기본 용량 폴백).
      settings_fetcher: 부팅 settings fetch seam 주입(테스트). 미주입이면 실 SSE 1회 읽기.

    Raises:
      ServerTargetError: 서버 base URL 미설정/미지원(fail-fast — config.server_target).
      BootstrapError:    deviceId(수집 시리얼) 부재 또는 등록 실패.
    """
    log = logger if logger is not None else StructuredLogger()
    # 1) 서버 타겟(base URL) 결정 — 미설정 시 ServerTargetError(fail-fast·prod 오접속 차단).
    server_config = ServerConfig.from_environ(environ)

    # 2) 정체성 확보 — 저장분 재사용 or 실 HTTP 등록.
    store = identity_store or DeviceIdentityStore(_identity_path(environ))
    if register:
        # [D-A] 수집 HW 시리얼 = deviceId(서버 발급 없음). 부재 시 fail-fast(임의값 금지).
        device_id = read_hardware_id(env=environ)
        if not device_id:
            raise BootstrapError(
                "deviceId(수집 시리얼) 확보 불가 — SENLYT_HARDWARE_ID 또는 /proc/cpuinfo Serial 필요"
            )
        # TOFU(2026-07-17): 공유키 없음 — deviceId 만 제시(등록 202 pending → 운영자 승인 후 토큰).
        transport = make_http_register_transport(server_config.register_url)
        client = RegistrationClient(
            transport,
            device_id=device_id,
            name=environ.get(SENLYT_DEVICE_NAME_ENV) or None,
        )
        try:
            identity = ensure_registered(store, client)
        except Exception as e:  # RegistrationError 포함 — fail-fast 표면화.
            log.error(
                "디바이스 등록 실패 — 부팅 중단",
                stage=STAGE_ERROR,
                error=str(e),
            )
            raise BootstrapError(f"등록 실패: {e}") from e
    else:
        loaded = store.load()
        if loaded is None:
            raise BootstrapError(
                "register=False 이지만 저장된 정체성이 없음(테스트는 identity_store 선주입 필요)"
            )
        identity = loaded

    log.bind_device(identity.device_id)

    # 3) 실 어댑터 조립 — 동일 base·동일 dispenserToken·동일 logger.
    # mode 는 서버 배정(identity.mode·TOFU 승인 시 하달)이 **우선** — 없으면 env(SENLYT_MODE)→flavor 폴백.
    #   서버가 SoT(운영자가 /admin 에서 기기 모드 배정) → SENLYT_MODE env 는 더 이상 필수가 아니다.
    mode = identity.mode or _resolve_mode(environ)

    # 부팅 1회 서버 settings 스냅샷(시린지 용량 SoT·O-18) — fetch_settings=True(실 데몬)일 때만.
    #   best-effort: 실패는 삼켜 None(모드 기본 용량 폴백). seam(settings_fetcher) 로 테스트 주입 가능.
    server_settings: "Mapping[str, Any] | None" = None
    if fetch_settings:
        fetcher = settings_fetcher if settings_fetcher is not None else fetch_settings_once
        try:
            server_settings = fetcher(server_config, identity.dispenser_token, mode)
        except Exception as e:  # noqa: BLE001 — settings fetch 실패는 부팅을 막지 않는다(폴백).
            log.warn(
                "부팅 settings fetch 실패 — 모드 기본 용량으로 폴백(best-effort)",
                stage=STAGE_ERROR,
                error=str(e),
            )
            server_settings = None

    command_source = SseCommandSourceAdapter(
        server_config=server_config,
        bearer_token=identity.dispenser_token,
        mode=mode,
        logger=log,
    )
    status_sink = HttpStatusSinkAdapter(
        server_config=server_config,
        bearer_token=identity.dispenser_token,
        mode=mode,
        # 관측 로그 디스크 스풀(단절 유실 0) — 전송 실패 배치를 보존, 재연결/재부팅 후 업로드.
        trace_spill=TraceSpill(_trace_spill_path(environ)),
        logger=log,
    )

    # 4) 엔진·밸브 자동감지 + 부팅 자가진단 로그(눈에 띄게) — "URL만" 설치에서 실제 하드웨어를
    #    무엇으로 잡았는지 운영자가 로그로 확인한다(silent auto 금지 — auto + visible self-diagnostic).
    engine_adapter = build_engine(environ, engine=engine, estop_event=estop_event, logger=log)
    valve_adapter = build_valve(environ)
    log.event(
        "하드웨어 자가진단 — 엔진·밸브 자동감지 결과",
        stage=STAGE_PI_RECEIVED,
        gpio_available=_gpio_available(),
        engine=type(engine_adapter).__name__,
        valve=type(valve_adapter).__name__ if valve_adapter is not None else "off",
        mode=mode,
        # 서버 settings 시린지 용량 반영 여부(None=서버 미제공→모드 기본 0.5mL 폴백·안전 급소 관측).
        syringeCapacityMl=syringe_capacity_from_settings(server_settings),
    )

    return DaemonComponents(
        device_id=identity.device_id,
        server_config=server_config,
        identity=identity,
        command_source=command_source,
        status_sink=status_sink,
        engine=engine_adapter,
        valve=valve_adapter,
        ledger=ledger if ledger is not None else InMemoryIdempotencyLedger(),
        logger=log,
        mode=mode,
        server_settings=server_settings,
    )
