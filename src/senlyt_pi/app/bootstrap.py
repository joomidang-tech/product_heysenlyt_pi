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

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping

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
from ..adapters.sse_command_source_adapter import SseCommandSourceAdapter
from ..config.server_target import ServerConfig
from ..core.pump_guard import PUMP_PRESETS, SyringeSpec, resolve_syringe_capacity_ml
from ..obs.log import STAGE_ERROR, STAGE_PI_RECEIVED, StructuredLogger
from ..persistence.file_idempotency_ledger import FileIdempotencyLedger
from ..persistence.idempotency_ledger import IdempotencyLedger, InMemoryIdempotencyLedger
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


class BootstrapError(Exception):
    """부팅 조립 실패 — deviceId(수집 시리얼) 부재·서버 타겟 미설정·등록 실패 등(fail-fast)."""


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
        return Sy01bEngineAdapter(port=port) if port else Sy01bEngineAdapter()
    return FakeEnginePort()


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


def build_ledger(environ: Mapping[str, str]) -> FileIdempotencyLedger:
    """crash-safe 파일 멱등 ledger 조립 — 상시 소비 루프의 IL-02/CR-01 물리 보증.

    경로: `SENLYT_LEDGER_PATH` > `LOG_DIR`/idempotency-ledger.log > cwd/idempotency-ledger.log.
    (InMemoryIdempotencyLedger 는 mark_running/recovery 스캔 미지원 — 실 루프엔 파일 ledger.)
    """
    return FileIdempotencyLedger.open(_ledger_path(environ))


def pump_map_from_addresses_env(raw: str | None) -> dict[int, SyringeSpec]:
    """`PUMP_ADDRESSES`("aroma:1,2,3;flavor:4") → pumpAddr→SyringeSpec 매핑(RR pump_map).

    settings-stream(O-18) 미배선 구간의 부트스트랩 pump_map — 모드 기본 용량 + sy01b 스트로크.
      - flavor  → 1.25mL(모드 기본), 그 외(aroma/fragrance) → 0.5mL(모드 기본).
      - 스트로크 = sy01b 프리셋(12000). 서버 settings 수신 시 이 매핑을 대체할 수 있다.
    누락/비정수 addr 는 건너뛴다(미매핑 addr 는 RR 게이트가 drop — silent 매핑 금지).
    """
    pump_map: dict[int, SyringeSpec] = {}
    if not raw:
        return pump_map
    stroke = PUMP_PRESETS["sy01b"].pump_full_stroke
    for group in raw.split(";"):
        group = group.strip()
        if not group or ":" not in group:
            continue
        mode, addrs = group.split(":", 1)
        is_flavor = mode.strip().lower() == "flavor"
        capacity = resolve_syringe_capacity_ml(None, is_flavor=is_flavor)  # 모드 기본값 폴백.
        spec = SyringeSpec(pump_full_stroke=stroke, syringe_capacity_ml=capacity)
        for a in addrs.split(","):
            a = a.strip()
            if a.isdigit():
                pump_map[int(a)] = spec
    return pump_map


def build_resolver(environ: Mapping[str, str]) -> RecipeResolver:
    """RecipeResolver 조립 — PUMP_ADDRESSES env 기반 pump_map(§9-1)."""
    return RecipeResolver(pump_map_from_addresses_env(environ.get(SENLYT_PUMP_ADDRESSES_ENV)))


def build_components(
    environ: Mapping[str, str],
    *,
    engine: EnginePort | None = None,
    ledger: IdempotencyLedger | None = None,
    logger: StructuredLogger | None = None,
    identity_store: DeviceIdentityStore | None = None,
    register: bool = True,
) -> DaemonComponents:
    """환경변수에서 실 어댑터 전체를 조립 — 서버 타겟 결정 + 등록 + 어댑터 결선.

    Args:
      register: True(기본)면 실 HTTP 등록을 수행. 테스트는 register=False + identity_store
                (선주입 정체성)로 네트워크 없이 조립 검증.
      engine:   주입 시 그대로(유일 mock=Fake). 미주입이면 SENLYT_ENGINE 분기.

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
        logger=log,
    )

    # 4) 엔진·밸브 자동감지 + 부팅 자가진단 로그(눈에 띄게) — "URL만" 설치에서 실제 하드웨어를
    #    무엇으로 잡았는지 운영자가 로그로 확인한다(silent auto 금지 — auto + visible self-diagnostic).
    engine_adapter = build_engine(environ, engine=engine)
    valve_adapter = build_valve(environ)
    log.event(
        "하드웨어 자가진단 — 엔진·밸브 자동감지 결과",
        stage=STAGE_PI_RECEIVED,
        gpio_available=_gpio_available(),
        engine=type(engine_adapter).__name__,
        valve=type(valve_adapter).__name__ if valve_adapter is not None else "off",
        mode=mode,
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
    )
