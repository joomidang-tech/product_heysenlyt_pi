"""SY-01B 시린지 펌프 RS485 실어댑터 — EnginePort 실구현.

v1.1.0 `pump_service.dart` 에서 **하드웨어를 동작시키는 원리만** 이식했다(계약·자료구조는 v1.2.0
것을 쓴다 — 사용자 지시 2026-07-17). 원리 = ASCII 프레임 문법 · 상태바이트 파싱 · 폴링 · 에러코드
분류 · 초기화 셋업.

──────────────────────────────────────────────────────────────────────────────
프로토콜 (SoT: SY-01B 매뉴얼 §4 · Tecan/Cavro OEM 표준 §3 · 03_core-algorithms §5)

  송신: `/{addr}{명령들}R[CR]`   — `/`=0x2F 시작 · `R`=실행(없으면 버퍼에 쌓기만)
  수신: `/0{상태바이트}{데이터}` — 마스터 주소 `0`. 종료 = ETX(0x03)
  상태바이트: `& 0x0F` = 에러코드 · `& 0x20` = Ready 비트

  낱말: `I{p}`=흡입포트로 밸브회전 · `P{n}`=상대흡입 · `O{p}`=배출포트로 회전 · `D{n}`=상대배출
        `Z/Z1/Z2`=초기화(힘 전/반/1-3) · `U200,{n}`=스톨전류 · `v/V/c/L`=속도 프로파일 · `T`=중단

  **한 번의 토출 = I → P → O → D** (이동 명령은 완료까지 Busy → `?` 폴링으로 대기)

──────────────────────────────────────────────────────────────────────────────
병렬성 (L2 "병렬 모션 + 시분할 버스") — 이 어댑터가 지키는 핵심 계약

  펌프들이 **RS485 한 버스를 공유**한다. 그런데 모터는 각자 독립이다. 그래서:
    - 버스 락은 **한 트랜잭션(송신+응답 수신·수 ms)만** 잡고 **즉시 놓는다**.
    - 이동 명령은 ACK 만 받고 반환 → 모터가 도는 **긴 시간 동안 버스는 비어 있다**.
    - 완료 대기는 `?` **폴링**으로 하고, 폴링 한 번도 락을 짧게 잡았다 놓는다.
  ⇒ 두 펌프의 모터가 물리적으로 동시에 돌면서 버스만 시분할된다.

  ⚠️ **락을 모션 내내 쥐면 병렬이 사라진다** — 상위 `pump_sequencer` 는 버스 락을 모르고
     (설계상 어댑터에 위임·F3) stage 태스크를 ThreadPool 로 동시에 띄울 뿐이다. 여기서
     락을 길게 잡으면 그 동시성이 통째로 직렬화된다.

──────────────────────────────────────────────────────────────────────────────
⛔ bounded-read 계약 (F1 방어 — 이 어댑터의 존재 이유 중 하나)

  모든 시리얼 read 에 타임아웃을 건다(`Serial(timeout=...)` + 전체 wall-clock 상한).
  상위 `pump_sequencer._run_stage` 는 `future.result()`(타임아웃 인자 **없음**)로 완주를
  기다린다 — 어댑터가 무한 블록하면 그게 곧 **제조 교착**이다. 배리어 타임아웃을 시퀀서에
  두지 않는 것은 **설계 의도**(모션 중 강제중단 금지·설계 §10)이고, 시간 경계는 여기 read
  타임아웃이 책임진다. read 타임아웃은 물리 모션을 중단시키지 않는다(명령은 이미 나갔고
  펌프의 유한 모션을 기다릴 뿐) — 그래서 안전하다.

──────────────────────────────────────────────────────────────────────────────
⛔ v1.1.0 의 알려진 함정 — 옮기지 않은 것들

  1. **초기화 벽돌 사고**: v1.1.0 `initialize` Step0 의 `TR`(상태리셋)이 `validate:true` 라
     latched 에러 상태에서 **throw** 해버렸다 → 에러를 지우려고 부르는 명령이 에러 때문에
     실패 → 전원 재투입 전 복구 불가. 여기선 **TR 결과를 검증하지 않는다** — TR 은 *에러를
     지우는* 명령이지 *에러가 없어야 도는* 명령이 아니다.
  2. **안전포트 9 → 12**: Port 9 는 향료8 과 충돌해 누액을 냈다. 안전포트(공기 구멍)는
     **서버가 스텝에 실어 준다**(배치가 정본) — 여기서 번호를 박지 않는다.
  3. **Code 10 누락 · Code 1/-4 silent success**: 에러 분류는 core `pump_guard.
     classify_engine_error_code` 단일 정본에 위임한다(0=정상 / 1·7·11·15=일시 / 2·3·9·10=구조).
     여기서 코드를 재분류하지 않는다 — raw 를 그대로 올린다.

pyserial 은 **실기기 배포에만 필요**하다(lazy import) — 미설치 환경에서도 이 모듈 import 는
성공하고, 실제 연결 시점에만 실패한다(테스트가 시리얼 없이 계약을 검증할 수 있게).
"""

from __future__ import annotations

import threading
import time
from typing import Callable, Iterable, Protocol, Sequence

from ..core.pump_guard import PumpPreset, SyringeSpec, clamp_pump_preset
from ..obs.log import STAGE_STEP_EXEC, StructuredLogger
from ..ports.engine_port import (
    OP_ESTOP,
    OP_INITIALIZE,
    OP_PLUNGER_FULL,
    OP_PLUNGER_HOME,
    EngineDispenseCommand,
    EngineOpCommand,
    EngineResult,
)
from ..test_seam.fake_engine_sentinels import FAKE_EMPTY_RAW_CODE, FAKE_TIMEOUT_RAW_CODE

# ── 프로토콜 상수 (SY-01B 매뉴얼 §4) ────────────────────────────────────────────
FRAME_START = "/"
FRAME_END = "\r"
ETX = 0x03  # 응답 종료 문자
STATUS_ERROR_MASK = 0x0F  # 상태바이트 하위 4비트 = 에러코드
STATUS_READY_BIT = 0x20  # bit5 = Ready(모터 정지·명령 수락 가능)
STATUS_QUERY = "?"  # 상태 조회(모터 회전 중에도 수락되는 몇 안 되는 명령)
TERMINATE = "TR"  # in-flight 이동 중단 + 포트 클린
# OSError(핫플러그) 재연결 후 **같은 프레임 재전송이 허용되는 명령** — 멱등(모션 무발생)만
#   (2026-07-19 P1 · 물리 이중 토출 방어). OSError 는 write 성공 후 read 대기(최대 5s — 이 링크는
#   무응답이 잦아 창이 넓다) 중에도 난다 — 그 시점 펌프는 이미 명령을 받아 모션을 시작했을 수
#   있다. 모션 명령(`…R` 실행: I/O 회전·P/D/A 이동·Z 홈)을 재전송하면 이중 모션(이중 토출·
#   busy NAK→Ready 대기 후 3번째 전송)이 된다 — EP-03(오성공 금지) 정신과 정면 충돌. 그래서
#   상태조회(?)·중단(TR — 두 번 걸어도 정지)만 재전송하고, 나머지는 재연결만 해 두고(다음
#   트랜잭션 회복) 이번 트랜잭션은 정직한 실패로 올린다(상위 재시도·폴 판정 몫).
_RECONNECT_RESEND_SAFE = frozenset({STATUS_QUERY, TERMINATE})
# ── 브로드캐스트(전 펌프 동시) — v1.1.0 RealPumpService._sendBroadcast 미러 ──────────────
#   `/_{cmd}` = 전 펌프가 같은 명령을 동시에 수신(명령 1발 → 반이중 RS485 경합 없음 = D38 stage:0
#   실패의 근본 회피). ⚠️ SY-01B 매뉴얼 §3.4 는 "_"(0x5F)를 스위치47 주소로만 적고 "브로드캐스트"로
#   문서화하지 않는다 — v1.1.0 실기기 관례가 유일 근거. **브로드캐스트 프레임 자체의 응답은
#   읽지 않는다**(무응답/충돌은 브로드캐스트의 물리 속성).
#   ⚠️ 브로드캐스트 뒤 `?` 확인(단발이든 Ready 폴이든)은 **초기화 시퀀스 안에서 금지**
#   (2026-07-19 실기기 2회 실측) — 브로드캐스트 직후/이후 이 기기 버스가 오염돼(펌프1 = 매번
#   다른 ETX 없는 쓰레기 프레임·펌프2 = 5s 무응답 반복) 건강한 펌프를 -1000 오탐한다. 초기화는
#   open-loop(명령 받으면 펌프가 알아서 홈)라 **fire-and-forget**(발사 후 고정 대기)으로 처리하고,
#   진짜 죽은 펌프는 이후 **토출 경로의 Ready 폴**에서 드러난다(v1.0.0 기기설정 툴 검증 방식).
BROADCAST_ADDR = "_"
SAFE_PORT = 12  # 안전 포트 = Air(누액 없는 자세) — v1.1.0 initializeAll [4/4] I12R.

DEFAULT_BAUDRATE = 9600  # 8N1

# ── 시간 상수 ──────────────────────────────────────────────────────────────────
# 한 트랜잭션(송신→ETX 수신)의 수신 상한. **무한 대기 금지가 불변식**(F1).
SERIAL_READ_TIMEOUT_S = 5.0
# 모션 완료 폴링의 wall-clock 상한. 최장 스트로크 물리 시간 + 여유.
#   초과 시 무응답 sentinel → ENGINE_TIMEOUT(transient) → 상위 EngineExecutor 가 R=3 재시도.
DEFAULT_MOTION_TIMEOUT_S = 40.0
# 초기화(홈 탐색)는 플런저가 끝까지 이동하므로 별도 상한.
DEFAULT_INIT_TIMEOUT_S = 30.0
# 폴링 간격 — 너무 촘촘하면 버스를 잡아먹고, 너무 성기면 완료 감지가 늦다.
POLL_INTERVAL_S = 0.05
# 송신 후 펌프가 응답을 만들 시간(v1.1.0 `Future.delayed(20ms)` 원리).
WRITE_SETTLE_S = 0.02
# 브로드캐스트 송신 후 펌프들이 명령을 소화할 시간(v1.1.0 `_sendBroadcast` 50ms). 응답을 안 읽으므로
#   진행 보장은 이 대기 + 스텝 간격 + 홈 고정 대기(HOME_SETTLE_S)뿐이다(fire-and-forget).
BROADCAST_SETTLE_S = 0.05
# 브로드캐스트 초기화 스텝(TR·U200) 사이 간격(v1.1.0 initializeAll 각 500ms).
BROADCAST_STEP_GAP_S = 0.5
# 브로드캐스트 홈(Z/Z1/Z2) 후 **고정 대기** — fire-and-forget 초기화의 유일한 완료 보장.
#   홈은 open-loop(펌프가 알아서 원점까지 감)라 Ready 폴 대신 물리 시간만 기다린다.
#   v1.0.0 기기설정 툴은 2.0s 로 실기기 검증됨 — 시린지 풀스트로크 여유로 4.0s(3~5s 권장 구간).
#   ⚠️ 값은 실측 조정·HW(오성연) 확인 대상 — 너무 짧으면 홈 이동 중에 safe-port 명령이 겹친다.
HOME_SETTLE_S = 4.0
# 프로브(장착 감지) — read-only 라 짧게. 살아있는 펌프는 첫 폴에서 답한다.
PROBE_READ_TIMEOUT_S = 1.5
PROBE_MAX_ATTEMPTS = 5
PROBE_DEADLINE_S = 6.0
PROBE_RETRY_GAP_S = 0.1

# 에러코드 도메인 **밖**의 사건 — 프레임을 못 받음(무응답) vs 받았는데 상태프레임이 아님.
#   둘 다 실패로 흐른다(EP-03 silent-success 금지). 공유 sentinel 을 쓰는 이유는 상위
#   EngineExecutor 가 Fake 와 **동일 상수**로 판정하기 때문(어댑터가 바뀌어도 판정은 그대로).
_NO_RESPONSE = FAKE_TIMEOUT_RAW_CODE
_MALFORMED = FAKE_EMPTY_RAW_CODE


class SerialLike(Protocol):
    """pyserial `Serial` 중 우리가 쓰는 표면만 — 테스트가 가짜를 꽂는 seam."""

    def write(self, data: bytes) -> int | None: ...

    def read(self, size: int = 1) -> bytes: ...

    @property
    def in_waiting(self) -> int: ...

    def close(self) -> None: ...


# 포트 문자열 → SerialLike. 실구현 = pyserial, 테스트 = 가짜.
SerialFactory = Callable[[str, int, float], SerialLike]


def _pyserial_factory(port: str, baudrate: int, timeout_s: float) -> SerialLike:
    """실 시리얼 연결 — pyserial. 미설치면 **여기서만** 실패한다(모듈 import 는 성공)."""
    try:
        import serial  # lazy — 실기기 배포 의존성.
    except ImportError as e:  # pragma: no cover - 실기기 전용 경로
        raise RuntimeError(
            "pyserial 미설치 — 실기기 토출에는 pyserial 이 필요하다 (pip install pyserial)"
        ) from e
    # timeout = **bounded-read 계약**(F1). 이 인자 없이 열면 read/write 가 무한 블록할 수 있다.
    #   ⚠️ write_timeout 도 반드시 건다 — pyserial write 기본은 블로킹(None)이라, 트랜시버가
    #   스톨하거나 흐름제어가 막히면 write 가 **버스 락을 쥔 채 영영 반환 안 한다**(제조 교착).
    #   write 초과 시 SerialTimeoutException → `_cycle`/`_settle` 이 실패 결과로 흡수한다.
    return serial.Serial(  # type: ignore[return-value]
        port=port, baudrate=baudrate, timeout=timeout_s, write_timeout=timeout_s
    )


# 응답 프레임 접두 — 펌프 응답은 **항상 마스터 주소 0**(`/0{상태바이트}…`)이다. 우리가 보낸
#   명령 프레임은 `/{1..10}…`(에코 포함)이라 `/0` 이 응답을 유일하게 식별한다.
_RESPONSE_PREFIX = FRAME_START + "0"


def _printable(raw: str) -> str:
    r"""로그용 응답 이스케이프 — 비인쇄 바이트를 `\xNN` 로 표기(2026-07-19 QA).

    깨진 링크의 응답엔 제어 바이트(BEL 0x07·ETX 0x03 등)가 섞인다. 그대로 로그에 실으면
    journald 가 `[300B blob data]` 로 뭉개거나 서버 뷰에서 `` 로만 보여 판독이 어렵다.
    사람이 읽는 `\x07` 표기로 바꿔 "어떤 바이트가 왔는지"가 어디서든 보이게 한다.
    """
    return "".join(ch if 32 <= ord(ch) < 127 else f"\\x{ord(ch):02x}" for ch in raw)


def parse_status(response: str) -> tuple[int, bool]:
    """상태 프레임 → `(error_code, ready)`. 프레임이 없으면 `(_NO_RESPONSE, False)`.

    프레임 = `/0{상태바이트}…` — `/0` 바로 뒤 글자가 상태바이트다.
    v1.1.0 `_parseStatus` 와 **동일 규칙**: `& 0x0F`=에러코드 · `& 0x20`=Ready 비트.
    Ready 는 **에러가 없을 때만** 참으로 본다 — 에러인데 Ready 라 하면 silent-success 가 난다.

    ⚠️ **에코 방어**: 반이중 RS485 + 다수 USB 어댑터(CH340 등)는 자기 송신을 에코한다. 버퍼는
    `에코(/1I3R\\r) + 실응답(/0{status})` 이 되는데, 단순 `find("/")` 는 에코의 `/` 를 잡아
    명령 문자(예: I=0x49)를 상태바이트로 오독한다(거짓 Code 9·거짓 성공). 그래서 **응답 접두
    `/0` 에 앵커**한다 — 우리가 보낸 프레임 주소는 1..10 이라 `/0` 은 응답에만 나온다.
    """
    i = response.find(_RESPONSE_PREFIX)
    if i == -1 or len(response) <= i + 2:
        return (_NO_RESPONSE, False)
    status_byte = ord(response[i + 2])
    error_code = status_byte & STATUS_ERROR_MASK
    ready = bool(status_byte & STATUS_READY_BIT) and error_code == 0
    return (error_code, ready)


class Sy01bEngineAdapter:
    """SY-01B RS485 시린지 펌프 어댑터 — EnginePort 실구현.

    **한 어댑터 = 한 버스**(한 시리얼 포트). 그 버스에 매달린 모든 펌프를 주소로 구분해 다루므로
    버스 락도 이 인스턴스가 소유한다.
    """

    def __init__(
        self,
        *,
        port: str = "/dev/ttyUSB0",
        baudrate: int = DEFAULT_BAUDRATE,
        read_timeout_s: float = SERIAL_READ_TIMEOUT_S,
        motion_timeout_s: float = DEFAULT_MOTION_TIMEOUT_S,
        init_timeout_s: float = DEFAULT_INIT_TIMEOUT_S,
        preset: PumpPreset | None = None,
        serial_factory: SerialFactory | None = None,
        stop_event: threading.Event | None = None,
        estop_event: threading.Event | None = None,
        logger: StructuredLogger | None = None,
        port_resolver: "Callable[[], list[str]] | None" = None,
    ) -> None:
        # 시리얼 관측 로거(RC4·2026-07-19) — 매 프레임 송신/응답·estop TR·홈상실·오버로드·브로드캐스트
        #   수신확인을 서버 trace 로 흘린다("실기기서 무슨 바이트가 오갔나"를 admin 에서). None 이면 무계측.
        self._log = logger
        self.port = port
        self.baudrate = baudrate
        # bounded-read 계약(F1) — 한 트랜잭션의 수신 상한.
        self.read_timeout_s = read_timeout_s
        # 모션 완료 폴링의 wall-clock 상한 — 초과 = ENGINE_TIMEOUT(transient·재시도).
        self.motion_timeout_s = motion_timeout_s
        # 초기화(홈 탐색) 폴링 상한 — 플런저가 끝까지 가므로 모션보다 길 수 있다.
        #   ⚠️ 이것도 **인스턴스 값**이어야 한다. 상수를 직접 쓰면 F1 경계를 설정으로 못 줄여
        #   테스트·운영에서 교착 상한을 통제할 수 없다(초기 구현의 실제 결함).
        self.init_timeout_s = init_timeout_s
        # 속도·스톨코드 상한의 정본. 미지정 = sy01b 빌트인(입력 무시·표값 강제).
        self.preset = preset if preset is not None else clamp_pump_preset(None)
        self._factory = serial_factory if serial_factory is not None else _pyserial_factory
        # 핫플러그 자가 회복(2026-07-19) — 장치 소멸 시 후보 포트를 재열거하는 seam(bootstrap 주입).
        #   미주입이면 현재 포트로만 재오픈 시도(구 동작 초과 금지·테스트 무영향).
        self._port_resolver = port_resolver
        # ⚠️ **버스 락** — 한 트랜잭션(송신+수신)만 감싼다. 모션 대기엔 절대 걸치지 않는다.
        self._bus = threading.Lock()
        self._serial: SerialLike | None = None
        self._open_lock = threading.Lock()
        self._stop = stop_event if stop_event is not None else threading.Event()
        # 긴급정지 래치(§9-4) — set 되면 in-flight 모션 폴이 즉시 빠져나온다(협조적 중단). shutdown(_stop)
        #   과 **분리**한다: estop 은 정지 후 복구(초기화)가 이어지지만 shutdown 은 프로세스 종료다.
        #   데몬 감시 스레드가 서버 estop 신호를 보고 set, 복구(초기화)가 clear 한다(공유 이벤트 주입).
        self._estop = estop_event if estop_event is not None else threading.Event()
        # 셋업을 마친 주소 — 매 스텝마다 U200/Z 를 재전송하지 않기 위한 캐시.
        self._initialized: set[int] = set()

    # ── 연결 ────────────────────────────────────────────────────────────────
    def _conn(self) -> SerialLike:
        """지연 연결(첫 트랜잭션에서 연다). 실패는 그대로 올려 상위가 실패로 흡수한다."""
        with self._open_lock:
            if self._serial is None:
                self._serial = self._factory(self.port, self.baudrate, self.read_timeout_s)
            return self._serial

    def close(self) -> None:
        """시리얼 정리(멱등) — 데몬 우아한 종료 경로."""
        with self._open_lock:
            s, self._serial = self._serial, None
        if s is not None:
            try:
                s.close()
            except Exception:  # noqa: BLE001 — 종료 경로에서 예외를 삼킨다.
                pass

    def signal_stop(self) -> None:
        """진행 중 폴링을 깨운다(취소·SIGTERM 우아한 종료)."""
        self._stop.set()

    def emergency_stop_all(self, addrs: "Iterable[int]") -> None:
        """긴급 정지(§9-4) — **전 펌프에 즉시 `TR`**(이동 중단·포트 클린)을 보낸다.

        ⚠️ **제조 중에도 안전하게 호출된다** — 감시 스레드에서 부른다. 버스 락이 트랜잭션 단위(ms)라,
           제조 스레드의 in-flight 트랜잭션 사이에 이 TR 이 끼어들어 모터를 물리적으로 멈춘다(v1.1.0
           앱이 시리얼 직접 쥐고 하던 즉시정지를 재현). 동시에 `_estop` 을 세워 제조 스레드의
           `_poll_until_ready` 가 즉시 빠져나오게 한다(협조적 abort — 멈춘 모션을 기다리지 않음).

        TR 후 홈 기준이 흔들리므로 각 펌프 셋업 캐시를 무효화한다(다음 토출이 재초기화). 미매핑/브로드
        캐스트(addr<1)는 건너뛴다. 개별 TR 실패는 삼켜 다른 펌프 정지를 막지 않는다(safety = best-effort
        전량 시도).
        """
        self._estop.set()
        # 긴급정지 = 안전상 가장 중요한 이벤트 → WARN(즉시 flush·headroom 보호)로 서버 표면화(RC4).
        if self._log is not None:
            self._log.warn("긴급정지 발동 — 전 펌프 TR 발송", stage=STAGE_STEP_EXEC)
        for addr in addrs:
            if addr < 1:
                continue  # 0 = RS485 브로드캐스트 금지
            try:
                self._txn(addr, TERMINATE)
            except Exception:  # noqa: BLE001 — 한 펌프 TR 실패가 나머지 정지를 막지 않는다.
                if self._log is not None:
                    self._log.warn(
                        "긴급정지 TR 발송 실패", stage=STAGE_STEP_EXEC, pumpAddr=addr
                    )
            self._initialized.discard(addr)

    def clear_estop(self) -> None:
        """긴급정지 래치 해제 — 복구(초기화) 경로가 부른다. 이후 모션 폴이 정상 대기한다."""
        self._estop.clear()

    # ── 핫플러그 자가 회복(2026-07-19 실측 ttyUSB0→ttyUSB1) ────────────────────
    def _reconnect_serial(self, *, reason: str) -> bool:
        """죽은 시리얼 핸들 폐기 + 후보 포트 재탐색 + 재오픈 — 데몬 재시작 없이 자가 회복.

        USB 를 뽑았다 꽂으면 장치 노드가 **옮겨 붙는다**(15:31 실측: ttyUSB0→ttyUSB1). 기존엔
        최초 open 핸들을 영구 캐시해, 재연결 후에도 죽은 FD 로 전 트랜잭션이 실패했고 복구는
        `systemctl restart` 뿐이었다("자동 인식해야지" — 사용자 요구 2026-07-19). 여기서:
          ① 캐시 close/폐기 → ② 후보 포트(`port_resolver` 재열거 + 현재 포트) 순서대로 재오픈
          → ③ 성공 포트로 `self.port` 갱신 + WARN 관측.
        반환 = 성공 여부. 실패면 호출자가 원 예외를 그대로 올린다(정직한 실패 — 다음
        트랜잭션이 다시 시도하므로 어댑터를 못 찾는 동안에도 데몬은 계속 산다).
        """
        self.close()
        cands: list[str] = []
        if self._port_resolver is not None:
            try:
                cands = [c for c in self._port_resolver() if isinstance(c, str) and c]
            except Exception:  # noqa: BLE001 — 재열거 실패는 현재 포트 재시도로 폴백.
                cands = []
        if self.port not in cands:
            cands.append(self.port)
        for cand in cands:
            try:
                with self._open_lock:
                    self._serial = self._factory(cand, self.baudrate, self.read_timeout_s)
                self.port = cand
                if self._log is not None:
                    self._log.warn(
                        "시리얼 자가 재연결 — 장치 소멸/핫플러그 복구",
                        stage=STAGE_STEP_EXEC,
                        port=cand,
                        reason=reason[:120],
                    )
                return True
            except Exception:  # noqa: BLE001 — 이 후보 실패 → 다음 후보.
                continue
        if self._log is not None:
            self._log.warn(
                "시리얼 재연결 실패 — 후보 포트 전멸(다음 트랜잭션에서 재시도)",
                stage=STAGE_STEP_EXEC,
                reason=reason[:120],
            )
        return False

    # ── 트랜잭션 (버스 락은 여기서만·짧게) ──────────────────────────────────
    def _txn(self, addr: int, command: str, *, read_timeout_s: float | None = None) -> str:
        """`/{addr}{command}[CR]` 송신 → ETX 까지 수신. **락은 이 함수 안에서만** 잡힌다.

        반환 = raw 응답(빈 문자열 = 무응답). 여기서 판정하지 않는다 — 판정은 호출자 몫.
        장치 소멸(OSError·핫플러그)이면 자가 재연결 후, **멱등 명령(`?`·TR)에 한해 1회 재시도**
        (위 `_reconnect_serial` + `_RECONNECT_RESEND_SAFE` — 모션 명령 재전송은 이중 토출 위험).
        """
        timeout_s = read_timeout_s if read_timeout_s is not None else self.read_timeout_s
        frame = f"{FRAME_START}{addr}{command}{FRAME_END}".encode("ascii")
        try:
            return self._txn_io(frame, addr, command, timeout_s)
        except OSError as e:
            # pyserial SerialException ⊂ OSError — 장치 소멸/IO 사망. 재연결은 항상 시도(다음
            # 트랜잭션 회복)하되, 재전송은 멱등 명령만(모션 이중 실행 방어·2026-07-19 P1).
            reconnected = self._reconnect_serial(reason=str(e))
            if not reconnected or command not in _RECONNECT_RESEND_SAFE:
                raise
            return self._txn_io(frame, addr, command, timeout_s)

    def _txn_io(self, frame: bytes, addr: int, command: str, timeout_s: float) -> str:
        """트랜잭션 I/O 본체 — 시리얼 예외는 그대로 올린다(재연결 판단은 `_txn`)."""
        conn = self._conn()
        t0 = time.monotonic()
        with self._bus:  # ← 수 ms. 모터가 도는 동안엔 절대 잡고 있지 않는다.
            # ⛔ **크로스-트랜잭션 입력 flush**(P2·2026-07-18) — 공유 버스라 직전 트랜잭션의 지연/부분
            #   프레임이 입력 버퍼에 남으면 이번 응답 앞에 붙어 상태바이트를 오독한다(silent-success/
            #   거짓 에러). write 직전에 잔여 바이트를 비운다. pyserial 은 reset_input_buffer, fake 는
            #   getattr 가드로 옵셔널(테스트 무영향).
            _reset = getattr(conn, "reset_input_buffer", None)
            if callable(_reset):
                try:
                    _reset()
                except Exception:  # noqa: BLE001 — flush 실패는 무시(최선노력).
                    pass
            else:
                while conn.in_waiting:
                    if not conn.read(conn.in_waiting):
                        break
            conn.write(frame)
            time.sleep(WRITE_SETTLE_S)
            buf = bytearray()
            deadline = time.monotonic() + timeout_s
            while time.monotonic() < deadline:
                n = conn.in_waiting
                if n:
                    chunk = conn.read(n)
                    if chunk:
                        buf.extend(chunk)
                        if ETX in chunk:
                            break
                        continue
                if self._stop.is_set():
                    break
                time.sleep(0.005)
            resp = buf.decode("ascii", errors="ignore")
        # 시리얼 왕복 관측(RC4) — 버스 락 **밖**에서 로깅(락 홀드 최소화). 매 프레임이라 DEBUG.
        #   traceId/commandSetId 는 시퀀서가 이 스레드에 바인딩한 컨텍스트로 자동 부착(2026-07-19) —
        #   "어느 명령의 어느 왕복이 깨졌는지"가 admin trace 타임라인에서 한 줄로 엮인다.
        #   response 는 \xNN 이스케이프(제어 바이트가 journald blob·판독 불가를 만들던 문제 해소),
        #   elapsedMs 로 "무응답 5초 대기 vs 즉답" 지연 분해가 로그만으로 가능해진다.
        if self._log is not None:
            self._log.debug(
                "시리얼 왕복",
                stage=STAGE_STEP_EXEC,
                pumpAddr=addr,
                command=command,
                response=(_printable(resp.strip()) or "(무응답)"),
                elapsedMs=round((time.monotonic() - t0) * 1000),
            )
        return resp

    def _query_status(self, addr: int, *, read_timeout_s: float | None = None) -> tuple[int, bool]:
        """`?` 한 번 — `(error_code, ready)`. 모터 회전 중에도 수락되는 명령이다."""
        return parse_status(self._txn(addr, STATUS_QUERY, read_timeout_s=read_timeout_s))

    def _poll_until_ready(self, addr: int, timeout_s: float) -> int:
        """Busy → Ready 를 **폴링**으로 기다린다. 반환 = 최종 error_code(0=정상).

        ⚠️ **락을 쥐지 않은 채 돈다** — 폴링 한 번(`?`)이 짧게 잡았다 놓을 뿐이라, 이 대기 동안
        다른 펌프의 명령·폴링이 버스를 쓸 수 있다. 이게 "병렬 모션 + 시분할 버스"의 실체다.

        상한 초과 = `_NO_RESPONSE`(→ ENGINE_TIMEOUT·transient·F1 방어).

        ⚠️ **v1.1.0 `_internalCheckStatus` 폴 루프와 동일 규칙**(필드 검증된 구조·농진원 박람회):
          - 무응답·**Code 7(미초기화)** = "아직 준비 안 됨" → 실패가 아니라 **계속 폴링**한다.
            (7 을 실패로 올리면 정상 진행 중 스텝을 죽인다 — v1.1.0 은 7 을 일시현상으로 본다.)
            ⚠️ **단, Code 7 을 볼 때마다 셋업 캐시를 무효화**한다(2026-07-18·브릭 회귀 봉합). 7 =
            "홈을 잃었다"는 가장 강한 재초기화 신호다. 셋업 중(addr 미등록)이면 discard 는 no-op 이고,
            토출 중(addr 등록됨)이면 이 폴이 끝난 뒤(타임아웃) 상위 재시도의 `_ensure_ready` 가
            재초기화(TR+U200+Z)를 태운다. 이게 없으면 한 번 de-init 된 펌프가 캐시-skip 때문에
            재초기화를 영영 못 해 후속 전 주문이 조용히 토출0로 **브릭**된다(전원순단→Code7 시나리오).
          - **Code 9/10(오버로드·재초기화 필수)** = 즉시 반환하되, `TR` 로 latched 에러를 지우고
            **셋업 캐시에서 이 펌프를 무효화**한다. 안 그러면 다음 스텝이 재초기화를 건너뛰어
            (`_ensure_ready` 가 `_initialized` 를 보고 skip) 오버로드 상태로 계속 밀어붙인다.
          - 그 외 에러코드 = 즉시 반환(상위 pump_guard 가 분류·재분류 금지).

        ※ stale-Ready(첫 폴 조기완료) 방어는 **별도 가드가 아니라 구조**다 — 이동 명령의 ACK 를
          `_settle` 이 읽고(왕복+settle) *난 뒤* 폴링을 시작하므로, 첫 폴 시점엔 이미 Busy 다.
          v1.1.0 도 같은 구조로 필드 검증됐다(Busy-먼저 가드 없음).
        """
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            # shutdown(_stop) 또는 긴급정지(_estop) → in-flight 모션 대기를 즉시 중단(협조적 abort).
            #   emergency_stop_all 이 이미 TR 로 물리 정지를 걸었으니, 여기선 그 모션의 완료를 기다리지
            #   않고 실패(무응답)로 빠져나와 상위가 제조를 종단하게 한다(v1.1.0 _isEmergencyStopped 체크).
            if self._stop.is_set() or self._estop.is_set():
                return _NO_RESPONSE
            code, ready = self._query_status(addr)
            if code == _NO_RESPONSE or code == 7 or code == 15:
                # 무응답·미초기화·Busy(15) = 아직 준비 안 됨(계속 폴링). v1.1.0 _pollUntilReady 의
                #   isBusyError(Code 15) → continue 미러 — 15 를 "그 외 에러 즉시 반환"에 태우면
                #   바쁜 펌프가 영구 고장으로 오판된다(2026-07-19 15:54 실기기). 7 만 캐시 무효화.
                if code == 7:
                    self._initialized.discard(addr)
                    if self._log is not None:
                        self._log.warn(
                            "펌프 홈 상실(Code 7) — 재초기화 예약",
                            stage=STAGE_STEP_EXEC,
                            pumpAddr=addr,
                            engineCode=7,
                        )
                time.sleep(POLL_INTERVAL_S)
                continue
            if code in (9, 10):
                # 오버로드 — TR 로 latched 에러 지우고 재초기화 강제(v1.1.0 parity·씰/모터 보호).
                try:
                    self._txn(addr, TERMINATE)
                except Exception:  # noqa: BLE001 — TR 실패가 에러 보고를 막지 않는다.
                    pass
                self._initialized.discard(addr)
                if self._log is not None:
                    self._log.warn(
                        "펌프 오버로드 래치 — TR+재초기화 강제",
                        stage=STAGE_STEP_EXEC,
                        pumpAddr=addr,
                        engineCode=code,
                    )
                return code
            if code != 0:
                return code  # 그 외 하드웨어 에러 — 즉시 상위 분류로.
            if ready:
                return 0
            time.sleep(POLL_INTERVAL_S)  # Busy — 모터가 도는 중.
        return _NO_RESPONSE  # 폴링 상한 초과 = ENGINE_TIMEOUT

    # ── 속도 프로파일 ───────────────────────────────────────────────────────
    def _speed_cmd(self, top_hz: int | None, slope: int | None) -> str:
        """`v{시작}V{최고}c{컷오프}L{경사}` — 프리셋 상한으로 **클램프**만 한다.

        서버가 정책(전역 × 포트 오버라이드)을 이미 확정해 보냈다. pi 는 그 값이 이 펌프의 물리
        상한을 넘지 않는지만 본다(하드웨어 보호). 제약 = `v ≤ c ≤ V`(느리게 출발·느리게 끝).
        """
        p = self.preset
        top = min(int(top_hz), p.pump_max_top_speed_hz) if top_hz else p.pump_max_top_speed_hz
        top = max(1, top)
        # 시작·컷오프는 top 을 넘지 못한다(단조성) + 각자의 프리셋 상한 안.
        start = min(p.pump_max_start_speed_hz, top)
        cutoff = max(min(p.pump_max_cutoff_speed_hz, top), start)
        lp = min(int(slope), p.pump_max_slope) if slope else p.pump_max_slope
        lp = max(1, lp)
        return f"v{start}V{top}c{cutoff}L{lp}"

    # ── 셋업 ────────────────────────────────────────────────────────────────
    def _settle(
        self,
        addr: int,
        command: str,
        timeout_s: float,
        *,
        poll: bool = False,
        ack_tolerant: bool = False,
        _busy_retry: bool = True,
    ) -> int:
        """명령 송신 → 즉답 판정 → (poll 이면) 완료까지 폴링. 반환 = error_code.

        `ack_tolerant`(poll=True 전용·2026-07-19 QA "흡입/배출 이슈"): 즉답이 무응답/깨진
        프레임이어도 즉시 실패하지 않고 **폴이 최종 판정**한다. 근거(실기기 로그 실측):
          - 이 기기 링크는 응답 프레임을 간헐적으로 깨뜨린다(`C`·`\\x07`·무응답). 그래도 명령
            자체는 펌프에 닿아 **플런저는 실제로 움직인다** — ACK 만 깨진 것.
          - 반면 폴(`_poll_until_ready`)은 쓰레기·무응답·Code 7 을 재시도로 견디며 진짜
            완료(Ready+위치)를 확인한다 — 13:37:39 성공 건이 증명(폴 중 쓰레기 3회에도 완주).
          - 즉 "즉답 1회 읽기"가 유일한 단일 실패 지점이라 깨진 링크에서 복권이 됐다.
        silent-success 아님 — 성공 판정은 언제나 폴의 실제 완료 확인이다. 진짜 죽은 펌프
        (전 왕복 무응답)는 폴 타임아웃(_NO_RESPONSE)으로 여전히 정직하게 실패한다.
        ⚠️ 즉답이 **명시적 에러 코드**(1~14)면 tolerant 여도 기존대로 즉시 실패(진짜 에러).

        **Code 15(Busy NAK) 은 에러가 아니다**(poll=True 한정·2026-07-19 15:54 실기기 실측):
        선행 모션(예: fire-and-forget 초기화의 홈) 실행 중 새 명령이 오면 펌프는 유효 프레임
        `/0O`(err 15)로 "지금 바쁨"을 답하고 **그 명령을 수행하지 않는다**(NAK). v1.1.0 은 15 를
        "기다려라"로 처리했는데(_validateResponse 통과 + 폴 continue) v1.2.0 이 이 미러를 빠뜨려
        초기화 done 12초 뒤 흡입이 PERMANENT 로 오판됐다. 처리 = **Ready 대기 후 명령 1회
        재전송**: 그냥 폴만 하고 진행하면 밸브 회전이 유실된 채 플런저를 밀어(엉뚱한 포트 흡입 —
        어제 QA 의 그 위험) 물리 사고가 되므로, NAK 로 버려진 명령을 반드시 다시 보낸다.
        재전송은 1회 한정(`_busy_retry`) — 재전송마저 Busy 면 15 를 정직하게 반환.
        """
        raw = self._txn(addr, command)
        code, _ready = parse_status(raw) if raw else (_NO_RESPONSE, False)
        garbled = code == _NO_RESPONSE  # 무응답이거나, 받긴 했는데 상태 프레임이 아님.
        if garbled and not (poll and ack_tolerant):
            # 빈 응답 = 실패(silent-success 금지·v1.0 콸콸콸 교훈) / 프레임 아님 = 실패측.
            return _NO_RESPONSE if not raw else _MALFORMED
        if not garbled and code == 15 and poll:
            # Busy NAK — 선행 모션 완료(Ready)까지 기다렸다가 유실된 명령을 재전송한다(위 docstring).
            if self._log is not None:
                self._log.info(
                    "명령 즉답 Busy(Code 15) — 선행 모션 Ready 대기 후 1회 재전송(v1.1.0 busy 의미론)",
                    stage=STAGE_STEP_EXEC,
                    pumpAddr=addr,
                    command=command,
                    retry=_busy_retry,
                )
            #   ⚠️ 대기 예산은 **선행 모션 기준**(2026-07-19 P1) — busy 의 원인은 "지금 명령"이
            #   아니라 **앞서 도는 모션**(대개 fire-and-forget 초기화의 홈 — 실측상 done 보고
            #   12s 뒤에도 진행·풀스트로크 홈은 수십 초)이다. 밸브 회전(I{p}R)의 read_timeout
            #   5s 를 그대로 쓰면 잔여 홈 >5s 에서 _NO_RESPONSE 로 실패해 원 사고("건강한
            #   펌프인데 실패")가 재발한다. 정비 op(run_op)는 단일 시도라 재시도 커버도 없다.
            wait_code = self._poll_until_ready(addr, max(timeout_s, self.init_timeout_s))
            if wait_code != 0:
                return wait_code
            if not _busy_retry:
                return 15  # 재전송 후에도 Busy — 비정상 지속, 정직하게 보고(무한 재귀 방지).
            return self._settle(
                addr, command, timeout_s, poll=poll, ack_tolerant=ack_tolerant, _busy_retry=False
            )
        if not garbled and code != 0:
            # ⚠️ **즉답 재초기화-필수 에러(7 미초기화·9/10 오버로드)는 셋업 캐시를 무효화**한다
            #   (2026-07-18·브릭 회귀 봉합). 폴링을 안 타는 즉답 경로라 여기서 discard 하지 않으면
            #   `_ensure_ready` 캐시-skip 이 재초기화를 영영 막아 그 펌프가 브릭된다. 셋업 중엔 addr 가
            #   아직 _initialized 에 없어 no-op(안전).
            if code in (7, 9, 10):
                self._initialized.discard(addr)
            return code
        if not poll:
            return 0
        if garbled and self._log is not None:
            # 관대 경로 진입 흔적 — "즉답이 깨졌지만 폴로 실제 완료를 확인한다"를 흐름 로그로 남긴다.
            self._log.info(
                "명령 즉답 깨짐/무응답 — 모션 폴로 완료 판정(ack-tolerant·링크 노이즈 관대)",
                stage=STAGE_STEP_EXEC,
                pumpAddr=addr,
                command=command,
                response=(_printable(raw.strip()) or "(무응답)"),
            )
        return self._poll_until_ready(addr, timeout_s)

    def _setup(self, addr: int, spec: SyringeSpec) -> int:
        """부팅 셋업 — 스톨전류 + 초기화힘. **전부 시린지 용량에서 파생**된다.

        `U{typeCode},{stall}R` = 과부하 감지선("이보다 힘들면 멈춰라" → Code 9). 없으면 막힌 채
        계속 밀어 펌프가 부서진다. `Z/Z1/Z2` = 홈 탐색인데 **힘이 다르다** — 작은 시린지에 전력을
        걸면 씰이 상한다(0.5mL → `Z1R` 반력). 둘 다 `SyringeSpec` 이 용량에서 파생하므로
        여기서 **모드로 분기하지 않는다** — v1.1.0 사고 경로가 정확히 "설정 용량을 무시하고 모드
        기본으로 초기화힘을 유도"였다.
        """
        stall = f"U{self.preset.pump_syringe_type_code},{spec.stall_current}R"
        code = self._settle(addr, stall, self.read_timeout_s)
        if code != 0:
            return code
        # 초기화 = 홈 탐색. 플런저가 끝까지 가므로 폴링 상한을 길게(인스턴스 값·F1 통제 가능).
        return self._settle(addr, spec.init_command, self.init_timeout_s, poll=True)

    def _ensure_ready(self, addr: int, spec: SyringeSpec) -> int:
        """이 펌프가 셋업됐는지 보장(펌프당 1회). 반환 = error_code."""
        if addr in self._initialized:
            return 0
        # ── 상태 리셋(TR) — ⚠️ **결과를 검증하지 않는다**. ─────────────────────
        #   TR 은 latched 에러를 *지우는* 명령이다. v1.1.0 은 여기서 응답을 검증해 throw 했고,
        #   그래서 에러 상태의 펌프는 초기화조차 못 해 전원 재투입 전까지 벽돌이 됐다
        #   (project_hey_senlyt_pump_recovery_brick). 결과를 무시하고 셋업으로 간다.
        try:
            self._txn(addr, TERMINATE)
        except Exception:  # noqa: BLE001 — TR 실패가 복구를 막으면 안 된다(그게 그 사고였다).
            pass
        code = self._setup(addr, spec)
        if code == 0:
            self._initialized.add(addr)
        return code

    def initialize(self) -> EngineResult:
        """셋업 캐시 무효화 — 다음 토출 때 그 펌프에 다시 `TR`+`U200`+`Z` 를 건다.

        v1.1.0 은 작업(제조·세척) 시작마다 `initialize(address:)` 를 불렀다. v1.2.0 pi 는 스텝
        단위로 돌고 **스텝에 시린지 스펙이 실려 오므로**(`cmd.spec`), 실제 셋업은 첫 토출 때 그
        펌프에 건다(`_ensure_ready`). 여기서 주소 없이 `Z` 를 쏘려면 브로드캐스트(addr 0)를 써야
        하는데 그러면 전 펌프가 동시 응답한다 — 그래서 캐시만 비운다.
        """
        self._initialized.clear()
        return EngineResult(raw_error_code=0, detail="setup cache cleared")

    # ── 브로드캐스트 초기화 (전 펌프 동시 홈 — v1.1.0 RealPumpService.initializeAll 미러) ──────
    def _broadcast(self, command: str) -> None:
        """`/_{command}[CR]` 전 펌프 동시 송신 — **프레임 자체의 응답은 읽지 않는다**.

        브로드캐스트는 무응답/충돌이 속성이라(BROADCAST_ADDR 주석) 여기선 write 만 한다.
        ⚠️ **초기화 시퀀스 안에서는 송신 뒤 `?` 판정을 일절 붙이지 말 것**(단발이든 폴이든 —
        2026-07-19 실기기 2회 실측) — 브로드캐스트 직후/이후 버스가 오염돼(ETX 없는 쓰레기
        바이트·수 초 무응답) 건강한 펌프를 무응답(-1000)으로 오탐한다. 초기화의 완료 보장은
        고정 대기(fire-and-forget·`initialize_broadcast`)이고, 연결성 검증은 이후 **토출 경로의
        Ready 폴**이 대신한다. 버스 락은 write 만 감싸고, 다음 주소지정 `_txn` 이 입력버퍼를
        flush 하므로 충돌 잔여 프레임은 정리된다.
        """
        frame = f"{FRAME_START}{BROADCAST_ADDR}{command}{FRAME_END}".encode("ascii")
        try:
            self._broadcast_io(frame)
        except OSError as e:
            # 장치 소멸/핫플러그 — 자가 재연결 후 1회 재송신(`_reconnect_serial` 주석).
            if not self._reconnect_serial(reason=str(e)):
                raise
            self._broadcast_io(frame)
        time.sleep(BROADCAST_SETTLE_S)  # 무응답이라 이 대기(+호출자의 고정 대기)로만 보장.
        # 브로드캐스트 송신 관측(RC4·검증 갭 봉합) — `_txn` 을 안 거치므로 여기서 별도 DEBUG.
        #   pumpAddr=0 = 전 펌프(브로드캐스트) 표식. fire-and-forget 이라 응답 로그는 없다 —
        #   "명령이 나갔다"가 초기화 시퀀스의 유일한 시리얼 관측 흔적이다.
        if self._log is not None:
            self._log.debug(
                "브로드캐스트 송신",
                stage=STAGE_STEP_EXEC,
                pumpAddr=0,
                command=f"/{BROADCAST_ADDR}{command}",
            )

    def _broadcast_io(self, frame: bytes) -> None:
        """브로드캐스트 I/O 본체(write-only) — 시리얼 예외는 그대로 올린다(재연결은 `_broadcast`)."""
        conn = self._conn()
        with self._bus:
            _reset = getattr(conn, "reset_input_buffer", None)
            if callable(_reset):
                try:
                    _reset()
                except Exception:  # noqa: BLE001 — flush 실패는 무시(최선노력).
                    pass
            conn.write(frame)
            time.sleep(WRITE_SETTLE_S)

    def initialize_broadcast(self, addrs: "Sequence[int]", spec: SyringeSpec) -> "dict[int, int]":
        """전 펌프 **동시** 초기화 — **fire-and-forget**(open-loop·응답 무시, 2026-07-19 확정).

        `/_TR → /_U{tc},{stall}R → /_{initCommand} → /_I{safe}R` 를 **눈감고 한 발씩**
        브로드캐스트하고(명령 1발 → 버스 경합 없음), **어떤 `?` 판정도 하지 않는다** — 홈은
        고정 대기(HOME_SETTLE_S)로만 완료를 보장하고 전 펌프를 성공(0)으로 반환한다.
        v1.0.0 Flask 기기설정 툴 `api_broadcast(action=init)`(poll 없이 sleep 후 성공)과
        동일 방식이며, 그 방식만 실기기에서 동작이 검증됐다.

        ⚠️ **Ready 폴을 제거한 이유**(2026-07-19 실기기 10000000b9166a1c·flavor 2펌프 2회 실측):
        브로드캐스트 뒤 이 기기 버스에서 `?` 응답이 깨지거나(펌프1 = 매번 다른 ETX 없는 쓰레기
        `[`·`>F[`·`6f7`…) 무응답(펌프2 = 5s 타임아웃 반복)이라, 단발 확인(-1000 오탐·10:13)도
        끝 Ready 폴(-1000 오탐·12:09, 91e3b08 이후에도 지속)도 **건강한 펌프를 permanent 로
        오판**했다. 초기화는 open-loop 동작 — 명령을 받으면 펌프가 알아서 홈을 잡는다(실측:
        "실패" 보고 중에도 홈은 실제 수행됨). 폴 자체가 오탐원이므로 걷어낸다.

        **silent-success 는 정비 한정 허용** (EP-03 빈응답=실패 규칙은 토출용):
          1. 초기화 = 홈 복귀(플런저 후퇴) — 오성공해도 위험한 물리 동작이 아니다.
          2. 진짜 죽은 펌프는 이후 **토출 경로의 Ready 폴**(`_settle`/`_ensure_ready`)에서
             드러난다 — 토출 셋업의 폴은 그대로 유지된다.

        반환 = {addr: 0}(전 펌프 성공). 단 시퀀스 도중 estop/shutdown 이 서면 남은 발사를
        중단하고 전 펌프 실패(_NO_RESPONSE)·캐시 미등록으로 반환한다(estop 우선).
        힘·스톨전류는 `spec` 에서 파생(모드 분기 없음).
        """
        targets = [a for a in addrs if a >= 1]
        results: dict[int, int] = {a: 0 for a in targets}
        if not targets:
            return results
        # 초기화 = estop 복구 경로 — 로컬 래치 해제 + 셋업 캐시 무효화(개별 initialize 와 동일 이중 방어).
        self._estop.clear()
        for a in targets:
            self._initialized.discard(a)
        # 스텝 대기는 전부 **중단 가능**(`_ff_wait`) — 폴을 걷어내며 estop 재확인 지점도 사라지면,
        #   시퀀스 도중(특히 홈 고정 대기) 감시 스레드의 estop 이 이 창을 뚫지 못한다(리뷰 P2).
        #   중단 시: 남은 브로드캐스트(홈·안전포트 = 물리 이동)를 발사하지 않고, 셋업 캐시도
        #   등록하지 않은 채 전 펌프 실패(_NO_RESPONSE)로 반환한다 — 운영자가 다시 누르게.
        # [0/4] 상태 리셋(TR) — 결과 검증 안 함(에러 지우기·브릭 회귀 봉합 취지).
        self._broadcast(TERMINATE)
        if not self._ff_wait(BROADCAST_STEP_GAP_S):
            return self._ff_abort(targets, results)
        # [1/4] 스톨전류(과부하 감지선·Code 9 방어) — spec 파생.
        self._broadcast(f"U{self.preset.pump_syringe_type_code},{spec.stall_current}R")
        if not self._ff_wait(BROADCAST_STEP_GAP_S):
            return self._ff_abort(targets, results)
        # [2/4] 원점 복귀(홈) — Z/Z1/Z2(힘은 spec.init_command 이 용량에서 파생).
        #   ★ fire-and-forget — Ready 폴 없음. 홈 탐색 물리 시간만 고정 대기.
        self._broadcast(spec.init_command)
        if not self._ff_wait(HOME_SETTLE_S):
            return self._ff_abort(targets, results)
        # [3/4] 안전 포트(Air) — 발사만(폴 없음).
        self._broadcast(f"I{SAFE_PORT}R")
        if not self._ff_wait(BROADCAST_STEP_GAP_S):
            return self._ff_abort(targets, results)
        # [4/4] 전 펌프 초기화 간주(캐시 등록) — 다음 토출이 셋업을 건너뛰고, 죽은 펌프는
        #   그 토출의 첫 주소지정 명령(Ready 폴)에서 무응답으로 드러난다.
        #   ⚠️ 마지막 재확인 — estop 의 `_initialized.discard`(안전 무효화)를 무조건 add 로
        #   덮으면, estop 이 홈을 중단시킨 펌프가 재홈 없이 다음 토출을 받는다(리뷰 P1).
        #   (이 확인 뒤 극소 창에서 estop 이 서더라도, emergency_stop_all 은 `_estop.set()` 후
        #   느린 per-pump TR 트랜잭션을 돌고 나서 discard 하므로 discard 가 add 뒤에 와 이긴다.)
        if self._estop.is_set() or self._stop.is_set():
            return self._ff_abort(targets, results)
        for a in targets:
            self._initialized.add(a)
        return results

    def _ff_wait(self, seconds: float) -> bool:
        """fire-and-forget 스텝 대기 — estop/shutdown 이 서면 조기 이탈(False = 중단하라)."""
        deadline = time.monotonic() + seconds
        while True:
            if self._estop.is_set() or self._stop.is_set():
                return False
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return True
            time.sleep(min(0.05, remaining))

    def _ff_abort(self, targets: "Sequence[int]", results: "dict[int, int]") -> "dict[int, int]":
        """fire-and-forget 중단 — 캐시 미등록(estop discard 존중) + 전 펌프 실패 보고."""
        for a in targets:
            results[a] = _NO_RESPONSE
        if self._log is not None:
            self._log.warn(
                "정비 초기화 중단 — estop/shutdown 개입(캐시 미등록·재시도 필요)",
                stage=STAGE_STEP_EXEC,
            )
        return results

    # ── 토출 ────────────────────────────────────────────────────────────────
    def _cycle(self, cmd: EngineDispenseCommand, *, aspirate_only: bool) -> EngineResult:
        """한 번의 토출 사이클 — `I{in}` → `P{n}` → `O{out}` → `D{n}`.

        이동 명령(P·D)은 ACK 만 받고 **폴링으로 완료를 기다린다** — 그 사이 버스는 비어 있어
        다른 펌프가 동시에 돈다(L2). 어느 단계든 에러면 즉시 반환하고, 분류·재시도는 상위
        `EngineExecutor` 가 한다(여기서 재시도하면 이중 재시도가 된다).
        """
        addr = cmd.pump_addr
        try:
            code = self._ensure_ready(addr, cmd.spec)
            if code != 0:
                return EngineResult(raw_error_code=code, detail="setup failed")

            speed_in = self._speed_cmd(cmd.aspirate_speed_hz, cmd.slope)
            # ① 흡입 구멍으로 밸브 회전 — 포트가 없으면(구계약) 회전을 건너뛴다(현 위치 유지).
            if cmd.in_port is not None:
                code = self._settle(addr, f"I{cmd.in_port}R", self.read_timeout_s, poll=True)
                if code != 0:
                    return EngineResult(raw_error_code=code, detail=f"valve I{cmd.in_port}")
            # ② 흡입 — 플런저 후퇴. 속도 프로파일을 같은 프레임에 실어 보낸다.
            code = self._settle(addr, f"{speed_in}P{cmd.steps}R", self.motion_timeout_s, poll=True)
            if code != 0:
                return EngineResult(raw_error_code=code, detail="aspirate")
            if aspirate_only:
                return EngineResult(raw_error_code=0)

            speed_out = self._speed_cmd(cmd.dispense_speed_hz, cmd.slope)
            # ③ 배출 구멍으로 밸브 회전.
            if cmd.out_port is not None:
                code = self._settle(addr, f"O{cmd.out_port}R", self.read_timeout_s, poll=True)
                if code != 0:
                    return EngineResult(raw_error_code=code, detail=f"valve O{cmd.out_port}")
            # ④ 배출 — 플런저 전진.
            code = self._settle(addr, f"{speed_out}D{cmd.steps}R", self.motion_timeout_s, poll=True)
            if code != 0:
                return EngineResult(raw_error_code=code, detail="dispense")
            return EngineResult(raw_error_code=0)
        except Exception as e:  # noqa: BLE001
            # 시리얼 예외(연결 끊김·pyserial 미설치 등) = 실패. 상위가 흡수해 형제 태스크를 완주시킨다.
            return EngineResult(raw_error_code=_NO_RESPONSE, detail=f"serial error: {e}")

    def aspirate(self, cmd: EngineDispenseCommand) -> EngineResult:
        """흡입만(`I` → `P`) — 배출 없이 통에서 빨아올린다."""
        return self._cycle(cmd, aspirate_only=True)

    # ── 엔진 조작(정비) — **의도 → 펌프 문법 번역은 여기서만** ──────────────────
    def run_op(self, cmd: EngineOpCommand) -> EngineResult:
        """관제 정비 버튼의 의도를 SY-01B 명령으로 번역해 수행한다.

        ⚠️ **`A{n}` 같은 문법을 아는 유일한 자리다.** 서버는 `op`(홈으로/끝까지/초기화)라는
        *의도*만 보낸다 — 그래야 "pi 만 하드웨어를 안다"는 경계가 유지되고, 펌프 벤더가 바뀌어도
        서버·wire 계약이 그대로다.

          - `estop`        → `TR`(in-flight 이동 즉시 중단 + 포트 클린·abort). **셋업 안 태운다**.
          - `initialize`   → `TR`(에러 지우기·검증 안 함) + `U200,{stall}` + `Z/Z1/Z2`
          - `plunger_full` → `A{fullStroke}` (튜브 프라이밍·기포 제거)
          - `plunger_home` → `A0`           (잔량 배출·보관 자세)

        절대 이동(`A`)은 **초기화로 원점이 잡혀 있어야** 의미가 있다(홈을 모르면 기준이 없다).
        그래서 `_ensure_ready` 를 먼저 태운다.
        """
        addr = cmd.pump_addr
        try:
            if cmd.op == OP_ESTOP:
                # 긴급 정지 — `TR`(terminate)로 in-flight 이동을 즉시 중단하고 포트를 클린한다(abort).
                #   ⚠️ **셋업/재초기화를 태우지 않는다**(정지가 목적). TR 후 홈 기준이 흔들릴 수 있으므로
                #   셋업 캐시를 무효화해 다음 토출이 재초기화하게 한다(안전측).
                self._initialized.discard(addr)
                raw = self._txn(addr, TERMINATE)
                # ⛔ **버퍼가 비지 않았다고 성공으로 보지 않는다**(리뷰 P1·2026-07-18). 반이중 RS485 +
                #   CH340 은 자기 송신을 에코(`/1TR\r`)하는데 이건 ETX 가 없어 read 가 타임아웃까지 돌다
                #   그 에코를 반환한다 → truthy. 옛 `0 if raw else …` 는 펌프가 죽어도(에코만) 성공으로
                #   보고했다(safety-stop 이 가장 하면 안 될 거짓 성공). 그래서 **응답 프레임(`/0…`)을
                #   parse_status 로 확인**한다 — 실제 펌프 상태바이트가 와야 정지를 성공으로 본다.
                error_code, _ready = parse_status(raw)
                return EngineResult(raw_error_code=error_code, detail="estop")
            if cmd.op == OP_INITIALIZE:
                # 강제 초기화 = 캐시를 버리고 TR+셋업을 다시 건다(latched 에러 복구 경로).
                #   ⚠️ **긴급정지 래치도 해제**한다 — 초기화는 estop 복구 경로라, 안 풀면 이 초기화의
                #   홈 탐색 폴이 `_estop` 때문에 즉시 빠져나와(무응답) 복구가 실패한다(관제도 세척/초기화
                #   전에 서버 신호를 clear 하지만, 로컬 즉시 해제로 이중 방어).
                self._estop.clear()
                self._initialized.discard(addr)
                code = self._ensure_ready(addr, cmd.spec)
                return EngineResult(raw_error_code=code, detail="initialize")

            # 절대 이동 — 원점이 잡혀 있어야 기준이 선다.
            code = self._ensure_ready(addr, cmd.spec)
            if code != 0:
                return EngineResult(raw_error_code=code, detail="setup failed")
            if cmd.op == OP_PLUNGER_FULL:
                target = cmd.spec.pump_full_stroke
            elif cmd.op == OP_PLUNGER_HOME:
                target = 0
            else:
                # 미지의 op = 거부(안전측). 새 동작은 계약에 명시적으로 추가해야 한다.
                return EngineResult(raw_error_code=_MALFORMED, detail=f"unknown op {cmd.op}")
            # ── v1.1.0 시퀀스 복원(2026-07-19 QA "흡입/배출 이슈") ──────────────────
            #   v1.1.0 aspirate/dispenseSyringe = **밸브 먼저 회전**(흡입=air·배출=output) 후
            #   플런저 이동. v1.2.0 은 회전 없이 A 만 쏴서, 마지막에 열려 있던 액체 포트로
            #   흡입/역류하는 격차가 있었다. 포트는 서버(배치 SoT)가 해석해 실어 준다 —
            #   None(구 서버)이면 기존처럼 회전 생략(하위호환). 회전 실패 시 이동하지 않는다
            #   (엉뚱한 포트로 플런저를 밀면 역류·과부하 — v1.1.0 도 moveValve 실패는 전파).
            if cmd.valve_port is not None:
                code = self._settle(
                    addr,
                    f"I{cmd.valve_port}R",
                    self.read_timeout_s,
                    poll=True,
                    ack_tolerant=True,
                )
                if code != 0:
                    return EngineResult(
                        raw_error_code=code, detail=f"valve I{cmd.valve_port} ({cmd.op})"
                    )
            # 이동 성패는 **폴의 실제 완료 확인**이 판정(ack_tolerant · v1.1.0 _validateResponse 가
            #   빈/깨진 즉답을 통과시키고 폴이 판정하던 필드 검증 구조의 미러) — 이 기기의 간헐
            #   프레임 파손(즉답 `C`·`\x07`·무응답)이 즉시 permanent 로 오판되던 것을 봉합.
            speed = self._speed_cmd(None, None)  # 정비 이동은 프리셋 기본 속도.
            code = self._settle(
                addr, f"{speed}A{target}R", self.motion_timeout_s, poll=True, ack_tolerant=True
            )
            return EngineResult(raw_error_code=code, detail=cmd.op)
        except Exception as e:  # noqa: BLE001
            return EngineResult(raw_error_code=_NO_RESPONSE, detail=f"serial error: {e}")

    def dispense(self, cmd: EngineDispenseCommand) -> EngineResult:
        """한 스텝의 **전체 물리 사이클**(`I`→`P`→`O`→`D`).

        상위 `EngineExecutor.run_step` 이 부르는 유일한 메서드다 — 실 하드웨어에선 흡입과 배출이
        하나의 물리 사이클이라, 이 한 번이 "한 스텝"의 전부다(Fake 어댑터와 동일 전제).
        """
        return self._cycle(cmd, aspirate_only=False)

    # ── 건강 프로브 (주기 HW 감시·2026-07-19 "데몬이 항상 감시해야" 요구) ────────
    def health_probe(self, addr: int) -> str:
        """1발 `?` 건강 판정 — `"ok"`(유효 프레임) / `"garbled"`(깨진 프레임) / `"silent"`(무응답).

        하트비트 주기 감시용 read-only 프로브. 짧은 타임아웃(PROBE_READ_TIMEOUT_S) 1회라
        비용이 ms 단위이고, 버스 락은 트랜잭션 단위라 형제 작업과 시분할된다 — 단 호출측
        (daemon)이 **idle 게이트**(제조·정비 중 스킵)를 걸어 모션 중 버스 잡음을 원천 회피한다.
        판정 의미는 오늘 진단 툴(pump_link_diag)과 동일: ok=응답 정상 · garbled=살아있는데
        링크 품질 문제(오늘 오전의 그 상태) · silent=전기적 침묵(결선/전원/주소).
        """
        try:
            raw = self._txn(addr, STATUS_QUERY, read_timeout_s=PROBE_READ_TIMEOUT_S)
        except Exception:  # noqa: BLE001 — 프로브 실패(포트 소멸 등) = 무응답 취급.
            return "silent"
        if not raw.strip():
            return "silent"
        code, _ready = parse_status(raw)
        return "garbled" if code == _NO_RESPONSE else "ok"

    # ── 프로브 (부팅 자동인식) ──────────────────────────────────────────────
    def probe(self, addr: int) -> bool:
        """이 주소에 펌프가 **응답하는가** — `pump_health.discover_pumps` 가 쓰는 seam.

        v1.1.0 `probePump` 원리: CH340 은 포트 open 직후·펌프 wake 순간 첫 폴에서 빈 프레임을
        주는 일이 잦다(전원 ON 인데 "무응답 → 미장착" 오판의 직접 원인). 그래서 단발이 아니라
        **상태 프레임을 받을 때까지 짧게 재시도**하고, 프레임이 한 번이라도 오면(에러·Busy·
        미초기화 Code 7 포함 = '응답함') True 다. 전부 빈 프레임일 때만 미장착으로 확정한다.

        ⚠️ read-only 라 시도당 짧은 타임아웃 + 주소당 wall-clock 상한을 둔다 — 주소 1..10 전수
        스캔이라 전원 OFF 펌프에서 누적되면 부팅이 늘어진다.
        """
        deadline = time.monotonic() + PROBE_DEADLINE_S
        for _ in range(PROBE_MAX_ATTEMPTS):
            if time.monotonic() >= deadline or self._stop.is_set():
                break
            try:
                code, _ready = self._query_status(addr, read_timeout_s=PROBE_READ_TIMEOUT_S)
            except Exception:  # noqa: BLE001 — 포트 오류 = 미장착으로 보고 스캔 지속.
                return False
            if code != _NO_RESPONSE:
                return True  # 프레임이 왔다 = 전원·통신 살아있음(에러 코드여도 '응답함').
            time.sleep(PROBE_RETRY_GAP_S)
        return False


def open_bus_probe(
    port: str,
    *,
    baudrate: int = DEFAULT_BAUDRATE,
    serial_factory: SerialFactory | None = None,
) -> Callable[[int], bool]:
    """포트 하나를 열어 그 버스용 `probe(addr)->bool` 을 만든다 — `pump_health.autodetect_bus` seam.

    ⚠️ VID/PID 로 찾지 않는다 — **실제로 펌프가 응답하는 포트**가 그 버스다(하드필터 금지).
    """
    adapter = Sy01bEngineAdapter(port=port, baudrate=baudrate, serial_factory=serial_factory)
    return adapter.probe
