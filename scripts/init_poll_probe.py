#!/usr/bin/env python3
"""SY-01B — 초기화 주소지정 폴링 프로브 (일회성 진단 · 데몬 아님, 2026-07-22).

목적: 설계 문서(99_daily/2026-07-21-초기화지연-30초-해결방안-주소지정폴링-설계.html)의
`initialize_polled()` 구현 전, 유일한 잔여 미지수와 설계 가정 전부를 실기기로 실측한다.

진단도구 모음 (scripts/) — 역할 구분:
    · pump_link_diag.py    — 읽기 전용 상태 진단(`?`만·모션 없음)
    · pump_motion_probe.py — 흡입/배출 모션·스톨 프로브
    · init_poll_probe.py(이 파일) — **초기화(Z) 중 주소지정 폴 생존율** + 조기완료 시간 실측

이 프로브가 답하려는 질문 (시나리오 대응):
  S0. 링크 베이스라인 — idle 상태 `?` 응답의 clean/garbled/silent 비율·왕복시간은?
  S1. 이미-홈 상태에서 주소지정 Z → 폴 — 평소 케이스가 정말 3~6초에 끝나는가?
  S2. 플런저 원거리 이동 후 Z → **홈 이동 중** 폴 — 응답이 살아있는가? (핵심 미지수)
      · clean idle / clean busy / garbled / silent 프레임 단위 전수 기록
      · "연속 2회 clean idle" 게이트가 실제 몇 초에 발화했을지 계산
  S3. 두 펌프 동시 홈 + 인터리브 폴 — 버스 경합에서도 폴이 사는가?
  S4. 폴 간격 스윕(0.05/0.2/0.5s) — 간격별 응답 생존율 (버스 부하 vs 감지 지연)
  S5. initialize_polled phase1~3 풀 리허설 — TR→U→Z(주소지정)→폴 조기완료→주차(I)
      end-to-end 소요 실측 (설계 기대: 이미 홈이면 3~6s)
  S6. (--broadcast 옵트인) 브로드캐스트 Z 직후 주소지정 폴 — 7/19 버스 오염 대조군.
      기본 OFF: 오염이 실측된 경로라 문서화 목적일 때만 켠다.

매뉴얼 근거(ASCII V1.2):
  §4.4.1 Z 기본 홈속도 500Hz(용량 무관) — 풀스트로크 12000이면 최악 24s.
  §4.4.2 Z<n1>,<n2>,<n3>R — 힘, 흡입포트, 배출포트. 포트 생략 = 펌웨어 기본(흡입=포트1 액체
         소모·2026-07-21 사고) → 이 프로브는 항상 명시(흡입=air·배출=output).
  §4.6.1 Bit5=1 Ready(새 명령 수락) / 0 Busy. `?` 는 Busy 중에도 수락된다.
  §4.6.3 err15(command overflow) = 완료 전 명령 겹침 — 재초기화 불필요.

안전:
  · 흡입은 air 포트(기본 12)에서만 — 액체를 빨지 않는다. 배출은 output(기본 2).
  · 모든 모션 명령은 Ready 확인 후 전송(err15 자가 유발 금지 — motion_probe v2 규칙).
  · Ctrl+C → 전 펌프 TR(안전 정지) 후 종료.

사용 (pi 에서):
    sudo systemctl stop senlytd            # ⚠️ 필수 — 데몬이 시리얼을 쥐면 배타 충돌
    python3 init_poll_probe.py             # 전 시나리오 (S6 제외) — 파일명은 저장한 이름대로
    python3 init_poll_probe.py --broadcast # S6 브로드캐스트 대조군까지
    python3 init_poll_probe.py --addrs 1,2 --air 12 --out 2   # 포트 레이아웃 다르면
    sudo systemctl start senlytd           # 끝나면 데몬 복구

출력: 콘솔 로그 전문 + ./init_poll_probe_result.jsonl (프레임 단위 전수 기록).
      **둘 다 복사해 주세요** — jsonl 이 구현 확정의 근거 데이터가 된다.
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
import time

try:
    import serial  # pyserial
except ImportError:
    print("ERROR: pyserial 없음 →  pip install pyserial", file=sys.stderr)
    sys.exit(1)

# ── 프로토콜 상수 (adapters/sy01b_engine_adapter.py 와 동일) ──────────────────
FRAME_START = "/"
FRAME_END = "\r"
ETX = 0x03
ERR_MASK = 0x0F
READY_BIT = 0x20
BAUD = 9600
RESP_PREFIX = b"/0"
BROADCAST_ADDR = "_"

# 데몬 상수 미러 (초기화 시퀀스 재현용)
HOME_SETTLE_S = 30.0          # 현행 fire-and-forget 고정 대기 = 폴백 상한
BROADCAST_STEP_GAP_S = 0.5
INIT_POLL_GRACE_S = 0.5       # 설계: Z 발사 후 첫 폴까지 유예(stale-Ready 방어)
CONSECUTIVE_IDLE_N = 2        # 설계: 조기완료 게이트 = 유효 프레임 연속 2회 idle

ERR_MEANING = {
    0: "정상", 1: "초기화 에러", 2: "잘못된 명령", 3: "Invalid operand",
    7: "미초기화", 9: "Plunger overload(물리 스톨)", 10: "Valve overload",
    15: "Command overflow(명령 겹침)",
}

RESULT_PATH = "init_poll_probe_result.jsonl"
_events: list[dict] = []
_t0 = time.monotonic()


def emit(**kv) -> dict:
    """이벤트 1건 — 콘솔 요약 + jsonl 축적. t = 프로브 시작 기준 경과초."""
    kv["t"] = round(time.monotonic() - _t0, 3)
    _events.append(kv)
    return kv


def flush_events() -> None:
    with open(RESULT_PATH, "w", encoding="utf-8") as f:
        for e in _events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")


def printable(raw: bytes) -> str:
    return "".join(
        chr(b) if 32 <= b < 127 else f"\\x{b:02x}" for b in raw
    )


def find_port() -> str | None:
    for pat in ("/dev/ttyUSB*", "/dev/ttyACM*"):
        hits = sorted(glob.glob(pat))
        if hits:
            return hits[0]
    return None


class Bus:
    """한 시리얼 포트 = 한 버스. 주소는 txn 인자로 — 다중 펌프를 한 연결로 다룬다."""

    def __init__(self, port: str, read_timeout_s: float = 1.0):
        self.ser = serial.Serial(
            port=port, baudrate=BAUD, timeout=read_timeout_s, write_timeout=read_timeout_s
        )
        time.sleep(0.2)
        self.ser.reset_input_buffer()

    def txn(self, addr: str, command: str, read_timeout_s: float = 1.0) -> bytes:
        """송신 → ETX 까지 수신 (bounded read). 브로드캐스트(addr='_')는 write-only."""
        frame = f"{FRAME_START}{addr}{command}{FRAME_END}".encode("ascii")
        self.ser.reset_input_buffer()
        self.ser.write(frame)
        self.ser.flush()
        if addr == BROADCAST_ADDR:
            return b""  # 브로드캐스트는 응답 규약 없음(읽지 않는다)
        deadline = time.time() + read_timeout_s
        buf = bytearray()
        while time.time() < deadline:
            n = self.ser.in_waiting
            chunk = self.ser.read(n if n else 1)
            if chunk:
                buf.extend(chunk)
                if ETX in chunk:
                    break
            else:
                time.sleep(0.005)
        return bytes(buf)

    def close(self) -> None:
        try:
            self.ser.close()
        except Exception:
            pass


def decode(raw: bytes) -> dict:
    """원시 응답 → 분류. quality: clean(유효 상태프레임) / garbled(뭔가 왔는데 무효) / silent."""
    out = {
        "raw": printable(raw), "quality": "silent",
        "ready": None, "err": None, "pos": None,
    }
    if not raw:
        return out
    i = raw.find(RESP_PREFIX)
    if i < 0 or len(raw) < i + 3:
        out["quality"] = "garbled"
        return out
    sb = raw[i + 2]
    data = raw[i + 3:]
    if ETX in data:
        data = data[: data.index(ETX)]
    out["quality"] = "clean"
    out["ready"] = bool(sb & READY_BIT)
    out["err"] = sb & ERR_MASK
    ds = data.strip()
    if ds:
        try:
            out["pos"] = int(ds)
        except ValueError:
            out["pos"] = None
    return out


def poll_once(bus: Bus, addr: str, scenario: str, read_timeout_s: float = 0.5) -> dict:
    t_send = time.monotonic()
    d = decode(bus.txn(addr, "?", read_timeout_s=read_timeout_s))
    d["rtt_ms"] = round((time.monotonic() - t_send) * 1000, 1)
    emit(scenario=scenario, addr=addr, event="poll", **d)
    return d


def send(bus: Bus, addr: str, cmd: str, scenario: str, timeout: float = 2.0) -> dict:
    """명령 1발 + 즉답 캡처 (완료 대기는 별도)."""
    d = decode(bus.txn(addr, cmd, read_timeout_s=timeout))
    emit(scenario=scenario, addr=addr, event="send", cmd=cmd, **d)
    e = d.get("err")
    em = f" [{ERR_MEANING.get(e, '?')}]" if e else ""
    print(f"  [{addr}] send {cmd:<14} → {d['quality']} Bit5={d['ready']} err={e}{em} pos={d['pos']}")
    return d


def wait_ready(bus: Bus, addr: str, scenario: str, timeout: float = 40.0) -> bool:
    """Bit5=1 까지 대기 — **다음 명령 전송 전 필수**(err15 자가 유발 금지)."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        d = poll_once(bus, addr, scenario)
        if d["quality"] == "clean" and d["ready"]:
            return True
        time.sleep(0.1)
    print(f"  [{addr}] ⚠️ wait_ready timeout({timeout}s) — 다음 단계로 넘어가지 않음")
    return False


def poll_during_motion(
    bus: Bus, addrs: list[str], scenario: str, interval_s: float,
    deadline_s: float, grace_s: float = INIT_POLL_GRACE_S,
) -> dict:
    """모션 발사 직후 호출 — 완료(연속 N clean idle)까지 전 펌프를 번갈아 폴, 전수 기록.

    반환: addr별 {polls, clean, garbled, silent, first_idle_t, early_exit_t(연속N 발화시각),
          err_seen, done}. early_exit_t 가 None 이면 deadline 까지 게이트 미발화(=폴백행).
    """
    t_fire = time.monotonic()
    time.sleep(grace_s)
    stats = {
        a: {"polls": 0, "clean": 0, "garbled": 0, "silent": 0,
            "first_idle_t": None, "early_exit_t": None, "err_seen": [],
            "consec_idle": 0, "done": False}
        for a in addrs
    }
    while time.monotonic() - t_fire < deadline_s and not all(s["done"] for s in stats.values()):
        for a in addrs:
            s = stats[a]
            if s["done"]:
                continue
            d = poll_once(bus, a, scenario)
            el = time.monotonic() - t_fire
            s["polls"] += 1
            s[d["quality"]] += 1
            if d["quality"] == "clean":
                if d["err"] not in (0, None) and d["err"] not in s["err_seen"]:
                    s["err_seen"].append(d["err"])
                    print(f"  [{a}] t={el:5.1f}s err={d['err']} [{ERR_MEANING.get(d['err'], '?')}] pos={d['pos']}")
                if d["ready"]:
                    if s["first_idle_t"] is None:
                        s["first_idle_t"] = round(el, 2)
                    s["consec_idle"] += 1
                    if s["consec_idle"] >= CONSECUTIVE_IDLE_N:
                        s["early_exit_t"] = round(el, 2)
                        s["done"] = True
                        print(f"  [{a}] ✅ 조기완료 게이트 발화 t={el:.2f}s (연속 {CONSECUTIVE_IDLE_N}회 clean idle · pos={d['pos']})")
                else:
                    s["consec_idle"] = 0
            else:
                s["consec_idle"] = 0  # 깨진 프레임은 연속성 리셋(설계 기둥 3)
            time.sleep(interval_s)
    for a, s in stats.items():
        s.pop("consec_idle", None)
        s.pop("done", None)
        emit(scenario=scenario, addr=a, event="motion_summary", interval_s=interval_s, **s)
    return stats


def summarize(scenario: str, stats: dict, note: str = "") -> None:
    print(f"  ── {scenario} 요약 {note}")
    for a, s in stats.items():
        total = s["polls"] or 1
        print(
            f"  [{a}] polls={s['polls']} clean={s['clean']}({100 * s['clean'] // total}%) "
            f"garbled={s['garbled']} silent={s['silent']} "
            f"첫idle={s['first_idle_t']}s 조기완료={s['early_exit_t']}s err={s['err_seen']}"
        )


# ── 시나리오 ──────────────────────────────────────────────────────────────────

def s0_baseline(bus: Bus, addrs: list[str], n: int = 20) -> None:
    print(f"\n━━ S0. 링크 베이스라인 — idle 상태 `?` × {n}회/펌프")
    for a in addrs:
        c = {"clean": 0, "garbled": 0, "silent": 0}
        rtts = []
        for _ in range(n):
            d = poll_once(bus, a, "S0")
            c[d["quality"]] += 1
            rtts.append(d["rtt_ms"])
            time.sleep(0.05)
        avg = round(sum(rtts) / len(rtts), 1)
        print(f"  [{a}] clean={c['clean']}/{n} garbled={c['garbled']} silent={c['silent']} 평균RTT={avg}ms")
        emit(scenario="S0", addr=a, event="baseline_summary", n=n, avg_rtt_ms=avg, **c)


def z_cmd(force: int, air: int, out: int) -> str:
    """Z<힘>,<흡입=air>,<배출=out>R — 포트를 주면 힘(n1) 명시 필수(매뉴얼 §4.4.2·pump_guard 동일)."""
    return f"Z{force},{air},{out}R"


def s1_home_when_home(bus: Bus, addrs: list[str], force: int, air: int, out: int, interval: float) -> None:
    print(f"\n━━ S1. 이미-홈 상태 주소지정 Z + 폴 — 평소 케이스 시간 실측 (기대 3~6s)")
    for a in addrs:
        if not wait_ready(bus, a, "S1"):
            continue
    cmd = z_cmd(force, air, out)
    for a in addrs:
        send(bus, a, cmd, "S1")
    stats = poll_during_motion(bus, addrs, "S1", interval_s=interval, deadline_s=HOME_SETTLE_S)
    summarize("S1", stats, "(이미 홈 — k-offset 왕복만)")


def s2_home_from_far(
    bus: Bus, addrs: list[str], force: int, air: int, out: int,
    interval: float, move_steps: int, scenario: str = "S2",
) -> None:
    print(f"\n━━ {scenario}. 원거리(A{move_steps})에서 Z — **홈 이동 중 폴 생존율** (핵심 미지수 · interval={interval}s)")
    for a in addrs:
        if not wait_ready(bus, a, scenario):
            return
        send(bus, a, f"I{air}R", scenario)          # 밸브 = air (액체 안 빨게)
        if not wait_ready(bus, a, scenario):
            return
        send(bus, a, f"A{move_steps}R", scenario)   # 절대 이동(공기 흡입) — 홈 거리 만들기
        if not wait_ready(bus, a, scenario):
            return
    cmd = z_cmd(force, air, out)
    for a in addrs:
        send(bus, a, cmd, scenario)                 # 발사만 순차 — 모터는 함께 돈다
    stats = poll_during_motion(bus, addrs, scenario, interval_s=interval, deadline_s=HOME_SETTLE_S)
    expect = move_steps / 500.0
    summarize(scenario, stats, f"(물리 홈 기대 ≈{expect:.1f}s @500Hz)")


def s3_dual_home_interleave(bus: Bus, addrs: list[str], force: int, air: int, out: int, interval: float, move_steps: int) -> None:
    if len(addrs) < 2:
        print("\n━━ S3. (펌프 1대 — 건너뜀)")
        return
    print(f"\n━━ S3. 두 펌프 동시 홈 + 인터리브 폴 — 버스 경합 실측")
    s2_home_from_far(bus, addrs, force, air, out, interval, move_steps, scenario="S3")


def s5_full_rehearsal(bus: Bus, addrs: list[str], force: int, air: int, out: int, u_params: str, interval: float) -> None:
    print(f"\n━━ S5. initialize_polled 풀 리허설 — TR→U→Z(주소지정)→폴 조기완료→주차 I{out}R")
    t0 = time.monotonic()
    for a in addrs:                                  # phase 1: 펌프별 발사(순차 발사·동시 모션)
        send(bus, a, "TR", "S5")                     # 상태 리셋 — 결과 검증 안 함(브릭 봉합 취지)
        time.sleep(0.05)
        send(bus, a, f"U{u_params}R", "S5")          # 스톨전류
        time.sleep(0.05)
        send(bus, a, z_cmd(force, air, out), "S5")   # 홈 — send-only, 폴이 완료를 잡는다
    stats = poll_during_motion(bus, addrs, "S5", interval_s=interval, deadline_s=HOME_SETTLE_S)  # phase 2
    for a in addrs:                                  # phase 3: 주차(배출구) — 규칙 "쉴 땐 output"
        send(bus, a, f"I{out}R", "S5")
        wait_ready(bus, a, "S5", timeout=10.0)
    total = round(time.monotonic() - t0, 2)
    summarize("S5", stats)
    print(f"  ⏱ 리허설 end-to-end: {total}s  (현행 고정대기 방식 ≈{HOME_SETTLE_S + 2 * BROADCAST_STEP_GAP_S + 0.5}s)")
    emit(scenario="S5", event="rehearsal_total", total_s=total)


def s6_broadcast_control(bus: Bus, addrs: list[str], force: int, air: int, out: int, interval: float) -> None:
    print(f"\n━━ S6. 브로드캐스트 Z 직후 주소지정 폴 — 7/19 버스 오염 대조군 (옵트인)")
    print("  ⚠️ 오염이 실측된 경로 — 프레임이 깨져도 놀라지 말 것. 문서화 목적.")
    for a in addrs:
        wait_ready(bus, a, "S6")
    bus.txn(BROADCAST_ADDR, z_cmd(force, air, out))  # write-only
    emit(scenario="S6", addr=BROADCAST_ADDR, event="send", cmd=z_cmd(force, air, out),
         quality="broadcast", ready=None, err=None, pos=None, raw="")
    print(f"  [_] broadcast {z_cmd(force, air, out)} (write-only)")
    stats = poll_during_motion(bus, addrs, "S6", interval_s=interval, deadline_s=HOME_SETTLE_S)
    summarize("S6", stats, "(브로드캐스트 직후 — 깨짐 비율이 S1/S2 와 어떻게 다른가)")


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", default=None, help="시리얼 포트 (기본: /dev/ttyUSB*/ttyACM* 자동)")
    ap.add_argument("--addrs", default="1,2", help="펌프 주소 콤마 (기본 1,2 — 식향)")
    ap.add_argument("--air", type=int, default=12, help="air(대기 개방) 포트 — 흡입용 (기본 12)")
    ap.add_argument("--out", type=int, default=2, help="output(배출구) 포트 — 배출/주차 (기본 2·식향)")
    ap.add_argument("--force", type=int, default=1, help="Z 초기화 힘 0=Full/1=Half/2=Third (기본 1 — 0.5mL 시린지)")
    ap.add_argument("--u", default="200,5", help="U 파라미터 (기본 200,5 — SY-01B·0.5mL 스톨전류)")
    ap.add_argument("--interval", type=float, default=0.2, help="기본 폴 간격 초 (기본 0.2)")
    ap.add_argument("--move-steps", type=int, default=6000, help="S2/S3 사전 이동 스텝 (기본 6000 ≈ 홈 12s)")
    ap.add_argument("--sweep", default="0.05,0.5", help="S4 폴 간격 스윕 값들 (기본 0.05,0.5 · 빈값=생략)")
    ap.add_argument("--broadcast", action="store_true", help="S6 브로드캐스트 대조군 포함 (기본 제외)")
    args = ap.parse_args()

    port = args.port or find_port()
    if not port:
        print("ERROR: 시리얼 포트를 못 찾음 (/dev/ttyUSB*, /dev/ttyACM*)", file=sys.stderr)
        return 1
    addrs = [a.strip() for a in args.addrs.split(",") if a.strip()]

    print(f"포트={port} 주소={addrs} air={args.air} out={args.out} force={args.force} U={args.u}")
    print("⚠️ senlytd 정지 확인:  sudo systemctl stop senlytd  (안 했으면 Ctrl+C 후 정지)")
    emit(scenario="meta", event="start", port=port, addrs=addrs,
         air=args.air, out=args.out, force=args.force, u=args.u,
         interval=args.interval, move_steps=args.move_steps)

    bus = Bus(port)
    try:
        s0_baseline(bus, addrs)
        s1_home_when_home(bus, addrs, args.force, args.air, args.out, args.interval)
        s2_home_from_far(bus, addrs, args.force, args.air, args.out, args.interval, args.move_steps)
        s3_dual_home_interleave(bus, addrs, args.force, args.air, args.out, args.interval, args.move_steps)
        for iv in [v.strip() for v in args.sweep.split(",") if v.strip()]:
            s2_home_from_far(bus, addrs, args.force, args.air, args.out,
                             float(iv), max(2000, args.move_steps // 2), scenario=f"S4@{iv}s")
        s5_full_rehearsal(bus, addrs, args.force, args.air, args.out, args.u, args.interval)
        if args.broadcast:
            s6_broadcast_control(bus, addrs, args.force, args.air, args.out, args.interval)
        # 마무리 — 홈+주차 상태 보증 (다음 사용자·데몬을 위한 정리)
        print("\n━━ 마무리 — 전 펌프 홈+주차 정리")
        for a in addrs:
            wait_ready(bus, a, "cleanup")
            send(bus, a, z_cmd(args.force, args.air, args.out), "cleanup")
        poll_during_motion(bus, addrs, "cleanup", interval_s=0.2, deadline_s=HOME_SETTLE_S)
        for a in addrs:
            send(bus, a, f"I{args.out}R", "cleanup")
            wait_ready(bus, a, "cleanup", timeout=10.0)
    except KeyboardInterrupt:
        print("\n⚠️ 중단 — 전 펌프 TR(안전 정지)")
        for a in addrs:
            try:
                bus.txn(a, "TR", read_timeout_s=0.5)
            except Exception:
                pass
    finally:
        flush_events()
        bus.close()
        print(f"\n결과 저장: ./{RESULT_PATH} (프레임 전수) — 콘솔 출력과 함께 복사해 주세요")
    return 0


if __name__ == "__main__":
    sys.exit(main())
