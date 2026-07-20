#!/usr/bin/env python3
"""SY-01B 시린지 펌프 — 모션/스톨 프로브 v2 (일회성 진단 · 데몬 아님, 2026-07-20).

진단도구 모음 (scripts/) — 역할 구분:
    · pump_link_diag.py   — 읽기 전용 **상태** 진단. `?` 만 보낸다(모션 명령 없음).
                            플런저·밸브 안 움직임. "링크가 되나"를 묻는다.
    · pump_motion_probe.py(이 파일) — 플런저를 **실제로 이동**시키는 모션/스톨 프로브.
                            초기화·흡입·배출을 직렬로 보내고 실측한다. 실물 플런저가 움직인다.
                            "명령이 목표에 도달하나(스톨인가 타이밍인가)"를 묻는다.

v2 (2026-07-20 매뉴얼 대조 후 재작성)
────────────────────────────────────
v1 은 "위치 무변 = 정지"로 판정해, 초기화가 아직 Busy 인데도 다음 명령을 보내
**Command Overflow(err 15)** 를 스스로 유발했다(오염된 데이터).

v2 는 매뉴얼(ASCII V1.2 §4.6.1)대로 **Ready 비트(Bit5=1)가 될 때까지 기다린 뒤**
다음 명령을 보낸다 — 명령을 절대 Busy 중에 겹쳐 보내지 않는다. 그래서 err 15 가
나오면 그건 "우리가 겹쳐 보낸 것"이 아니라 진짜 신호다.

이 프로브가 답하려는 질문:
  Q. 명령을 제대로(Ready 기다려) 직렬로 보내면, 흡입 A{steps} 가 목표에 **도달하는가**?
     - 도달(err 0, pos≈목표) → 원래 실패는 **명령 타이밍(overflow)** 이었다. (SW 직렬화로 해결)
     - err 9(overload)로 멈춤 → **진짜 물리 스톨**. (속도/스톨전류/프라이밍 = 하드웨어)
     - Bit5 안 켜지고 위치 고정 → **stuck-busy**(펌웨어가 계속 Busy). 프레임/전원/링크 의심.
     - err 3(invalid operand) → 범위 초과(설정 오류).

매뉴얼 근거(ASCII V1.2):
  §2.2/§2.4 모든 시린지 Full Step=12000(표준)·96000(파인). 3000 모드 없음.
  §4.5.2   A<n> 절대이동 범위 0..12000(표준). A9600 은 범위 안.
  §4.6.1   Bit5=1 → Ready(새 명령 수락) · Bit5=0 → Busy(Report/Terminate만).
  §4.6.2   err: 3=Invalid operand · 9=Plunger overload · 10=Valve overload · 15=Command overflow.
  §4.6.3   err 15 = 재초기화 불필요, `?`로 완료 확인 후 재전송.
  §4.5.6   `?`=commanded 위치 · `?4`=actual(엔코더) 위치. 둘이 벌어지면 실제 모터 스톨.

사용 (pi 에서):
    sudo systemctl stop senlytd                     # ⚠️ 필수 — 데몬이 포트를 쥐면 시리얼 배타 충돌
    python3 scripts/pump_motion_probe.py            # 실패 케이스(9600스텝·5000Hz·라임포트3)
    python3 scripts/pump_motion_probe.py --safe-air # 공기(12번)로 흡입 — 액 안 빨기
    python3 scripts/pump_motion_probe.py --steps 2400 --speed 2000   # 임의 값
    sudo systemctl start senlytd                    # 끝나면 데몬 복구

⚠️ 실행 전
  1) 데몬 정지:  sudo systemctl stop senlytd     (시리얼 배타)
  2) pyserial 필요
  3) 실물 플런저가 움직인다 — 기기 앞에서 감독. 액 없이 상태만 보려면 --safe-air.

출력 전문을 복사해 주세요.
"""

from __future__ import annotations

import argparse
import glob
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
ERR_MASK = 0x0F      # 하위 4비트 = 에러코드
READY_BIT = 0x20     # bit5 = Ready(1=새 명령 수락 / 0=Busy)
BAUD = 9600          # 8N1
RESP_PREFIX = b"/0"

ERR_MEANING = {
    0: "정상",
    1: "초기화 에러(재초기화 필요)",
    2: "잘못된 명령",
    3: "Invalid operand(범위 초과 — 최종위치 >12000/<0)",
    7: "미초기화",
    9: "Plunger overload(플런저 과부하·진짜 물리 스톨)",
    10: "Valve overload(밸브 과부하)",
    15: "Command overflow(이전 명령 완료 전 새 명령 — 재초기화 불필요)",
}


def find_port() -> str | None:
    for pat in ("/dev/ttyUSB*", "/dev/ttyACM*"):
        hits = sorted(glob.glob(pat))
        if hits:
            return hits[0]
    return None


class Pump:
    def __init__(self, port: str, addr: str, read_timeout_s: float = 1.0):
        self.addr = addr
        self.ser = serial.Serial(port=port, baudrate=BAUD, timeout=read_timeout_s,
                                 write_timeout=read_timeout_s)
        time.sleep(0.2)
        self.ser.reset_input_buffer()

    def txn(self, command: str, read_timeout_s: float = 1.0) -> bytes:
        frame = f"{FRAME_START}{self.addr}{command}{FRAME_END}".encode("ascii")
        self.ser.reset_input_buffer()
        self.ser.write(frame)
        self.ser.flush()
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

    def close(self):
        try:
            self.ser.close()
        except Exception:
            pass


def decode(raw: bytes) -> dict:
    """원시 응답 → {raw, char, hex, ready(Bit5), err, pos}. ready=Bit5(합성 아님)."""
    i = raw.find(RESP_PREFIX)
    out = {"raw": repr(raw), "char": None, "hex": None, "ready": None, "err": None, "pos": None}
    if i < 0 or len(raw) < i + 3:
        return out
    sb = raw[i + 2]
    data = raw[i + 3:]
    if ETX in data:
        data = data[: data.index(ETX)]
    out["char"] = chr(sb) if 32 <= sb < 127 else f"\\x{sb:02x}"
    out["hex"] = f"0x{sb:02X}"
    out["ready"] = bool(sb & READY_BIT)   # Bit5 = 새 명령 수락 가능(idle)
    out["err"] = sb & ERR_MASK
    ds = data.strip()
    if ds:
        try:
            out["pos"] = int(ds)
        except ValueError:
            out["pos"] = None
    return out


def show(label: str, d: dict):
    e = d.get("err")
    em = f" [{ERR_MEANING.get(e, '?')}]" if e else ""
    print(f"  [{label:<14}] {d['hex']}({d['char']}) Bit5={d['ready']} err={e}{em} pos={d['pos']}  raw={d['raw']}")


def send(pump: Pump, cmd: str, label: str, timeout: float = 2.0):
    """명령 1발 전송 + 즉답 로그. (완료 대기는 wait_idle 이 별도로.)"""
    d = decode(pump.txn(cmd, read_timeout_s=timeout))
    show(f"send {label}", d)
    return d


def wait_idle(pump: Pump, label: str, timeout: float, target: int | None = None) -> dict:
    """⭐ 매뉴얼 §4.6.1 — **Bit5=1(Ready) 될 때까지** `?` 폴링. Busy 중엔 절대 다음 명령 안 보냄.

    반환 dict 에 verdict 추가:
      'ready'  — Bit5=1 (idle). err 로 성공/실패 판정.
      'stuck'  — timeout 인데 위치가 고정(모터 정지했는데 Busy 안 풀림) = 물리 스톨 의심.
      'timeout'— timeout 이고 위치가 계속 변함(이례적).
    """
    print(f"  ▶ wait Ready: {label} (target={target}, timeout={timeout}s)")
    t0 = time.monotonic()
    last_pos, stable, last = None, 0, {}
    n = 0
    while time.monotonic() - t0 < timeout:
        d = decode(pump.txn("?", read_timeout_s=0.5))
        last = d
        n += 1
        el = time.monotonic() - t0
        changed = d["pos"] != last_pos
        if changed or d["err"] not in (0, None) or n <= 2:
            e = d.get("err")
            em = f" [{ERR_MEANING.get(e, '?')}]" if e else ""
            print(f"    t={el:5.1f}s {d['hex']}({d['char']}) Bit5={d['ready']} err={e}{em} pos={d['pos']}")
        if d["ready"]:  # Bit5=1 → idle, 명령 수락 가능. 이동 끝.
            d["verdict"] = "ready"
            print(f"    → Ready (경과 {el:.1f}s). err={d['err']} pos={d['pos']}"
                  + (f" · 목표 {target} 대비 {100.0*d['pos']/target:.1f}%" if target and d['pos'] is not None else ""))
            return d
        # Bit5=0 (Busy) — 위치 변화 추적(정지-중-Busy = stuck 스톨 후보)
        if d["pos"] is not None and d["pos"] == last_pos:
            stable += 1
        else:
            stable = 0
            last_pos = d["pos"]
        time.sleep(0.05)
    # timeout — 위치 고정이면 stuck(물리 스톨), 아니면 timeout
    last["verdict"] = "stuck" if stable >= 10 else "timeout"
    print(f"    → {last['verdict'].upper()} (timeout {timeout}s). Bit5={last.get('ready')} "
          f"err={last.get('err')} pos={last.get('pos')}"
          + (f" · 목표 {target} 대비 {100.0*last['pos']/target:.1f}%" if target and last.get('pos') is not None else ""))
    return last


def phase_aspirate(pump: Pump, label: str, speed_cmd: str, steps: int, in_port: int,
                   out_port: int, timeout: float):
    """깨끗한 1회 흡입: (Ready 확인 →) I{in} → wait → A{steps} → wait → ?4 대조 → O{out} → A0 → wait."""
    print(f"\n### {label}: I{in_port} → {speed_cmd}A{steps}R → O{out_port} → A0R (매 단계 Ready 대기)")
    send(pump, f"I{in_port}R", f"valve I{in_port}", timeout=2.0)
    wait_idle(pump, f"밸브 I{in_port} 완료", 8.0)
    send(pump, f"{speed_cmd}A{steps}R", f"흡입 A{steps}", timeout=2.0)
    asp = wait_idle(pump, "흡입 완료", timeout, target=steps)
    # commanded(?) vs actual(?4) 대조 — 벌어지면 모터가 실제로 못 간 것(silent stall).
    act = decode(pump.txn("?4", read_timeout_s=0.5))
    show("?4 actual", act)
    if asp.get("pos") is not None and act.get("pos") is not None:
        print(f"    → commanded={asp['pos']} vs actual(?4)={act['pos']} "
              f"(차이 {abs(asp['pos'] - act['pos'])} — 크면 모터 스톨)")
    send(pump, f"O{out_port}R", f"valve O{out_port}", timeout=2.0)
    wait_idle(pump, f"밸브 O{out_port} 완료", 8.0)
    send(pump, f"{speed_cmd}A0R", "배출 A0", timeout=2.0)
    disp = wait_idle(pump, "배출 완료", 15.0, target=0)
    return asp, disp


def main():
    ap = argparse.ArgumentParser(description="SY-01B 모션/스톨 프로브 (플런저 실이동 · 데몬 정지 필요)")
    ap.add_argument("--port", default=None)
    ap.add_argument("--addr", default="1")
    ap.add_argument("--steps", type=int, default=9600, help="흡입 스텝(0.4mL@0.5mL/12000=9600)")
    ap.add_argument("--speed", type=int, default=5000, help="흡입 top speed Hz(실패 재현=5000)")
    ap.add_argument("--slow", type=int, default=2000, help="저속 비교 Hz")
    ap.add_argument("--in", dest="in_port", type=int, default=3)
    ap.add_argument("--out", dest="out_port", type=int, default=2)
    ap.add_argument("--move-timeout", type=float, default=20.0)
    ap.add_argument("--safe-air", action="store_true", help="in=12(공기)로 — 액 안 빨기")
    ap.add_argument("--skip-slow", action="store_true")
    args = ap.parse_args()
    if args.safe_air:
        args.in_port = 12

    port = args.port or find_port()
    if not port:
        print("ERROR: 시리얼 포트 못 찾음. --port /dev/ttyUSB1 로 지정.", file=sys.stderr)
        sys.exit(1)

    FAST = f"v1000V{args.speed}c{args.speed}L14"
    SLOW = f"v1000V{args.slow}c{args.slow}L14"
    print("=" * 72)
    print(f" SY-01B 프로브 v2 (Ready 직렬화)  port={port} addr={args.addr}")
    print(f" 흡입 {args.steps}스텝  fast={args.speed}Hz slow={args.slow}Hz  in={args.in_port} out={args.out_port}")
    print("=" * 72)

    pump = Pump(port, args.addr)
    try:
        # Phase 0 — 베이스라인
        print("\n### Phase 0 — 베이스라인 (?)")
        for i in range(3):
            show(f"baseline{i}", decode(pump.txn("?")))
            time.sleep(0.15)

        # Phase 1 — 초기화 (매 명령 Ready 대기로 직렬화)
        print("\n### Phase 1 — 초기화 TR → U200,5R → Z1R (0.5mL=Half), 각 단계 Ready 대기")
        send(pump, "TR", "TR", timeout=1.0)
        wait_idle(pump, "TR 후", 5.0)
        send(pump, "U200,5R", "U200,5R", timeout=1.0)
        wait_idle(pump, "스톨전류 설정 후", 5.0)
        send(pump, "Z1R", "Z1R(초기화)", timeout=1.0)
        init = wait_idle(pump, "초기화 홈 완료", 30.0)
        if init.get("err") not in (0, None):
            print(f"  ⚠️ 초기화가 err={init['err']} 로 끝남 — 이후 결과 신뢰도 낮음.")

        # Phase 2 — 흡입 스톨 재현(고속) — 깨끗한 직렬 전송
        asp_f, disp_f = phase_aspirate(pump, "Phase 2 흡입/배출(고속)", FAST, args.steps,
                                       args.in_port, args.out_port, args.move_timeout)

        # Phase 3 — 재초기화 후 저속 비교
        if not args.skip_slow:
            print("\n### Phase 3 — 재초기화 후 저속 비교")
            send(pump, "TR", "TR", timeout=1.0)
            wait_idle(pump, "TR 후", 5.0)
            send(pump, "Z1R", "Z1R", timeout=1.0)
            wait_idle(pump, "재초기화 완료", 30.0)
            asp_s, disp_s = phase_aspirate(pump, "Phase 3 흡입/배출(저속)", SLOW, args.steps,
                                           args.in_port, args.out_port, args.move_timeout)
        else:
            asp_s = None

        # Cleanup — 안전 자세
        print("\n### Cleanup — TR → Z1R → I12R")
        send(pump, "TR", "TR", timeout=1.0)
        wait_idle(pump, "TR 후", 5.0)
        send(pump, "Z1R", "Z1R", timeout=1.0)
        wait_idle(pump, "홈 복귀", 30.0)
        send(pump, "I12R", "I12(공기)", timeout=2.0)
        wait_idle(pump, "안전 포트", 8.0)

        # 요약
        print("\n" + "=" * 72)
        print(" 요약 (핵심 — 이 줄들이 결론)")

        def summarize(tag, asp):
            if not asp:
                return
            v, e, p = asp.get("verdict"), asp.get("err"), asp.get("pos")
            verdict = ("도달=err9(진짜 물리 스톨)" if e == 9 else
                       "도달=err3(범위초과)" if e == 3 else
                       "도달=err15(overflow-직렬화 실패)" if e == 15 else
                       "정상 도달(원인=명령타이밍이었음)" if v == "ready" and e in (0, None) and p and p >= args.steps * 0.95 else
                       "Ready지만 목표 미달" if v == "ready" else
                       "stuck-busy(펌웨어 Busy 안 풀림)" if v == "stuck" else
                       f"{v}")
            print(f"  {tag}: pos={p}/{args.steps} err={e} → {verdict}")

        summarize("고속 흡입", asp_f)
        summarize("저속 흡입", asp_s)
        print("=" * 72)
    finally:
        pump.close()


if __name__ == "__main__":
    main()
