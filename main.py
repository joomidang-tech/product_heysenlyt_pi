#!/usr/bin/env python3
"""SY-01B 시린지 펌프 — 상태/스톨/복구 프로브 (일회성 진단 스크립트 · 데몬 아님).

목적
────
2026-07-20 QA "점검시 용량 조절 X" 실기기 실측. 흡입 P{steps} 가 목표 근처(9583/9600)에서
스톨(오버로드)했을 때 **펌프가 실제로 어떤 상태바이트/위치/ready 비트를 내는지**, 그리고
TR·재초기화로 **복구되는지**를 캡처한다. 이 데이터로 pi 폴링/근접완료/복구 로직을 확정한다.
(추측으로 짜지 않는다 — heysenlyt-pi/CLAUDE.md 제1원칙.)

⚠️ 실행 전
────────
1) 시리얼 포트는 배타적이다. **데몬을 먼저 멈춰라**:  sudo systemctl stop senlytd
   (또는 senlytd 프로세스 종료). 안 멈추면 포트 점유로 이 프로브가 못 연다.
2) pyserial 필요:  python3 -c "import serial" 로 확인 (없으면 pip install pyserial).
3) 이 프로브는 플런저를 실제로 움직인다(흡입/배출). 기기 앞에서 감독하며 실행.
   흡입 포트(IN_PORT)에 액이 있으면 그 액을 빤다 — 액 없이 상태만 보려면 IN_PORT=12(공기)로.

사용
────
  python3 main.py                 # 기본: 실패 케이스 재현(9600스텝·5000Hz·라임포트3)
  python3 main.py --port /dev/ttyUSB0 --addr 1 --steps 9600 --speed 5000 --in 3 --out 2
  python3 main.py --safe-air      # IN_PORT=12(공기)로 — 액 안 빨고 상태바이트만 관찰

출력을 그대로 복사해 주면 그걸로 폴링/허용오차 로직을 확정한다.
"""

from __future__ import annotations

import argparse
import glob
import sys
import time

try:
    import serial  # pyserial
except ImportError:
    print("ERROR: pyserial 없음 →  pip install pyserial  (또는 데몬 venv 사용)", file=sys.stderr)
    sys.exit(1)

# ── 프로토콜 상수 (adapters/sy01b_engine_adapter.py 와 동일) ──────────────────
FRAME_START = "/"
FRAME_END = "\r"
ETX = 0x03
STATUS_ERROR_MASK = 0x0F  # 하위 4비트 = 에러코드
STATUS_READY_BIT = 0x20   # bit5 = Ready(모터 정지·명령 수락 가능)
BAUD = 9600               # 8N1
RESP_PREFIX = b"/0"       # 마스터 주소 0 응답


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
        # 열자마자 낀 바이트 비움.
        time.sleep(0.2)
        self.ser.reset_input_buffer()

    def txn(self, command: str, read_timeout_s: float = 1.0) -> bytes:
        """`/{addr}{command}\\r` 송신 → ETX(0x03) 까지 수신. 원시 바이트 반환(에코 포함 가능)."""
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
    """원시 응답 → {raw, status_char, status_hex, ready, err, position}. 프레임 없으면 err=None."""
    i = raw.find(RESP_PREFIX)
    out = {"raw": repr(raw), "status_char": None, "status_hex": None,
           "ready": None, "err": None, "position": None}
    if i < 0 or len(raw) < i + 3:
        return out
    status_byte = raw[i + 2]
    data = raw[i + 3:]
    if ETX in data:
        data = data[: data.index(ETX)]
    out["status_char"] = chr(status_byte) if 32 <= status_byte < 127 else f"\\x{status_byte:02x}"
    out["status_hex"] = f"0x{status_byte:02X}"
    out["ready"] = bool(status_byte & STATUS_READY_BIT)
    out["err"] = status_byte & STATUS_ERROR_MASK
    try:
        out["position"] = int(data.decode("ascii").strip()) if data.strip() else None
    except ValueError:
        out["position"] = None
    return out


def show(label: str, d: dict):
    print(f"  [{label:<16}] status={d['status_hex']}({d['status_char']}) "
          f"ready={d['ready']} err={d['err']} pos={d['position']}  raw={d['raw']}")


def poll_after_move(pump: Pump, label: str, move_cmd: str, target: int | None,
                    timeout_s: float, settle_polls: int = 5):
    """이동 명령 송신 → 즉답 로그 → `?` 로 위치/상태를 timeout 까지 추적.
    '위치 정지 + 상태 안정'이 settle_polls 회 연속이면 조기 종료(= 멈춤 감지 관찰)."""
    print(f"\n▶ {label}: send '{move_cmd}'  (target={target}, timeout={timeout_s}s)")
    imm = decode(pump.txn(move_cmd, read_timeout_s=1.0))
    show("즉답", imm)
    t0 = time.time()
    last_pos = None
    stable = 0
    n = 0
    while time.time() - t0 < timeout_s:
        d = decode(pump.txn("?", read_timeout_s=0.5))
        n += 1
        el = time.time() - t0
        # 처음/변화/에러/최근 몇 개만 자세히, 나머지는 압축.
        changed = d["position"] != last_pos
        if changed or d["err"] not in (0, None) or n <= 3:
            print(f"  t={el:5.1f}s  status={d['status_hex']}({d['status_char']}) "
                  f"ready={d['ready']} err={d['err']} pos={d['position']}")
        if d["position"] is not None and d["position"] == last_pos:
            stable += 1
        else:
            stable = 0
        last_pos = d["position"]
        if stable >= settle_polls:
            print(f"  → 위치 {last_pos} 에서 {settle_polls}회 연속 정지 감지 "
                  f"(경과 {el:.1f}s). 최종: ready={d['ready']} err={d['err']}")
            if target is not None and last_pos is not None:
                pct = 100.0 * last_pos / target if target else 0
                print(f"  → 목표 {target} 대비 {last_pos} = {pct:.1f}% 도달")
            return d, last_pos
        time.sleep(0.1)
    d = decode(pump.txn("?", read_timeout_s=0.5))
    print(f"  → TIMEOUT {timeout_s}s. 최종 pos={d['position']} ready={d['ready']} err={d['err']}")
    if target is not None and d["position"] is not None and target:
        print(f"  → 목표 {target} 대비 {d['position']} = {100.0*d['position']/target:.1f}% 도달")
    return d, d["position"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", default=None)
    ap.add_argument("--addr", default="1")
    ap.add_argument("--steps", type=int, default=9600, help="흡입 스텝(0.4mL@0.5mL/12000=9600)")
    ap.add_argument("--speed", type=int, default=5000, help="흡입 top speed Hz(실패 재현=5000)")
    ap.add_argument("--slow", type=int, default=2000, help="비교용 저속 Hz")
    ap.add_argument("--in", dest="in_port", type=int, default=3, help="흡입 밸브 구멍(라임=3)")
    ap.add_argument("--out", dest="out_port", type=int, default=2, help="배출 밸브 구멍(output=2)")
    ap.add_argument("--stall-timeout", type=float, default=25.0)
    ap.add_argument("--safe-air", action="store_true", help="IN_PORT=12(공기)로 — 액 안 빨기")
    ap.add_argument("--skip-slow", action="store_true", help="저속 비교 생략")
    args = ap.parse_args()

    if args.safe_air:
        args.in_port = 12

    port = args.port or find_port()
    if not port:
        print("ERROR: 시리얼 포트를 못 찾음. --port /dev/ttyUSB0 로 지정.", file=sys.stderr)
        sys.exit(1)

    STALL = f"v1000V{args.speed}c{args.speed}L14"
    SLOW = f"v1000V{args.slow}c{args.slow}L14"
    print("=" * 72)
    print(f" SY-01B 스톨/복구 프로브  port={port} addr={args.addr}")
    print(f" 흡입 {args.steps}스텝 @ {args.speed}Hz  in={args.in_port} out={args.out_port}")
    print("=" * 72)

    pump = Pump(port, args.addr)
    try:
        # ── Phase 0: 베이스라인 ──────────────────────────────────────────────
        print("\n### Phase 0 — 베이스라인 상태 (?)")
        for i in range(3):
            show(f"baseline{i}", decode(pump.txn("?")))
            time.sleep(0.2)

        # ── Phase 1: 초기화 (TR → U200,5R → Z1R) ─────────────────────────────
        print("\n### Phase 1 — 초기화 (TR → U200,5R → Z1R · 0.5mL=Half)")
        for cmd in ("TR", "U200,5R", "Z1R"):
            show(f"init:{cmd}", decode(pump.txn(cmd, read_timeout_s=1.0)))
            time.sleep(0.3)
        print("  초기화 완료 대기(홈 탐색)…")
        poll_after_move(pump, "init settle", "?", None, timeout_s=15.0)

        # ── Phase 2: 흡입 스톨 재현 (밸브 회전 → 고속 흡입) ──────────────────
        print(f"\n### Phase 2 — 흡입 스톨 재현: I{args.in_port}R → {STALL}A{args.steps}R")
        show("valve-in", decode(pump.txn(f"I{args.in_port}R", read_timeout_s=2.0)))
        time.sleep(0.5)
        _, stall_pos = poll_after_move(
            pump, "ASPIRATE(fast)", f"{STALL}A{args.steps}R", args.steps,
            timeout_s=args.stall_timeout,
        )

        # ── Phase 3: 스톨 후 상태를 여러 번 관찰 (ready/err 안정값) ──────────
        print("\n### Phase 3 — 스톨 직후 상태 반복 관찰 (?×8) — ready 비트/에러코드 안정값")
        for i in range(8):
            show(f"post-stall{i}", decode(pump.txn("?")))
            time.sleep(0.3)

        # ── Phase 4: 복구 시도 (TR → Z1R) ───────────────────────────────────
        print("\n### Phase 4 — 복구: TR → Z1R (재초기화로 홈 회복되나)")
        show("recover:TR", decode(pump.txn("TR", read_timeout_s=1.0)))
        time.sleep(0.5)
        show("recover:Z1R", decode(pump.txn("Z1R", read_timeout_s=1.0)))
        rec, rec_pos = poll_after_move(pump, "recover settle", "?", None, timeout_s=15.0)

        # ── Phase 5: 배출(절대 홈 A0) ───────────────────────────────────────
        print(f"\n### Phase 5 — 배출: O{args.out_port}R → A0R (절대 홈)")
        show("valve-out", decode(pump.txn(f"O{args.out_port}R", read_timeout_s=2.0)))
        time.sleep(0.5)
        poll_after_move(pump, "DISPENSE(A0)", f"{SLOW}A0R", 0, timeout_s=15.0)

        # ── Phase 6: 저속 흡입 비교 (스톨 안 나는지) ────────────────────────
        if not args.skip_slow:
            print(f"\n### Phase 6 — 저속 비교: I{args.in_port}R → {SLOW}A{args.steps}R "
                  f"({args.slow}Hz — 스톨 안 나는지)")
            show("valve-in2", decode(pump.txn(f"I{args.in_port}R", read_timeout_s=2.0)))
            time.sleep(0.5)
            poll_after_move(pump, "ASPIRATE(slow)", f"{SLOW}A{args.steps}R", args.steps,
                            timeout_s=args.stall_timeout)
            # 마무리 배출.
            show("valve-out2", decode(pump.txn(f"O{args.out_port}R", read_timeout_s=2.0)))
            time.sleep(0.5)
            poll_after_move(pump, "DISPENSE2(A0)", f"{SLOW}A0R", 0, timeout_s=15.0)

        # ── Cleanup: 안전 정지 + 재초기화(홈·공기 포트) ─────────────────────
        print("\n### Cleanup — TR → Z1R → I12R (안전 자세)")
        for cmd in ("TR", "Z1R"):
            show(f"cleanup:{cmd}", decode(pump.txn(cmd, read_timeout_s=1.0)))
            time.sleep(0.3)
        poll_after_move(pump, "cleanup settle", "?", None, timeout_s=15.0)
        show("cleanup:I12R", decode(pump.txn("I12R", read_timeout_s=2.0)))

        print("\n" + "=" * 72)
        print(" 완료. 위 출력 전체를 복사해 주세요.")
        print(f"  핵심: 스톨 위치={stall_pos} (목표 {args.steps}) · 복구후 위치={rec_pos}")
        print("=" * 72)
    finally:
        pump.close()


if __name__ == "__main__":
    main()
