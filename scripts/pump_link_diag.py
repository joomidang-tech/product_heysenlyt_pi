#!/usr/bin/env python3
"""SY-01B 시리얼 링크 진단 — 읽기 전용 절분 툴 (2026-07-19).

pi↔펌프 링크가 "물리(배선·종단·주소)" 문제인지 "설정(DTR/RTS·baud)" 문제인지 절분한다.
상태 조회 `?` 만 보낸다 — **모션 명령 없음**(플런저·밸브 안 움직임). TR 도 기본 미사용.

사용 (pi 에서):
    sudo systemctl stop senlytd          # ⚠️ 필수 — 데몬이 포트를 쥐고 있으면 진단 불가/오염
    python3 scripts/pump_link_diag.py                    # 기본: 자동포트·주소 1,2·전체 시나리오
    python3 scripts/pump_link_diag.py --port /dev/ttyUSB0 --addrs 1,2
    python3 scripts/pump_link_diag.py --quick            # 주소별 통계만(매트릭스·스캔 생략)
    sudo systemctl start senlytd         # 끝나면 데몬 복구

판독 가이드는 마지막에 자동 출력된다. 원시 바이트(hex)가 전부 찍히므로 그 출력을 그대로
공유하면 원격 판독이 가능하다.
"""

from __future__ import annotations

import argparse
import sys
import time

try:
    import serial  # pyserial
    from serial.tools import list_ports
except ImportError:
    print("pyserial 미설치 — pip install pyserial 후 재실행")
    sys.exit(1)

ETX = 0x03
DEFAULT_BAUD = 9600
# 시도당 수신 상한 — 유효 응답은 보통 수십 ms 안에 온다. 1.5s = v1.1.0 probe와 동일.
READ_WINDOW_S = 1.5
KNOWN_ADAPTERS = {(0x1A86, 0x7523): "CH340", (0x1A86, 0x5523): "CH341", (0x0403, 0x6001): "FT232R"}


def printable(raw: bytes) -> str:
    return "".join(chr(b) if 32 <= b < 127 else f"\\x{b:02x}" for b in raw) or "(무응답)"


def hexdump(raw: bytes) -> str:
    return " ".join(f"{b:02x}" for b in raw) or "-"


def autodetect_port() -> str | None:
    cands = []
    for p in list_ports.comports():
        name = KNOWN_ADAPTERS.get((p.vid or 0, p.pid or 0))
        cands.append((0 if name else 1, p.device, name or "?"))
    cands.sort()
    if not cands:
        return None
    for prio, dev, name in cands:
        print(f"  포트 후보: {dev} ({name})")
    return cands[0][1]


def open_port(port: str, baud: int, dtr: bool, rts: bool) -> serial.Serial:
    """open 전에 dtr/rts 를 지정해 open 순간의 라인 상태까지 통제한다."""
    s = serial.Serial()
    s.port = port
    s.baudrate = baud
    s.bytesize = serial.EIGHTBITS
    s.parity = serial.PARITY_NONE
    s.stopbits = serial.STOPBITS_ONE
    s.timeout = 0
    s.write_timeout = 1.0
    s.dtr = dtr
    s.rts = rts
    s.open()
    time.sleep(0.1)  # 어댑터/라인 안정화 (v1.1.0 connect 후 100ms 동일)
    return s


def txn(s: serial.Serial, addr: int | str, cmd: str = "?", window_s: float = READ_WINDOW_S):
    """`/{addr}{cmd}\\r` 송신 → window 동안 수신. 반환 (raw, 첫바이트지연s, ETX지연s)."""
    try:
        s.reset_input_buffer()
    except Exception:
        pass
    frame = f"/{addr}{cmd}\r".encode("ascii")
    t0 = time.monotonic()
    s.write(frame)
    buf = bytearray()
    first = etx = None
    deadline = t0 + window_s
    while time.monotonic() < deadline:
        n = s.in_waiting
        if n:
            chunk = s.read(n)
            if chunk and first is None:
                first = time.monotonic() - t0
            buf.extend(chunk)
            if ETX in chunk:
                etx = time.monotonic() - t0
                time.sleep(0.02)  # 꼬리 바이트 회수
                tail = s.read(s.in_waiting or 0)
                if tail:
                    buf.extend(tail)
                break
        time.sleep(0.005)
    return bytes(buf), first, etx


def classify(raw: bytes, sent_cmd: str, addr: int | str) -> tuple[str, str]:
    """(등급, 상세). 등급: VALID / GARBLED / SILENT / ECHO_ONLY."""
    if not raw:
        return "SILENT", ""
    echo = f"/{addr}{sent_cmd}".encode("ascii") in raw
    i = raw.find(b"/0")
    if i != -1 and len(raw) > i + 2:
        status = raw[i + 2]
        err = status & 0x0F
        ready = bool(status & 0x20)
        detail = f"status=0x{status:02X} err={err} ready={ready}" + (" (+에코)" if echo else "")
        return "VALID", detail
    if echo and len(raw) <= len(sent_cmd) + 4:
        return "ECHO_ONLY", "자기 송신 에코만 수신(펌프 응답 없음)"
    return "GARBLED", "유효 /0 프레임 없음"


def probe_stats(s: serial.Serial, addr: int, n: int, label: str, verbose: bool = True) -> dict:
    stats = {"VALID": 0, "GARBLED": 0, "SILENT": 0, "ECHO_ONLY": 0}
    for i in range(n):
        raw, first, etx = txn(s, addr)
        grade, detail = classify(raw, "?", addr)
        stats[grade] += 1
        if verbose:
            t_first = f"{first * 1000:.0f}ms" if first is not None else "-"
            t_etx = f"{etx * 1000:.0f}ms" if etx is not None else "ETX없음"
            print(
                f"    [{label}] addr={addr} #{i + 1}: {grade:9s} raw=[{printable(raw)}] "
                f"hex=[{hexdump(raw)}] 첫바이트={t_first} {t_etx} {detail}"
            )
        time.sleep(0.1)
    return stats


def fmt_stats(st: dict, n: int) -> str:
    return f"유효 {st['VALID']}/{n} · 쓰레기 {st['GARBLED']} · 무응답 {st['SILENT']} · 에코만 {st['ECHO_ONLY']}"


def main() -> None:
    ap = argparse.ArgumentParser(description="SY-01B 시리얼 링크 진단 (읽기 전용)")
    ap.add_argument("--port", help="시리얼 포트 (미지정 시 자동 감지)")
    ap.add_argument("--addrs", default="1,2", help="점검할 펌프 주소 (기본 1,2)")
    ap.add_argument("-n", type=int, default=10, help="주소별 ? 반복 횟수 (기본 10)")
    ap.add_argument("--bauds", default="9600,19200,38400", help="baud 스윕 목록")
    ap.add_argument("--quick", action="store_true", help="주소별 통계만 (매트릭스·스캔·스윕 생략)")
    ap.add_argument("--tr", action="store_true", help="각 주소에 TR(상태 리셋) 1발 선행 — latched 에러 배제용")
    args = ap.parse_args()

    addrs = [int(a) for a in args.addrs.split(",") if a.strip()]
    bauds = [int(b) for b in args.bauds.split(",") if b.strip()]

    print("═" * 100)
    print("SY-01B 시리얼 링크 진단 — 읽기 전용 (모션 명령 없음)")
    print("═" * 100)

    print("\n[0] 포트 열거")
    port = args.port or autodetect_port()
    if not port:
        print("  ✗ 시리얼 포트가 하나도 없음 — USB 어댑터 미인식(케이블/전원/드라이버 확인: dmesg | tail)")
        sys.exit(2)
    print(f"  사용 포트: {port}")

    # ── [1] 기본 상태 (프로덕션 동일 조건: pyserial 기본 = dtr/rts assert) ──────
    print(f"\n[1] 기본 진단 — {DEFAULT_BAUD} 8N1, DTR=1 RTS=1, 주소별 ? ×{args.n}")
    try:
        s = open_port(port, DEFAULT_BAUD, dtr=True, rts=True)
    except serial.SerialException as e:
        msg = str(e)
        print(f"  ✗ 포트 열기 실패: {msg}")
        if "busy" in msg.lower() or "denied" in msg.lower():
            print("  → senlytd 가 포트를 쥐고 있을 가능성: sudo systemctl stop senlytd 후 재실행")
        sys.exit(2)

    base: dict[int, dict] = {}
    with s:
        if args.tr:
            for a in addrs:
                raw, _, _ = txn(s, a, "TR")
                print(f"    TR → addr={a}: [{printable(raw)}]")
                time.sleep(0.3)
        for a in addrs:
            base[a] = probe_stats(s, a, args.n, "기본")
            print(f"  ▶ addr={a}: {fmt_stats(base[a], args.n)}")

    if not args.quick:
        # ── [2] DTR/RTS 매트릭스 — 어댑터 방향제어/전원 의존 절분 ────────────────
        print("\n[2] DTR/RTS 4조합 매트릭스 — 주소별 ? ×3")
        matrix: dict[tuple[bool, bool], dict[int, dict]] = {}
        for dtr in (True, False):
            for rts in (True, False):
                combo: dict[int, dict] = {}
                try:
                    with open_port(port, DEFAULT_BAUD, dtr=dtr, rts=rts) as s2:
                        for a in addrs:
                            combo[a] = probe_stats(s2, a, 3, f"DTR={int(dtr)},RTS={int(rts)}", verbose=False)
                except serial.SerialException as e:
                    print(f"  DTR={int(dtr)} RTS={int(rts)}: 열기 실패 {e}")
                    continue
                matrix[(dtr, rts)] = combo
                line = " · ".join(f"addr{a} 유효 {combo[a]['VALID']}/3" for a in addrs)
                print(f"  DTR={int(dtr)} RTS={int(rts)}: {line}")

        # ── [3] baud 스윕 — baud 불일치 절분 ─────────────────────────────────────
        print(f"\n[3] baud 스윕 {bauds} — 주소별 ? ×3")
        baud_hit: dict[int, list[int]] = {a: [] for a in addrs}
        for baud in bauds:
            try:
                with open_port(port, baud, dtr=True, rts=True) as s3:
                    for a in addrs:
                        st = probe_stats(s3, a, 3, f"baud={baud}", verbose=False)
                        if st["VALID"]:
                            baud_hit[a].append(baud)
                        print(f"  baud={baud} addr={a}: {fmt_stats(st, 3)}")
            except serial.SerialException as e:
                print(f"  baud={baud}: 열기 실패 {e}")

        # ── [4] 주소 스캔 — 로터리 스위치 오설정 절분 ────────────────────────────
        # ⚠️ 스캔 상한 = 9. `/{addr}` 문자열 인코딩상 두 자리 주소(10↑)는 `/1` + "0…"으로
        #   오독돼 pump1 이 대답한다(유령 응답 — 2026-07-19 실측: addr=10 "유효"가 실은 pump1).
        #   스위치 10 이상 주소는 별도 문자(':' 등)라 이 툴 범위 밖.
        print("\n[4] 주소 스캔 0~9 — 어느 주소가 응답하나 (? ×2)")
        found: list[tuple[int, str]] = []
        with open_port(port, DEFAULT_BAUD, dtr=True, rts=True) as s4:
            for a in range(0, 10):
                st = probe_stats(s4, a, 2, "스캔", verbose=False)
                if st["VALID"] or st["GARBLED"]:
                    kind = "유효" if st["VALID"] else "쓰레기응답"
                    found.append((a, kind))
                    print(f"  addr={a}: 응답 있음 ({kind})")
        if not found:
            print("  (응답하는 주소 없음)")

    # ── 판독 가이드 ──────────────────────────────────────────────────────────────
    print("\n" + "═" * 100)
    print("판독 가이드")
    print("═" * 100)
    print(
        """
  A. [1]에서 유효율 높음(예 8/10↑)      → 링크 정상. 문제는 senlytd 쪽(설정/타이밍) — 이 출력 그대로 공유.
  B. [1] 쓰레기인데 [2] 특정 DTR/RTS 조합만 유효 → 어댑터 방향제어 의존 확정 → _pyserial_factory 에 그 조합 명시.
  C. [1] 쓰레기인데 [3] 다른 baud 에서 유효   → baud 불일치 확정(펌프 통신설정이 바뀜) → 펌프/코드 baud 정렬.
  D. [4]에서 예상 밖 주소가 응답           → 로터리 주소 스위치 오설정 확정(예: pump2 가 2가 아님).
  E. 전 시나리오 쓰레기/무응답            → pi 쪽 설정 아님 → 물리(종단 120Ω·배선·간섭·전원) 또는
                                            노트북(v1.1.0 기기설정)으로 같은 리그를 찔러 pi측/버스측 최종 절분.
  F. ECHO_ONLY 다수                       → 어댑터가 자기 송신만 되돌림 = 펌프 쪽에서 응답 자체가 안 옴(결선/주소/전원).

  ⚠️ 끝나면: sudo systemctl start senlytd
"""
    )


if __name__ == "__main__":
    main()
