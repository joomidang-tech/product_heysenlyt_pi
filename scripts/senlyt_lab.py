#!/usr/bin/env python3
"""senlyt 실기기 실험 랩 (2026-07-19) — 두 가지 가설을 실기기에서 실증한다.

┌─────────────────────────────────────────────────────────────────────────────┐
│ 실험 1: SSE — `sudo python3 senlyt_lab.py sse`  (데몬 켜둔 채 병행 OK)        │
│   서버 스트림에 직접 붙어 스냅샷 도착을 실측한다. 실행해 두고 admin 에서       │
│   정비 버튼을 눌러 보라. 봉투가 "발행 후 몇 초 만에" 이 스트림에 오는지        │
│   ★ 줄로 찍힌다 — 수 초면 서버 push 는 즉각(= 5분 지연은 데몬 소비 구조 탓)   │
│   이 실기기에서 확정된다. 스트림 수명(서버 ~301s 로테이션)도 측정된다.        │
│   옵션: --rotate 60  → 수정판 데몬과 같은 60s 자가 로테이션으로 동작 검증.     │
│                                                                             │
│ 실험 2: 핫플러그 — `sudo systemctl stop senlytd` 후                          │
│         `python3 senlyt_lab.py hotplug`                                     │
│   1초마다 펌프에 `?` 를 치는 중에 **USB 어댑터를 뽑았다 꽂아** 보라.          │
│   ① 뽑는 순간 pyserial 이 실제로 무슨 예외를 던지는지(수정판 재연결 로직이    │
│      기대는 가정: OSError 계열) ② 재열거→재오픈 자가 회복이 실제로 되는지     │
│   ③ 회복까지 걸린 시간이 전부 찍힌다. 끝나면 Ctrl+C → 요약 출력.             │
│   ⚠️ 끝나면 sudo systemctl start senlytd                                     │
└─────────────────────────────────────────────────────────────────────────────┘
모션 명령 없음(`?` 만) · 서버엔 읽기 스트림 1개 추가될 뿐(데몬과 독립).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

IDENTITY_PATH = "/var/lib/senlyt/device-identity.json"
DEVICE_ENV_PATH = "/etc/senlyt/device.env"
ENV_TO_BASE = {
    "prod": "https://senlyt.com",
    "dev": "https://dev-env.senlyt.com",
    "v1_2_0": "https://v1-2-0.env.senlyt.com",
    "v1_1_0": "https://v1-1-0.env.senlyt.com",
}


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:12]


def _log(msg: str) -> None:
    print(f"[{_now()}] {msg}", flush=True)


# ═══════════════════════════════ 실험 1: SSE ═══════════════════════════════


def _load_identity() -> tuple[str, str]:
    try:
        j = json.load(open(IDENTITY_PATH))
    except PermissionError:
        print(f"✗ {IDENTITY_PATH} 읽기 권한 없음 — sudo 로 실행하세요")
        sys.exit(2)
    except FileNotFoundError:
        print(f"✗ {IDENTITY_PATH} 없음 — 등록된 기기가 아님")
        sys.exit(2)
    return j["deviceId"], j["dispenserToken"]


def _base_url() -> str:
    env = os.environ.get("SENLYT_ENV")
    if not env:
        try:
            for line in open(DEVICE_ENV_PATH):
                if line.startswith("SENLYT_ENV="):
                    env = line.split("=", 1)[1].strip()
        except OSError:
            pass
    base = ENV_TO_BASE.get(env or "", None)
    if not base:
        print(f"✗ SENLYT_ENV 미확정({env}) — --base 로 지정하세요")
        sys.exit(2)
    return base


def _age_s(created_at: str) -> float | None:
    try:
        t = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - t).total_seconds()
    except Exception:
        return None


def run_sse(rotate_s: float | None, base_override: str | None) -> None:
    device_id, token = _load_identity()
    base = base_override or _base_url()
    url = f"{base}/api/dispenser/orders/stream?mode=flavor&view=pending&deviceId={device_id}"
    _log(f"SSE 실측 시작 — {url}")
    _log(f"자가 로테이션: {rotate_s or '없음(서버 수명 따라감)'}s · Ctrl+C 로 종료")
    _log("→ 지금 admin 에서 정비 버튼을 눌러 보세요. ★ 줄 = 봉투가 이 스트림에 도착한 순간")

    seen: set[str] = set()
    conn_n = 0
    while True:
        conn_n += 1
        t_open = time.monotonic()
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        try:
            resp = urllib.request.urlopen(req, timeout=90)
        except KeyboardInterrupt:
            return
        except Exception as e:
            _log(f"연결 실패({type(e).__name__}: {e}) — 3s 후 재연결")
            time.sleep(3)
            continue
        _log(f"── 스트림 #{conn_n} 연결 (HTTP {resp.status})")
        event = None
        hb = 0
        try:
            for raw in resp:
                if rotate_s and (time.monotonic() - t_open) > rotate_s:
                    _log(f"── 자가 로테이션({rotate_s}s) — 재연결 (수정판 데몬 동작 검증)")
                    break
                line = raw.decode("utf-8", "ignore").rstrip("\n").rstrip("\r")
                if line.startswith(":"):
                    hb += 1
                    if hb % 4 == 1:
                        _log(f"  (server heartbeat ×{hb})")
                    continue
                if line.startswith("event:"):
                    event = line.split(":", 1)[1].strip()
                    continue
                if line.startswith("data:") and event == "snapshot":
                    try:
                        d = json.loads(line.split(":", 1)[1])
                    except ValueError:
                        continue
                    sets = d.get("commandSets") or []
                    cmds = d.get("commands") or []
                    brief = ", ".join(
                        f"{(s.get('commandSetId') or '?')[:12]}({s.get('status')})" for s in sets
                    )
                    _log(f"  snapshot — 봉투 {len(sets)}건 [{brief}] · command {len(cmds)}건")
                    for s in sets:
                        sid = s.get("commandSetId") or ""
                        if sid and sid not in seen:
                            seen.add(sid)
                            age = _age_s(s.get("createdAt") or "")
                            _log(
                                f"  ★ 봉투 최초 관측: {sid[:16]} status={s.get('status')} — "
                                f"발행 후 {age:.1f}s" if age is not None else f"  ★ 봉투 최초 관측: {sid[:16]}"
                            )
        except KeyboardInterrupt:
            _log("종료")
            return
        except Exception as e:
            _log(f"스트림 예외({type(e).__name__}: {e})")
        finally:
            dur = time.monotonic() - t_open
            _log(f"── 스트림 #{conn_n} 종료 — 수명 {dur:.1f}s · heartbeat {hb}회")
            try:
                resp.close()
            except Exception:
                pass


# ═══════════════════════════ 실험 2: 핫플러그 ═══════════════════════════════


def _candidates() -> list[str]:
    from serial.tools import list_ports

    out = []
    for p in list_ports.comports():
        d = p.device.lower()
        if any(h in d for h in ("bluetooth", "debug")):
            continue
        out.append(p.device)
    return out


def run_hotplug(addrs: list[int]) -> None:
    import serial

    exceptions_seen: dict[str, int] = {}
    t_lost: float | None = None
    recoveries: list[float] = []

    def open_first() -> tuple[serial.Serial, str] | None:
        for cand in _candidates():
            try:
                s = serial.Serial(port=cand, baudrate=9600, timeout=1.0, write_timeout=1.0)
                time.sleep(0.1)
                return s, cand
            except Exception as e:
                _log(f"  후보 {cand} 열기 실패({type(e).__name__})")
        return None

    got = open_first()
    if got is None:
        print("✗ 열 수 있는 시리얼 포트 없음 — 어댑터 연결/senlytd stop 확인")
        sys.exit(2)
    ser, port = got
    _log(f"핫플러그 실측 시작 — {port} · 1초마다 ? 폴 · **이제 USB 를 뽑았다 꽂아 보세요**")
    _log("Ctrl+C 로 종료(요약 출력)")

    n_ok = 0
    try:
        while True:
            for a in addrs:
                try:
                    ser.reset_input_buffer()
                    ser.write(f"/{a}?\r".encode())
                    time.sleep(0.05)
                    deadline = time.monotonic() + 1.0
                    buf = bytearray()
                    while time.monotonic() < deadline:
                        n = ser.in_waiting
                        if n:
                            buf.extend(ser.read(n))
                            if 0x03 in buf:
                                break
                        time.sleep(0.005)
                    ok = b"/0" in buf
                    n_ok += 1 if ok else 0
                    if t_lost is not None and ok:
                        dt = time.monotonic() - t_lost
                        recoveries.append(dt)
                        _log(f"  ✅ 자가 회복 완료 — 단절 후 {dt:.1f}s (포트 {ser.port})")
                        t_lost = None
                    if not ok:
                        _log(f"  addr={a}: 응답없음/깨짐 [{buf.hex(' ')}]")
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    # ★ 핵심 데이터 — 뽑는 순간 실제로 던져지는 예외의 정체.
                    key = f"{type(e).__name__} (OSError 계열={isinstance(e, OSError)})"
                    exceptions_seen[key] = exceptions_seen.get(key, 0) + 1
                    if t_lost is None:
                        t_lost = time.monotonic()
                        _log(f"  ⚡ 시리얼 예외: {key}: {str(e)[:80]}")
                        _log("  → 자가 재연결 루프 진입(0.5s 간격 재열거)")
                    try:
                        ser.close()
                    except Exception:
                        pass
                    # 재연결 루프 — 수정판 어댑터 _reconnect_serial 미러.
                    while True:
                        time.sleep(0.5)
                        got2 = open_first()
                        if got2 is not None:
                            ser, port = got2
                            _log(f"  ↻ 재오픈 성공 — {port} (응답 확인은 다음 폴에서)")
                            break
            time.sleep(1.0)
    except KeyboardInterrupt:
        pass
    print("\n═══ 요약 ═══")
    print(f"정상 폴 응답: {n_ok}회")
    print(f"관측된 예외: {json.dumps(exceptions_seen, ensure_ascii=False) or '없음'}")
    print(f"자가 회복: {len(recoveries)}회 — 소요 {[f'{r:.1f}s' for r in recoveries]}")
    print("⚠️ 판독: 예외가 전부 'OSError 계열=True' 면 수정판 재연결 로직의 가정이 실기기에서 유효.")
    print("⚠️ 끝났으면: sudo systemctl start senlytd")


def main() -> None:
    ap = argparse.ArgumentParser(description="senlyt 실기기 실험 랩")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p1 = sub.add_parser("sse", help="SSE 스냅샷 도착 실측 (데몬 병행 OK · sudo)")
    p1.add_argument("--rotate", type=float, default=None, help="자가 로테이션 초 (수정판=60)")
    p1.add_argument("--base", default=None, help="서버 base URL 수동 지정")
    p2 = sub.add_parser("hotplug", help="USB 핫플러그 예외/자가회복 실측 (senlytd stop 필수)")
    p2.add_argument("--addrs", default="1", help="폴할 펌프 주소 (기본 1)")
    args = ap.parse_args()
    if args.cmd == "sse":
        run_sse(args.rotate, args.base)
    else:
        run_hotplug([int(a) for a in args.addrs.split(",") if a.strip()])


if __name__ == "__main__":
    main()
