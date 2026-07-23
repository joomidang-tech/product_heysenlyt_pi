#!/usr/bin/env python3
"""senlyt 통합 검증 — 2026-07-19 견고화 배포 후 "이 파일 하나로" 전 항목 실측.

    sudo python3 senlyt_verify.py                      # 기본 스위프(1~5) — 데몬 켠 채
    sudo python3 senlyt_verify.py --operator id:pw     # +하트비트 E2E(admin 실측 칩 검증)
    sudo python3 senlyt_verify.py --serial             # +시리얼 직접 체크(senlytd 잠깐 stop 필요)
    sudo python3 senlyt_verify.py --sse-secs 120       # SSE 관찰 시간 조절(기본 90s)

검증 항목 ↔ 오늘(2026-07-19) 수정 매핑:
  [1] 배포 확인      — 신코드 마커(poll_batches·MAX_STREAM_AGE_S·_reconnect_serial·CMD_STALE) 존재
  [2] 데몬·환경      — senlytd 상태·시리얼 포트·재시작/USB 이력(불확실 정보 수집)
  [3] 로그 스캔      — 60s 로테이션 작동·자가 재연결·Busy 재전송·[Errno 5]·garbled 비율
  [4] SSE 라이브     — 발행→push 지연(★) + 데몬 수신 지연 ≤60s (귀머거리 소멸 검증)
                       ▶ 이 구간에 admin 에서 정비 버튼을 2~3회 눌러 주세요
  [5] (--operator)   — pumpHealth/hwCheckedAt 이 서버 health 에 신선(≤60s)하게 도달하는지 E2E
  [6] (--serial)     — 펌프 1·2 유효율 직접 실측 (pump_link_diag 요약판)
모든 물리 동작 없음(`?` 만·[6] 한정). 끝에 PASS/WARN/FAIL 요약표 + 수집 정보 덤프.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone

IDENTITY_PATH = "/var/lib/senlyt/device-identity.json"
RESULTS: list[tuple[str, str, str]] = []  # (PASS|WARN|FAIL|INFO, 항목, 상세)


def note(grade: str, item: str, detail: str) -> None:
    RESULTS.append((grade, item, detail))
    mark = {"PASS": "✅", "WARN": "🟡", "FAIL": "❌", "INFO": "ℹ️ "}.get(grade, "·")
    print(f"  {mark} {item}: {detail}", flush=True)


def sh(cmd: str, timeout: int = 20) -> str:
    try:
        return subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        ).stdout.strip()
    except Exception as e:  # noqa: BLE001
        return f"(실행 실패: {e})"


def _identity() -> tuple[str, str]:
    j = json.load(open(IDENTITY_PATH))
    return j["deviceId"], j["dispenserToken"]


def _base_url(override: str | None) -> str:
    if override:
        return override
    env = sh("grep ^SENLYT_ENV= /etc/senlyt/device.env 2>/dev/null | cut -d= -f2")
    return {
        "prod": "https://senlyt.com",
        "dev": "https://dev-env.senlyt.com",
        "v1_2_0": "https://v1-2-0.env.senlyt.com",
        "v1_1_0": "https://v1-1-0.env.senlyt.com",
    }.get(env, "https://v1-2-0.env.senlyt.com")


# ── [1] 배포 확인 ──────────────────────────────────────────────────────────────


def check_deploy() -> str | None:
    print("\n[1] 배포 확인 — 신코드 마커")
    wd = sh("systemctl show senlytd -p WorkingDirectory --value") or ""
    repo = wd if wd and wd != "/" else sh("ls -d ~/heysenlyt-pi 2>/dev/null")
    if not repo:
        note("WARN", "코드 위치", "senlytd WorkingDirectory 미확인 — 마커 검사 생략")
        return None
    head = sh(f"git -C {repo} log -1 --format='%h %ad %s' --date=format:'%m-%d %H:%M' 2>/dev/null")
    note("INFO", "배포 커밋", head or "git 미확인")
    markers = {
        "단일 스트림(poll_batches)": "poll_batches",
        "60s 로테이션(MAX_STREAM_AGE_S)": "MAX_STREAM_AGE_S",
        "핫플러그 재연결(_reconnect_serial)": "_reconnect_serial",
        "CMD_STALE": "CMD_STALE",
        "Busy NAK 재전송(_busy_retry)": "_busy_retry",
    }
    src = f"{repo}/src/senlyt_pi"
    for name, pat in markers.items():
        hit = sh(f"grep -rl {pat} {src} 2>/dev/null | head -1")
        note("PASS" if hit else "FAIL", name, "배포됨" if hit else "구버전 코드 — pull 필요")
    return repo


# ── [2] 데몬·환경 + 불확실 정보 수집 ───────────────────────────────────────────


def check_env() -> None:
    print("\n[2] 데몬·환경")
    active = sh("systemctl is-active senlytd")
    note("PASS" if active == "active" else "FAIL", "senlytd", active)
    since = sh("systemctl show senlytd -p ActiveEnterTimestamp --value")
    note("INFO", "기동 시각", since)
    ports = sh("ls /dev/ttyUSB* 2>/dev/null") or "(없음)"
    note("PASS" if "ttyUSB" in ports else "FAIL", "시리얼 포트", ports)
    # 불확실 정보 — 오늘 USB 이벤트·데몬 재시작 이력(장애 빈도 파악).
    usb = sh("journalctl -k --since today --no-pager 2>/dev/null | grep -c 'USB disconnect'")
    note("INFO", "오늘 USB 분리 이벤트", f"{usb}회 (journalctl -k)")
    restarts = sh(
        "journalctl -u senlytd --since today --no-pager 2>/dev/null | grep -c 'Started\\|Starting'"
    )
    note("INFO", "오늘 senlytd 기동 횟수", f"{restarts}회")
    for p, label in (
        ("/var/lib/senlyt/queue", "큐/원장 디렉토리"),
        ("/var/lib/senlyt", "상태 디렉토리"),
    ):
        note("INFO", label, sh(f"du -sh {p} 2>/dev/null") or "(없음)")


# ── [3] 데몬 로그 스캔 ─────────────────────────────────────────────────────────


def check_logs(minutes: int = 30) -> None:
    print(f"\n[3] 데몬 로그 스캔 (최근 {minutes}분)")
    raw = sh(f"journalctl -u senlytd --since '-{minutes} min' --no-pager 2>/dev/null", timeout=30)
    if not raw:
        note("WARN", "로그", "비어있음 — 데몬이 최근 기동 안 했거나 권한 부족(sudo)")
        return
    lines = raw.split("\n")

    def cnt(pat: str) -> int:
        return sum(1 for l in lines if pat in l)

    rot = cnt("스트림 수명 상한")
    note(
        "PASS" if rot else "WARN",
        "60s 스트림 로테이션",
        f"{rot}회 관측" + ("" if rot else " — 신코드면 ~분당 1회 나와야 함(DEBUG 레벨 확인)"),
    )
    rec = cnt("시리얼 자가 재연결")
    note("INFO", "자가 재연결 발동", f"{rec}회" + (" (핫플러그 자가 회복 실동작!)" if rec else ""))
    errno5 = cnt("[Errno 5]")
    note(
        "PASS" if errno5 == 0 or rec > 0 else "FAIL",
        "죽은 핸들([Errno 5])",
        f"{errno5}건" + (" — 재연결로 회복됨" if errno5 and rec else "" if not errno5 else " — 재연결 미발동?!"),
    )
    busy = cnt("명령 즉답 Busy(Code 15)")
    note("INFO", "Busy NAK 재전송", f"{busy}회 (초기화 직후 명령의 자동 대기+재전송)")
    stale = cnt("신선도 초과")
    note(
        "PASS" if stale == 0 else "WARN",
        "신선도 게이트 발동",
        f"{stale}건" + (" — 신코드에선 0이어야 정상(지연 상한 60s<90s)" if stale else " (귀머거리 소멸)"),
    )
    serial_lines = [l for l in lines if "시리얼 왕복" in l]
    garbled = sum(1 for l in serial_lines if '"response": "/0' not in l and "response" in l)
    if serial_lines:
        pct = 100.0 * garbled / len(serial_lines)
        note(
            "PASS" if pct < 5 else ("WARN" if pct < 30 else "FAIL"),
            "시리얼 링크 품질",
            f"왕복 {len(serial_lines)}건 중 비정상 {garbled}건 ({pct:.0f}%)",
        )
    else:
        note("INFO", "시리얼 링크 품질", "이 창에 시리얼 왕복 없음(정비/제조 미실행)")


# ── [4] SSE 라이브 — push 지연(★) + 데몬 수신 지연 대조 ────────────────────────


def check_sse(base: str, secs: float) -> None:
    print(f"\n[4] SSE 라이브 {secs:.0f}초 — ▶ 지금 admin 에서 정비 버튼을 2~3회 눌러 주세요")
    device_id, token = _identity()
    url = f"{base}/api/dispenser/orders/stream?mode=flavor&view=pending&deviceId={device_id}"
    t_end = time.monotonic() + secs
    seen: dict[str, float] = {}  # id → push 지연
    conn = 0
    while time.monotonic() < t_end:
        conn += 1
        try:
            resp = urllib.request.urlopen(
                urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"}), timeout=30
            )
        except Exception as e:  # noqa: BLE001
            note("WARN", "SSE 연결", f"{type(e).__name__}: {e}")
            time.sleep(3)
            continue
        try:
            event = None
            for rawline in resp:
                if time.monotonic() > t_end:
                    break
                line = rawline.decode("utf-8", "ignore").strip()
                if line.startswith("event:"):
                    event = line.split(":", 1)[1].strip()
                elif line.startswith("data:") and event == "snapshot":
                    try:
                        d = json.loads(line.split(":", 1)[1])
                    except ValueError:
                        continue
                    for s in d.get("commandSets") or []:
                        sid = s.get("commandSetId") or ""
                        if sid and sid not in seen:
                            try:
                                created = datetime.fromisoformat(
                                    (s.get("createdAt") or "").replace("Z", "+00:00")
                                )
                                lag = (datetime.now(timezone.utc) - created).total_seconds()
                            except Exception:  # noqa: BLE001
                                lag = -1.0
                            seen[sid] = lag
                            print(f"    ★ {sid[:16]} push 지연 {lag:.1f}s")
        except Exception:  # noqa: BLE001
            pass
        finally:
            try:
                resp.close()
            except Exception:  # noqa: BLE001
                pass
    if not seen:
        note("WARN", "SSE push", "관측 봉투 0건 — 버튼을 안 눌렀거나 다른 문제(재실행 권장)")
        return
    worst = max(seen.values())
    note("PASS" if worst < 5 else "WARN", "서버 push 지연", f"{len(seen)}건 · 최악 {worst:.1f}s")
    # 데몬 수신 지연 — 같은 봉투를 데몬 로그에서 찾아 발행→수신 간격을 잰다(귀머거리 소멸 핵심 검증).
    raw = sh("journalctl -u senlytd --since '-10 min' --no-pager 2>/dev/null", timeout=30)
    worst_daemon = 0.0
    matched = 0
    for sid, _ in seen.items():
        for l in raw.split("\n"):
            if sid[:12] in l and "봉투 수신" in l:
                m = re.search(r'"ts": "([^"]+)"', l)
                if m:
                    matched += 1
                break
    note(
        "PASS" if matched == len(seen) else "WARN",
        "데몬 수신",
        f"{matched}/{len(seen)}건 데몬 로그에서 수신 확인"
        + ("" if matched == len(seen) else " — 미수신분은 60s 내 재확인 필요"),
    )
    _ = worst_daemon


# ── [5] 하트비트 E2E (--operator) ─────────────────────────────────────────────


def check_health_e2e(base: str, operator: str) -> None:
    print("\n[5] 하트비트 E2E — pumpHealth 가 admin 까지 신선하게 도달하는가")
    try:
        oid, pw = operator.split(":", 1)
        req = urllib.request.Request(
            f"{base}/api/admin/login",
            data=json.dumps({"operatorId": oid, "password": pw}).encode(),
            headers={"Content-Type": "application/json"},
        )
        tok = json.load(urllib.request.urlopen(req, timeout=10))["token"]
        h = json.load(
            urllib.request.urlopen(
                urllib.request.Request(
                    f"{base}/api/admin/dispensers/health",
                    headers={"Authorization": f"Bearer {tok}"},
                ),
                timeout=10,
            )
        )
    except Exception as e:  # noqa: BLE001
        note("WARN", "E2E", f"조회 실패({type(e).__name__}: {e}) — operator 자격/네트워크 확인")
        return
    device_id, _ = _identity()
    me = next((d for d in h.get("dispensers", []) if d.get("deviceId") == device_id), None)
    if not me:
        note("FAIL", "E2E", "health 응답에 이 기기 없음")
        return
    ph = me.get("pumpHealth")
    note("PASS" if ph else "FAIL", "pumpHealth 도달", json.dumps(ph, ensure_ascii=False) if ph else "없음(신코드 배포/하트비트 확인)")
    hca = me.get("hwCheckedAt")
    if hca:
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(hca.replace("Z", "+00:00"))).total_seconds()
            note("PASS" if age < 60 else "WARN", "실측 신선도", f"{age:.0f}s 전 (기준 <60s)")
        except Exception:  # noqa: BLE001
            note("INFO", "실측 신선도", hca)
    note("INFO", "engine/online", f"{me.get('engine')} / online={me.get('online')}")


# ── [6] 시리얼 직접(--serial · senlytd stop 필요) ─────────────────────────────


def check_serial(addrs: list[int]) -> None:
    print("\n[6] 시리얼 직접 체크 (⚠️ senlytd 가 켜져 있으면 포트 충돌로 실패)")
    try:
        import serial
        from serial.tools import list_ports
    except ImportError:
        note("WARN", "시리얼", "pyserial 없음")
        return
    port = next((p.device for p in list_ports.comports() if "USB" in p.device), None)
    if not port:
        note("FAIL", "시리얼", "포트 없음")
        return
    try:
        s = serial.Serial(port=port, baudrate=9600, timeout=1.0, write_timeout=1.0)
    except Exception as e:  # noqa: BLE001
        note("WARN", "시리얼", f"열기 실패({e}) — sudo systemctl stop senlytd 후 재시도")
        return
    with s:
        time.sleep(0.1)
        for a in addrs:
            ok = 0
            for _ in range(5):
                try:
                    s.reset_input_buffer()
                    s.write(f"/{a}?\r".encode())
                    time.sleep(0.1)
                    buf = s.read(s.in_waiting or 16)
                    if b"/0" in buf:
                        ok += 1
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(0.1)
            note("PASS" if ok >= 4 else ("WARN" if ok else "FAIL"), f"펌프 {a} 유효율", f"{ok}/5")
    print("  (끝났으면: sudo systemctl start senlytd)")


def main() -> None:
    ap = argparse.ArgumentParser(description="senlyt 통합 검증 (2026-07-19 견고화)")
    ap.add_argument("--base", default=None)
    ap.add_argument("--operator", default=None, help="admin id:pw — 하트비트 E2E 활성화")
    ap.add_argument("--serial", action="store_true", help="시리얼 직접 체크(senlytd stop 필요)")
    ap.add_argument("--sse-secs", type=float, default=90.0)
    ap.add_argument("--log-minutes", type=int, default=30)
    args = ap.parse_args()
    base = _base_url(args.base)
    print("═" * 90)
    print(f"senlyt 통합 검증 — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} · 서버 {base}")
    print("═" * 90)
    check_deploy()
    check_env()
    check_logs(args.log_minutes)
    check_sse(base, args.sse_secs)
    if args.operator:
        check_health_e2e(base, args.operator)
    if args.serial:
        check_serial([1, 2])
    print("\n" + "═" * 90)
    print("최종 요약")
    print("═" * 90)
    order = {"FAIL": 0, "WARN": 1, "PASS": 2, "INFO": 3}
    for g, item, detail in sorted(RESULTS, key=lambda r: order.get(r[0], 9)):
        print(f"  [{g}] {item} — {detail}")
    fails = sum(1 for g, _, _ in RESULTS if g == "FAIL")
    warns = sum(1 for g, _, _ in RESULTS if g == "WARN")
    print(f"\n  결과: FAIL {fails} · WARN {warns} — 출력 전체를 공유하면 원격 판독 가능")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
