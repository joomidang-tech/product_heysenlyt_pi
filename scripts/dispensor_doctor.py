#!/usr/bin/env python3
"""senlyt 기능 테스트 CLI — 실제 명령을 쏴서 "잘 되는지·에러가 제대로 나는지·무슨 코드가 오는지" 확인.

  ⚠️ 이 프로그램은 **펌프를 실제로 움직입니다** (플런저 흡입/배출·홈 복귀).
     시린지·튜브 상태를 확인하고 실행하세요. 각 동작 전에 확인을 묻습니다(--yes 로 생략).

  어디서 실행? — 네트워크만 되면 어디서든(라즈베리파이·Mac 무관). admin 서버 API 로 발행하고
  전이(queued→delivered→running→done|failed)를 실시간 추적한다. senlyt_verify.py(수동 점검·
  로그 스캔)와 짝 — 이쪽은 **능동 시나리오 실행기**다.

사용:
  python3 dispensor_doctor.py --operator 아이디:비번            # 메뉴 모드
  python3 dispensor_doctor.py --operator 아이디:비번 run        # 전체 시나리오 자동(각 동작 확인 물음)
  python3 dispensor_doctor.py --operator 아이디:비번 run --yes  # 확인 없이 전부
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime

# ── 에러코드 해설 (00_research/2026-07-19-serial-link-failure-modes.md §2·§4 미러) ──
ERROR_GUIDE = {
    None: "정상 완료",
    "CMD_STALE": "시효 만료(90s) — 전달 지연이 컸다는 신호. 신코드(60s 로테이션)에선 안 나와야 정상 → 나오면 SSE/데몬 확인",
    "ESTOP_CANCELED": "긴급정지로 취소됨 — estop 큐 정리의 정상 동작",
    "CMD_VALIDATION_FAILED": "명령 형식/검증 실패 — 구버전 pi 면 '시효만료 위장'일 수 있음(신코드는 CMD_STALE 분리)",
    "ENGINE_ERROR_PERMANENT": "엔진 영구 오류 — 상세는 pi journalctl 의 engineCode 확인(15=Busy 오판[구코드]·-1000=무응답/[Errno 5]=죽은핸들)",
    "ENGINE_TIMEOUT": "엔진 타임아웃(재시도 소진) — 링크 품질/무응답 의심(pump_link_diag 로 절분)",
    "ENGINE_ERROR_TRANSIENT": "일시 오류(재시도 소진)",
    "INTERRUPTED": "중단됨 — estop/재기동/큐 정지",
    "DUPLICATE_DROPPED": "중복 재전달 드롭(멱등 정상 동작)",
}


def now() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:12]


class Api:
    def __init__(self, base: str, operator: str):
        self.base = base
        oid, pw = operator.split(":", 1)
        self.token = self._post("/api/admin/login", {"operatorId": oid, "password": pw})["token"]

    def _req(self, method: str, path: str, body: dict | None = None) -> dict:
        req = urllib.request.Request(
            self.base + path,
            method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={
                "Content-Type": "application/json",
                **({"Authorization": f"Bearer {self.token}"} if hasattr(self, "token") else {}),
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"HTTP {e.code} {path}: {e.read().decode()[:200]}") from e

    def _post(self, path: str, body: dict) -> dict:
        return self._req("POST", path, body)

    def get(self, path: str) -> dict:
        return self._req("GET", path)

    # ── 도메인 호출 ──
    def pick_device(self, want: str | None) -> dict:
        ds = self.get("/api/admin/dispensers/health").get("dispensers", [])
        if want:
            d = next((x for x in ds if x["deviceId"] == want), None)
            if not d:
                sys.exit(f"✗ 기기 {want} 없음. 목록: {[x['deviceId'] for x in ds]}")
            return d
        real = [x for x in ds if not x["deviceId"].startswith("test")]
        if len(real) == 1:
            return real[0]
        sys.exit(f"✗ 기기 자동선택 불가 — --device 로 지정. 목록: {[x['deviceId'] for x in ds]}")

    def estop(self, device_id: str, active: bool) -> dict:
        return self._post(f"/api/admin/dispensers/{device_id}/estop", {"active": active})

    def issue(self, device_id: str, steps: list[dict], note: str) -> str:
        r = self._post(
            f"/api/admin/dispensers/{device_id}/commands",
            {"kind": "maintenance", "steps": steps, "note": note},
        )
        return r["commandSetId"]

    def status(self, device_id: str, cs_id: str) -> dict:
        return self.get(f"/api/admin/dispensers/{device_id}/commands/{cs_id}")


def engine_op(pump: int, op: str, idx: int = 0, stage: int = 0) -> dict:
    return {"kind": "engineOp", "idx": idx, "stage": stage, "pumpAddr": pump, "op": op}


def init_steps(pumps: list[int]) -> list[dict]:
    # 약한 초기화 = 전 펌프 같은 stage(0) → pi 브로드캐스트 동시 홈 (admin forceInitAll 동형).
    return [engine_op(p, "initialize", idx=i, stage=0) for i, p in enumerate(pumps)]


class Runner:
    def __init__(self, api: Api, device_id: str, yes: bool):
        self.api = api
        self.device_id = device_id
        self.yes = yes
        self.results: list[tuple[str, str, str, float]] = []  # (시나리오, 판정, 상세, 소요)

    def confirm(self, what: str) -> bool:
        if self.yes:
            return True
        try:
            return input(f"  ⚠️ {what} — 펌프가 움직입니다. 진행? [Enter=예 / n=건너뜀] ").strip().lower() != "n"
        except EOFError:
            return False

    def track(self, cs_id: str, timeout_s: float = 90.0) -> tuple[str, str | None, float]:
        """전이를 실시간 출력하며 terminal 까지 추적 — (최종 status, errorCode, 소요 s)."""
        t0 = time.monotonic()
        last = None
        while time.monotonic() - t0 < timeout_s:
            try:
                s = self.api.status(self.device_id, cs_id)
            except RuntimeError as e:
                print(f"    [{now()}] 상태조회 오류: {e}")
                time.sleep(1)
                continue
            st = s.get("status")
            if st != last:
                extra = f" errorCode={s.get('errorCode')}" if s.get("errorCode") else ""
                print(f"    [{now()}] {last or '발행'} → {st}{extra}  (+{time.monotonic()-t0:.1f}s)")
                last = st
            if st in ("done", "failed"):
                return st, s.get("errorCode"), time.monotonic() - t0
            time.sleep(0.5)
        return "timeout", None, time.monotonic() - t0

    def scenario(
        self,
        name: str,
        steps: list[dict],
        note: str,
        expect: str = "done",
        expect_code: str | None = None,
        timeout_s: float = 90.0,
        confirm: bool = True,
    ) -> tuple[str, str | None]:
        print(f"\n▶ {name}")
        if confirm and not self.confirm(name):
            self.results.append((name, "SKIP", "사용자 건너뜀", 0.0))
            return "skip", None
        cs = self.api.issue(self.device_id, steps, note)
        print(f"    발행됨: {cs[:20]}…")
        st, code, dur = self.track(cs, timeout_s)
        ok = st == expect and (expect_code is None or code == expect_code)
        guide = ERROR_GUIDE.get(code, code or "")
        verdict = "PASS" if ok else "FAIL"
        print(f"    ⇒ {verdict} — {st}" + (f" [{code}] {guide}" if code else "") + f" ({dur:.1f}s)")
        if not ok:
            print(f"       기대: {expect}" + (f" [{expect_code}]" if expect_code else ""))
        self.results.append((name, verdict, f"{st}{f' [{code}]' if code else ''}", dur))
        return st, code

    # ── 시나리오들 ─────────────────────────────────────────────────────────

    def s_init(self):
        self.api.estop(self.device_id, False)  # admin forceInitAll 동형: estop 해제 후 발사.
        return self.scenario("약한 초기화(전 펌프)", init_steps([1, 2]), "약한 초기화(CLI 테스트)")

    def s_pump(self, pump: int, op: str):
        label = f"{pump}펌프 {'전량 흡입' if op=='plungerFull' else '전량 배출'}"
        return self.scenario(label, [engine_op(pump, op)], f"{label}(CLI 테스트)")

    def s_busy_chain(self):
        """초기화 직후 즉시 흡입 — 신코드 기대: 자동 대기+재전송으로 성공(구코드=engineCode 15 즉사)."""
        print("\n▶ [에러경로] 초기화 직후 연타 — Busy(Code 15) 자동 처리 검증")
        if not self.confirm("초기화+즉시 흡입 연타"):
            self.results.append(("Busy 연타 검증", "SKIP", "사용자 건너뜀", 0.0))
            return
        self.api.estop(self.device_id, False)
        cs1 = self.api.issue(self.device_id, init_steps([1, 2]), "초기화(연타 검증 1/2)")
        print(f"    초기화 발행: {cs1[:16]}… — done 표시 즉시 흡입을 쏜다(홈 모션은 계속 중)")
        st1, _, _ = self.track(cs1, 30)
        if st1 != "done":
            self.results.append(("Busy 연타 검증", "FAIL", f"초기화가 {st1}", 0.0))
            return
        # fire-and-forget done 직후 = 실제 홈 모션 진행 중일 확률 최대 — 그 순간 흡입.
        self.scenario(
            "Busy 연타: done 직후 1펌프 흡입",
            [engine_op(1, "plungerFull")],
            "흡입(연타 검증 2/2)",
            timeout_s=60,
            confirm=False,
        )

    def s_estop_cancel(self):
        """estop 중 신규 발행 → 서버 409 거부(estop_active) → 해제+복구.

        2026-07-19 게이트 확정: estop = 전 펌프 TR + 초기화 캐시 무효화라 성공 가능한 명령이
        없다 → 서버가 발행 시점에 409 로 거부한다(기기 왕복·INTERRUPTED 노이즈 제거).
        (estop SET 시점에 이미 대기 중이던 봉투는 종전대로 ESTOP_CANCELED 로 서버가 정리.)
        """
        print("\n▶ [에러경로] 긴급정지 발행 게이트 — estop 중 신규 발행이 409 로 거부되는가")
        if not self.confirm("estop 후 명령 발행(409 거부돼야 정상)"):
            self.results.append(("estop 발행 게이트", "SKIP", "사용자 건너뜀", 0.0))
            return
        self.api.estop(self.device_id, True)
        print(f"    [{now()}] estop SET")
        time.sleep(1.0)
        t0 = time.monotonic()
        try:
            cs = self.api.issue(self.device_id, [engine_op(1, "plungerFull")], "estop 중 발행(409 기대)")
            # 구 서버(게이트 이전)면 발행이 통과 — 봉투 추적으로 폴백(하위호환 판독).
            print("    (구 서버 폴백 — 발행이 통과됨: 봉투 종단 추적)")
            self.scenario_from_issued("estop 중 발행 → 거부 기대", cs, expect="failed", expect_code="ESTOP_CANCELED", timeout_s=30)
        except urllib.error.HTTPError as e:
            dur = time.monotonic() - t0
            body = e.read().decode(errors="replace")
            ok = e.code == 409 and "estop_active" in body
            verdict = "PASS" if ok else "FAIL"
            print(f"    ⇒ {verdict} — HTTP {e.code} {body[:80]} ({dur:.1f}s)  (기대 409 estop_active)")
            self.results.append(("estop 발행 게이트", verdict, f"HTTP {e.code} estop_active", dur))
        self.api.estop(self.device_id, False)
        print(f"    [{now()}] estop CLEAR — 복구 초기화 발행")
        self.scenario("estop 해제 후 복구 초기화", init_steps([1, 2]), "복구 초기화(CLI)", confirm=False)

    def scenario_from_issued(self, name, cs_id, expect, expect_code, timeout_s):
        st, code, dur = self.track(cs_id, timeout_s)
        ok = st == expect and code == expect_code
        verdict = "PASS" if ok else "FAIL"
        guide = ERROR_GUIDE.get(code, code or "")
        print(f"    ⇒ {verdict} — {st} [{code}] {guide} ({dur:.1f}s)  (기대 {expect} [{expect_code}])")
        self.results.append((name, verdict, f"{st} [{code}]", dur))

    def summary(self):
        print("\n" + "═" * 78)
        print("결과 요약")
        print("═" * 78)
        for name, v, detail, dur in self.results:
            mark = {"PASS": "✅", "FAIL": "❌", "SKIP": "➖"}.get(v, "·")
            print(f"  {mark} {name:<34} {detail:<28} {dur:5.1f}s")
        fails = sum(1 for _, v, _, _ in self.results if v == "FAIL")
        print(f"\n  FAIL {fails}건 — 실패가 있으면 errorCode 해설과 pi journalctl 로 절분하세요.")


FULL_SUITE = "초기화 → 1흡입 → 1배출 → 2흡입 → 2배출 → Busy연타 → estop큐정리"


def main() -> None:
    ap = argparse.ArgumentParser(description="senlyt 기능 테스트 CLI (펌프 실동작)")
    ap.add_argument("cmd", nargs="?", default="menu", choices=["menu", "run"], help="run=전체 시나리오")
    ap.add_argument("--operator", required=True, help="admin 아이디:비밀번호")
    ap.add_argument("--base", default="https://v1-2-0.env.senlyt.com")
    ap.add_argument("--device", default=None)
    ap.add_argument("--yes", action="store_true", help="동작 확인 생략(전부 예)")
    args = ap.parse_args()

    api = Api(args.base, args.operator)
    dev = api.pick_device(args.device)
    print(f"기기: {dev['deviceId']} · online={dev.get('online')} · engine={dev.get('engine')}")
    if dev.get("engine") == "fake":
        print("⚠️ fake 엔진(실펌프 미연결) — 동작은 모의로만 수행됨")
    r = Runner(api, dev["deviceId"], args.yes)

    if args.cmd == "run":
        print(f"전체 시나리오: {FULL_SUITE}")
        r.s_init()
        r.s_pump(1, "plungerFull")
        r.s_pump(1, "plungerHome")
        r.s_pump(2, "plungerFull")
        r.s_pump(2, "plungerHome")
        r.s_busy_chain()
        r.s_estop_cancel()
        r.summary()
        return

    MENU = """
  1) 약한 초기화(전 펌프)      2) 1펌프 흡입   3) 1펌프 배출
  4) 2펌프 흡입               5) 2펌프 배출
  6) [에러경로] Busy 연타 검증(초기화 직후 흡입 — 신코드면 성공해야 함)
  7) [에러경로] estop 큐 정리 검증(ESTOP_CANCELED 기대)
  8) 전체(1~7)                9) 결과 요약     0) 종료
"""
    while True:
        print(MENU)
        try:
            c = input("선택> ").strip()
        except EOFError:
            break
        if c == "0":
            break
        elif c == "1":
            r.s_init()
        elif c == "2":
            r.s_pump(1, "plungerFull")
        elif c == "3":
            r.s_pump(1, "plungerHome")
        elif c == "4":
            r.s_pump(2, "plungerFull")
        elif c == "5":
            r.s_pump(2, "plungerHome")
        elif c == "6":
            r.s_busy_chain()
        elif c == "7":
            r.s_estop_cancel()
        elif c == "8":
            for f in (r.s_init, lambda: r.s_pump(1, "plungerFull"), lambda: r.s_pump(1, "plungerHome"),
                      lambda: r.s_pump(2, "plungerFull"), lambda: r.s_pump(2, "plungerHome"),
                      r.s_busy_chain, r.s_estop_cancel):
                f()
        elif c == "9":
            r.summary()
    r.summary()


if __name__ == "__main__":
    main()
