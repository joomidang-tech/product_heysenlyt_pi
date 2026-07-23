#!/usr/bin/env bash
# heysenlyt-pi provision — 브랜치=환경 자동 각인 (로컬 방식 · 02_infra §4.9).
#
# git 브랜치에서 SENLYT_ENV 를 파생해 device.env 에 각인한다. 실기(RPi) 프로비저닝 시 실행.
# pi 는 "실배포" 개념이 없어(물리 기기·수동 셋업) 이 로컬 각인이 브랜치→서버 안전의 주(主) 관문이다.
#
# 규칙 SoT = src/senlyt_pi/config/server_target.py::branch_to_env — 아래 case 는 그 미러이고,
# 패키지가 설치돼 있으면 Python SoT 와 교차검증해 bash/python 드리프트를 0 으로 만든다.
#   main → prod · dev → dev · vX.Y.Z → vX_Y_Z · 그 외 → 에러(수동 지정 요구)
#
# 우선순위: device.env 에 이미 SENLYT_ENV 또는 SENLYT_SERVER_BASE_URL(명시 탈출구)이 있으면
#   건드리지 않는다(override 존중). --force 로 덮어쓴다.
#
# 사용:  DEVICE_ENV=/etc/senlyt/device.env bash scripts/provision-env.sh [--force]
set -euo pipefail

DEVICE_ENV="${DEVICE_ENV:-/etc/senlyt/device.env}"
FORCE="${1:-}"

BRANCH="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
if [ -z "$BRANCH" ]; then
	echo "❌ git 브랜치를 읽을 수 없습니다(레포 밖?) — SENLYT_ENV 를 수동 지정하세요." >&2
	exit 1
fi

# ── 규칙(bash 미러) — main→prod · dev→dev · vX.Y.Z→vX_Y_Z · 그 외→거부 ──
case "$BRANCH" in
	main) ENV="prod" ;;
	dev) ENV="dev" ;;
	v[0-9]*.[0-9]*.[0-9]*) ENV="${BRANCH//./_}" ;; # v1.2.0 → v1_2_0
	*)
		echo "❌ '$BRANCH' 는 배포 브랜치(main·dev·vX.Y.Z)가 아닙니다." >&2
		echo "   SENLYT_ENV 또는 SENLYT_SERVER_BASE_URL 을 device.env 에 수동 지정하세요." >&2
		exit 1
		;;
esac

# ── Python SoT 교차검증(설치돼 있을 때만 · 드리프트 방지) ──
if command -v python3 >/dev/null 2>&1; then
	PYENV="$(python3 -c "from senlyt_pi.config.server_target import branch_to_env; print(branch_to_env('$BRANCH') or '')" 2>/dev/null || true)"
	if [ -n "$PYENV" ] && [ "$PYENV" != "$ENV" ]; then
		echo "❌ 규칙 불일치: bash='$ENV' vs python='$PYENV' — server_target.branch_to_env 와 이 스크립트 정합 확인." >&2
		exit 1
	fi
fi

mkdir -p "$(dirname "$DEVICE_ENV")"
touch "$DEVICE_ENV"

# ── 명시 override 존중 ──
if [ "$FORCE" != "--force" ] && grep -qE '^[[:space:]]*(SENLYT_ENV|SENLYT_SERVER_BASE_URL)[[:space:]]*=' "$DEVICE_ENV"; then
	echo "ℹ️ device.env 에 명시 env override(SENLYT_ENV/SENLYT_SERVER_BASE_URL) 존재 — 유지(--force 로 덮어쓰기)."
	exit 0
fi

# ── 각인(멱등: 기존 SENLYT_ENV 줄 제거 후 재기록) ──
tmp="$(mktemp)"
grep -vE '^[[:space:]]*SENLYT_ENV[[:space:]]*=' "$DEVICE_ENV" >"$tmp" || true
echo "SENLYT_ENV=$ENV" >>"$tmp"
mv "$tmp" "$DEVICE_ENV"

echo "✅ SENLYT_ENV=$ENV 각인 → $DEVICE_ENV (브랜치 '$BRANCH')"
