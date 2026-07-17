#!/usr/bin/env bash
# hey senlyt pi daemon — 라즈베리파이 1줄 설치·구동 (다운로드부터 자동).
#
# 사용 (Pi에서 한 줄):
#   curl -fsSL https://raw.githubusercontent.com/joomidang-tech/product_heysenlyt_pi/v1.2.0/install.sh \
#     | sudo bash -s -- https://v1-2-0.env.senlyt.com
#
# 사람이 넣는 건 **서버 URL 하나**뿐. 나머지는 켜진 뒤 자동:
#   - deviceId  = HW 시리얼 자동수집(RPi4=cpuinfo·RPi5=device-tree)
#   - mode      = admin에서 승인할 때 배정 → 서버가 기기에 내려줌
#   - engine/valve = 부팅 자동감지(실 Pi+시리얼 어댑터→sy01b·GPIO→gpio·아니면 fake)
# 등록은 키 없이 신청(TOFU) → admin에서 "승인"해야 online.  (재실행 안전·멱등)
set -euo pipefail

SERVER_URL="${1:-}"
REPO="https://github.com/joomidang-tech/product_heysenlyt_pi.git"
BRANCH="v1.2.0"
APP_DIR="/opt/senlyt/heysenlyt-pi"
ENV_DIR="/etc/senlyt"
ENV_FILE="$ENV_DIR/device.env"
LOG_DIR="/var/log/senlyt"
STATE_DIR="/var/lib/senlyt"
SERVICE="/etc/systemd/system/senlytd.service"

# ── 0. 인자·권한 체크 ──────────────────────────────────────────────────────
if [ -z "$SERVER_URL" ]; then
	echo "❌ 서버 URL이 필요합니다." >&2
	echo "   예: curl -fsSL .../install.sh | sudo bash -s -- https://v1-2-0.env.senlyt.com" >&2
	exit 1
fi
case "$SERVER_URL" in
	http://*|https://*) : ;;
	*) echo "❌ 서버 URL은 http(s):// 로 시작해야 합니다 (받은 값: $SERVER_URL)" >&2; exit 1 ;;
esac
if [ "$(id -u)" -ne 0 ]; then
	echo "❌ root 권한이 필요합니다(systemd·GPIO·시리얼). sudo 로 실행하세요." >&2
	exit 1
fi

echo "▶ hey senlyt pi 설치 — server=$SERVER_URL  (branch=$BRANCH)"

# ── 1. 시스템 의존성 (Raspberry Pi OS Bookworm 기준 · Python 3.11+) ─────────
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq git python3 python3-venv python3-pip

# ── 2. 코드 다운로드(clone) 또는 갱신(pull) — 멱등 ─────────────────────────
mkdir -p "$(dirname "$APP_DIR")"
if [ -d "$APP_DIR/.git" ]; then
	echo "  ↻ 기존 설치 갱신(pull)"
	git -C "$APP_DIR" fetch -q --depth 1 origin "$BRANCH"
	git -C "$APP_DIR" checkout -q "$BRANCH" 2>/dev/null || git -C "$APP_DIR" checkout -q -B "$BRANCH" "origin/$BRANCH"
	git -C "$APP_DIR" reset -q --hard "origin/$BRANCH"
else
	echo "  ⬇ 다운로드(clone)"
	git clone -q --depth 1 --branch "$BRANCH" "$REPO" "$APP_DIR"
fi

# ── 3. venv + 설치 (+ 실기기 하드웨어 라이브러리) ──────────────────────────
#   런타임 의존성 0(stdlib) — 데몬 자체는 venv만으로 동작. gpiozero/pyserial 은 실 밸브·
#   펌프 자동감지에만 필요(없으면 자동으로 fake 로 안전 폴백 — 등록·모니터링엔 무영향).
python3 -m venv "$APP_DIR/.venv"
"$APP_DIR/.venv/bin/pip" install -q --upgrade pip
"$APP_DIR/.venv/bin/pip" install -q -e "$APP_DIR"
"$APP_DIR/.venv/bin/pip" install -q gpiozero lgpio pyserial \
	|| echo "  ⚠️ gpiozero/pyserial 설치 실패 — 밸브·실토출은 fake 로 동작(등록·모니터링엔 무영향)"

# ── 4. 환경파일 — 넣는 값은 서버 URL 하나뿐 ────────────────────────────────
mkdir -p "$ENV_DIR" "$LOG_DIR" "$STATE_DIR/queue"
umask 077
cat > "$ENV_FILE" <<EOF
# hey senlyt pi — 설치가 각인한 값. 넣는 건 서버 URL 하나(나머지는 런타임 자동).
#   deviceId=HW시리얼 자동 · mode=admin 승인 시 배정 · engine/valve=부팅 자동감지
SENLYT_SERVER_BASE_URL=$SERVER_URL
SENLYT_RUN=1
LOG_DIR=$LOG_DIR
SENLYT_LEDGER_PATH=$STATE_DIR/queue/idempotency-ledger.log
SENLYT_IDENTITY_PATH=$STATE_DIR/device-identity.json
EOF

# ── 5. systemd 유닛 — 부팅 자동시작 + 무인 복구(Restart=always) ────────────
cat > "$SERVICE" <<EOF
[Unit]
Description=hey senlyt pi daemon (senlytd)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=$ENV_FILE
ExecStart=$APP_DIR/.venv/bin/senlytd
Restart=always
RestartSec=5
# GPIO/시리얼 접근을 위해 root 실행(단일 목적 기기). 로그는 journald.

[Install]
WantedBy=multi-user.target
EOF

# ── 6. 기동 ────────────────────────────────────────────────────────────────
systemctl daemon-reload
systemctl enable -q --now senlytd

ADMIN_URL="${SERVER_URL%/}/admin"
echo ""
echo "✅ 설치·기동 완료 — senlytd 가 부팅 자동시작으로 돕니다."
echo "   상태:  systemctl status senlytd --no-pager"
echo "   로그:  journalctl -u senlytd -f     (\"하드웨어 자가진단\" 줄로 engine/valve 확인)"
echo ""
echo "👉 다음(마지막 한 걸음): 이 기기가 서버 admin에 \"승인 대기\"로 나타납니다."
echo "   $ADMIN_URL 에서 이 기기를 \"승인 + 모드 배정\" 하면 online 됩니다."
echo "   (승인 전에는 \"승인 대기\"로 폴링만 합니다 = 정상)"
