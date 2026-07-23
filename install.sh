#!/usr/bin/env bash
# hey senlyt pi daemon — 라즈베리파이 1줄 설치·구동 (다운로드부터 자동).
#
# 사용 (Pi에서 한 줄) — 명령어는 하나, 바꾸는 건 **서버 URL 하나**뿐:
#   curl -fsSL https://raw.githubusercontent.com/joomidang-tech/product_heysenlyt_pi/main/install.sh \
#     | sudo bash -s -- https://senlyt.com                 # prod
#     | sudo bash -s -- https://dev-env.senlyt.com         # dev
#     | sudo bash -s -- https://v1-2-0.env.senlyt.com      # 버전 프리뷰
#
# pi 코드는 항상 main(승격된 안정본)에서 받는다 — 데몬은 서버-불가지(어느 서버를 보든 인자로 받음)라,
# 환경 구분은 "어느 서버 URL을 보게 하나" 하나로만 한다. (아직 main 미승격 코드를 먼저 시험할 때만
# SENLYT_INSTALL_BRANCH=dev 처럼 브랜치를 덮어쓴다 — raw URL 경로도 그 브랜치로 함께 바꿔 실행.)
#
# 사람이 넣는 건 **서버 URL 하나**뿐. 나머지는 켜진 뒤 자동:
#   - deviceId  = HW 시리얼 자동수집(RPi4=cpuinfo·RPi5=device-tree)
#   - mode      = admin에서 승인할 때 배정 → 서버가 기기에 내려줌
#   - engine/valve = 부팅 자동감지(실 Pi+시리얼 어댑터→sy01b·GPIO→gpio·아니면 fake)
# 등록은 키 없이 신청(TOFU) → admin에서 "승인"해야 online.  (재실행 안전·멱등)
set -euo pipefail

SERVER_URL="${1:-}"
REPO="https://github.com/joomidang-tech/product_heysenlyt_pi.git"
# 기본 main(승격 안정본) — 환경 구분은 서버 URL 하나로만. main 미승격 코드 시험 시에만 override.
BRANCH="${SENLYT_INSTALL_BRANCH:-main}"
APP_DIR="/opt/senlyt/heysenlyt-pi"
ENV_DIR="/etc/senlyt"
ENV_FILE="$ENV_DIR/device.env"
LOG_DIR="/var/log/senlyt"
STATE_DIR="/var/lib/senlyt"
SERVICE="/etc/systemd/system/senlytd.service"

# ── 0. 인자·권한 체크 ──────────────────────────────────────────────────────
if [ -z "$SERVER_URL" ]; then
	echo "❌ 서버 URL이 필요합니다." >&2
	echo "   예: curl -fsSL .../main/install.sh | sudo bash -s -- https://senlyt.com" >&2
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
#   하드웨어 라이브러리는 **apt 프리빌트**로 설치한다(pip 소스컴파일 = swig/컴파일러 필요 → 실패).
#   lgpio = Pi4·Pi5 **공통 현대 표준**(Pi5 는 RP1 칩이라 옛 RPi.GPIO 불가 — lgpio 라야 GPIO 동작).
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq git python3 python3-venv python3-pip
# 하드웨어 라이브러리(밸브 gpiozero+lgpio · 펌프 시리얼 pyserial) — 프리빌트 deb, 컴파일 없음.
#   없는 패키지는 건너뛴다(구 OS 폴백 — 각각 개별 설치라 하나 없어도 나머지는 깔림).
for pkg in python3-gpiozero python3-lgpio python3-serial; do
	apt-get install -y -qq "$pkg" 2>/dev/null && echo "  ✓ $pkg" || echo "  (skip $pkg — 이 OS엔 미제공)"
done

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

# ── 3. venv(--system-site-packages) + 데몬 설치 ────────────────────────────
#   데몬은 런타임 의존성 0(stdlib) — pip 은 데몬 패키지 등록만. --system-site-packages 로 위에서 apt
#   설치한 하드웨어 라이브러리(gpiozero/lgpio/serial)를 venv 가 그대로 본다(pip 컴파일 없음).
#   재실행 시 실행 중 데몬이 옛 venv 를 물고 있으면 재생성이 위험 → 먼저 멈추고, --system-site-packages
#   flag 반영을 위해 venv 를 새로 만든다(마지막에 restart 로 새 코드 기동).
systemctl stop senlytd 2>/dev/null || true
rm -rf "$APP_DIR/.venv"
python3 -m venv --system-site-packages "$APP_DIR/.venv"
"$APP_DIR/.venv/bin/pip" install -q -e "$APP_DIR"

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
# 펌프 RS485 주소 → RecipeResolver pump_map(부트스트랩). 없으면 pump_map 이 비어
# 모든 레시피 스텝이 CMD_VALIDATION_FAILED 로 drop(토출 0)되어 주문이 실패한다.
#   flavor(식향)=addr 1,2(시린지 2펌프) · fragrance(향장향)=addr 1,2,3(3펌프).
#   ⚠️ addr 0 은 RS485 브로드캐스트라 기기주소로 쓰지 않는다. 용량은 양 모드 공통 0.5mL.
#   서버 settings(GET-SSE) 수신 시 이 부트스트랩 매핑을 대체할 수 있다.
PUMP_ADDRESSES=flavor:1,2;fragrance:1,2,3;aroma:1,2,3
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
#   restart 사용(enable --now 아님) — 재실행 시 **이미 켜진 서비스는 --now 로 재시작되지 않아** 옛 코드가
#   계속 돈다. restart 는 꺼져 있으면 시작·켜져 있으면 새 코드로 재시작(멱등 재실행 정확성).
systemctl daemon-reload
systemctl enable -q senlytd
systemctl restart senlytd

ADMIN_URL="${SERVER_URL%/}/admin"
echo ""
echo "✅ 설치·기동 완료 — senlytd 가 부팅 자동시작으로 돕니다."
echo "   상태:  systemctl status senlytd --no-pager"
echo "   로그:  journalctl -u senlytd -f     (\"하드웨어 자가진단\" 줄로 engine/valve 확인)"
echo ""
echo "👉 다음(마지막 한 걸음): 이 기기가 서버 admin에 \"승인 대기\"로 나타납니다."
echo "   $ADMIN_URL 에서 이 기기를 \"승인 + 모드 배정\" 하면 online 됩니다."
echo "   (승인 전에는 \"승인 대기\"로 폴링만 합니다 = 정상)"
