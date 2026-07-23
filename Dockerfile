# heysenlyt-pi/Dockerfile — E2E/CI 전용 (운영 배포는 systemd·OTA·02_infra §4.7)
# SoT: developer/hey_senlyt/v1.2.0/02_infra/hey_senlyt_infra.md §10.3
#
# 운영(Pi 실기)은 provision.sh + systemd `senlytd`. 이 이미지는 docker-compose E2E 에서
# senlytd 데몬을 컨테이너로 띄우기 위한 테스트 전용 규격이다.
#   - register/SSE/status/heartbeat/trace 어댑터 = 실 HTTP 클라이언트(web:3000 호출)
#   - 엔진 포트만 FakeEngineAdapter(SENLYT_ENGINE=fake) — 이미지에 시리얼 HW(/dev/senlyt-pump) 없음
#   - 무인 복구(systemd Restart)는 compose `restart: unless-stopped` 로 대체

FROM python:3.12-slim

WORKDIR /app

# 의존성 설치(editable) → [project.scripts] senlytd 콘솔 스크립트 등록.
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir -e .

# 로컬 2싱크 로그·오프라인 큐 디렉토리(§4.8·700).
RUN install -d -m700 /var/log/senlyt /var/lib/senlyt/queue

# 컨테이너가 데몬을 직접 실행(systemd 없이). 서버 타겟은 env(SENLYT_SERVER_BASE_URL)로 주입.
# ⚡ 상시 소비 루프는 SENLYT_RUN=1(compose env)에서 boot() 로 구동 — 무설정은 안전 종료(0).
CMD ["senlytd"]
