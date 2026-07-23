"""디바이스 등록 정체성(deviceId·dispenserToken) 파일 영속 — 계약 RegisterResponse.

등록(POST /api/dispensers/register) 결과를 로컬에 저장해 재부팅 간 유지한다.
file_idempotency_ledger 의 crash-safe 결(temp 파일 + os.replace atomic swap + fsync)을
따르되, 단일 JSON 문서(정체성 1건)라 append 로그는 불필요.

[2026-07-10 D-A] **deviceId = pi 수집 하드웨어 시리얼 그대로**(서버 발급/파생 없음). 구
`hardwareId` 필드는 폐기 — 시리얼이 곧 deviceId 라 별도 자연키가 불필요(단일 정체성).
구 정체성 파일(dsp-<hash> deviceId + hardwareId)은 읽을 때 hardwareId 를 무시하고,
저장된 deviceId 가 현재 시리얼과 다르면 재등록으로 자연 승격된다(ensure_registered 게이트).

토큰은 **opaque**(부록A P-5) — 여기서는 문자열 그대로 저장/전송만. 만료 판단은
RegisterResponse.exp(epoch 초) 필드로 한다(payload 재파싱 불필요·서버 검증 대체 아님).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _fsync_dir(path: Path) -> None:
    """부모 디렉터리 fsync — os.replace(rename) 자체의 내구성 보증(감사 P3 봉합·2026-07-15).

    파일 fsync 만으로는 전원 단절 시 rename 이 비내구일 수 있다(POSIX) —
    file_idempotency_ledger 와 동일 결. 일부 FS 는 디렉터리 fsync 미지원 → OSError 삼킴.
    """
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


@dataclass(frozen=True, slots=True)
class DeviceIdentity:
    """등록 정체성 — deviceId(=수집 HW 시리얼·D-A) + RegisterResponse(dispenserToken·exp·mode)."""

    device_id: str  # = pi 수집 하드웨어 시리얼(D-A). SSE 구독·CS-08 필터·heartbeat 라우팅 키.
    dispenser_token: str  # opaque(부록A P-5) — 저장/전송만.
    exp: int  # 만료 epoch(초) — RegisterResponse.exp
    # 서버 배정 모드(TOFU 승인 시 하달·flavor|fragrance) — pi 의 SSE 구독/역보고 큐를 결정한다.
    #   부재(None)면 pi 는 env(SENLYT_MODE) 또는 flavor 폴백. SENLYT_MODE env 대체(서버가 SoT).
    mode: str | None = None
    # 이 정체성을 등록·발급받은 서버 base URL(2026-07-23) — **서버 바인딩**. 토큰·deviceId 는 그 서버
    #   레지스트리에서만 의미가 있다(서버마다 DB·HMAC 서명키가 다름). URL 만 바꿔 재설치하면
    #   ensure_registered 가 이 값과 현재 서버를 비교해, 다르면 저장분을 버리고 그 서버에 **재등록**한다
    #   (안 그러면 옛 서버 정체성을 재사용해 새 서버엔 register 가 안 가 admin 후보에 안 뜬다 — 페어링 실패).
    #   부재(None)=구 정체성 파일(상위호환) → 서버 미상이므로 재등록 유도(안전한 fail-safe).
    server_base_url: str | None = None

    def to_json(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "deviceId": self.device_id,
            "dispenserToken": self.dispenser_token,
            "exp": self.exp,
        }
        if self.mode is not None:
            d["mode"] = self.mode
        if self.server_base_url is not None:
            d["serverBaseUrl"] = self.server_base_url
        return d

    @staticmethod
    def from_json(j: Any) -> "DeviceIdentity | None":
        """방어 파싱 — 형식이 어긋나면 None(재등록 유도·crash 금지).

        구 정체성 파일의 `hardwareId` 키가 있어도 무시한다(상위호환) — deviceId 가 곧 시리얼.
        구 파일(mode 부재)도 유효 — mode=None(폴백). 승인 재등록 시 mode 가 채워진다.
        구 파일(serverBaseUrl 부재)도 유효 — None(서버 미상) → ensure_registered 가 재등록 유도.
        """
        if not isinstance(j, dict):
            return None
        device_id = j.get("deviceId")
        token = j.get("dispenserToken")
        exp = j.get("exp")
        if not isinstance(device_id, str) or device_id == "":
            return None
        if not isinstance(token, str) or token == "":
            return None
        if isinstance(exp, bool) or not isinstance(exp, int):
            return None
        raw_mode = j.get("mode")
        mode = raw_mode if isinstance(raw_mode, str) and raw_mode else None
        raw_server = j.get("serverBaseUrl")
        server_base_url = raw_server if isinstance(raw_server, str) and raw_server else None
        return DeviceIdentity(
            device_id=device_id,
            dispenser_token=token,
            exp=exp,
            mode=mode,
            server_base_url=server_base_url,
        )


def is_identity_expired(identity: DeviceIdentity, *, now_seconds: int) -> bool:
    """exp ≤ now 면 만료(strict — dispenser_session.is_token_expired 와 동일 결).
    만료 시 재등록으로 재발급(계약 RegisterResponse.dispenserToken)."""
    return identity.exp <= now_seconds


class DeviceIdentityStore:
    """정체성 파일 저장소 — 원자 저장(temp+replace+fsync)·방어 로드."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    def load(self) -> DeviceIdentity | None:
        """저장된 정체성 로드. 부재/파손 → None(재등록 유도)."""
        try:
            raw = self.path.read_text(encoding="utf-8")
        except (FileNotFoundError, OSError):
            return None
        try:
            decoded = json.loads(raw)
        except ValueError:
            return None
        return DeviceIdentity.from_json(decoded)

    def save(self, identity: DeviceIdentity) -> None:
        """원자 저장 — temp 파일에 쓰고 fsync 후 os.replace(atomic swap)."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        data = json.dumps(identity.to_json(), ensure_ascii=False)
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)
        # rename 내구화 — 부모 디렉터리 fsync(전원 단절 시 정체성 유실 방지·감사 P3).
        _fsync_dir(self.path.parent)

    def clear(self) -> None:
        """정체성 삭제(재등록 강제)."""
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
