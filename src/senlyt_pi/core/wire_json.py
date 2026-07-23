"""와이어 JSON 직렬화 헬퍼 — SoT 투영 불변식(§5-3) / includeIfNull:false 규칙(부록A P-4).

Dart `lib/core/wire_json.dart` 포팅. 두 규칙:
  (P-4) 옵셔널 필드가 None 이면 **키 자체를 방출하지 않는다**(JSON 에 null 금지).
  (P-3) createdAt/updatedAt/ts 는 서버가 준 ISO8601 문자열을 **재포맷 없이 그대로 보존**한다.
"""

from __future__ import annotations

from typing import Any


def put_if_present(mapping: dict[str, Any], key: str, value: Any) -> None:
    """None 이 아닐 때만 키를 넣는다 — `includeIfNull:false` 등가(부록A P-4)."""
    if value is not None:
        mapping[key] = value
