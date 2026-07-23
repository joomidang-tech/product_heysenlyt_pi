/// 서버 SSE CommandSourcePort 실어댑터 — ⛔ TODO 스텁(이후 웨이브).
///
/// 실 SSE 클라이언트(http·재연결·resync·deviceId 필터)는 이후 웨이브. 지금은 포트 계약만.
library;

import '../core/wire_messages.dart' show Command;
import '../ports/command_source_port.dart';

/// 서버 SSE command 구독 어댑터 — 미구현 스텁.
class SseCommandSourceAdapter implements CommandSourcePort {
  @override
  Stream<Command> commands(String deviceId) {
    // TODO(wave-2): GET SSE snapshot 구독 → DTO→Command 파생 → deviceId 필터(CS-08).
    throw UnimplementedError('SSE command source — 이후 웨이브');
  }
}
