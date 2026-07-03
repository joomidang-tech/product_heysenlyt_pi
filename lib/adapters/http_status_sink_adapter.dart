/// HTTP StatusSinkPort 실어댑터 — ⛔ TODO 스텁(이후 웨이브).
///
/// 실 http 클라이언트(PATCH orders/heartbeat·POST trace·Bearer·오프라인 큐 flush)는
/// 이후 웨이브. 지금은 포트 계약만.
library;

import '../core/wire_messages.dart' show Heartbeat, StatusReport;
import '../ports/status_sink_port.dart';

/// 서버 경유 status/heartbeat/trace 역보고 어댑터 — 미구현 스텁.
class HttpStatusSinkAdapter implements StatusSinkPort {
  @override
  Future<void> reportStatus(StatusReport report) {
    // TODO(wave-2): PATCH /api/dispenser/orders/[id] (Bearer dispenser) + OQ flush.
    throw UnimplementedError('HTTP status sink — 이후 웨이브');
  }

  @override
  Future<void> sendHeartbeat(Heartbeat hb) {
    // TODO(wave-2): PATCH /api/dispenser/heartbeat 30s 주기.
    throw UnimplementedError('HTTP heartbeat — 이후 웨이브');
  }

  @override
  Future<void> shipTrace(List<TraceSpan> spans) {
    // TODO(wave-2): POST /api/dispenser/trace best-effort 배치(최대 100 span).
    throw UnimplementedError('HTTP trace ship — 이후 웨이브');
  }
}
