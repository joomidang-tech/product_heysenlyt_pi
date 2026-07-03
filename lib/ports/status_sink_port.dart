/// StatusSinkPort — status 역보고 + heartbeat + trace 전송(인터페이스만) — SoT §9 / §10.
///
/// pi 는 status 전진 write 주체(§4-5·pi 단독)이나 **직결 0** — 유일 경로 =
/// PATCH /api/dispenser/orders/[id] (Bearer dispenser). heartbeat·trace 도 서버 경유.
/// 실 http 클라이언트·오프라인 큐(OQ) flush 는 이후 웨이브(TODO).
library;

import '../core/wire_messages.dart' show Heartbeat, StatusReport;

/// trace span 배치 항목 — SoT §10-4 (detail allowlist 는 서버가 2차 sanitize).
class TraceSpan {
  const TraceSpan({
    required this.ts,
    required this.traceId,
    required this.spanId,
    required this.service,
    required this.event,
    required this.level,
    this.parentSpanId,
    this.orderId,
    this.deviceId,
    this.attempt,
    this.detail,
  });

  final String ts; // ISO8601·밀리초 Z
  final String traceId;
  final String spanId; // 16-hex
  final String? parentSpanId; // 16-hex | null
  final String service; // pi 전송분은 서버가 'pi' 강제(§10-4)
  final String event; // 점표기(§10-2)
  final String level; // DEBUG|INFO|WARN|ERROR
  final String? orderId;
  final String? deviceId;
  final int? attempt;
  final Map<String, Object?>? detail; // 비식별만(§10-3)
}

/// status/heartbeat/trace 역보고 싱크(서버 경유).
abstract interface class StatusSinkPort {
  /// PATCH /api/dispenser/orders/[id] — status 전진 역보고(§9-2).
  /// OQ flush at-least-once → requestId 로 서버 dedup(§4-6).
  Future<void> reportStatus(StatusReport report);

  /// PATCH /api/dispenser/heartbeat — 30s 주기(§9-3).
  Future<void> sendHeartbeat(Heartbeat hb);

  /// POST /api/dispenser/trace — best-effort 배치(최대 100 span·§10-4/O-19).
  Future<void> shipTrace(List<TraceSpan> spans);
}
