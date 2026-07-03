/// heysenlyt_pi — 라이브러리 배럴(공개 API 재노출).
///
/// v1.2.0 동결 계약 SoT(04_erd) 코어 포팅 + 헥사고날 포트/어댑터.
library heysenlyt_pi;

// ── core (SoT 바이트 동일 포팅) ──
export 'core/order_status.dart';
export 'core/pump_guard.dart';
export 'core/wire_messages.dart';
export 'core/dispenser_order_dto.dart';
export 'core/dispenser_session.dart';
export 'core/wire_json.dart';

// ── ports (인터페이스) ──
export 'ports/engine_port.dart';
export 'ports/command_source_port.dart';
export 'ports/status_sink_port.dart';

// ── persistence ──
export 'persistence/idempotency_ledger.dart';

// ── app ──
export 'app/daemon.dart';
