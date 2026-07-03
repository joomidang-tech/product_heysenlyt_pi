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

// ── test seam (공유 sentinel) ──
export 'test_seam/fake_engine_sentinels.dart';

// ── ports (인터페이스) ──
export 'ports/engine_port.dart';
export 'ports/command_source_port.dart';
export 'ports/status_sink_port.dart';

// ── persistence ──
export 'persistence/idempotency_ledger.dart';
export 'persistence/file_idempotency_ledger.dart';

// ── pipeline (제조 파이프라인) ──
export 'pipeline/recipe_resolver.dart';
export 'pipeline/engine_executor.dart';
export 'pipeline/status_reporter.dart';
export 'pipeline/offline_queue.dart';
export 'pipeline/boot_recovery.dart';
export 'pipeline/pump_sequencer.dart';

// ── app ──
export 'app/daemon.dart';
export 'app/dispatcher.dart';
