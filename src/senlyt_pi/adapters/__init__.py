"""adapters — 물리 I/O 어댑터 (Dart lib/adapters 미러).

  fake_engine_adapter          Fake EnginePort 시뮬레이션(호출 카운터 = P0 게이트 관찰 렌즈)
  sy01b_engine_adapter         EnginePort 실어댑터 — ⛔ TODO(실 RS485 시리얼·pyserial 유보)
  sse_command_source_adapter   CommandSourcePort 실어댑터 — ⛔ TODO(실 서버 SSE 연결 유보)
  http_status_sink_adapter     StatusSinkPort 실어댑터 — ⛔ TODO(실 서버 HTTP 연결 유보)
  settings_source              pi settings read-only 소비 → pumpAddr→SyringeSpec(O-18)
"""
