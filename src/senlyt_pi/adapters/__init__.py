"""adapters — 물리 I/O 어댑터 (Dart lib/adapters 미러).

  http_client                  표준 urllib HTTP 하부(JSON 왕복 + SSE 스트리밍·외부 의존 0)
  fake_engine_adapter          Fake EnginePort 시뮬레이션(호출 카운터 = P0 게이트 관찰 렌즈·유일 mock)
  sy01b_engine_adapter         EnginePort 실어댑터 — ⛔ TODO(실 RS485 시리얼·pyserial 유보)
  sse_command_source_adapter   CommandSource/CommandSetSource 실어댑터 — 실 서버 SSE 구독(CS-08 필터)
  http_status_sink_adapter     StatusSinkPort 실어댑터 — 실 HTTP 역보고(orders/heartbeat/trace/봉투전이·OQ)
  registration_client          디바이스 등록 + make_http_register_transport(실 POST register)
  settings_source              pi settings read-only 소비 → pumpAddr→SyringeSpec(O-18)
"""
