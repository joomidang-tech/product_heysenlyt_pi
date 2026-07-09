"""pipeline — 파이프라인 (Dart lib/pipeline 미러).

  offline_queue     오프라인 큐(FIFO) + resync cursor
  recipe_resolver   주문 → 정렬·검증·steps 파생 (+ expoRecipe/flavor_recipes 폴백 헬퍼)
  engine_executor   EnginePort 재시도/오류분류 (EP-03 silent-success 금지)
  status_reporter   phase 단조·역행거부·멱등 StatusReport 조립
  pump_sequencer    직렬 토출·진행보고·안전정지·동시1제조·graceful
  boot_recovery     재부팅 복구 결정(CR-01 자동재실행 금지·엔진 미주입)
"""
