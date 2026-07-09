"""senlyt_pi — hey senlyt pi 데몬 (Python 재작성 · 헥사고날).

구조는 Dart 구현(lib/)의 미러:
  core/        순수 도메인 (상태 전이표·PumpGuard·DTO·와이어 모델·Bearer 만료판단)
  ports/       포트 프로토콜 (EnginePort·CommandSource·StatusSink)
  adapters/    물리 I/O 어댑터 (Fake 시뮬레이션 + 실기기/실서버 스텁 + settings read-only)
  persistence/ 멱등 ledger (파일 fsync 영속)
  pipeline/    OQ·RecipeResolver·EngineExecutor·StatusReporter·PumpSequencer·BootRecovery
  app/         디스패처(CS→IL→RR→PS→EP→SR 봉합)·데몬 골격·senlytd 진입점
  test_seam/   Fake 엔진 sentinel 공유 상수(EP-03)
"""

__version__ = "1.2.0"
