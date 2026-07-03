/// senlytd — hey_senlyt v1.2.0 라즈베리파이 headless 디스펜서 데몬 진입점.
///
/// 이번 웨이브 = 골격 + SoT 코어 계약 포팅. 실 소비 루프·펌프 구동은 유보(안전상 이후 웨이브).
/// 실행 시 명확히 미구현임을 알리고 종료한다(오작동으로 펌프를 구동하지 않도록).
library;

import 'dart:io';

void main(List<String> args) {
  stderr.writeln(
    'senlytd (heysenlyt-pi v1.2.0): 골격 웨이브 — 소비 루프/펌프 구동 미구현(안전상 유보).\n'
    '코어 계약(전이표·PumpGuard·DTO·와이어)은 lib/core 에 포팅됨. 테스트: dart test.',
  );
  exit(0);
}
