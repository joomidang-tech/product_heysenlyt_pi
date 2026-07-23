/// Fake CommandSource — server-mediated 단일 어댑터(질의서 §0·Q5 종결) 하네스.
///
/// 실 SSE 대신 테스트가 command 를 push 하여 dispatch 봉합을 검증한다. deviceId 필터는
/// dispatcher 가 하므로 이 fake 는 필터 없이 그대로 흘린다(필터 검증을 위해).
library;

import 'dart:async';

import 'package:heysenlyt_pi/heysenlyt_pi.dart';

class FakeCommandSource implements CommandSourcePort {
  final _controller = StreamController<Command>.broadcast();

  /// command 주입(테스트).
  void push(Command c) => _controller.add(c);

  Future<void> close() => _controller.close();

  @override
  Stream<Command> commands(String deviceId) => _controller.stream;
}
