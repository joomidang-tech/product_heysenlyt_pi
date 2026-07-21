"""초기화 포트 지정(2026-07-21 QA "초기화시 흡입/배출 포트 변경") 계약 테스트.

근본원인(성연 실기기 확정): `Z{힘}R`(포트 미지정)은 펌웨어 기본값 = **흡입=포트1·배출=마지막
포트(12)** 로 돈다 → 매 초기화마다 포트1 **향료**가 빨렸다가 12번(대기 개방)으로 폐기 +
`I12R` 주차로 잔여액이 계속 배수. D45(매 제조 초기화)로 제조 횟수만큼 증폭됐다.

수정 계약(2026-07-21 확정 — "밸브가 쉴 땐 언제나 배출구"):
  - 서버가 포트 매핑에서 (air, output) 파생 → wire `initInPort`/`initOutPort`(op=initialize 전용)
  - pi: 홈 = `Z{힘},{air},{output}R`(흡입=공기·소모 0 / 배출=정상 출구) · 주차 = `I{output}R`
  - 부재(구 서버) = 기존 동작(포트 기본값·SAFE_PORT 주차) 하위호환
  - 힘(n1)은 용량 파생 유지 — 0.5mL=Half(1). 포트를 주면 n1 명시 필수(Manual §4.4.2).

⚠️ 실기기 실측 게이트: 배포 전 펌프 1대에서 "초기화 시 포트1 액체가 안 빨리는지" 확인 후 전체 적용.
"""

from __future__ import annotations

from test_sy01b_broadcast_init_20260719 import BusScriptedSerial
from test_sy01b_engine_adapter import SPEC_05, FakeSerial, adapter_with

from senlyt_pi.core.pump_guard import SyringeSpec
from senlyt_pi.core.wire_messages import RecipeStep
from senlyt_pi.pipeline.recipe_resolver import RecipeResolver, ResolvedOpStep
from senlyt_pi.ports.engine_port import EngineOpCommand

SPEC_125 = SyringeSpec(pump_full_stroke=12000, syringe_capacity_ml=1.25)


# ── A. pump_guard — 포트 지정 홈 명령 파생 ──────────────────────────────────────


class TestInitCommandWith:
    def test_ports_appended_with_capacity_derived_force(self):
        """`Z{힘},{air},{output}R` — 힘은 용량 파생 유지(0.5mL=Half=1)."""
        assert SPEC_05.init_command_with(12, 2) == "Z1,12,2R"
        # ≥1.0mL = Full. 포트를 주면 n1=0 을 **명시**해야 한다(생략 불가 — Manual §4.4.2).
        assert SPEC_125.init_command_with(12, 2) == "Z0,12,2R"

    def test_missing_port_falls_back_to_base(self):
        """어느 한쪽이라도 없으면 기본형(구 서버 하위호환) — 포트 반쪽 지정 금지."""
        assert SPEC_05.init_command_with(None, None) == "Z1R"
        assert SPEC_05.init_command_with(12, None) == "Z1R"
        assert SPEC_05.init_command_with(None, 2) == "Z1R"
        assert SPEC_125.init_command_with(None, None) == "ZR"


# ── B. wire ↔ resolver — initInPort/initOutPort 전파·왕복·검증 ──────────────────


class TestWireInitPorts:
    def test_roundtrip_and_resolver_propagation(self):
        """서버 `initInPort/initOutPort` → RecipeStep → ResolvedOpStep.init_* + 왕복 보존."""
        step = RecipeStep.from_json(
            {"idx": 0, "stage": 0, "kind": "engineOp", "pumpAddr": 1, "op": "initialize",
             "initInPort": 12, "initOutPort": 2}
        )
        assert step.in_port == 12 and step.out_port == 2
        j = step.to_json()
        assert j["initInPort"] == 12 and j["initOutPort"] == 2  # 재전송/영속 왕복.
        assert "valvePort" not in j  # plunger 계열 키와 상호배타.

        out = RecipeResolver({1: SPEC_05}).resolve([step]).steps[0]
        assert isinstance(out, ResolvedOpStep)
        assert out.init_in_port == 12 and out.init_out_port == 2
        assert out.valve_port is None  # initialize 는 valve_port 를 쓰지 않는다.

    def test_legacy_and_out_of_range(self):
        """부재(구 서버)·1~12 밖 = None(기존 동작 하위호환·안전측 무시)."""
        legacy = RecipeStep.from_json(
            {"idx": 0, "stage": 0, "kind": "engineOp", "pumpAddr": 1, "op": "initialize"}
        )
        out = RecipeResolver({1: SPEC_05}).resolve([legacy]).steps[0]
        assert isinstance(out, ResolvedOpStep)
        assert out.init_in_port is None and out.init_out_port is None

        bad = RecipeStep.from_json(
            {"idx": 0, "stage": 0, "kind": "engineOp", "pumpAddr": 1, "op": "initialize",
             "initInPort": 13, "initOutPort": 0}
        )
        out2 = RecipeResolver({1: SPEC_05}).resolve([bad]).steps[0]
        assert isinstance(out2, ResolvedOpStep)
        assert out2.init_in_port is None and out2.init_out_port is None

    def test_plunger_valveport_unaffected(self):
        """plunger 계열 valvePort 경로는 불변(회귀 방지)."""
        step = RecipeStep.from_json(
            {"idx": 0, "stage": 0, "kind": "engineOp", "pumpAddr": 1, "op": "plungerFull",
             "valvePort": 12}
        )
        out = RecipeResolver({1: SPEC_05}).resolve([step]).steps[0]
        assert isinstance(out, ResolvedOpStep)
        assert out.valve_port == 12
        assert out.init_in_port is None and out.init_out_port is None


# ── C. 어댑터 — 브로드캐스트/개별 초기화 와이어 ────────────────────────────────


class TestBroadcastInitPorts:
    def test_broadcast_home_with_ports_and_output_parking(self):
        """브로드캐스트 홈 = `Z1,12,2R` · 주차 = `I2R`(배출구) — 12 주차 폐지."""
        fake = BusScriptedSerial()
        a = adapter_with(fake, read_timeout_s=0.1, init_timeout_s=1.0)
        results = a.initialize_broadcast([1, 2], SPEC_05, init_in_port=12, init_out_port=2)
        assert results == {1: 0, 2: 0}
        assert fake.written[:4] == ["/_TR\r", "/_U200,5R\r", "/_Z1,12,2R\r", "/_I2R\r"], (
            "초기화 와이어 — 홈은 포트 지정(흡입=air·배출=output)·주차는 배출구"
        )

    def test_broadcast_legacy_keeps_safe_port(self):
        """포트 부재(구 서버) — 기존 `Z1R`+`I12R` 그대로(하위호환·기존 테스트와 동일)."""
        fake = BusScriptedSerial()
        a = adapter_with(fake, read_timeout_s=0.1, init_timeout_s=1.0)
        a.initialize_broadcast([1, 2], SPEC_05)
        assert fake.written[:4] == ["/_TR\r", "/_U200,5R\r", "/_Z1R\r", "/_I12R\r"]


class TestRunOpInitPorts:
    def test_run_op_initialize_uses_ports_and_parks_at_output(self):
        """개별(순차) 초기화도 `Z1,12,2R` + 주차 `I2R` — 브로드캐스트와 동일 규칙."""
        fake = FakeSerial()
        a = adapter_with(fake, read_timeout_s=0.1, init_timeout_s=1.0)
        res = a.run_op(
            EngineOpCommand(
                pump_addr=1, op="initialize", spec=SPEC_05, init_in_port=12, init_out_port=2
            )
        )
        assert res.raw_error_code == 0
        assert any("Z1,12,2R" in w for w in fake.written), "홈이 포트 지정으로 나가야 한다"
        assert any(w.startswith("/1I2R") for w in fake.written), "주차 = 배출구(I2R)"
        assert not any("I12R" in w for w in fake.written), "12(대기 개방) 주차 금지"

    def test_run_op_initialize_legacy_no_parking(self):
        """포트 부재(구 서버) — 기본 `Z1R`·주차 없음(기존 동작 그대로)."""
        fake = FakeSerial()
        a = adapter_with(fake, read_timeout_s=0.1, init_timeout_s=1.0)
        res = a.run_op(EngineOpCommand(pump_addr=1, op="initialize", spec=SPEC_05))
        assert res.raw_error_code == 0
        assert any("Z1R" in w for w in fake.written)
        assert not any("I2R" in w or "I12R" in w for w in fake.written)
