"""matrix_native 特征计划回归: 合成字段 price_limit_pct 不被存储白名单丢弃。"""
from __future__ import annotations

from pathlib import Path

from app.backtest.strategy import StrategyDependencyResolver
from app.strategy.engine import StrategyEngine


def _engine() -> StrategyEngine:
    backend_root = Path(__file__).resolve().parent.parent / "app" / "strategy" / "builtin"
    return StrategyEngine(strategy_dirs=[backend_root])


def test_near_limit_up_plan_keeps_price_limit_pct():
    se = _engine()
    s = se.get("near_limit_up")
    assert s.execution_backend == "matrix_native"
    plan = StrategyDependencyResolver().resolve(
        s, params={}, basic_filter={}, overrides={},
        entry_signals=s.entry_signals, exit_signals=s.exit_signals,
        asset_type="stock",
    )
    assert "price_limit_pct" in plan.matrix_columns, (
        "price_limit_pct 被存储白名单丢弃 → near_limit_up 运行时报 unsupported matrix feature"
    )


def test_matrix_strategy_required_fields_flow_through():
    """所有声明了 price_limit_pct 的 matrix 策略都应通过 (涨停系家族)。"""
    se = _engine()
    checked = 0
    for item in se.list_strategies():
        sid = item.get("id") if isinstance(item, dict) else None
        if not sid:
            continue
        s = se.get(sid)
        if s.execution_backend != "matrix_native" or s.matrix_strategy is None:
            continue
        if "price_limit_pct" not in s.matrix_strategy.required_fields():
            continue
        plan = StrategyDependencyResolver().resolve(
            s, params={}, basic_filter={}, overrides={},
            entry_signals=s.entry_signals, exit_signals=s.exit_signals,
            asset_type="stock",
        )
        assert "price_limit_pct" in plan.matrix_columns, f"{s.meta.get('id')} 丢失 price_limit_pct"
        checked += 1
    assert checked >= 1, "涨停系策略家族应至少有一只命中"
