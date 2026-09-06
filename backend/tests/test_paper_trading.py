"""模拟盘核心测试: 首夜信号 → 次日开盘成交 / 涨停不可买 / 跌停顺延+强平 / 到期平仓 / 台账持久化。"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from app.services import paper_trading as pt


CONFIG = {
    "strategy_id": "test_strategy",
    "symbols": None,
    "start": "2026-08-01",
    "initial_capital": 100_000.0,
    "max_positions": 2,
    "holding_days": 3,
    "fees_pct": 0.0002,
    "stamp_tax_pct": 0.0005,
    "params": None,
    "overrides": None,
}


def _prices(frame_rows: list[dict]):
    def provide(symbols: list[str], day: date) -> pl.DataFrame:
        return pl.DataFrame(
            [r for r in frame_rows if r["symbol"] in symbols and r["_day"] == day]
        ).select("symbol", "open", "close", "prev_close")
    return provide


def _signals(by_day: dict[str, list[dict]]):
    def provide(day: date) -> list[dict]:
        return by_day.get(day.isoformat(), [])
    return provide


def test_first_night_signal_then_next_open_fill(tmp_path: Path):
    day1, day2 = date(2026, 9, 1), date(2026, 9, 2)
    prices = _prices([
        {"symbol": "600000.SH", "_day": day2, "open": 10.0, "close": 10.4, "prev_close": 9.8},
    ])
    signals = _signals({"2026-09-01": [{"symbol": "600000.SH", "kind": "buy", "score": 80.0}]})

    state = pt.PaperState(cash=0.0)
    r1 = pt.settle_day(tmp_path, day1, config=CONFIG, state=state,
                       price_provider=lambda s, d: pl.DataFrame(), signal_provider=signals)
    assert r1["status"] == "ok"
    assert len(state.pending_orders) == 1 and state.pending_orders[0]["kind"] == "buy"
    assert state.cash == 100_000.0  # 首夜只挂单不动现金

    r2 = pt.settle_day(tmp_path, day2, config=CONFIG, state=state,
                       price_provider=prices, signal_provider=signals)
    assert r2["status"] == "ok"
    assert "600000.SH" in state.positions
    pos = state.positions["600000.SH"]
    assert pos.qty == 5000  # slot=5万, 10元/股 → 5000 股
    # 现金 = 100000 - 5000*10*(1+0.0002)
    expected_cash = 100_000 - 5000 * 10.0 * (1 + 0.0002)
    assert abs(state.cash - expected_cash) < 1e-6
    # 净值 = 现金 + 5000*10.4
    assert abs(r2["equity"] - (state.cash + 5000 * 10.4)) < 1e-6
    trades = pl.read_parquet(tmp_path / "paper_trading" / "trades.parquet")
    assert trades.height == 1 and trades["side"].to_list() == ["buy"]


def test_limit_up_open_blocks_buy(tmp_path: Path):
    day1, day2 = date(2026, 9, 1), date(2026, 9, 2)
    # 主板 10%: prev 10.0 → 涨停 11.0; open=11.0 一字 → 买不进
    prices = _prices([
        {"symbol": "600001.SH", "_day": day2, "open": 11.0, "close": 11.0, "prev_close": 10.0},
    ])
    signals = _signals({"2026-09-01": [{"symbol": "600001.SH", "kind": "buy", "score": 90.0}]})
    state = pt.PaperState(cash=100_000.0)
    pt.settle_day(tmp_path, day1, config=CONFIG, state=state,
                  price_provider=lambda s, d: pl.DataFrame(), signal_provider=signals)
    r2 = pt.settle_day(tmp_path, day2, config=CONFIG, state=state,
                       price_provider=prices, signal_provider=signals)
    assert r2["status"] == "ok"
    assert state.positions == {}          # 挂单作废
    assert state.cash == 100_000.0        # 现金未动
    # open 10.99 < 11.0 可买 (边界内)
    prices_ok = _prices([
        {"symbol": "600001.SH", "_day": day2, "open": 10.99, "close": 11.0, "prev_close": 10.0},
    ])
    state2 = pt.PaperState(cash=100_000.0)
    pt.settle_day(tmp_path / "b", day1, config=CONFIG, state=state2,
                  price_provider=lambda s, d: pl.DataFrame(), signal_provider=signals)
    pt.settle_day(tmp_path / "b", day2, config=CONFIG, state=state2,
                  price_provider=prices_ok, signal_provider=signals)
    assert "600001.SH" in state2.positions


def test_limit_down_sell_defers_then_fills(tmp_path: Path):
    day1, day2, day3 = date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3)
    state = pt.PaperState(cash=0.0)
    state.cash = 100_000.0
    state.positions["600002.SH"] = pt.Position(
        symbol="600002.SH", qty=1000, avg_cost=10.0, opened="2026-08-28")
    # 9-1 晚信号卖出 → 9-2 跌停 (prev 10 → 跌停 9.0, open=9.0) 顺延
    prices = _prices([
        {"symbol": "600002.SH", "_day": day2, "open": 9.0, "close": 9.0, "prev_close": 10.0},
        {"symbol": "600002.SH", "_day": day3, "open": 9.2, "close": 9.3, "prev_close": 9.0},
    ])
    signals = _signals({"2026-09-01": [{"symbol": "600002.SH", "kind": "sell"}]})
    pt.settle_day(tmp_path, day1, config=CONFIG, state=state,
                  price_provider=lambda s, d: pl.DataFrame(), signal_provider=signals)
    assert any(o["kind"] == "sell" for o in state.pending_orders)
    pt.settle_day(tmp_path, day2, config=CONFIG, state=state,
                  price_provider=prices, signal_provider=signals)
    assert "600002.SH" in state.positions          # 跌停没卖掉
    assert state.positions["600002.SH"].blocked_days == 1
    pt.settle_day(tmp_path, day3, config=CONFIG, state=state,
                  price_provider=prices, signal_provider=signals)
    assert "600002.SH" not in state.positions      # 9-3 开盘 9.2 > 跌停 8.1 成交
    # 卖出所得 = 1000*9.2*(1-0.0002-0.0005)
    expected = 100_000 + 1000 * 9.2 * (1 - 0.0002 - 0.0005)
    assert abs(state.cash - expected) < 1e-6


def test_max_hold_forces_close_at_close_price(tmp_path: Path):
    day1 = date(2026, 9, 1)
    state = pt.PaperState(cash=50_000.0)
    state.positions["600003.SH"] = pt.Position(
        symbol="600003.SH", qty=500, avg_cost=8.0, opened="2026-08-28", hold_days=2)
    # holding_days=3 → 今夜 hold_days+1=3 触发强平
    prices = _prices([
        {"symbol": "600003.SH", "_day": day1, "open": 8.5, "close": 8.6, "prev_close": 8.4},
    ])
    r = pt.settle_day(tmp_path, day1, config=CONFIG, state=state,
                      price_provider=prices, signal_provider=_signals({}))
    assert "600003.SH" not in state.positions
    trades = pl.read_parquet(tmp_path / "paper_trading" / "trades.parquet")
    row = trades.filter(pl.col("symbol") == "600003.SH")
    assert row["reason"].to_list() == ["max_hold"]
    assert row["price"].item() == 8.6      # 收盘价强平
    expected = 50_000 + 500 * 8.6 * (1 - 0.0002 - 0.0005)
    assert abs(state.cash - expected) < 1e-6


def test_buy_slots_respect_max_positions(tmp_path: Path):
    day1 = date(2026, 9, 1)
    state = pt.PaperState(cash=100_000.0)
    state.positions["600100.SH"] = pt.Position(
        symbol="600600.SH", qty=100, avg_cost=5.0, opened="2026-08-30")
    state.positions["600200.SH"] = pt.Position(
        symbol="600200.SH", qty=100, avg_cost=5.0, opened="2026-08-30")
    signals = _signals({"2026-09-01": [
        {"symbol": "600300.SH", "kind": "buy", "score": 50.0},
        {"symbol": "600400.SH", "kind": "buy", "score": 70.0},
    ]})
    pt.settle_day(tmp_path, day1, config=CONFIG, state=state,
                  price_provider=lambda s, d: pl.DataFrame(), signal_provider=signals)
    buys = [o for o in state.pending_orders if o["kind"] == "buy"]
    assert len(buys) == 0    # 已满仓 (2/2), 无卖出释放槽位


def test_state_roundtrip_and_config_gate(tmp_path: Path):
    base = tmp_path / "paper_trading"
    base.mkdir(parents=True)
    assert pt.load_config(tmp_path) is None          # 无 config = disabled
    (base / "config.json").write_text(
        '{"strategy_id": "x", "start": "2026-08-01"}', encoding="utf-8")
    cfg = pt.load_config(tmp_path)
    assert cfg is not None and cfg["max_positions"] == 10  # 默认合并
    state = pt.PaperState(cash=123.0)
    state.positions["600500.SH"] = pt.Position(
        symbol="600500.SH", qty=200, avg_cost=3.0, opened="2026-09-01")
    pt.save_state(tmp_path, state)
    loaded = pt.load_state(tmp_path)
    assert loaded.cash == 123.0
    assert loaded.positions["600500.SH"].qty == 200
