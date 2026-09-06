"""模拟盘 (paper trading) — 信号 → 次日真实开盘价成交 → 独立持仓台账 → 与回测对拍。

为什么 (2026-09-06 缺口修复③):
    回测撮合/成本模型是否可信, 需要一条独立于回测内部状态的执行链来对照。
    模拟盘每晚抽取策略信号 (回测引擎 open_t+1 口径下 entry/exit_signal_date==T
    的信号单), 次日用 T+1 真实 OHLC 成交 (涨停不可买 / 跌停不可卖), 维护自己的
    现金 / 持仓 / 净值曲线, 与同参数回测对拍 — 偏差即回测成本模型的漏洞。

语义 (v1):
- 挂单: settle(T) 跑策略回测 [config.start, T], 买入候选 = entry_signal_date==T
  的成交按 entry_score 降序; 卖出候选 = exit_signal_date==T 且在台账持仓中。
  卖出候选来自回测自身的持仓时间线, 与台账可能因历史成交差异发散 — 台账侧
  以 holding_days 到期强平兜底 (按收盘价), 发散持仓至多多持数日。
- 成交: settle(T+1) 先用 T+1 真实 OHLC 处理前夜挂单。买入: open < 理论涨停价
  (前收 × 板块限幅) 才成交, 否则挂单作废 (当晚重新出信号); 卖出: open > 理论
  跌停价才成交, 否则顺延次日, 连续 blocked_max_days 未成交按当日收盘强平。
- 计费: 买入 value×(1+fees_pct); 卖出 value×(1-fees_pct-stamp_tax_pct)。
  无滑点 — 真实开盘价即成交价 (这正是与回测对拍的观测点)。
- 仓位: equal — 单仓目标 = initial_capital / max_positions, 受现金约束。
- 存储: data/paper_trading/{config.json, state.json, trades.parquet,
  equity.parquet, orders.parquet}; config.json 不存在 = 功能关闭 (管道零影响)。
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable

import polars as pl

from app.price_limits import price_limit_pct

logger = logging.getLogger(__name__)

BLOCKED_MAX_DAYS = 5

CONFIG_DEFAULTS: dict[str, Any] = {
    "strategy_id": None,          # 必填
    "symbols": None,
    "start": None,                # ISO, 信号抽取回测窗口起点 (必填)
    "initial_capital": 1_000_000.0,
    "max_positions": 10,
    "holding_days": 5,
    "fees_pct": 0.0002,
    "stamp_tax_pct": 0.0005,
    "params": None,
    "overrides": None,
}


# ── 存储层 ─────────────────────────────────────────────────────────


def load_config(data_dir: Path) -> dict | None:
    path = data_dir / "paper_trading" / "config.json"
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("paper trading config 解析失败: %s", exc)
        return None
    if not raw.get("strategy_id") or not raw.get("start"):
        return None
    merged = {**CONFIG_DEFAULTS, **raw}
    return merged


@dataclass
class Position:
    symbol: str
    qty: float            # 股
    avg_cost: float       # 含费每股成本
    opened: str           # ISO 成交日
    blocked_days: int = 0  # 卖出被跌停阻塞的连续天数
    hold_days: int = 0


@dataclass
class PaperState:
    cash: float
    positions: dict[str, Position] = field(default_factory=dict)
    # 前夜挂单: {"kind": "buy"|"sell", "symbol", "score", "created": ISO}
    pending_orders: list[dict] = field(default_factory=list)
    last_settled: str | None = None   # 已完成"成交+信号抽取"的交易日 ISO
    last_error: str | None = None
    trade_no: int = 0


def load_state(data_dir: Path) -> PaperState:
    path = data_dir / "paper_trading" / "state.json"
    if not path.exists():
        return PaperState(cash=0.0)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("paper state 损坏, 重置: %s", exc)
        return PaperState(cash=0.0)
    return PaperState(
        cash=float(raw.get("cash", 0.0)),
        positions={
            k: Position(**v) for k, v in (raw.get("positions") or {}).items()
        },
        pending_orders=list(raw.get("pending_orders") or []),
        last_settled=raw.get("last_settled"),
        last_error=raw.get("last_error"),
        trade_no=int(raw.get("trade_no", 0)),
    )


def save_state(data_dir: Path, state: PaperState) -> None:
    out = data_dir / "paper_trading"
    out.mkdir(parents=True, exist_ok=True)
    payload = {
        "cash": state.cash,
        "positions": {k: vars(v) for k, v in state.positions.items()},
        "pending_orders": state.pending_orders,
        "last_settled": state.last_settled,
        "last_error": state.last_error,
        "trade_no": state.trade_no,
    }
    tmp = out / "state.json.tmp"
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(out / "state.json")


def _append_parquet(data_dir: Path, name: str, rows: list[dict]) -> None:
    if not rows:
        return
    out = data_dir / "paper_trading"
    out.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(rows)
    path = out / name
    if path.exists():
        frame = pl.concat([pl.read_parquet(path), frame], how="diagonal_relaxed")
    tmp = out / f"{name}.tmp"
    frame.write_parquet(tmp)
    tmp.replace(path)


# ── 结算核心 ───────────────────────────────────────────────────────

def _theoretical_limit(prev_close: float, symbol: str, day: date, *, up: bool) -> float:
    rate = price_limit_pct(symbol, day)
    return round(prev_close * (1 + rate), 2) if up else round(prev_close * (1 - rate), 2)


def settle_day(
    data_dir: Path,
    day: date,
    *,
    config: dict,
    state: PaperState,
    price_provider: Callable[[list[str], date], pl.DataFrame],
    signal_provider: Callable[[date], list[dict]],
) -> dict:
    """单日结算: 先用 day 真实 OHLC 处理前夜挂单, 再抽取当晚新信号。

    price_provider(symbols, day) → DataFrame[symbol, open, close, prev_close]
        (prev_close 为前一交易日 raw 收盘, 供涨跌停判定);
    signal_provider(day) → [{"symbol", "kind": "buy"|"sell", "score"}, ...]
        (T 日信号, 回测引擎口径)。
    首次结算 (last_settled is None 且无挂单/持仓) 只做信号抽取, 现金补开仓金。
    """
    first_run = state.last_settled is None
    if first_run and state.cash <= 0:
        state.cash = float(config["initial_capital"])
    elif not first_run and date.fromisoformat(state.last_settled) >= day:
        return {"status": "skipped", "day": day.isoformat()}

    trades: list[dict] = []
    # ── 1. 处理前夜挂单 (真实 OHLC) ──
    if state.pending_orders:
        needed = sorted({o["symbol"] for o in state.pending_orders} | set(state.positions))
        prices = price_provider(needed, day)
        px = {
            r["symbol"]: r
            for r in prices.iter_rows(named=True)
        }
        remaining_orders: list[dict] = []
        for order in state.pending_orders:
            sym = order["symbol"]
            row = px.get(sym)
            if row is None or row.get("open") is None:
                # 数据缺失: 买入作废, 卖出顺延
                if order["kind"] == "buy":
                    continue
                remaining_orders.append(order)
                continue
            if order["kind"] == "buy":
                limit_up = _theoretical_limit(row["prev_close"], sym, day, up=True)
                if row["open"] >= limit_up:
                    continue  # 一字/高开涨停: 买不进, 作废
                slot = float(config["initial_capital"]) / int(config["max_positions"])
                budget = min(state.cash, slot)
                if budget < row["open"] * 100:
                    continue  # 现金不足一手
                qty = float(int(budget / (row["open"] * 100)) * 100)  # 整百股
                if qty <= 0:
                    continue
                fee = qty * row["open"] * float(config["fees_pct"])
                state.cash -= qty * row["open"] + fee
                state.trade_no += 1
                state.positions[sym] = Position(
                    symbol=sym, qty=qty,
                    avg_cost=(qty * row["open"] + fee) / qty,
                    opened=day.isoformat(),
                )
                trades.append({
                    "no": state.trade_no, "date": day.isoformat(), "symbol": sym,
                    "side": "buy", "price": row["open"], "qty": qty,
                    "fee": round(fee, 2), "reason": order.get("reason") or "signal",
                })
            else:  # sell
                pos = state.positions.get(sym)
                if pos is None:
                    continue  # 台账已无此仓 (对拍发散或已强平)
                limit_down = _theoretical_limit(row["prev_close"], sym, day, up=False)
                force = pos.blocked_days + 1 >= BLOCKED_MAX_DAYS
                if row["open"] <= limit_down and not force:
                    pos.blocked_days += 1
                    remaining_orders.append(order)  # 跌停卖不出, 顺延
                    continue
                fill_price = row["open"] if row["open"] > limit_down else row["close"]
                gross = pos.qty * fill_price
                fee = gross * float(config["fees_pct"])
                stamp = gross * float(config["stamp_tax_pct"])
                state.cash += gross - fee - stamp
                state.trade_no += 1
                trades.append({
                    "no": state.trade_no, "date": day.isoformat(), "symbol": sym,
                    "side": "sell", "price": fill_price, "qty": pos.qty,
                    "fee": round(fee + stamp, 2),
                    "reason": "forced_close" if row["open"] <= limit_down else (order.get("reason") or "signal"),
                })
                del state.positions[sym]
        state.pending_orders = remaining_orders

    # ── 2. 持仓持有天数推进 + 到期强平 (收盘价) ──
    expires_today = [
        sym for sym, pos in state.positions.items()
        if pos.opened != day.isoformat() and pos.hold_days + 1 >= int(config["holding_days"])
    ]
    if expires_today:
        prices = price_provider(sorted(expires_today), day)
        px = {r["symbol"]: r for r in prices.iter_rows(named=True)}
        for sym in expires_today:
            pos = state.positions.get(sym)
            row = px.get(sym)
            if pos is None or row is None or row.get("close") is None:
                continue
            gross = pos.qty * row["close"]
            fee = gross * float(config["fees_pct"])
            stamp = gross * float(config["stamp_tax_pct"])
            state.cash += gross - fee - stamp
            state.trade_no += 1
            trades.append({
                "no": state.trade_no, "date": day.isoformat(), "symbol": sym,
                "side": "sell", "price": row["close"], "qty": pos.qty,
                "fee": round(fee + stamp, 2), "reason": "max_hold",
            })
            del state.positions[sym]
    for pos in state.positions.values():
        if pos.opened != day.isoformat():
            pos.hold_days += 1
            # blocked_days 只在卖出成交 (仓位删除) 时自然清零, 连续跌停日持续累计

    # ── 3. 今晚新信号 → 挂单 ──
    signals = signal_provider(day)
    sells = [s for s in signals if s["kind"] == "sell" and s["symbol"] in state.positions]
    buys = sorted(
        (s for s in signals if s["kind"] == "buy" and s["symbol"] not in state.positions),
        key=lambda s: -(s.get("score") or 0.0),
    )
    free_slots = int(config["max_positions"]) - len(state.positions) + len(sells)
    new_orders: list[dict] = [
        {"kind": "sell", "symbol": s["symbol"], "score": s.get("score"),
         "created": day.isoformat(), "reason": "signal"}
        for s in sells
    ]
    for s in buys:
        if free_slots <= 0:
            break
        new_orders.append({
            "kind": "buy", "symbol": s["symbol"], "score": s.get("score"),
            "created": day.isoformat(), "reason": "signal",
        })
        free_slots -= 1
    state.pending_orders = state.pending_orders + new_orders

    # ── 4. 收盘估值 ──
    held = sorted(state.positions)
    equity_market = 0.0
    if held:
        closes = price_provider(held, day)
        equity_market = sum(
            (r["close"] or 0.0) * state.positions[r["symbol"]].qty
            for r in closes.iter_rows(named=True)
            if r.get("close") is not None
        )
    equity = state.cash + equity_market

    _append_parquet(data_dir, "trades.parquet", trades)
    _append_parquet(data_dir, "orders.parquet", new_orders)
    _append_parquet(data_dir, "equity.parquet", [{
        "date": day.isoformat(), "equity": round(equity, 2),
        "cash": round(state.cash, 2), "market_value": round(equity_market, 2),
        "positions": len(state.positions), "pending": len(state.pending_orders),
    }])
    state.last_settled = day.isoformat()
    state.last_error = None
    save_state(data_dir, state)
    return {
        "status": "ok", "day": day.isoformat(), "trades": trades,
        "equity": round(equity, 2), "positions": len(state.positions),
        "pending_orders": len(state.pending_orders),
    }


# ── 生产装配 (默认 provider 实现) ──────────────────────────────────

def _make_price_provider(engine) -> Callable[[list[str], date], pl.DataFrame]:
    from app.backtest.engine import BacktestEngine

    assert isinstance(engine, BacktestEngine)

    def provide(symbols: list[str], day: date) -> pl.DataFrame:
        start = day - timedelta(days=20)
        frame = engine.load_panel(
            sorted(symbols), start, day,
            columns=["symbol", "date", "open", "close", "raw_close"],
            asset_type="stock",
        )
        if frame.is_empty():
            return pl.DataFrame(schema={
                "symbol": pl.Utf8, "open": pl.Float64, "close": pl.Float64,
                "prev_close": pl.Float64,
            })
        frame = frame.sort(["symbol", "date"]).with_columns(
            pl.col("raw_close").shift(1).over("symbol").alias("prev_close")
        ).filter(pl.col("date") == day)
        return frame.select("symbol", "open", "close", "prev_close")

    return provide


def _make_signal_provider(engine, strategy_engine, config: dict):
    from app.backtest.strategy import StrategyBacktestConfig, StrategyBacktestService

    svc = StrategyBacktestService(engine, strategy_engine)
    start = date.fromisoformat(config["start"])

    def provide(day: date) -> list[dict]:
        cfg = StrategyBacktestConfig(
            strategy_id=config["strategy_id"],
            symbols=config.get("symbols"),
            start=start,
            end=day,
            params=config.get("params"),
            overrides=config.get("overrides"),
            # 信号抽取用 close_t: open_t+1 口径下 T 日信号要 T+1 成交, 而回测
            # 窗口止于 T, 最新一天的信号不会出现在 trades 里。close_t 让当日
            # 信号物化为交易记录; 只取 symbol+score, 成交由台账用真实 T+1 开盘。
            matching="close_t",
            max_positions=int(config["max_positions"]),
            initial_capital=float(config["initial_capital"]),
            holding_days=int(config["holding_days"]),
            fees_pct=float(config["fees_pct"]),
            slippage_bps=0.0,
        )
        result = svc.run(cfg)
        if getattr(result, "error", None):
            raise RuntimeError(f"paper trading 信号回测失败: {result.error}")
        signals: list[dict] = []
        # 末日入场: 撮合器不记录 sim 末日入场 (trades 永远缺最新一天),
        # 优先读信号矩阵末行快照 last_day_entries
        for entry in getattr(result, "last_day_entries", None) or []:
            signals.append({
                "symbol": entry["symbol"], "kind": "buy",
                "score": entry.get("score"),
                "signal_id": entry.get("signal_id"),
            })
        # 卖出候选: 来自回测自身持仓的退出信号 (含 sim 末日)
        for tr in result.trades or []:
            if not isinstance(tr, dict):
                tr = vars(tr)
            exit_sig = tr.get("exit_signal_date")
            if str(exit_sig or "")[:10] == day.isoformat():
                signals.append({"symbol": tr["symbol"], "kind": "sell",
                                "score": tr.get("entry_score")})
        if not any(s["kind"] == "buy" for s in signals):
            # 回退 (非矩阵策略无 last_day_entries): 从 trades 抽当日入场
            for tr in result.trades or []:
                if not isinstance(tr, dict):
                    tr = vars(tr)
                if str(tr.get("entry_signal_date") or "")[:10] == day.isoformat():
                    signals.append({"symbol": tr["symbol"], "kind": "buy",
                                    "score": tr.get("entry_score")})
        return signals

    return provide


def _build_strategy_engine(data_dir: Path):
    """独立构建 StrategyEngine (与 main.py 同构: builtin + custom/ai/composite)。"""
    from app.strategy import config as strategy_config
    from app.strategy.engine import StrategyEngine

    backend_root = Path(__file__).resolve().parent.parent
    strategy_dirs = [
        backend_root / "strategy" / "builtin",
        data_dir / "strategies" / "custom",
        data_dir / "strategies" / "ai",
        data_dir / "strategies" / "composite",
    ]
    return StrategyEngine(
        strategy_dirs=strategy_dirs,
        override_loader=lambda sid: strategy_config.load_override(data_dir, sid),
    )


def settle(data_dir: Path, repo, *, day: date | None = None) -> dict:
    """管道/手动触发入口: 读 config → 结算最近一个已落库交易日。"""
    config = load_config(data_dir)
    if config is None:
        return {"status": "disabled"}
    state = load_state(data_dir)
    latest = repo.latest_daily_date()
    if latest is None:
        return {"status": "no-data"}
    target = day or latest
    if target > latest:
        return {"status": "skipped", "reason": f"数据仅到 {latest}"}
    if state.last_settled and date.fromisoformat(state.last_settled) >= target:
        return {"status": "skipped", "day": target.isoformat()}
    from app.backtest.engine import BacktestEngine

    engine = BacktestEngine(repo)
    try:
        result = settle_day(
            data_dir, target,
            config=config, state=state,
            price_provider=_make_price_provider(engine),
            signal_provider=_make_signal_provider(
                engine, _build_strategy_engine(data_dir), config
            ),
        )
    except Exception as exc:  # noqa: BLE001
        state.last_error = str(exc)
        save_state(data_dir, state)
        logger.warning("paper trading settle failed: %s", exc)
        return {"status": "error", "error": str(exc)}
    return result


def overview(data_dir: Path) -> dict:
    """台账总览: 配置 / 持仓 / 净值曲线 / 最近成交 / 对拍偏差。"""
    config = load_config(data_dir)
    if config is None:
        return {"enabled": False}
    state = load_state(data_dir)
    base = data_dir / "paper_trading"
    equity = (
        pl.read_parquet(base / "equity.parquet").to_dicts()
        if (base / "equity.parquet").exists() else []
    )
    trades = (
        pl.read_parquet(base / "trades.parquet").sort("no").tail(50).to_dicts()
        if (base / "trades.parquet").exists() else []
    )
    initial = float(config["initial_capital"])
    last_equity = equity[-1]["equity"] if equity else initial
    return {
        "enabled": True,
        "config": {k: config[k] for k in ("strategy_id", "start", "max_positions", "holding_days")},
        "cash": round(state.cash, 2),
        "positions": [vars(p) for p in state.positions.values()],
        "pending_orders": state.pending_orders,
        "last_settled": state.last_settled,
        "last_error": state.last_error,
        "trades_recent": trades,
        "equity_curve": equity,
        "total_return_pct": round((last_equity / initial - 1) * 100, 3) if equity else 0.0,
    }


def reset(data_dir: Path) -> dict:
    """清空台账 (保留 config.json)。"""
    base = data_dir / "paper_trading"
    for name in ("state.json", "trades.parquet", "equity.parquet", "orders.parquet"):
        (base / name).unlink(missing_ok=True)
    return {"status": "reset"}
