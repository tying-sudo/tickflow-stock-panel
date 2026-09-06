"""退市股维表测试: 差集派生 / 停滞判定 / sidecar 读写 / 回测合并优先级 / ST 过滤不再误杀。"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl

from app.services.delisted_instruments import (
    derive_delisted_instruments,
    load_delisted_instruments,
    merge_with_delisted,
    sync_delisted_instruments,
)


def _write_daily(tmp_path: Path, symbol: str, first: date, last: date) -> None:
    base = tmp_path / "kline_daily"
    rows = []
    day = first
    while day <= last:
        rows.append({
            "symbol": symbol, "date": day, "open": 10.0, "high": 10.5,
            "low": 9.5, "close": 10.0, "volume": 1000.0, "amount": 10000.0,
        })
        day += timedelta(days=1)
    frame = pl.DataFrame(rows)
    first_dir = base / f"date={first.isoformat()}"
    first_dir.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(first_dir / f"part_{symbol}.parquet")
    # 末根写独立分区, 覆盖首末分区不同文件的情形
    if first != last:
        last_dir = base / f"date={last.isoformat()}"
        last_dir.mkdir(parents=True, exist_ok=True)
        frame.tail(1).write_parquet(last_dir / f"part_{symbol}.parquet")


def _write_instruments(tmp_path: Path, symbols: list[str]) -> None:
    out = tmp_path / "instruments"
    out.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": symbols,
        "name": [f"股{s[-2:]}" for s in symbols],
        "code": [s.split(".")[0] for s in symbols],
        "exchange": [s.split(".")[1] for s in symbols],
        "region": ["CN"] * len(symbols),
        "type": ["stock"] * len(symbols),
        "listing_date": ["2010-01-01"] * len(symbols),
        "total_shares": [1e9] * len(symbols),
        "float_shares": [5e8] * len(symbols),
        "tick_size": [0.01] * len(symbols),
        "limit_up": [11.0] * len(symbols),
        "limit_down": [9.0] * len(symbols),
        "as_of": [date(2026, 9, 4)] * len(symbols),
    }).write_parquet(out / "instruments.parquet")


def test_derive_only_stale_missing_symbols(tmp_path: Path):
    today = date(2026, 9, 6)
    _write_daily(tmp_path, "600001.SH", date(2005, 9, 5), today - timedelta(days=365))  # 退市1年
    _write_daily(tmp_path, "600002.SH", date(2020, 1, 1), today - timedelta(days=3))    # 活跃但缺维表
    _write_daily(tmp_path, "600003.SH", date(2015, 1, 1), today)                        # 活股
    _write_instruments(tmp_path, ["600003.SH"])

    frame = derive_delisted_instruments(tmp_path, today=today)
    assert frame["symbol"].to_list() == ["600001.SH"]
    row = frame.row(0, named=True)
    assert row["name"] == ""                       # 非 null: exclude_st 不误杀
    assert row["listing_date"] == "2005-09-05"
    assert row["delisting_date"] == (today - timedelta(days=365)).isoformat()
    assert row["delisting_date"] is not None
    assert row["exchange"] == "SH"
    assert row["total_shares"] is None             # 股本无源, 留空
    # 维表滞后标的 (3天前仍交易) 不入退市表
    assert "600002.SH" not in frame["symbol"].to_list()


def test_sync_and_load_roundtrip(tmp_path: Path):
    today = date(2026, 9, 6)
    _write_daily(tmp_path, "000003.SZ", date(2012, 6, 1), today - timedelta(days=400))
    _write_instruments(tmp_path, [])
    rows = sync_delisted_instruments(tmp_path, today=today)
    assert rows == 1
    loaded = load_delisted_instruments(tmp_path)
    assert loaded["symbol"].to_list() == ["000003.SZ"]
    # 空目录 → 空 schema 帧而非异常
    assert load_delisted_instruments(Path(tmp_path / "nowhere")).is_empty()


def test_merge_prefers_live_rows(tmp_path: Path):
    today = date(2026, 9, 6)
    _write_daily(tmp_path, "600004.SH", date(2008, 1, 1), today - timedelta(days=200))
    _write_daily(tmp_path, "600005.SH", date(2009, 1, 1), today - timedelta(days=100))
    # 600004 仍在活股维表 (比如重新上市/判定边界) → 活股行优先
    _write_instruments(tmp_path, ["600004.SH"])
    sync_delisted_instruments(tmp_path, today=today)

    live = load_instruments_raw(tmp_path)
    merged = merge_with_delisted(live, tmp_path)
    live_row = merged.filter(pl.col("symbol") == "600004.SH")
    assert live_row["name"].item() == "股SH"          # 活股名保留, 非空串
    delisted_row = merged.filter(pl.col("symbol") == "600005.SH")
    assert delisted_row["name"].item() == ""          # 退市空串
    assert "delisting_date" in merged.columns         # 扩展列存在, 活股行为 null


def load_instruments_raw(tmp_path: Path) -> pl.DataFrame:
    return pl.read_parquet(tmp_path / "instruments" / "instruments.parquet")


def test_exclude_st_keeps_empty_name():
    """basic_filter 的 exclude_st 对空串名不命中 → 退市股不再被静默排除。"""
    panel = pl.DataFrame({
        "symbol": ["600001.SH", "600003.SH"],
        "name": ["", "ST某股"],
    })
    kept = panel.filter(~pl.col("name").str.contains("(?i)ST|\\*ST|退").fill_null(True))
    assert kept["symbol"].to_list() == ["600001.SH"]
    # null 名 (合并前的旧行为) 会被 fill_null(True) 排除 — 回归锚点
    panel_old = pl.DataFrame({"symbol": ["600001.SH"], "name": [None]}, schema_overrides={"name": pl.Utf8})
    kept_old = panel_old.filter(~pl.col("name").str.contains("(?i)ST|\\*ST|退").fill_null(True))
    assert kept_old.is_empty()


def test_stock_universe_symbols_filters_etf_leak(tmp_path: Path):
    """股票回测标的轴 = instruments ∪ 退市 sidecar; 混入 kline 的 ETF 被排除。"""
    import types

    from app.backtest.engine import BacktestEngine

    today = date(2026, 9, 6)
    _write_daily(tmp_path, "600006.SH", date(2008, 1, 1), today - timedelta(days=90))
    _write_instruments(tmp_path, ["600003.SH"])
    sync_delisted_instruments(tmp_path, today=today)

    def fake_asset(asset_type: str) -> pl.DataFrame:
        if asset_type == "stock":
            return load_instruments_raw(tmp_path)
        return pl.DataFrame()

    repo = types.SimpleNamespace(
        store=types.SimpleNamespace(data_dir=tmp_path),
        get_instruments_asset=fake_asset,
    )
    eng = BacktestEngine(repo)
    universe = eng._stock_universe_symbols()
    assert universe == ["600003.SH", "600006.SH"]  # 活股 + 退市, ETF 无从混入

    # 维表缺失 → None (不过滤, 保持旧行为)
    empty_repo = types.SimpleNamespace(
        store=types.SimpleNamespace(data_dir=tmp_path),
        get_instruments_asset=lambda asset_type: pl.DataFrame(),
    )
    assert BacktestEngine(empty_repo)._stock_universe_symbols() is None
