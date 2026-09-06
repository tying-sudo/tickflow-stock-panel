"""退市股维表: 从 kline_daily 与 instruments 的差集派生, 回测读点合并。

背景 (幸存者偏差修复 2026-09-06):
- kline_daily 保留历史退市股 (约 1600+ 只), 回测面板/矩阵从全量 daily 重建,
  行情层天然无偏; 但 instruments 维表只含当前上市标的, name/total_shares 等
  元数据经 left join 后对退市股为 null, basic_filter 的市值/换手界与
  exclude_st 以 fill_null(False) 收敛 → 设了这些过滤的策略会把退市股整体
  静默排除, 长周期回测系统性高估。
- 本服务把差集标的落成 sidecar 表 data/instruments_delisted/part.parquet,
  name 置空串 (非 null: exclude_st 的 str.contains 对空串不命中, 退市股不再
  被误杀; 其 ST 时期的 5% 档位无从考证, 涨跌停幅度按非 ST 板块规则计算 —
  相比"整段消失"是更小的偏差)。listing/delisting 取首/末根日K近似。
- 股本列留 null: 市值/换手界仍无法覆盖退市股 (需 gpcw 历史股本源, 二期)。
- 合并只发生在回测读点 (backtest/engine._instruments_for_backtest), 不进
  repository.get_instruments() — 盘前维表同步、实时监控等活股链路不受影响,
  日K同步 universe 也不会去拉已退市标的。
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from app.parquet import scan_daily_parquet

logger = logging.getLogger(__name__)

# 末根K线距今不足该天数 且不在 instruments → 视为维表同步滞后的次新股,
# 不入退市表 (次日 instruments 同步追上后按活股合并)。
_STALE_DAYS = 30

_SIDECAR_DIR = "instruments_delisted"

# dtype 对齐线上 instruments.parquet 实测 schema (listing_date 为 Utf8,
# as_of 为 Date); delisting_date 为本表扩展列, 同样存 ISO 字符串。
_EMPTY_COLUMNS: dict[str, pl.DataType] = {
    "symbol": pl.Utf8,
    "name": pl.Utf8,
    "code": pl.Utf8,
    "exchange": pl.Utf8,
    "region": pl.Utf8,
    "type": pl.Utf8,
    "listing_date": pl.Utf8,
    "delisting_date": pl.Utf8,
    "total_shares": pl.Float64,
    "float_shares": pl.Float64,
    "tick_size": pl.Float64,
    "limit_up": pl.Float64,
    "limit_down": pl.Float64,
    "as_of": pl.Date,
}


def derive_delisted_instruments(
    data_dir: Path, *, today: date | None = None
) -> pl.DataFrame:
    """kline_daily 有、instruments 无、且末根K线已停滞的标的 → 退市维表行。"""
    today = today or date.today()
    instruments_path = data_dir / "instruments" / "instruments.parquet"
    live_symbols: set[str] = set()
    if instruments_path.exists():
        live = pl.read_parquet(instruments_path, columns=["symbol"])
        live_symbols = set(live["symbol"].cast(pl.Utf8).to_list())

    daily_glob = (data_dir / "kline_daily" / "**" / "*.parquet").as_posix()
    spans = (
        scan_daily_parquet(daily_glob)
        .group_by("symbol")
        .agg(
            pl.col("date").min().alias("listing_date"),
            pl.col("date").max().alias("delisting_date"),
        )
        .collect()
    )
    stale_before = today - timedelta(days=_STALE_DAYS)
    rows = spans.filter(
        ~pl.col("symbol").is_in(sorted(live_symbols))
        & (pl.col("delisting_date") <= stale_before)
    )
    if rows.is_empty():
        return pl.DataFrame(schema=_EMPTY_COLUMNS)
    result = rows.select(
        pl.col("symbol").cast(pl.Utf8),
        pl.lit("").alias("name"),
        pl.col("symbol").str.split(".").list.first().alias("code"),
        pl.col("symbol").str.split(".").list.last().alias("exchange"),
        pl.lit("CN").alias("region"),
        pl.lit("stock").alias("type"),
        pl.col("listing_date").cast(pl.Date).dt.to_string("%Y-%m-%d"),
        pl.col("delisting_date").cast(pl.Date).dt.to_string("%Y-%m-%d"),
        pl.lit(None, dtype=pl.Float64).alias("total_shares"),
        pl.lit(None, dtype=pl.Float64).alias("float_shares"),
        pl.lit(None, dtype=pl.Float64).alias("tick_size"),
        pl.lit(None, dtype=pl.Float64).alias("limit_up"),
        pl.lit(None, dtype=pl.Float64).alias("limit_down"),
        pl.lit(today).alias("as_of"),
    )
    return result.sort("symbol")


def sync_delisted_instruments(data_dir: Path, *, today: date | None = None) -> int:
    """派生并写入 sidecar 表, 返回行数。失败向上抛出, 由调用方决定降级。"""
    frame = derive_delisted_instruments(data_dir, today=today)
    out_dir = data_dir / _SIDECAR_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp = out_dir / "part.parquet.tmp"
    frame.write_parquet(tmp)
    tmp.replace(out_dir / "part.parquet")
    logger.info("delisted instruments synced: %d symbols", frame.height)
    return frame.height


def load_delisted_instruments(data_dir: Path) -> pl.DataFrame:
    path = data_dir / _SIDECAR_DIR / "part.parquet"
    if not path.exists():
        return pl.DataFrame(schema=_EMPTY_COLUMNS)
    try:
        return pl.read_parquet(path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("退市股维表读取失败: %s", exc)
        return pl.DataFrame(schema=_EMPTY_COLUMNS)


def merge_with_delisted(instruments: pl.DataFrame, data_dir: Path) -> pl.DataFrame:
    """活股维表 + 退市 sidecar 合并; 同 symbol 时活股行优先。

    仅回测读点调用。diagonal concat 容忍两侧列集差异 (退市侧多 delisting_date)。
    """
    if instruments.is_empty():
        return instruments
    delisted = load_delisted_instruments(data_dir)
    if delisted.is_empty():
        return instruments
    merged = pl.concat([instruments, delisted], how="diagonal_relaxed")
    return merged.unique(subset=["symbol"], keep="first", maintain_order=True)
