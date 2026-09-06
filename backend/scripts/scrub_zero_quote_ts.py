"""清洗日K表残留的零点 quote_ts (bar 级 timestamp 被 normalize_daily 误收)。

背景 (2026-09-06 周末实时开关 409 事故): easy-tdx /bars 的 bar 级
timestamp (=bar 的北京零点) 曾被当作 quote_ts 写入 kline_index_daily /
kline_etf_daily (09-04 修复 provider 的无条件 strip 前的残留行)。
完整性扫描按 "quote_ts < 当日 15:00" 判盘中快照 → 零点戳被误判 →
实时开关被 409 门禁锁死, 且每次开开关都触发修复任务无限循环。

判定: quote_ts 对应北京时间时刻 == 当日 00:00 (±5min) → 零点戳 → 置 null。
真盘中快照的 quote_ts 是盘中分钟级时刻, 不会被误伤; 管道覆写的收盘数据
本就该无 quote_ts。

用法 (backend 目录): .venv/bin/python scripts/scrub_zero_quote_ts.py [--dry]
覆盖表: kline_daily / kline_index_daily / kline_etf_daily 全部分区。
"""
from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

CN_OFFSET_MS = 8 * 3600 * 1000
TOLERANCE_MS = 5 * 60 * 1000  # 零点 ±5min


def _zero_timestamps(day: date, series: pl.Series) -> pl.Series:
    """返回布尔 mask: quote_ts 对应北京时间 == 该日 00:00 (±5min)。

    北京零点 (UTC 前一日 16:00) 的 epoch 毫秒 = 当日 UTC 零点 - 8h。
    """
    utc_midnight_ms = int(
        datetime(day.year, day.month, day.day).timestamp()
    ) * 1000
    cn_midnight_ms = utc_midnight_ms - CN_OFFSET_MS
    return (series.cast(pl.Int64, strict=False) - cn_midnight_ms).abs() <= TOLERANCE_MS


def scrub_table(base: Path, table: str, dry: bool) -> dict:
    stats = {"partitions": 0, "rows_scrubbed": 0, "files": 0}
    tdir = base / table
    if not tdir.exists():
        return stats
    for part in sorted(tdir.glob("date=*")):
        day_text = part.name[5:]
        try:
            day = date.fromisoformat(day_text)
        except ValueError:
            continue
        for path in sorted(part.glob("*.parquet")):
            try:
                df = pl.read_parquet(path)
            except Exception:
                continue
            if "quote_ts" not in df.columns or df.height == 0:
                continue
            q = df["quote_ts"]
            if q.null_count() == df.height:
                continue
            mask = _zero_timestamps(day, q.cast(pl.Int64, strict=False)).fill_null(False)
            n = int(mask.sum())
            if n == 0:
                continue
            stats["partitions"] += 1
            stats["rows_scrubbed"] += n
            stats["files"] += 1
            print(f"[{table}] {day_text}: {n}/{df.height} 行零点 quote_ts → null")
            if dry:
                continue
            df = df.with_columns(
                pl.when(mask).then(None).otherwise(pl.col("quote_ts")).alias("quote_ts")
            )
            tmp = path.with_suffix(".parquet.tmp")
            df.write_parquet(tmp)
            tmp.replace(path)
    return stats


def main() -> int:
    dry = "--dry" in sys.argv
    from app.config import settings

    base = Path(settings.data_dir)
    print(f"data_dir={base} dry={dry}")
    total = 0
    for table in ("kline_daily", "kline_index_daily", "kline_etf_daily"):
        stats = scrub_table(base, table, dry)
        total += stats["rows_scrubbed"]
        print(f"[{table}] partitions={stats['partitions']} rows={stats['rows_scrubbed']}")
    print(f"total rows scrubbed: {total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
