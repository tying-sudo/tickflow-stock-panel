"""修复指定交易日分钟K总量受损: 自动检测 (分钟Σ vs 日K×100) → 重拉 → 覆盖分区行。

背景 (2026-09-06 对账首跑发现): 2026-09-04 (easy_tdx 切换后首个交易日) 的
分钟库 4559 只总量偏低 10~50% — 行数满 (241 根) 但部分盘中增量轮次的量未并
入。provider 重拉与日K×100 精确相等 (002460/001316 探针 ratio=1.0000),
库内行可安全按 symbol 整体替换。

用法 (backend 目录):
    .venv/bin/python scripts/repair_minute_day.py 2026-09-04 [--lo 0.9] [--hi 1.1] [--batch 200] [--dry]

只处理 ratio 越界的 symbol; 分钟 volume 契约=股, 与日K(手)×100 对账。
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def damaged_symbols(data_dir: Path, day: str, lo: float, hi: float) -> pl.DataFrame:
    minute = (
        pl.scan_parquet(
            (data_dir / "kline_minute" / "**" / "*.parquet").as_posix(),
            extra_columns="ignore", missing_columns="insert",
        )
        .filter(pl.col("datetime").cast(pl.Utf8).str.starts_with(day))
        .group_by("symbol")
        .agg(pl.col("volume").sum().alias("msum"))
    )
    daily = (
        pl.scan_parquet(
            (data_dir / "kline_daily" / "**" / "*.parquet").as_posix(),
            extra_columns="ignore", missing_columns="insert",
        )
        .filter(pl.col("date") == pl.lit(date.fromisoformat(day)))
        .select("symbol", (pl.col("volume") * 100.0).alias("dvol"))
    )
    joined = minute.join(daily, on="symbol", how="inner").with_columns(
        (pl.col("msum") / pl.col("dvol")).alias("ratio")
    )
    return joined.filter(
        (pl.col("ratio") < lo) | (pl.col("ratio") > hi)
    ).sort("ratio").collect()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("day")
    parser.add_argument("--lo", type=float, default=0.9)
    parser.add_argument("--hi", type=float, default=1.1)
    parser.add_argument("--batch", type=int, default=200)
    parser.add_argument("--dry", action="store_true")
    parser.add_argument("--all", action="store_true", help="全量重拉当日所有 symbol (系统性短缺时用)")
    args = parser.parse_args()

    from app.config import settings

    data_dir = Path(settings.data_dir)
    if args.all:
        all_symbols = sorted(
            pl.scan_parquet(
                (data_dir / "kline_daily" / "**" / "*.parquet").as_posix(),
                extra_columns="ignore", missing_columns="insert",
            )
            .filter(pl.col("date") == pl.lit(date.fromisoformat(args.day)))
            .select("symbol").unique().collect()["symbol"].to_list()
        )
        bad = pl.DataFrame({"symbol": all_symbols})
        print(f"[{args.day}] --all: 全量重拉 {bad.height} 只")
    else:
        bad = damaged_symbols(data_dir, args.day, args.lo, args.hi)
        print(f"[{args.day}] damaged symbols: {bad.height} (ratio<{args.lo} or >{args.hi})")
    if bad.is_empty() or args.dry:
        if not bad.is_empty():
            print(bad.head(10).to_pandas().to_string(index=False))
        return 0

    from app.plugins.easy_tdx.provider import EasyTdxProvider

    provider = EasyTdxProvider()
    part_path = data_dir / "kline_minute" / f"date={args.day}" / "part.parquet"
    partition = pl.read_parquet(part_path)
    partition_cols = partition.columns
    day_prefix = args.day

    symbols = bad["symbol"].to_list()
    repaired = 0
    failed: list[str] = []
    for i in range(0, len(symbols), args.batch):
        chunk = symbols[i : i + args.batch]
        try:
            fresh = provider.get_minute(
                chunk,
                datetime.fromisoformat(args.day + "T00:00:00"),
                datetime.fromisoformat(args.day + "T23:59:59"),
                "stock", "1m",
            )
        except Exception as exc:  # noqa: BLE001
            print(f"  batch {i//args.batch}: fetch failed: {exc}")
            failed.extend(chunk)
            continue
        if fresh.is_empty():
            failed.extend(chunk)
            continue
        fresh = fresh.filter(
            pl.col("datetime").cast(pl.Utf8).str.slice(0, 10) == day_prefix
        )
        # 对齐分区列集 (fresh 可能多/少列)
        cols = [c for c in partition_cols if c in fresh.columns]
        fresh = fresh.select(cols).with_columns(
            pl.col("datetime").cast(partition.schema["datetime"])
        )
        partition = pl.concat(
            [
                partition.filter(
                    ~pl.col("symbol").is_in(chunk)
                    | (pl.col("datetime").cast(pl.Utf8).str.slice(0, 10) != day_prefix)
                ),
                fresh,
            ],
            how="diagonal_relaxed",
        )
        repaired += fresh.height
        print(f"  batch {i//args.batch}: +{fresh.height} rows (累计 {repaired})")

    if args.dry or repaired == 0:
        return 1
    partition = partition.sort(["symbol", "datetime"])
    tmp = part_path.with_suffix(".parquet.tmp")
    partition.write_parquet(tmp)
    tmp.replace(part_path)
    print(f"written: {part_path} rows={partition.height} (+{repaired} 替换行, {len(failed)} 只拉取失败)")

    # 复验
    recheck = damaged_symbols(data_dir, args.day, args.lo, args.hi)
    still = recheck.height
    print(f"recheck damaged: {still} (修复前 {bad.height})")
    if still:
        print(recheck.head(10).to_pandas().to_string(index=False))
    return 0 if still < bad.height else 1


if __name__ == "__main__":
    raise SystemExit(main())
