"""清理混入股票库的 ETF 行: 迁移优先 (ETF 库缺失的行并入) → 备份 → 从股票库移除。

背景 (2026-09-06): 2026-08-18 起 ETF 批次混入股票日K库 (~19.7k 行); 分钟库的
ETF 污染更早更广 (~637k symbol-日对, 其中 ~627k 在 kline_etf_minute 缺失 —
股票分钟库是这份历史的唯一副本, 必须**迁移**而非删除)。

流程 (每分区):
  1. 读股票库分区 → 拆 污染/干净 两半;
  2. 污染行 anti-join ETF 库同分区 (symbol+datetime) → 缺失部分并入 ETF 库
     (已有行以 ETF 库为准, 不覆盖);
  3. 污染行原文备份 data/backup_etf_cleanup_20260906/;
  4. 干净行原子写回股票库分区。

幂等: 重跑时已无污染行则跳过。用法:
  setsid nohup .venv/bin/python scripts/cleanup_etf_pollution.py > /tmp/etf_cleanup.log 2>&1 &
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

BACKUP_DIR_NAME = "backup_etf_cleanup_20260906"


def _atomic_write(frame: pl.DataFrame, path: Path) -> None:
    tmp = path.with_suffix(".parquet.tmp")
    frame.write_parquet(tmp)
    tmp.replace(path)


def _backup(frame: pl.DataFrame, backup_root: Path, store: str, part_date: str) -> None:
    if frame.is_empty():
        return
    out = backup_root / store / f"date={part_date}"
    out.mkdir(parents=True, exist_ok=True)
    _atomic_write(frame, out / "polluted.parquet")


def migrate_store(
    data_dir: Path,
    stock_store: str,
    etf_store: str,
    etf_symbols: list[str],
    backup_root: Path,
) -> dict:
    """一个存储对 (如 kline_minute → kline_etf_minute) 的清理。"""
    key = "datetime" if stock_store == "kline_minute" else "date"
    stock_dir = data_dir / stock_store
    partitions = sorted(p for p in stock_dir.glob("date=*") if p.is_dir())
    stats = {"partitions": 0, "polluted_rows": 0, "migrated_rows": 0, "removed_partitions": 0}
    t0 = time.perf_counter()
    for idx, part in enumerate(partitions):
        part_date = part.name[5:]
        files = sorted(part.glob("*.parquet"))
        files = [f for f in files if not f.name.endswith(".tmp")]
        if not files:
            continue
        frame = pl.read_parquet(files[0]) if len(files) == 1 else pl.concat(
            [pl.read_parquet(f) for f in files], how="diagonal_relaxed"
        )
        polluted = frame.filter(pl.col("symbol").is_in(etf_symbols))
        if polluted.is_empty():
            continue
        clean = frame.filter(~pl.col("symbol").is_in(etf_symbols))
        stats["partitions"] += 1
        stats["polluted_rows"] += polluted.height

        # 迁移: ETF 库同分区缺失的行并入
        etf_part = data_dir / etf_store / part.name
        etf_path = etf_part / "part.parquet"
        incoming = polluted
        if etf_path.exists():
            existing = pl.read_parquet(etf_path)
            incoming = polluted.join(
                existing.select(["symbol", key]), on=["symbol", key], how="anti"
            )
            merged = pl.concat([existing, incoming], how="diagonal_relaxed")
        else:
            etf_part.mkdir(parents=True, exist_ok=True)
            merged = incoming
        if not incoming.is_empty():
            _atomic_write(merged.sort(["symbol", key]), etf_path)
            stats["migrated_rows"] += incoming.height

        _backup(polluted, backup_root, stock_store, part_date)

        # 股票库写回干净行 (全空则删分区)
        if clean.is_empty():
            for f in part.glob("*"):
                f.unlink()
            part.rmdir()
            stats["removed_partitions"] += 1
        else:
            for f in files:
                f.unlink()
            _atomic_write(clean.sort(["symbol", key]), part / "part.parquet")

        if idx % 50 == 0 or polluted.height > 0:
            print(
                f"[{stock_store}] {part_date}: polluted={polluted.height} "
                f"migrated={incoming.height} clean={clean.height} "
                f"({idx+1}/{len(partitions)} 分区, {time.perf_counter()-t0:.0f}s)",
                flush=True,
            )
    return stats


def main() -> int:
    from app.config import settings

    data_dir = Path(settings.data_dir)
    backup_root = data_dir / BACKUP_DIR_NAME
    etf_symbols = (
        pl.read_parquet(data_dir / "instruments_etf" / "instruments_etf.parquet", columns=["symbol"])["symbol"].to_list()
    )
    print(f"ETF symbols: {len(etf_symbols)}; backup root: {backup_root}", flush=True)

    results = {}
    for stock_store, etf_store in (("kline_daily", "kline_etf_daily"), ("kline_minute", "kline_etf_minute")):
        print(f"===== {stock_store} → {etf_store} =====", flush=True)
        results[stock_store] = migrate_store(data_dir, stock_store, etf_store, etf_symbols, backup_root)

    # 验证: 股票库残留
    for store in ("kline_daily", "kline_minute"):
        key = "datetime" if store == "kline_minute" else "date"
        leftover = (
            pl.scan_parquet((data_dir / store / "**" / "*.parquet").as_posix(), extra_columns="ignore", missing_columns="insert")
            .filter(pl.col("symbol").is_in(etf_symbols))
            .select(pl.len())
            .collect()
            .item()
        )
        print(f"验证 {store} 残留污染行: {leftover}", flush=True)
    print("done:", results, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
