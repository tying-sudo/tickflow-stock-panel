"""财务五表独立重建 (2026-09-05, 独立进程 — 免疫 uvicorn 热重载).

背景: 后端进程内 sync/all 两次被并行会话的文件部署触发的热重载杀死
(18:38/18:45), balance_sheet/cash_flow 中断, shares 因 /finance 无历史被拒.
本脚本直接在 venv 进程里完成剩余工作, 不依赖后端:

  1. balance_sheet / cash_flow: EasyTdxProvider 新浪 f10 全量拉 8 期
     (31 实例并发), 与 08-28 备份 (fuyao 23 期) 逐列合并 — 新值覆盖,
     旧行补缺列 (roe 等), 期数并集.
  2. metrics / income: 已拉好的新浪数据 (磁盘当前版) 与备份同法合并.
  3. shares: easy_tdx 无历史能力 (仅最新单期) → 从备份恢复
     (增量同步 latest_only=True 路径今后可正常保鲜).

写盘: .tmp + os.replace 原子替换. 表间串行 (共享 31 并发额度与新浪限流预算).
"""
import os
import sys
import time

sys.path.insert(0, '/opt/tickflow-stock-panel/backend')
os.chdir('/opt/tickflow-stock-panel/backend')

import polars as pl

from app.plugins.easy_tdx.provider import EasyTdxProvider

DATA = '/opt/tickflow-stock-panel/data'
FIN = f'{DATA}/financials'
BAK = f'{DATA}/financials_bak_20260905'
T0 = time.time()


def log(msg: str) -> None:
    print(f'[{time.time() - T0:7.1f}s] {msg}', flush=True)


def merge_history(*frames: pl.DataFrame) -> pl.DataFrame:
    """与 financial_sync._merge_report_history 同语义: 逐列取最新非空, 期并集."""
    valid = [f for f in frames if not f.is_empty() and {'symbol', 'period_end'} <= set(f.columns)]
    if not valid:
        return pl.DataFrame()
    merged = pl.concat(valid, how='diagonal_relaxed').filter(
        pl.col('symbol').is_not_null() & pl.col('period_end').is_not_null())
    sort_keys = ['symbol', 'period_end'] + (['announce_date'] if 'announce_date' in merged.columns else [])
    merged = merged.sort(sort_keys, nulls_last=True)
    value_cols = [c for c in merged.columns if c not in ('symbol', 'period_end')]
    return (merged.group_by('symbol', 'period_end')
            .agg([pl.col(c).drop_nulls().last() for c in value_cols])
            .sort(['symbol', 'period_end']))


def write_table(table: str, df: pl.DataFrame) -> None:
    out = f'{FIN}/{table}/part.parquet'
    df.write_parquet(out + '.tmp')
    os.replace(out + '.tmp', out)
    log(f'  written {table}: {df.height} rows, {df["symbol"].n_unique()} symbols')


def read(path: str) -> pl.DataFrame:
    try:
        return pl.read_parquet(path)
    except Exception:
        return pl.DataFrame()


provider = EasyTdxProvider()
symbols = read(f'{DATA}/instruments/instruments.parquet')['symbol'].to_list()
log(f'universe = {len(symbols)} symbols')

# ---- 1. balance_sheet / cash_flow: 新浪全量 + 备份合并 ----
for table in ('balance_sheet', 'cash_flow'):
    log(f'fetch {table} via sina f10 (8 periods, 31-instance pool)...')
    t0 = time.time()
    sina_df = provider.get_financials(table, symbols, latest_only=False)
    log(f'  fetched {sina_df.height} rows in {time.time() - t0:.0f}s')
    merged = merge_history(read(f'{BAK}/{table}.parquet'), sina_df)
    if merged.is_empty():
        log(f'  ERROR: {table} merge empty — skip write (保留备份不动)')
        continue
    write_table(table, merged)

# ---- 2. metrics / income: 磁盘新浪版 + 备份合并 ----
for table in ('metrics', 'income'):
    merged = merge_history(read(f'{BAK}/{table}.parquet'), read(f'{FIN}/{table}/part.parquet'))
    if merged.is_empty():
        log(f'ERROR: {table} merge empty — skip')
        continue
    write_table(table, merged)

# ---- 3. shares: 备份恢复 (easy_tdx 无历史能力) ----
shares_bak = read(f'{BAK}/shares.parquet')
if shares_bak.is_empty():
    log('ERROR: shares backup missing!')
else:
    write_table('shares', shares_bak)

log('ALL DONE')
