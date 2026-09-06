#!/bin/bash
# 对拍 v2: 修复 join 类型; 量化 tiny volume; 旧分区抽查
cd /opt/tickflow-stock-panel/backend
timeout 500 .venv/bin/python -u - <<'PYEOF'
import sys, os, glob
sys.path.insert(0, '.')
from datetime import datetime, timedelta
import polars as pl
from app.plugins.easy_tdx.provider import EasyTdxProvider

DATA = '/opt/tickflow-stock-panel/data'
p = EasyTdxProvider()

def load_window(base, days):
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    parts = sorted(glob.glob(f'{DATA}/{base}/date=*/part.parquet'))
    sel = [x for x in parts if x.split('date=')[1][:10] >= cutoff]
    if not sel:
        return pl.DataFrame()
    return pl.concat([pl.read_parquet(x) for x in sel], how='diagonal_relaxed')

def cmp_daily(tag, sym, local, easy):
    if local.is_empty() or easy.is_empty():
        print(f'{tag} {sym}: EMPTY local={local.height} easy={easy.height}'); return
    l = local.select(pl.col('date').cast(pl.String).alias('date'), 'close', 'volume', 'amount')
    e = easy.select(pl.col('date').cast(pl.String).alias('date'), 'close', 'volume', 'amount')
    m = l.join(e, on='date', suffix='_easy')
    if m.is_empty():
        print(f'{tag} {sym}: NO OVERLAP local={local.height} easy={easy.height} '
              f'local_last={local["date"].max()} easy_last={easy["date"].max()}'); return
    m2 = m.filter(pl.col('volume_easy') > 0)
    vr = (m2['volume'] / m2['volume_easy']).median()
    ar = (m2['amount'] / m2['amount_easy']).median()
    cr = (m['close'] / m['close_easy']).median()
    print(f'{tag} {sym} n={m.height} vol_med_ratio={vr:.6f} amt_med_ratio={ar:.6f} close_med_ratio={cr:.6f}')

def cmp_minute(tag, sym, local, easy):
    if local.is_empty() or easy.is_empty():
        print(f'{tag} {sym}: EMPTY local={local.height} easy={easy.height}'); return
    l = local.select(pl.col('datetime').cast(pl.Datetime('us')).alias('datetime'), 'close', 'volume', 'amount')
    e = easy.select(pl.col('datetime').cast(pl.Datetime('us')).alias('datetime'), 'close', 'volume', 'amount')
    m = l.join(e, on='datetime', suffix='_easy')
    if m.is_empty():
        print(f'{tag} {sym}: NO OVERLAP local_dt={local["datetime"].max()} easy_dt={easy["datetime"].max()}'); return
    m2 = m.filter((pl.col('volume_easy') > 0) & (pl.col('volume') > 0.01))
    vr = (m2['volume'] / m2['volume_easy']).median()
    ar = (m2['amount'] / m2['amount_easy']).median()
    px = (m['close'] - m['close_easy']).abs().max()
    print(f'{tag} {sym} n={m.height} vol_med_ratio={vr:.4f} amt_med_ratio={ar:.6f} close_maxdiff={px}')

print('===== 日K 对拍 =====')
for base, syms, atype in [
    ('kline_daily', ['600519.SH', '300750.SZ'], 'stock'),
    ('kline_etf_daily', ['510300.SH', '159915.SZ'], 'etf'),
]:
    if not os.path.isdir(f'{DATA}/{base}'):
        print('MISSING DIR:', base); continue
    local = load_window(base, 40)
    for sym in syms:
        loc1 = local.filter(pl.col('symbol') == sym) if not local.is_empty() else pl.DataFrame()
        easy = p.get_daily([sym], datetime.now() - timedelta(days=45), None, asset_type=atype)
        cmp_daily(base, sym, loc1, easy)

done_idx = False
for cand in ('kline_index_daily', 'kline_index', 'index_daily'):
    if os.path.isdir(f'{DATA}/{cand}'):
        local = load_window(cand, 40).filter(pl.col('symbol') == '000001.SH')
        easy = p.get_daily(['000001.SH'], datetime.now() - timedelta(days=45), None, asset_type='index')
        cmp_daily(cand, '000001.SH', local, easy)
        done_idx = True
        break
if not done_idx:
    print('index daily dir not found')

print('===== 分钟对拍 =====')
for base, syms, atype in [('kline_minute', ['600519.SH'], 'stock'), ('kline_etf_minute', ['510300.SH'], 'etf')]:
    local = load_window(base, 4)
    if local.is_empty():
        print(base, 'NO LOCAL WINDOW'); continue
    tiny = local.filter((pl.col('volume') > 0) & (pl.col('volume') < 0.01)).height
    zero = local.filter(pl.col('volume') == 0).height
    print(f'{base} window rows={local.height} vol_dtype={local.schema.get("volume")} tiny={tiny} zero={zero}')
    for sym in syms:
        loc1 = local.filter(pl.col('symbol') == sym)
        easy = p.get_minute([sym], datetime.now() - timedelta(days=4), None, asset_type=atype)
        cmp_minute(base, sym, loc1, easy)

print('===== 旧分区 tiny volume 抽查 (2024-01 与 2025-06 各一) =====')
for d in ('2024-01-02', '2025-06-06'):
    fp = f'{DATA}/kline_minute/date={d}/part.parquet'
    if not os.path.exists(fp):
        print(d, 'no partition'); continue
    df = pl.read_parquet(fp, columns=['symbol', 'volume'])
    tiny = df.filter((pl.col('volume') > 0) & (pl.col('volume') < 0.01)).height
    zero = df.filter(pl.col('volume') == 0).height
    print(f'{d} rows={df.height} tiny={tiny} zero={zero} vol_dtype={df.schema.get("volume")}')
print('COMPARE DONE')
PYEOF
echo "=== script exit: $? ==="
