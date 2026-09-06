#!/bin/bash
# 对拍: 本地 parquet 历史(VM102 时代, 手) vs easy_tdx 现拉(股), 验证单位倍数关系
echo "--- data/ 顶层目录 ---"
ls /opt/tickflow-stock-panel/data/ | head -30
echo "--- t0-minute / minute-pool 单元定义 ---"
grep -E "ExecStart|OnCalendar|OnUnitActiveSec" /etc/systemd/system/t0-minute.service /etc/systemd/system/t0-minute.timer /etc/systemd/system/minute-pool.timer 2>/dev/null
echo "--- VM102 首次不可达时间 (tickflow-dev 日志) ---"
journalctl -u tickflow-dev --no-pager --since "2026-09-03 00:00" 2>&1 | grep -m1 "No route to host"
echo ""
cd /opt/tickflow-stock-panel/backend
timeout 400 .venv/bin/python -u - <<'PYEOF'
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
    return pl.concat([pl.read_parquet(x, columns=None) for x in sel], how='diagonal_relaxed')

def ratio_report(tag, sym, local, easy, keys):
    if local.is_empty() or easy.is_empty():
        print(f'{tag} {sym}: EMPTY local={local.height} easy={easy.height}')
        return
    e = easy.select(*[[pl.col(k).cast(pl.String).alias(k) if k == 'date' else k for k in keys]])
    m = local.select(*keys).join(e, on=[k for k in keys if k in ('date', 'datetime')], how='inner',
                                 suffix='_easy')
    if m.is_empty():
        print(f'{tag} {sym}: NO OVERLAP local={local.height} easy={easy.height}')
        return
    out = [f'{tag} {sym} n={m.height}']
    for col in ('close', 'volume', 'amount'):
        if f'{col}_easy' not in m.columns:
            continue
        r = (pl.col(col) / pl.col(f'{col}_easy'))
        m2 = m.filter(pl.col(f'{col}_easy') > 0)
        med = m2.select(r.alias('r'))['r'].median()
        out.append(f'{col} med_ratio={med:.6f}')
    print(' | '.join(out))

# ---- 日K 对拍: 股票 + ETF (验证 easy_tdx /100 是否成立) ----
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
        ratio_report(base, sym, loc1, easy, ['date', 'close', 'volume', 'amount'])

# ---- 指数日K 对拍 (验证 vol 透传) ----
for cand in ('kline_index_daily', 'kline_index', 'index_daily'):
    if os.path.isdir(f'{DATA}/{cand}'):
        local = load_window(cand, 40).filter(pl.col('symbol') == '000001.SH')
        easy = p.get_daily(['000001.SH'], datetime.now() - timedelta(days=45), None, asset_type='index')
        ratio_report(cand, '000001.SH', local, easy, ['date', 'close', 'volume', 'amount'])
        break
else:
    print('index daily dir not found (检查 data/ 顶层列表)')

# ---- 分钟对拍: 股票 + ETF (验证 手 vs 股 = 100 倍) ----
for base, syms, days in [('kline_minute', ['600519.SH'], 4), ('kline_etf_minute', ['510300.SH'], 4)]:
    local = load_window(base, days)
    if local.is_empty():
        print(base, 'NO LOCAL WINDOW'); continue
    # 异常 volume 量化 (denormal 探查)
    tiny = local.filter((pl.col('volume') > 0) & (pl.col('volume') < 0.01)).height
    zero = local.filter(pl.col('volume') == 0).height
    print(f'{base} window rows={local.height} dtype={local.schema.get("volume")} tiny(0<v<0.01)={tiny} zero={zero}')
    for sym in syms:
        loc1 = local.filter(pl.col('symbol') == sym)
        easy = p.get_minute([sym], datetime.now() - timedelta(days=days), None, asset_type='etf' if 'etf' in base else 'stock')
        if loc1.is_empty() or easy.is_empty():
            print(base, sym, 'EMPTY', loc1.height, easy.height); continue
        m = loc1.select('datetime', 'close', 'volume', 'amount').join(
            easy.select('datetime', 'close', 'volume', 'amount'), on='datetime', how='inner', suffix='_easy')
        m2 = m.filter((pl.col('volume_easy') > 0) & (pl.col('volume') > 0.01))
        vr = m2.select((pl.col('volume') / pl.col('volume_easy')).alias('r'))['r']
        ar = m2.select((pl.col('amount') / pl.col('amount_easy')).alias('r'))['r']
        px = m.select((pl.col('close') - pl.col('close_easy')).abs().max()).item()
        print(f'{base} {sym} n={m.height} vol_med_ratio={vr.median() if len(vr) else None} '
              f'amt_med_ratio={ar.median() if len(ar) else None} close_maxdiff={px}')
print('COMPARE DONE')
PYEOF
echo "=== compare done ==="
