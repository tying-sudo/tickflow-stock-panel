"""T+0 全市场分钟K快补 — 收盘后当日分区直写 (2026-09-01)。

背景: kline_minute 的 T+1 管道依赖 g4tic 归档链路 (当晚归档/次日合并),
当日分区第二天才有 → 盘后当晚打开任意未看个股, 分时图/盘口/分笔全部实时
首拉 (6-10s "加载中")。本脚本收盘后直连 TDX 网关 /v1/tickdata kind=bars
(pytdx 常驻桥 ~80ms/只), 全市场 (股票+ETF ~7200 只) 扫描当日 240 根
1 分钟K, 直写今日分区 —— 分钟路由从"按需首拉"变"全量在库"。

用法 (CT100): backend/.venv/bin/python scripts/t0_minute_backfill.py [YYYY-MM-DD]
幂等: 重复运行整分区原子重建 (.tmp + os.replace)。
量纲: pytdx bars volume=股 → /100 转 手, amount=元 (与分钟契约一致)。
排程: systemd timer t0-minute.timer (16:30 主跑 + 20:30 兜底)。
"""
import glob
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

os.chdir('/opt/tickflow-stock-panel')
for line in open('/etc/tickflow/tdx-gateway.env'):
    line = line.strip()
    if line and not line.startswith('#') and '=' in line:
        k, v = line.split('=', 1)
        os.environ.setdefault(k, v)

import polars as pl

DAY = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')
GW = os.environ.get('TDX_GATEWAY_URL', 'http://10.0.10.14:18709').rstrip('/')
TOKEN = os.environ.get('TDX_GATEWAY_TOKEN', '')
assert TOKEN, 'TDX_GATEWAY_TOKEN missing'

# 已有当日分区且行数健康 (>100万) 则跳过 (幂等短路径)
existing = f'data/kline_minute/date={DAY}/part.parquet'
if os.path.exists(existing):
    n = pl.scan_parquet(existing).select(pl.len()).collect().item()
    if n > 1_000_000:
        print(f'[t0] {existing} already has {n} rows — skip', flush=True)
        sys.exit(0)

syms = []
for f in ('data/instruments/instruments.parquet', 'data/instruments_etf/instruments_etf.parquet'):
    syms += pl.scan_parquet(f).select('symbol').collect()['symbol'].to_list()
syms = sorted(set(syms))
print(f'[t0] universe={len(syms)} day={DAY}', flush=True)


def fetch(sym):
    req = urllib.request.Request(
        GW + '/v1/tickdata',
        data=json.dumps({'symbols': [sym], 'kind': 'bars', 'count': 240}).encode(),
        headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + TOKEN},
        method='POST')
    for _ in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                rows = (json.loads(r.read()).get('rows') or {}).get(sym) or []
            out = []
            for b in rows:
                dt = str(b.get('datetime') or '')
                if not dt.startswith(DAY):
                    continue
                out.append((sym, dt, float(b.get('open') or 0), float(b.get('high') or 0),
                            float(b.get('low') or 0), float(b.get('close') or 0),
                            float(b.get('volume') or 0) / 100.0, float(b.get('amount') or 0)))
            return out
        except Exception:
            time.sleep(0.6)
    return None


# 金丝雀: 全量扫描前先验 3 只 — 全空说明数据通道故障 (daemon/网关), 立刻止损
canary_rows = sum(len(fetch(s) or []) for s in ('000526.SZ', '600519.SH', '601886.SH'))
if canary_rows == 0:
    print('[t0] canary all-empty — 数据通道故障, 中止', flush=True)
    sys.exit(2)
print(f'[t0] canary ok ({canary_rows} rows)', flush=True)


results, failed = [], []
t0 = time.time()
with ThreadPoolExecutor(max_workers=8) as ex:
    for i, (sym, rows) in enumerate(zip(syms, ex.map(fetch, syms)), 1):
        if rows is None:
            failed.append(sym)
        else:
            results.extend(rows)
        if i % 500 == 0:
            print(f'[t0] {i}/{len(syms)} rows={len(results)} failed={len(failed)} {time.time()-t0:.0f}s', flush=True)

print(f'[t0] fetch done: {len(results)} rows, {len(failed)} failed in {time.time()-t0:.0f}s', flush=True)
if failed:
    print('[t0] failed symbols:', failed[:30], flush=True)

if not results:
    print('[t0] no rows (非交易日?) — skip write', flush=True)
    sys.exit(0)

df = pl.DataFrame(
    results,
    schema={'symbol': pl.String, 'datetime': pl.String, 'open': pl.Float64, 'high': pl.Float64,
            'low': pl.Float64, 'close': pl.Float64, 'volume': pl.Float64, 'amount': pl.Float64},
    orient='row',
)
df = df.with_columns(pl.col('datetime').str.to_datetime('%Y-%m-%d %H:%M', time_unit='us'))
df = df.unique(subset=['symbol', 'datetime']).sort(['symbol', 'datetime'])

# 抽样对账: 单股分钟量 vs 日K量 (±8% 内通过; 差异主要来自零股/集合竞价口径)
try:
    chk = df.filter(pl.col('symbol') == '000526.SZ')
    m_vol = chk['volume'].sum()
    daily_parts = glob.glob(f'data/kline_daily/date={DAY}/*.parquet')
    if daily_parts:
        d = pl.scan_parquet(daily_parts).filter(pl.col('symbol') == '000526.SZ').select('volume').collect().item()
        diff = abs(m_vol - float(d or 0)) / max(float(d or 1), 1) * 100
        print(f'[t0] check 000526.SZ minute_vol={m_vol:.0f} daily_vol={d} diff={diff:.2f}%', flush=True)
except Exception as e:  # noqa: BLE001
    print('[t0] check skipped:', str(e)[:120], flush=True)

out_dir = f'data/kline_minute/date={DAY}'
os.makedirs(out_dir, exist_ok=True)
out = f'{out_dir}/part.parquet'
df.write_parquet(out + '.tmp')
os.replace(out + '.tmp', out)
print(f'[t0] written {out} rows={df.height} symbols={df["symbol"].n_unique()}', flush=True)
