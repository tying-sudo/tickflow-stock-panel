"""盘中全市场分钟K增量同步 — minute-pool.timer 每 2 分钟触发 (2026-09-02)。

背景: t0-minute 每天仅 09:33/16:30/20:30 三次冷启动扫, 盘中分钟分区断供
(09:44 起无人写入)。本脚本接管盘中增量: 直连 TDX 网关 /v1/tickdata
kind=bars (pytdx 常驻桥), 全市场 (股票+ETF ~7271 只) 按缺口增量拉取:
  - 已有当日分区: 按 max(datetime) 算缺口, 只拉 缺口分钟+8 根 (cap 240)
  - 无当日分区:   全量拉 240 根 (可提前接管 09:33 冷启动扫职责)
幂等合并 unique(symbol,datetime), 原子写 (.tmp + os.replace)。

自gate: 仅 A 股连续竞价时段运行 (北京时间 09:31-11:30 / 13:00-15:01),
午休/收盘/周末自动退出; 节假日靠 canary 全空判定, 警告后安静退出。
量纲: pytdx bars volume=股 -> /100 转 手, amount=元 (与分钟契约一致)。

用法 (CT100): backend/.venv/bin/python scripts/minute_intraday_sync.py [--force]
  --force: 跳过交易时段门控 (部署验证/手动补跑用)。
systemd: minute-pool.service (ExecStart 本脚本, TimeoutStartSec=10min),
oneshot 语义保证不重叠 — 上一轮未跑完时 timer 触发自动跳过。
"""
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

os.chdir('/opt/tickflow-stock-panel')
for line in open('/etc/tickflow/tdx-gateway.env'):
    line = line.strip()
    if line and not line.startswith('#') and '=' in line:
        k, v = line.split('=', 1)
        os.environ.setdefault(k, v)

import polars as pl

GW = os.environ.get('TDX_GATEWAY_URL', 'http://10.0.10.14:18709').rstrip('/')
TOKEN = os.environ.get('TDX_GATEWAY_TOKEN', '')
assert TOKEN, 'TDX_GATEWAY_TOKEN missing'

FORCE = '--force' in sys.argv
BJ = timezone(timedelta(hours=8))
now = datetime.now(BJ)
DAY = now.strftime('%Y-%m-%d')

# ── 交易时段门控 (北京时间 09:31-11:30 / 13:00-15:01; 周末退出) ──
hm = now.hour * 60 + now.minute
in_session = (570 <= hm <= 690) or (780 <= hm <= 901)  # 09:31-11:30, 13:00-15:01
if not in_session and not FORCE:
    print(f'[mmin] non-session (bj={now:%H:%M}) — exit', flush=True)
    sys.exit(0)
if now.weekday() >= 5 and not FORCE:
    print(f'[mmin] weekend (bj={now:%A}) — exit', flush=True)
    sys.exit(0)

existing = f'data/kline_minute/date={DAY}/part.parquet'
old = None
count = 240
gap_min = None
if os.path.exists(existing):
    old = pl.read_parquet(existing)
    max_dt = old['datetime'].max()
    gap_min = (now.replace(tzinfo=None) - max_dt).total_seconds() / 60.0
    count = min(240, max(8, int(gap_min) + 8))
gtxt = '-' if gap_min is None else f'{gap_min:.1f}min'
print(f'[mmin] bj={now:%H:%M} gap={gtxt} count={count} old_rows={old.height if old is not None else 0}', flush=True)

syms = []
for f in ('data/instruments/instruments.parquet', 'data/instruments_etf/instruments_etf.parquet'):
    syms += pl.scan_parquet(f).select('symbol').collect()['symbol'].to_list()
syms = sorted(set(syms))


def fetch(sym):
    req = urllib.request.Request(
        GW + '/v1/tickdata',
        data=json.dumps({'symbols': [sym], 'kind': 'bars', 'count': count}).encode(),
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


# 金丝雀: 全量扫描前先验 3 只 — 全空多为节假日或数据通道故障
canary_rows = sum(len(fetch(s) or []) for s in ('000526.SZ', '600519.SH', '601886.SH'))
if canary_rows == 0:
    print('[mmin] WARNING canary all-empty (节假日或通道故障) — exit 0', flush=True)
    sys.exit(0)
print(f'[mmin] canary ok ({canary_rows} rows)', flush=True)

results, failed = [], []
t0 = time.time()
with ThreadPoolExecutor(max_workers=8) as ex:
    for i, (sym, rows) in enumerate(zip(syms, ex.map(fetch, syms)), 1):
        if rows is None:
            failed.append(sym)
        else:
            results.extend(rows)
        if i % 1000 == 0:
            print(f'[mmin] {i}/{len(syms)} rows={len(results)} failed={len(failed)} {time.time()-t0:.0f}s', flush=True)

print(f'[mmin] fetch done: {len(results)} rows, {len(failed)} failed in {time.time()-t0:.0f}s', flush=True)
if len(failed) > 0.3 * len(syms):
    print(f'[mmin] abort: failed {len(failed)}/{len(syms)} >30% — 本轮不写盘', flush=True)
    sys.exit(1)
if failed:
    print('[mmin] failed symbols:', failed[:20], flush=True)

new_df = pl.DataFrame(
    results,
    schema={'symbol': pl.String, 'datetime': pl.String, 'open': pl.Float64, 'high': pl.Float64,
            'low': pl.Float64, 'close': pl.Float64, 'volume': pl.Float64, 'amount': pl.Float64},
    orient='row',
)
new_df = new_df.with_columns(pl.col('datetime').str.to_datetime('%Y-%m-%d %H:%M', time_unit='us'))

if old is not None:
    if new_df.height == 0:
        print('[mmin] no new rows — skip write', flush=True)
        sys.exit(0)
    comb = pl.concat([old, new_df])
else:
    comb = new_df
comb = comb.unique(subset=['symbol', 'datetime']).sort(['symbol', 'datetime'])

out_dir = f'data/kline_minute/date={DAY}'
os.makedirs(out_dir, exist_ok=True)
out = f'{out_dir}/part.parquet'
comb.write_parquet(out + '.tmp')
os.replace(out + '.tmp', out)
added = comb.height - (old.height if old is not None else 0)
print(f'[mmin] written {out} rows={comb.height} (+{added}) symbols={comb["symbol"].n_unique()}', flush=True)
