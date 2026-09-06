"""T+0 全市场分钟K快补 — 收盘后当日分区直写 (2026-09-04 改接 easy-tdx)。

背景: kline_minute 的 T+1 管道依赖 g4tic 归档链路 (当晚归档/次日合并),
当日分区第二天才有 → 盘后当晚打开任意未看个股, 分时图/盘口/分笔全部实时
首拉 (6-10s "加载中")。本脚本收盘后直连本机 easy-tdx serve 实例池
(EASY_TDX_WORKER_PORTS 默认 8000-8006), 全市场 (股票+ETF ~7200 只)
扫描当日 240 根 1 分钟K, 直写今日分区 —— 分钟路由从"按需首拉"变"全量在库"。

用法 (CT100): backend/.venv/bin/python scripts/t0_minute_backfill.py [YYYY-MM-DD]
幂等: 重复运行整分区原子重建 (.tmp + os.replace)。
量纲 (2026-09-04 起新契约): easy_tdx 分钟 volume=股, amount=元 — 全部透传。
历史库已由 scripts/migrate_minute_vol_x100.py 统一为股; 旧 VM102
/v1/tickdata + /100 链路废弃 (VM102 = 冷备)。
排程: systemd timer t0-minute.timer (09:33/16:30/20:30 北京时间)。
"""
import glob
import http.client
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

os.chdir('/opt/tickflow-stock-panel')

import polars as pl

API_HOST = '127.0.0.1'  # easy-tdx serve 实例池固定走本机回环


def worker_ports():
    raw = os.environ.get('EASY_TDX_WORKER_PORTS', '8000,8001,8002,8003,8004,8005,8006')
    ports = [int(p) for p in raw.split(',') if p.strip().isdigit() and 0 < int(p) < 65536]
    return ports or [8000]


def api(port, path, timeout=15):
    """仅允许本机回环 + 白名单端口/路径 (防 SSRF: 主机/协议固定, 路径白名单, 不跟随重定向)."""
    if not (isinstance(port, int) and 0 < port < 65536):
        raise ValueError(f'blocked port {port!r}')
    if not (path == '/market/session' or path.startswith('/bars?')):
        raise ValueError(f'blocked api path {path[:40]!r}')
    conn = http.client.HTTPConnection(API_HOST, port, timeout=timeout)
    try:
        conn.request('GET', '/api/v1' + path, headers={'Accept': 'application/json'})
        resp = conn.getresponse()
        payload = json.loads(resp.read())
    finally:
        conn.close()
    return payload


def alive_ports():
    out = []
    for p in worker_ports():
        try:
            api(p, '/market/session', timeout=5)
            out.append(p)
        except Exception:
            continue
    return out


def split_symbol(sym):
    code, _, market = sym.partition('.')
    market = market.upper()
    if market not in ('SH', 'SZ', 'BJ') or not (code.isdigit() and len(code) == 6):
        return None
    return market, code


def bars_path(sym, count=240):
    mc = split_symbol(sym)
    if mc is None:
        return None
    market, code = mc
    return f'/bars?market={market}&code={code}&category=MIN_1&start=0&count={count}&adjust=NONE'


DAY = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')

# 已有当日分区且行数健康 (>100万) 则跳过 (幂等短路径)
existing = f'data/kline_minute/date={DAY}/part.parquet'
if os.path.exists(existing):
    n = pl.scan_parquet(existing).select(pl.len()).collect().item()
    if n > 1_000_000:
        print(f'[t0] {existing} already has {n} rows — skip', flush=True)
        sys.exit(0)

PORTS = alive_ports()
if not PORTS:
    print('[t0] ABORT: no alive easy-tdx worker', flush=True)
    sys.exit(1)
print(f'[t0] easy-tdx workers: {PORTS}', flush=True)

syms = []
for f in ('data/instruments/instruments.parquet', 'data/instruments_etf/instruments_etf.parquet'):
    syms += pl.scan_parquet(f).select('symbol').collect()['symbol'].to_list()
syms = sorted(set(syms))
print(f'[t0] universe={len(syms)} day={DAY}', flush=True)


def fetch(sym, port):
    path = bars_path(sym)
    if path is None:
        return None
    for _ in range(2):
        try:
            rows = api(port, path).get('data') or []
            out = []
            for b in rows:
                dt = str(b.get('datetime') or '').replace('T', ' ')[:16]
                if not dt.startswith(DAY):
                    continue
                out.append((sym, dt, float(b.get('open') or 0), float(b.get('high') or 0),
                            float(b.get('low') or 0), float(b.get('close') or 0),
                            float(b.get('vol') or 0), float(b.get('amount') or 0)))
            return out
        except Exception:
            time.sleep(0.6)
    return None


# 金丝雀: 全量扫描前先验 3 只 — 全空说明数据通道故障或非交易日, 立刻止损
canary_syms = ('000526.SZ', '600519.SH', '601886.SH')
canary_rows = sum(len(fetch(s, PORTS[i % len(PORTS)]) or []) for i, s in enumerate(canary_syms))
if canary_rows == 0:
    print('[t0] canary all-empty — 数据通道故障或非交易日, 中止', flush=True)
    sys.exit(2)
print(f'[t0] canary ok ({canary_rows} rows)', flush=True)

# 每实例同时 1 个在途请求 (serve 单连接 IO 串行): 符号按端口分组, 组内串行
groups = [[] for _ in PORTS]
for i, s in enumerate(syms):
    groups[i % len(PORTS)].append(s)

results, failed = [], []
t0 = time.time()
done_cnt = [0]
dlock = threading.Lock()


def run_group(port, group):
    for sym in group:
        rows = fetch(sym, port)
        if rows is None:
            failed.append(sym)
        else:
            results.extend(rows)
        with dlock:
            done_cnt[0] += 1
            if done_cnt[0] % 500 == 0:
                print(f'[t0] {done_cnt[0]}/{len(syms)} rows={len(results)} failed={len(failed)} {time.time()-t0:.0f}s', flush=True)


with ThreadPoolExecutor(max_workers=len(PORTS)) as ex:
    list(ex.map(run_group, PORTS, groups))

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

# 抽样对账: 单股分钟量(股)/100 vs 日K量(手) (±8% 内通过; 差异主要来自零股/集合竞价口径)
try:
    chk = df.filter(pl.col('symbol') == '000526.SZ')
    m_vol = chk['volume'].sum() / 100.0
    daily_parts = glob.glob(f'data/kline_daily/date={DAY}/*.parquet')
    if daily_parts and m_vol:
        d = pl.scan_parquet(daily_parts).filter(pl.col('symbol') == '000526.SZ').select('volume').collect().item()
        diff = abs(m_vol - float(d or 0)) / max(float(d or 1), 1) * 100
        print(f'[t0] check 000526.SZ minute_vol={m_vol:.0f}(手) daily_vol={d}(手) diff={diff:.2f}%', flush=True)
except Exception as e:  # noqa: BLE001
    print('[t0] check skipped:', str(e)[:120], flush=True)

out_dir = f'data/kline_minute/date={DAY}'
os.makedirs(out_dir, exist_ok=True)
out = f'{out_dir}/part.parquet'
df.write_parquet(out + '.tmp')
os.replace(out + '.tmp', out)
print(f'[t0] written {out} rows={df.height} symbols={df["symbol"].n_unique()} volume_unit=股', flush=True)
