"""盘中全市场分钟K增量同步 — minute-pool.timer 每 2 分钟触发 (2026-09-04 改接 easy-tdx)。

背景: t0-minute 每天仅 09:33/16:30/20:30 三次冷启动扫, 盘中分钟分区断供
(09:44 起无人写入)。本脚本接管盘中增量: 直连本机 easy-tdx serve 实例池
(EASY_TDX_WORKER_PORTS 默认 8000-8006, MAC 协议, 公网通达信服务器),
全市场 (股票+ETF ~7271 只) 按缺口增量拉取:
  - 已有当日分区: 按 max(datetime) 算缺口, 只拉 缺口分钟+8 根 (cap 240)
  - 无当日分区:   全量拉 240 根 (可提前接管 09:33 冷启动扫职责)
幂等合并 unique(symbol,datetime), 原子写 (.tmp + os.replace)。

自gate: 仅 A 股连续竞价时段运行 (北京时间 09:31-11:30 / 13:00-15:01),
午休/收盘/周末自动退出; 节假日靠 canary 全空判定, 警告后安静退出。
量纲 (2026-09-04 起新契约): easy_tdx 分钟 volume=股, amount=元 — 全部透传。
历史库已由 scripts/migrate_minute_vol_x100.py 统一为股 (标记
data/.minute_vol_unit_migrated); 旧 VM102 /v1/tickdata + /100 链路废弃。

用法 (CT100): backend/.venv/bin/python scripts/minute_intraday_sync.py [--force]
  --force: 跳过交易时段门控 (部署验证/手动补跑用)。
systemd: minute-pool.service (ExecStart 本脚本, TimeoutStartSec=10min),
oneshot 语义保证不重叠 — 上一轮未跑完时 timer 触发自动跳过。
"""
import http.client
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

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


def bars_path(sym, count):
    mc = split_symbol(sym)
    if mc is None:
        return None
    market, code = mc
    count = max(8, min(int(count), 240))
    return f'/bars?market={market}&code={code}&category=MIN_1&start=0&count={count}&adjust=NONE'


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

PORTS = alive_ports()
if not PORTS:
    print('[mmin] ABORT: no alive easy-tdx worker — exit 1', flush=True)
    sys.exit(1)
print(f'[mmin] easy-tdx workers: {PORTS}', flush=True)

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


def fetch(sym, port):
    path = bars_path(sym, count)
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


# 金丝雀: 全量扫描前先验 3 只 — 全空多为节假日或数据通道故障
canary_syms = ('000526.SZ', '600519.SH', '601886.SH')
canary_rows = sum(len(fetch(s, PORTS[i % len(PORTS)]) or []) for i, s in enumerate(canary_syms))
if canary_rows == 0:
    print('[mmin] WARNING canary all-empty (节假日或通道故障) — exit 0', flush=True)
    sys.exit(0)
print(f'[mmin] canary ok ({canary_rows} rows)', flush=True)

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
            if done_cnt[0] % 1000 == 0:
                print(f'[mmin] {done_cnt[0]}/{len(syms)} rows={len(results)} failed={len(failed)} {time.time()-t0:.0f}s', flush=True)


with ThreadPoolExecutor(max_workers=len(PORTS)) as ex:
    list(ex.map(run_group, PORTS, groups))

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
print(f'[mmin] written {out} rows={comb.height} (+{added}) symbols={comb["symbol"].n_unique()} volume_unit=股', flush=True)
