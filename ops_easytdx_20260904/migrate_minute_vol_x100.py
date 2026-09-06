#!/usr/bin/env python
"""历史分钟库 volume 手→股 ×100 迁移 (2026-09-04, easy_tdx 切换配套).

范围: data/kline_minute/date=*/part.parquet + data/kline_etf_minute/date=*/part.parquet
依据: 双源对拍 2026-09-04 — 本地(手) vs easy_tdx(股) vol_med_ratio=0.0100 (股票+ETF 一致),
      日K/指数库 ratio=1.0 不动。迁移后本地分钟库与 easy_tdx provider 输出同单位(股)。

幂等三重防护 (防 ×200):
  1. 总标记 data/.minute_vol_unit_migrated (JSON) 存在 → 拒绝执行
  2. 进度日志 data/.minute_x100_progress.log 逐分区 fsync 追加 → 断点续跑跳过已完成分区
  3. 内容级探测: kline_minute 最新分区 600519 尾盘 bar 若已 >1000 (股量级) → 视为已迁移,
     仅补写标记后退出; 若 <1 则人工检查

运行约束: 北京时间 9:00-15:00 (盘中) 拒绝执行; 每分区 tmp 写入 + fsync + 原子 rename。
所有路径经 _safe() 校验, 限制在 DATA 根目录内。
"""
import glob
import json
import os
import sys
import time
from pathlib import Path

import polars as pl

DATA = '/opt/tickflow-stock-panel/data'
MARKER = DATA + '/.minute_vol_unit_migrated'
PROGRESS = DATA + '/.minute_x100_progress.log'
BASES = ['kline_minute', 'kline_etf_minute']
PROBE_SYMBOL = '600519.SH'


def fail(msg: str) -> None:
    print(f'[ABORT] {msg}', flush=True)
    sys.exit(1)


def _safe(path: str) -> str:
    """校验路径: 必须落在 DATA 根目录内, 拒绝任何 .. 分量."""
    parts = os.path.normpath(path).split(os.sep)
    if '..' in parts:
        fail(f'路径含 .. 分量, 拒绝: {path}')
    rp = os.path.realpath(path)
    root = os.path.realpath(DATA)
    if rp != root and not rp.startswith(root + os.sep):
        fail(f'路径越界 (超出 {root}): {path}')
    return path


def write_json(path: str, payload: dict) -> None:
    _safe(path)
    Path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def bj_now() -> time.struct_time:
    return time.gmtime(time.time() + 8 * 3600)


def main() -> None:
    t0 = time.time()
    print(f'[start] {time.strftime("%F %T", bj_now())} 北京时间', flush=True)

    if os.path.exists(_safe(MARKER)):
        fail(f'总标记已存在: {MARKER} — 已迁移过, 拒绝重复执行 (×200 防护)')

    hour = bj_now().tm_hour
    if 9 <= hour < 15:
        fail('北京时间盘中 (9:00-15:00) 禁止执行, 避免 t0/minute-pool 写入竞争')

    # ---- 内容级防重探测 ----
    probe_path = _safe(DATA + '/kline_minute/date=2026-09-03/part.parquet')
    pre_samples: dict[str, tuple[str, float]] = {}
    if os.path.exists(probe_path):
        df = pl.read_parquet(probe_path)
        tail = df.filter(pl.col('symbol') == PROBE_SYMBOL).sort('datetime').tail(1)
        if not tail.is_empty():
            v = float(tail['volume'][0])
            dt = str(tail['datetime'][0])
            print(f'[probe] {PROBE_SYMBOL} 09-03 尾盘 bar {dt} volume={v}', flush=True)
            if v > 1000:
                write_json(MARKER, {'already_migrated': True, 'probe_volume': v,
                                    'probed_at': time.strftime('%F %T', bj_now())})
                print('[OK] 数据已是股单位 (无需 ×100), 仅补写总标记后退出', flush=True)
                return
            if v < 1:
                fail(f'探测 volume={v} 异常 (预期 手量级 1-1e4), 人工检查后再跑')
            pre_samples[PROBE_SYMBOL] = (dt, v)

    # ---- 附加分钟库侦测 (漏迁防护; *_bak_* 为 08-28 单位修复冻结备份, 不迁移) ----
    extra = [d for d in sorted(os.listdir(DATA))
             if 'minute' in d.lower() and d not in BASES
             and '_bak_' not in d.lower() and os.path.isdir(os.path.join(DATA, d))]
    if extra:
        fail(f'发现未纳入迁移范围的分钟库目录 {extra} — 确认是否需要 ×100 后把目录加入 BASES 再跑')

    # ---- 断点续跑进度 ----
    done: set[str] = set()
    if os.path.exists(_safe(PROGRESS)):
        with open(_safe(PROGRESS), encoding='utf-8') as f:
            done = {line.strip() for line in f if line.strip()}
        print(f'[resume] 进度日志存在, 已完成 {len(done)} 分区, 跳过它们', flush=True)

    partitions: list[str] = []
    for base in BASES:
        partitions.extend(sorted(glob.glob(DATA + '/' + base + '/date=*')))
    partitions = [_safe(p) for p in partitions if os.path.isdir(p)]
    print(f'[plan] 共 {len(partitions)} 分区 ({", ".join(BASES)})', flush=True)

    total_rows = 0
    tiny_before = 0
    zero_cnt = 0
    migrated = 0
    for i, pdir in enumerate(partitions, 1):
        rel = os.path.relpath(pdir, DATA)
        pf = _safe(pdir + '/part.parquet')
        if not os.path.exists(pf):
            print(f'[warn] {rel} 无 part.parquet, 跳过', flush=True)
            continue
        if rel in done:
            continue
        df = pl.read_parquet(pf)
        n = df.height
        if 'volume' not in df.columns:
            fail(f'{rel} 缺 volume 列, 人工检查')
        tiny = df.filter((pl.col('volume') > 0) & (pl.col('volume') < 0.01)).height
        zero = df.filter(pl.col('volume') == 0).height
        df = df.with_columns((pl.col('volume').cast(pl.Float64) * 100.0).alias('volume'))
        tmp = _safe(pf + '.tmp_x100')
        df.write_parquet(tmp)
        fd = os.open(tmp, os.O_RDWR)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, pf)
        with open(_safe(PROGRESS), 'a', encoding='utf-8') as f:
            f.write(rel + '\n')
            f.flush()
            os.fsync(f.fileno())
        total_rows += n
        tiny_before += tiny
        zero_cnt += zero
        migrated += 1
        if i % 40 == 0 or i == len(partitions):
            rate = total_rows / max(time.time() - t0, 1e-6)
            print(f'[progress] {i}/{len(partitions)} 行={total_rows} ({rate / 1e6:.2f}M 行/s) '
                  f'tiny={tiny_before} zero={zero_cnt}', flush=True)

    # ---- 迁移后抽样验证 ----
    print('[verify] 抽样复核 ×100 结果', flush=True)
    ok = True
    for sym, (dt, v_pre) in pre_samples.items():
        df = pl.read_parquet(probe_path)
        tail = df.filter(pl.col('symbol') == sym).sort('datetime').tail(1)
        v_post = float(tail['volume'][0])
        expect = v_pre * 100.0
        status = 'OK' if abs(v_post - expect) < max(expect * 1e-9, 1e-6) else 'FAIL'
        if status == 'FAIL':
            ok = False
        print(f'[verify] {sym} {dt} pre={v_pre} post={v_post} expect={expect} → {status}', flush=True)
    if not ok:
        fail('抽样验证失败, 总标记未写入; 修复后依据进度日志续跑')

    write_json(MARKER, {
        'migrated_at': time.strftime('%F %T', bj_now()) + ' 北京时间',
        'bases': BASES,
        'partitions_total': len(partitions),
        'partitions_migrated_this_run': migrated,
        'rows': total_rows,
        'tiny_volume_rows_before': tiny_before,
        'zero_volume_rows': zero_cnt,
        'duration_s': round(time.time() - t0, 1),
        'note': 'volume 手→股 ×100; 进度日志 data/.minute_x100_progress.log 保留备查',
    })
    print(f'[DONE] partitions={len(partitions)} rows={total_rows} '
          f'duration={round(time.time() - t0, 1)}s tiny_before={tiny_before} zero={zero_cnt}', flush=True)


if __name__ == '__main__':
    main()
