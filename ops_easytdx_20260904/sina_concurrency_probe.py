"""新浪 f10 通道并发能力实测 (2026-09-05 凌晨, 盘前低风险窗口).

目的: 确定 serve /sina/financial-report 的并发模型, 指导 _SINA_CONCURRENCY 调优.
实验:
  T1 单实例串行 8 请求      → 单实例串行吞吐
  T2 单实例 8 线程并发      → 若耗时 ≈ T1 → 实例内串行 (池上限=实例数)
  T3 31 实例跨实例并发 8 个 → 跨实例吞吐
  T4 24 线程跨 31 实例拉 24 只 → 当前生产形态实测
输出: 各组耗时/吞吐/HTTP 错误数.
"""
import http.client
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor

CODES = ["600519", "000001", "300750", "601312", "000858", "600036",
         "002415", "600276", "000651", "601899", "300059", "600030",
         "002594", "600900", "601166", "300124", "000333", "600887",
         "002304", "601088", "300274", "600809", "000568", "601633"]

def fetch(port, code, timeout=30):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        conn.request("GET", f"/api/v1/sina/financial-report?code={code}&type=lrb&num=8",
                     headers={"Accept": "application/json"})
        resp = conn.getresponse()
        raw = resp.read()
        return resp.status, len(raw)
    except Exception as e:
        return -1, str(e)[:60]
    finally:
        conn.close()

def run_group(tag, jobs, workers):
    """jobs = [(port, code), ...]"""
    errors = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(lambda j: fetch(*j), jobs))
    dt = time.time() - t0
    errors = sum(1 for s, _ in results if s != 200)
    rows = sum(r for s, r in results if s == 200 and isinstance(r, int))
    rate = len(jobs) / dt if dt > 0 else 0
    print(f"{tag}: {len(jobs)} req in {dt:.2f}s = {rate:.1f} req/s, errors={errors}, bytes={rows}")
    return dt

# T1: 单实例串行
jobs1 = [(8001, c) for c in CODES[:8]]
run_group("T1 单实例8001串行x8 ", jobs1, 1)

# T2: 单实例 8 线程并发 (同一实例)
jobs2 = [(8001, c) for c in CODES[:8]]
run_group("T2 单实例8001并发x8 ", jobs2, 8)

# T3: 8 个不同实例各 1 请求
jobs3 = [(8000 + i, CODES[i]) for i in range(8)]
run_group("T3 跨实例各1并发x8   ", jobs3, 8)

# T4: 生产形态 — 24 线程跨 31 实例
jobs4 = [(8000 + (i % 31), CODES[i]) for i in range(24)]
run_group("T4 24线程跨31实例    ", jobs4, 24)

# T5: 极限探测 — 62 线程跨 31 实例 (每实例 2 在途)
jobs5 = [(8000 + (i % 31), CODES[i % len(CODES)]) for i in range(62)]
run_group("T5 62线程跨31实例    ", jobs5, 62)

# T6: 复跑 T4 确认稳定性
run_group("T6 24线程复跑        ", jobs4, 24)
print("DONE")
