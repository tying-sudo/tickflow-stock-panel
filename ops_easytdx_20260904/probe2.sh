#!/bin/bash
# 排查: VM102 何时不可达 / tickflow-dev 加载状态 / preferences 全文 / 09-03 分区完整性
echo "--- minute-pool.service 最近日志 (VM102 失败起点) ---"
journalctl -u minute-pool.service --no-pager -n 40 2>&1 | tail -40
echo ""
echo "--- minute-pool 首次报错时间 ---"
journalctl -u minute-pool.service --no-pager --since "2026-09-03 12:00" 2>&1 | grep -iE "error|fail|refus|route|timeout" | head -5
journalctl -u minute-pool.service --no-pager --since "2026-09-03 12:00" 2>&1 | grep -iE "error|fail|refus|route|timeout" | tail -3
echo ""
echo "--- tickflow-dev 最近日志 (provider 加载) ---"
journalctl -u tickflow-dev --no-pager -n 300 2>&1 | grep -iE "provider|easy.?tdx|tdx_gateway|plugin" | tail -15
echo ""
echo "--- tickflow-dev 最近 ERROR ---"
journalctl -u tickflow-dev --no-pager -n 200 2>&1 | grep -iE "error|traceback|exception" | tail -8
echo ""
echo "--- preferences.json 全文 ---"
cat /opt/tickflow-stock-panel/data/user_data/preferences.json
echo ""
echo "--- PVE 层 VM102 连通性 (对照) ---"
echo "(在容器内 ping 测试, CT100 可能禁 ping)"
ping -c 2 -W 2 10.0.10.14 2>&1 | tail -2
timeout 5 bash -c 'echo > /dev/tcp/10.0.10.14/18709' 2>&1 && echo "TCP 18709 OPEN" || echo "TCP 18709 CLOSED/FILTERED"
echo ""
echo "--- 09-03 分钟分区完整性 (最后 bar 时间 + volume 抽样) ---"
cd /opt/tickflow-stock-panel/backend
timeout 60 .venv/bin/python -u -c "
import polars as pl
df = pl.read_parquet('/opt/tickflow-stock-panel/data/kline_minute/date=2026-09-03/part.parquet')
print('rows:', df.height, 'cols:', df.columns)
sub = df.filter(pl.col('symbol') == '600519.SH')
print('600519 rows:', sub.height)
print('last 3 bars:', sub.sort('datetime').tail(3).select('datetime','close','volume').to_dicts())
print('volume range:', sub.select(pl.col('volume').min()).item(), '-', sub.select(pl.col('volume').max()).item())
"
echo "=== probe done ==="
