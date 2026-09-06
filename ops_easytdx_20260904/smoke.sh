#!/bin/bash
# 2026-09-04 凌晨冒烟: easy-tdx serve 池 + provider + VM102 可达 + 迁移前置盘点
echo "=== time: $(date -u '+%F %T') UTC (北京时间 $(date -u -d '+8hour' '+%F %T')) ==="
echo "--- services ---"
for s in easy-tdx-main easy-tdx-worker@8001 easy-tdx-worker@8002 easy-tdx-worker@8003 easy-tdx-worker@8004 easy-tdx-worker@8005 easy-tdx-worker@8006 tickflow-dev minute-pool.timer; do
  printf "%-26s %s\n" "$s" "$(systemctl is-active "$s" 2>&1)"
done
echo "--- listen ports 8000-8006 ---"
ss -tln | awk '{print $4}' | grep -E ':800[0-6]$' | sort | tr '\n' ' '; echo
echo "--- VM102 gateway probe (对拍前置) ---"
cd /opt/tickflow-stock-panel/backend
timeout 15 .venv/bin/python -u -c "
import urllib.request
try:
    r = urllib.request.urlopen('http://10.0.10.14:18709/', timeout=6)
    print('gateway HTTP', r.status)
except Exception as e:
    print('gateway probe:', type(e).__name__, str(e)[:120])
"
echo "--- provider smoke (六端点抽样) ---"
timeout 150 .venv/bin/python -u -c "
import sys; sys.path.insert(0, '.')
from app.plugins.easy_tdx.provider import EasyTdxProvider, availability
print('availability:', availability())
p = EasyTdxProvider()
for ds in ('daily', 'minute'):
    r = p.test_dataset(ds, ['600519.SH'])
    print(ds, 'rows:', r['rows'], 'cols:', r['columns'])
    for row in r['preview'][:2]:
        print('   ', row)
r = p.test_dataset('realtime', ['600519.SH'])
print('realtime rows:', r['rows'])
for row in r['preview'][:1]:
    print('   ', row)
r = p.test_dataset('depth5', ['600519.SH'])
print('depth5 rows:', r['rows'])
for row in r['preview'][:1]:
    print('   ', {k: row.get(k) for k in list(row)[:6]})
r = p.test_dataset('financial', ['600519.SH'])
print('financial rows:', r['rows'])
r = p.test_dataset('adj_factor', ['600519.SH'])
print('adj_factor rows:', r['rows'])
"
echo "--- preferences 现状 (6 provider keys) ---"
grep -o '"[a-z0-9_]*_data_provider"[[:space:]]*:[[:space:]]*"[^"]*"' /opt/tickflow-stock-panel/data/user_data/preferences.json || echo "PREFS GREP EMPTY - 检查路径"
echo "--- 分钟分区盘点 ---"
echo "kline_minute 分区数: $(ls /opt/tickflow-stock-panel/data/kline_minute/ 2>/dev/null | wc -l)"
ls /opt/tickflow-stock-panel/data/kline_minute/ 2>/dev/null | head -2
ls /opt/tickflow-stock-panel/data/kline_minute/ 2>/dev/null | tail -2
echo "kline_etf_minute 分区数: $(ls /opt/tickflow-stock-panel/data/kline_etf_minute/ 2>/dev/null | wc -l)"
ls /opt/tickflow-stock-panel/data/kline_minute/$(ls /opt/tickflow-stock-panel/data/kline_minute/ | tail -1)/ 2>/dev/null
echo "迁移标记: $(ls /opt/tickflow-stock-panel/data/.minute_vol_unit_migrated 2>&1 | tail -1)"
echo "磁盘: $(df -h /opt/tickflow-stock-panel/data 2>/dev/null | tail -1 | awk '{print $2" total, "$4" free"}')"
echo "--- minute-pool timer 排程 ---"
systemctl list-timers --all 2>/dev/null | grep -i minute || echo "无 minute timer"
echo "--- 同步脚本现状 (前 5 行) ---"
head -5 /opt/tickflow-stock-panel/scripts/minute_intraday_sync.py 2>&1
echo "=== smoke done ==="
