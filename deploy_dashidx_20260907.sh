#!/usr/bin/env bash
# 看板指数扩展 + 移动端3列适配 部署 (2026-09-07 16:0x 收盘后窗口)
# 后端 4 文件 + 前端 3 文件: scp -> PVE /tmp -> pct push -> md5 核验
set -e
KEY=~/.ssh/id_ed25519_10.0.10.5_pve
PVE=root@10.0.10.5
BASE="/d/Workbuddy工作空间/2026-08-02-15-04-38/tickflow-stock-panel"
CT=/opt/tickflow-stock-panel

FILES="backend/app/services/preferences.py backend/app/api/settings.py backend/app/api/overview.py backend/app/services/market_overview_builder.py frontend/src/lib/api.ts frontend/src/pages/Dashboard.tsx frontend/src/pages/Indices.tsx"

# PVE 侧按目录结构暂存
ssh -i "$KEY" "$PVE" "rm -rf /tmp/tf_dashidx"
for f in $FILES; do
  ssh -i "$KEY" "$PVE" "mkdir -p /tmp/tf_dashidx/$(dirname "$f")"
  scp -i "$KEY" -q "$BASE/$f" "$PVE:/tmp/tf_dashidx/$f"
done

echo "=== PVE 侧 md5 ==="
ssh -i "$KEY" "$PVE" "cd /tmp/tf_dashidx && md5sum $FILES"

echo "=== pct push ==="
for f in $FILES; do
  ssh -i "$KEY" "$PVE" "pct push 100 /tmp/tf_dashidx/$f $CT/$f"
done

echo "=== 容器内 md5 (须与 PVE 侧一致) ==="
ssh -i "$KEY" "$PVE" "pct exec 100 -- bash -c 'cd $CT && md5sum $FILES'"

echo "=== 本地 md5 (对照) ==="
cd "$BASE" && md5sum $FILES
