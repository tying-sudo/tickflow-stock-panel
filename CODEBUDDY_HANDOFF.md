# 项目移交文档 — tick-stock-panel（2026-08-31）

> 交接方：CodeBuddy 会话（v0.2.2 升级 + 容器统一重建 + 五档盘口面板）
> 接收方：WorkBuddy
> 本文包含：当前状态 / 本次完成工作 / 未完成事项 / 关键记忆与坑 / 操作手册

---

## 一、项目当前状态（2026-08-31 13:49 UTC 全部验证通过）

- **版本**：上游 v0.2.2（2026-08-30 发布）+ 全部容器定制，功能无缺失
- **本地仓库**：`D:\Workbuddy工作空间\2026-08-02-15-04-38\tickflow-stock-panel`
  - `main` 分支 = 上游 v0.2.2（4d27f31）
  - **`deploy/v0.2.2-container` 分支 = 部署用集成分支（容器与它完全同源）**，领先 main 5 个提交：
    1. `7585ff6` 基金分组后端 + 休市日实时落盘周末守卫 + watchlist_enriched symbols 参数
    2. `d0f1f3b` 收编容器侧后端工作（depth5_api/tick_download/constituency 多源判定/multi-source holdings/fuyao THS）
    3. `fc276e4` 个股详情等宽布局 + 原生五档盘口面板
    4. `26465ff` 五档数据源改 TDX 网关优先（tickflow 兜底）
    5. `68b1689` 五档面板升级为实时订单列表（买卖 5+5 档、单快照原子渲染、变动闪烁）
- **远程**：`origin = https://github.com/shy3130/tick-stock-panel`（旧名 tickflow-stock-panel 已改名；本地未推送 deploy 分支）
- **生产部署**：PVE 宿主机 `10.0.10.5` 的 **LXC 100 "Tick-Stock-Panel"**（IP `10.0.10.25`）
  - 后端 uvicorn :3018（`--reload`，systemd 服务名 `tickflow-dev.service`）
  - 前端 vite dev :3011（HMR）
  - 部署目录 `/opt/tickflow-stock-panel`，数据目录 `data/`（含 .env、secrets、分组映射等，**不可覆盖**）
- **最近回滚备份**：容器 `/opt/backup_mixed_20260831_134920.tgz`（升级前混合状态）

### 已上线功能清单
- v0.2.2 上游全部功能（能力路由矩阵、fuyao 同花顺插件、分钟策略回测、异动中心、龙虎榜+盘前风向标复盘、板块分时、盘中分钟增量等）
- 基金/ETF 成分股分组：`add-etf-group`（ETF F10 全量/季报 top10）+ 场外基金（pingzhongdata 前十大）+ **自动同步**（cron 工作日 16:10 `etf_groups_sync`；手动 `POST /api/watchlist/sync-etf-groups`；多源判定 constituency.py：csindex→cnindex→fuyao THS→披露兜底）
- 场外基金净值落盘：`fund_sync`（cron 工作日 18:00；`/api/data/funds/sync|status`；数据页"基金"卡片）
- 五档盘口面板：个股详情右侧实时订单列表（买卖 5+5 档、单快照原子渲染、变动高亮闪烁）；**数据源 TDX 网关优先**（无套餐限制），TickFlow 兜底
- 逐笔成交：`/api/kline/transactions`（TDX 网关）
- 休市日实时落盘周末守卫（防假日K脏分区把看板打成全 0）

---

## 二、未完成事项（按优先级）

1. **depth5 双实现待合并**：`intraday.py` 的 `/api/intraday/depth5`（前端面板在用）与 `api/depth5_api.py`（另一会话所建，含 tick_download）功能重叠。两者目前共存无冲突，建议后续合并去重（保留 TDX 优先逻辑）。
2. **前端"基金入组"UI 缺失**：后端 `/api/fund/*`（搜索/持仓/导入）保留可用，但前端入口在 v0.2.2 的 Watchlist 重构中未重建（旧 UI 基于已废弃的单分组架构被放弃）。可选：基于新 M:N 分组体系重做，或把容器验证过的 `add-etf-group` 前端交互移植过来。
3. **工作日节假日实时落盘风险**：周末守卫已挡住周六/周日；但法定节假日（工作日休市）手动刷新行情仍可能产生假日K脏分区（症状：看板全 0）。临时处置：删掉 `data/kline_daily*/date=<假日>` 分区 + 重启。根治需接入交易日历。
4. **tickflow 档位为 Free**：实时五档已绕开（走 TDX），但其他 Pro+ 能力（如 tickflow 深度分钟）受限于档位；`capabilities.json` 探针结果为 Free。
5. **deploy 分支未推送 GitHub**：如需备份到远程，走镜像 `git push https://gh-proxy.com/https://github.com/shy3130/tick-stock-panel.git deploy/v0.2.2-container`。
6. **htc_crack 临时脚本未清理**：`c:\Users\Administrator\CodeBuddy\20260830002804\htc_crack\` 下有本会话的 probe/deploy/diff 脚本（safe-delete 守卫拦截了批量删除），可手动删除。
7. **浏览器插件"五档盘口"浮层**：不是本应用功能（代码中不存在），是用户浏览器扩展注入的，遮挡图表需用户自行在扩展管理中禁用。
8. **intraday.py 的 `logging` 导入**：depth5 端点内 `import logging` 在函数内做了（顶部已有全局 logger，可顺手清理为直接用 `logger`）。

---

## 三、关键记忆（必须知道的操作知识与坑）

### 基础设施
- **PVE 宿主机**：`ssh root@10.0.10.5`（key `C:\Users\Administrator\.ssh\id_ed25519_10.0.10.5_pve`）
  - LXC 100 "Tick-Stock-Panel"（10.0.10.25）：本看板。进容器 `pct exec 100 -- ...`，传文件 `pct push/pull`
  - VM 102 "windows-TDX"（10.0.10.14）：通达信 + TdxGateway（`C:\TdxGateway`，HTTP :18709）；命令通道 `htc_crack/vm_exec.py`
  - LXC 101 Niulink、LXC 103 Fund-app
- **TDX 网关插件**：`backend/app/plugins/tdx_gateway/`（独立目录，勿散落到核心代码）；`TDX_GATEWAY_TOKEN` 存 secrets.json/.env；网关只暴露 /v1/health、/v1/kline、/v1/realtime、/v1/tickdata；README 有能力边界，勿加交易路由

### 部署流程（可复用脚本思路）
1. 本地打包 tar：backend/app（剔除 `__pycache__`/`*.bak*`）、pyproject.toml、uv.lock、frontend/src、package.json
2. scp → PVE → `pct push 100 → /opt/xxx.tgz`
3. 容器内：先备份 `tar czf /opt/backup_<说明>_$TS.tgz ...` → `tar xzf --no-same-owner`（**Windows 打包必须加，否则 chown 失败中止**）→ 替换 backend/app、frontend/src、pyproject、uv.lock、package.json → `cd backend && /root/.local/bin/uv sync` → `systemctl restart tickflow-dev`
4. 冒烟：`/api/overview/market`、`/api/data/status`（含 funds 字段）、`/api/watchlist/groups`、`/api/intraday/depth5?symbol=`、前端 3011 HTTP 200
5. 日志：`journalctl -u tickflow-dev.service`

### 踩坑记录（重要）
- **PowerShell 中文路径不可靠**：`D:\Workbuddy工作空间\...` 在内联命令里会被 GBK/UTF-8 搞坏（间歇性）。**git/文件操作用 git-bash 脚本文件方式**（write_to_file 写 .sh → git-bash 执行）；或 cmd 短路径 `D:\WORKBU~2\...`（git 对短路径支持差，仅用于存在性判断）
- **容器 .env 是 CRLF**：bash `source` 会给变量附 `\r`；独立 python 进程测试必须显式 `DATA_DIR=/opt/tickflow-stock-panel/data`
- **git-bash 内 ssh 用 -i 连 PVE 会 publickey 失败**：容器文件同步一律走 PowerShell scp/ssh
- **`.git` 曾被外部破坏**（refs/ 目录被删 → "not a git repository"）：先查 `.git/refs/` 是否存在，pack 文件完好时重建 refs 即可恢复
- **多会话并发编辑容器会互相覆盖**：恢复/重部署前先 `find backend/app frontend/src -mmin -150` 确认无活跃编辑；本地 deploy 分支是所有工作的汇聚点
- **独立 python 进程读不到 .env**（uvicorn --env-file 只注入服务进程）：测试 fuyao/依赖 env 的功能必须走 HTTP 端点
- **request() 抛错只含 detail 文本**（不含 HTTP 状态码）：前端判断错误类型不能 `includes('403')`；轮询请求加 `{ quiet: true }` 防 toast 刷屏

### 数据契约
- 分组：`watchlist_groups.json`（M:N，group_ids 多值）+ `etf_group_sources.json`（分组→基金映射，kind: etf|fund）
- 五档：`tf.depth.get` → MarketDepth{ask_prices/ask_volumes/bid_prices/bid_volumes, timestamp}；TDX Buyp/Buyv/Sellp/Sellv 缺级以 0 价格补位（端点过滤 p<=0，涨停单档属正常）
- 涨跌停价：`price_limits.polars_limit_price` 已加 NaN→null 守卫，勿回退

---

## 四、快速回滚
```bash
# 容器内
cd /opt/tickflow-stock-panel
tar xzf /opt/backup_mixed_20260831_134920.tgz -C /tmp/restore --no-same-owner  # 按需取回
# 或整体恢复 backend/app + frontend/src 后 systemctl restart tickflow-dev
```
