# ZCODE 移交文档 v2 — 量化底座修复工程 + 全项目源码导览

> 移交时间：2026-09-06 | 移交方：ZCode 会话（量化底座缺口审查与修复 + ETF 污染事故 + 周末实时开关事故）
> 前置文档：`ZCODE_DEV_HANDOFF.md`（09-04 easy_tdx 切换工程，本档不重复，仅引用）
> Git：本地完整状态已推 `github.com/tying-sudo/tickflow-stock-panel` 分支 `deploy/v0.2.2-container`（commit `36b17df`）；**目标仓库 main = 上游合并分支，用户明令禁止覆盖**。

---

## 0. 一句话状态

**✅ 量化底座四项缺口修复 + 三个数据质量事故（分钟量缺损 / ETF 跨库污染 / 周末实时开关 409）全部闭环**，全部有服务器实测验证与回归测试锚定。项目已从"数据面板"进化为具备 PIT 财务因子、退市股框架、模拟盘闭环、每日对账的量化研究底座；**实盘执行层（broker/OMS）仍未建设**，是当前最大空白。

---

## 1. 项目全景（源码导览）

### 1.1 技术栈与运行形态

| 层 | 技术 | 运行位置 |
|---|---|---|
| 前端 | React 18 + TS + Vite dev server | CT100 `:3011`（`tickflow-dev.service` 内 vite dev，**直接服务 src 源码，改文件即热更新**） |
| 后端 | FastAPI + Polars + DuckDB + NumPy/Numba | CT100 `:3018`（同 service 内 uvicorn `--reload`——⚠️ 编辑文件会热重载并**杀掉进程内任务**） |
| 数据层 | Parquet 分区目录（无数据库服务），DuckDB 仅作查询视图 | `/opt/tickflow-stock-panel/data/` |
| 数据源 | easy-tdx serve 实例池（31 个 systemd 单元 :8000-8030） | CT100 本机，公网通达信 52 台候选 |
| 部署 | 本地 D:\ 工作区 → tar/scp → PVE 跳板 → pct push → CT100 | **非同步挂载**，md5 必核验 |

### 1.2 后端源码地图（`backend/app/`）

```
main.py                    # 应用装配: repo/engine/调度器/路由注册(31 个 router)
config.py                  # pydantic-settings; data_dir 解析链
parquet.py                 # DAILY/ENRICHED_STORAGE_SCHEMA + scan_*_compat 扫描器
                           #   ⚠️ 分区 schema 有历史漂移(quote_ts 列), 一律走这里扫描,
                           #   裸 pl.scan_parquet 会 SchemaError
price_limits.py            # 涨跌停规则引擎(symbol+date→幅度; ST/北交所/科创板分档)
market_time.py / enriched_generation.py / share_capital.py / db_safe.py
api/                       # 31 个路由模块(见 §1.4)
backtest/                  # 回测内核
  engine.py                # BacktestEngine: 面板/矩阵加载(load_panel*)+撮合(simulate)
                           #   + _instruments_for_backtest(活股∪退市合并) ⭐新
                           #   + _stock_universe_symbols(ETF 排除读点过滤) ⭐新
  strategy.py              # StrategyBacktestService.run(): 特征解析/信号/撮合/结果
                           #   + last_day_entries 末日信号字段 ⭐新
  fundamentals.py          # PIT 财务因子 ⭐重写: 法定披露期限兜底
  matrix.py                # MarketDataMatrix(TxN float32)+磁盘 mmap 缓存
  factor.py / mining*.py / walkforward.py / optimizer.py / worker.py
  minute_replay.py         # 分钟回放(T+0 日内策略)
indicators/
  pipeline.py              # compute_indicators/signals/limit_signals + run_pipeline
                           #   (enriched 重建: 全量/增量/除权三种模式)
  levels.py
strategy/
  engine.py                # StrategyEngine: 策略加载(builtin/custom/ai/composite)
  builtin/ 19 只内置策略    # near_limit_up 等涨停系 = matrix_native 后端
  monitor.py / monitor_rules.py  # 实时监控告警
  scoring.py / composite.py / custom_signals*.py
services/ 66 个            # 见 §1.5 重点服务
plugins/
  easy_tdx/                # ⭐ 生产数据源(见 §2)
_removed_plugins/
  tdx_gateway/             # 已下线归档(g4tic 通道已删, 端点 410)
jobs/
  daily_pipeline.py        # 盘前 09:10 维表 / 盘后 15:30 管道(默认16:00, 偏好可调)
tickflow/                  # repository.py(数据访问核心) / policy.py(档位能力) / client.py
```

### 1.3 测试（`backend/tests/`，149 个文件）

- 本地跑法：`cd backend && PYTHONUTF8=1 .venv/Scripts/python -m pytest tests -q`
  （⚠️ `uv run --frozen` 在本机不可用——会找错 python；pytest 需 `uv pip install pytest` 已装）
- 本次新增锚定测试：
  - `test_fundamental_factors.py`（PIT 兜底 +4 用例）
  - `test_delisted_instruments.py`（退市 sidecar + 引擎过滤）
  - `test_paper_trading.py`（模拟盘 6 用例）
  - `test_reconciliation.py`（对账 6 用例）
  - `test_matrix_plan_price_limit.py`（price_limit_pct 合成字段回归）

### 1.4 API 面（31 路由，前端 `lib/api.ts` 一一对应）

重点端点：`/api/backtest/strategy/stream`(SSE 回测)、`/api/paper/*` ⭐新(模拟盘 overview/settle/reset)、`/api/pipeline/run|jobs`、`/api/kline/minute*`、`/api/intraday/depth5|stream`、`/api/settings/preferences/*`、`/api/financials/*`、`/api/mining/*`、`/api/screener`、`/api/regime`、`/api/depth5`。

### 1.5 关键服务（`backend/app/services/`）

| 服务 | 职责 | 备注 |
|---|---|---|
| `paper_trading.py` ⭐新 | 模拟盘闭环 | 开关=`data/paper_trading/config.json`；详见 §3.3 |
| `reconciliation.py` ⭐新 | 每日对账 | 详见 §3.4 |
| `delisted_instruments.py` ⭐新 | 退市股 sidecar | 详见 §3.2 |
| `tick_archive.py` / `live_tick_service.py` | 分笔归档 / 当日分笔实时 | 15:35 EOD 归档；归档→live→消抖读取链 |
| `kline_sync.py` | 日K/分钟同步 | per-symbol 水位线跳过；分钟 volume=**股** |
| `financial_sync.py` | 财务五表 | 披露日历增量 + 新浪通道 31 并发 |
| `quote_service.py` | 实时行情轮询 | 盘前 flush quote_ts 置 null(修死循环)；休市探针 30min 复探 |
| `depth_service.py` | 五档/连板梯队定版 | 定版前置当日 enriched 等待 |
| `index_sync.py` | 指数/ETF 同步 | ⚠️ ETF 除权因子(sync_etf_adj_factor)疑似仍硬连 TickFlow 未修 |
| `data_integrity.py` | 完整性扫描 | "盘中快照"判定=quote_ts<当日15:00 → 曾是 409 事故根因 |

### 1.6 前端源码地图（`frontend/src/`）

- `pages/` 26 页：Dashboard/Watchlist/FundWatchlist/Indices/Backtest(+backtest/)/Mining/Screener/Regime/LimitUpLadder/Monitor/Financials/Data/Settings(+settings/)/Review/Analysis 系/AbnormalMoves/Auth/Onboarding/Dev/Branding
- `components/`：EChartsIntraday、StockMultiDayIntradayChart、TickTransactionsPanel(追加式流式)、Depth5Panel、stock-table/ 等
- `lib/`：`api.ts`(全 API 客户端+react-query)、`intraday-chart.ts`(时间网格/均价——⚠️ 时区与单位修复过，勿回退)、`useFinancials.ts` 等
- 构建验证：`pnpm build`（tsc -b + vite，`noUnusedLocals: true` 硬门槛）

---

## 2. 数据源与数据层（现状契约）

### 2.1 easy-tdx serve 池

- 31 实例 :8000-8030（`easy-tdx-main@8000` + `easy-tdx-worker@8001..8030`），easy-tdx 版本 **1.32.3**（升级纪律与回滚见记忆/tickflow 项目档案 §升级条目）。
- **quotes/当日分笔白名单仅 4 台公网机**（`DEFAULT_QUOTES_HOSTS`）；provider `_maybe_align` 自动把 worker 切到白名单机。
- **⚠️ 重启 serve 会丢 /server/switch 主机布局**——升级/重启前先快照 31 端口 `is_current`，重启后逐端口恢复（09-05 实操过 31/31 还原）。
- serve 单连接串行 → 并发=实例数；新浪通道并发勿超 31（62 并发触发限流崩塌）。

### 2.2 数据目录与单位契约（现行，**改动必读**）

| 表 | 位置 | volume 单位 |
|---|---|---|
| kline_daily(股票) | data/kline_daily/date=*/ | **手**（provider /100） |
| kline_minute(股票) | data/kline_minute/date=*/ | **股**（透传；勿再 ×100） |
| kline_etf_daily / kline_etf_minute | 同构目录 | 手 / 股 |
| kline_index_daily | | 量纲透传 |
| enriched(kline_daily_enriched) | 14 列窄表 | 手（volume 同日K） |
| financials/* 五表 | metrics/income/balance_sheet/cash_flow/shares | 股本=股（scale 1.0 已修） |
| transactions(分笔归档) | date=*/part.parquet | 手 |

**单位契约防回归**：`reconciliation.py` 每日管道跑三项校验（日K×100 vs Σ分钟[±3%]、收盘价互检[±0.1%]、600519/000001 股本量级探针），报告 `data/reconciliation/latest.json`+`history.jsonl`。改任何单位换算点前后必看它。

### 2.3 数据质量边界（已查明的口径噪声，勿当 bug 修）

- 北交所个别股（如 920371.BJ）分钟Σ/日K×100 可达 1.3（竞价 bar 口径），单只属正常。
- 09-03/09-04 分钟数据已全量重拉修复（中位 ratio=1.0000）；更早日期未回扫（分钟库 2024-01 起，如需体检可跑 `scripts/repair_minute_day.py <day> --dry`）。
- ETF 污染已清除（见 §3.1），备份在 `data/backup_etf_cleanup_20260906/`。

---

## 3. 本工程（09-05/09-06）完成明细

### 3.1 ETF 跨库污染事故（根因链三 bug，源头已封堵）

- **现象**：kline_daily 混入 1685 只 ETF（2026-08-18 起）；kline_minute 混入约一年半（578 分区 9,164 万行，其中 8,962 万行 ETF 库缺失=股票库是唯一副本）。
- **根因链**：①同步脚本 universe=instruments+instruments_etf 全写股票库；②mmin 增量合并 `unique(keep='first')` 让盘中部分 bar 永远压住完整 bar（09-04 全市场分钟量中位缺 18% 的根因）；③t0 "分区>100万行跳过"短路径让 16:30/20:30 治愈轮被跳过（缺口固化）。
- **修复**（`scripts/` 服务器端 + systemd 单元引用）：
  - `cleanup_etf_pollution.py`：迁移式清除（ETF 库缺失行并入 kline_etf_*，非删除），备份按分区落 `data/backup_etf_cleanup_20260906/`。两库残留实测 0。
  - `repair_minute_day.py`：坏日重拉修复（`--all` 全量/阈值检测模式，探针验证 provider 数据精确=日K×100）。09-03/09-04 已修，严校验 ±3% 全绿。
  - **`/opt/tickflow-stock-panel/scripts/minute_intraday_sync.py` 与 `t0_minute_backfill.py` 已重写部署**（旧版 `.bak-20260906-pollution`）：ETF 按 symbol 分流各自库、合并 `keep='last'`（fresh bar 优先）、t0 15:05 后治愈轮无条件执行且合并式写盘。**注意：这两个脚本在服务器 scripts/ 目录，不在 git 仓库 backend/scripts/ 里——仓库里的同代文件在 `ops_easytdx_20260904/`（旧版）**。
- enriched 已全量重建冲刷（1,488.6 万行）。

### 3.2 幸存者偏差修复（退市股框架）

- **审查误判更正**：1685 差集=ETF 非退市股；**真退市股历史从未拉取**（全库无末根K线早于 2026-06-01 的股票）。补齐需 gpcw（serve `/financial/file-list`，1988 年起 147 文件；⚠️ 实测端点路径与 1.30.3 记忆不符，重探 easy-tdx 1.32.3 实际路由）。
- 已落地框架：`delisted_instruments.py`（kline_daily-instruments 差集+30 天停滞守卫→sidecar 表 `data/instruments_delisted/part.parquet`，name=空串防 exclude_st 误杀）；`engine._instruments_for_backtest` 回测读点合并（**不进 repository.get_instruments()**——活股链路零影响）；`_stock_universe_symbols` 股票全市场轴收窄（ETF 排除，实测 5558）。当前 sidecar 0 行（无退市股在库）。

### 3.3 模拟盘闭环（`paper_trading.py` + `api/paper.py`）

- **机制**：每晚 settle(T) 跑策略回测 [start,T] 抽信号（close_t 口径+`last_day_entries` 末日信号字段）挂单 → settle(T+1) 用真实 OHLC 成交（开盘≥理论涨停不买 / ≤跌停顺延 5 日强平）→ 仓位=capital/max_positions 整百股 → 买×(1+fees) 卖×(1-fees-stamp) 无滑点 → holding_days 到期收盘强平 → trades/equity/orders parquet 落盘。
- **开关**：`data/paper_trading/config.json` 存在与否（当前=near_limit_up / start=2026-08-25 / 百万本金 / 10 仓 / holding=5）。管道 enriched 后自动 settle，失败只记 last_error。
- **已验证**：settle(09-02) 与 settle(09-04) 各 10 笔真实买入挂单；09-07（周一）管道将完成首次真实成交。
- **顺带修复的两个引擎存量 bug**：①`price_limit_pct` 被 enriched 存储白名单丢弃→涨停系 matrix 策略家族此前从未跑通（`_MATRIX_SYNTHESIZED_FIELDS` 修复+回归测试）；②撮合器不记录 sim 末日入场→`last_day_entries` 字段。

### 3.4 对账管道化（`reconciliation.py`）

见 §2.2 契约表。管道 stage `reconciliation`（92%），违例只记录不阻断。首跑即抓到 09-04 分钟量全市场缺损（后确认 82% 越界）——证明该防线有效。

### 3.5 周末实时开关 409 事故（09-06 下午，用户报障）

- **根因**：easy-tdx /bars 的 bar 级 timestamp（=北京零点）不分资产类型都带；09-04 修复只 strip 了 980xxx → 指数(5 行)+ETF(230 行) 09-04 分区带零点 quote_ts → 完整性扫描误判"盘中快照" → 开关被 409 门禁 → 修复任务重拉写回同样坏戳 → 无限循环。
- **修复**：provider `_daily_one` 的 `row.pop("timestamp")` 改**无条件**（部署版 `.bak-20260906-zerots`）；`scripts/scrub_zero_quote_ts.py` 清洗存量（235 行置 null，幂等）。验证：扫描 0 issues、开关 PUT 200、周末休市探针正常（轮询暂停 30min 复探=设计行为）。
- API 细节：`PUT /api/settings/preferences/realtime-quotes` body 字段=`realtime_quotes_enabled`。

### 3.6 PIT 财务（`fundamentals.py` 重写要点）

- 新浪通道 announce_date=None → `statutory_disclosure_deadline`（Q1→4/30、H1→8/31、Q3→10/31、年报→次年 4/30）兜底，真实公告日恒≤期限故无未来函数；同日撞期按报告期新者优先。快照 125431→125900 行（469 行恢复）。

---

## 4. 运维手册（CT100）

### 4.1 连接与部署

```bash
# SSH 一律经 PVE 跳板; 复合命令用 stdin 管道 (引号会被吃)
ssh -i ~/.ssh/id_ed25519_10.0.10.5_pve root@10.0.10.5 "pct exec 100 -- bash -s" <<'EOF'
...
EOF
# 文件: scp 到 PVE /tmp → pct push 100 /tmp/x /opt/... → md5 双侧核验
# ⚠️ Git Bash tar -czf 用 /c/... 路径 (C: 冒号被当远程主机)
```

- **部署守则**（血泪教训）：①部署前重读本地工作区当前版本（防并行会话覆盖）；②md5 必核验；③**北京 00:00-00:45 管道窗口禁推 backend 文件/升级 venv**（reload 杀任务）；④凌晨批量改动合并为一次 push；⑤长拉取走独立 venv 进程（`setsid nohup`，模式见 `ops_easytdx_20260904/rebuild_financials_standalone.py`）。
- 后端热重载即生效；改前端=改 CT100 上 `frontend/src` 源文件（vite 热更新）。
- 缓存刷新：`POST /api/data/refresh-cache`（磁盘重建后必做——后端内存 enriched 缓存不会自己感知独立进程写盘）。

### 4.2 systemd 单元全景

`tickflow-dev`（前后端 :3011/:3018）、`easy-tdx-main@8000`、`easy-tdx-worker@8001..8030`、`minute-pool.timer`（盘中每 2min）、`t0-minute.timer`（09:33/16:30/20:30 北京）。

### 4.3 周一（09-07）观察点

1. t0 09:33 早间轮 morning-round 短路径应正常跳过（分区已健康）；
2. 15:05 mmin 定盘轮 + 16:30 t0 治愈轮应产出 `keep='last'` 干净分区（ETF 分流）；
3. 管道 reconciliation 应保持 0 violations（北交所单只口径噪声属已知）；
4. 模拟盘 09-07 挂单应以真实开盘价成交（`/api/paper/overview` 可查）；
5. 实时开关周五已开+休市探针暂停 → 周一 09:30 前后应自动恢复轮询。

### 4.4 已知未修问题清单（按优先级）

1. **ETF 除权因子**（`sync_etf_adj_factor`）疑似硬连 TickFlow 未切 easy_tdx（同类 index_sync 老问题）。
2. **退市股历史补拉**：gpcw 枚举+逐 symbol 拉 bars（endpoint 路径待重探）。
3. **uvicorn --reload 生产化**：改为生产模式 systemd 守护（reload 杀任务是结构性隐患）。
4. **异地备份**：data/ 9.2G+ 分钟库无异地副本（仅零散 _bak 目录）。
5. **组合级风控/收益归因**：多策略资金分配、行业暴露上限、回撤熔断、alpha/beta 拆解均未建设。
6. **xdxr 精确除权**：阈值法 92 事件里 52 个伪事件（±0.15% 噪声），二期用 /xdxr 锚定（已验证可行）。
7. 前端 `pnpm build` 的 echarts 大 chunk 警告（既有）。
8. ZCode 平台坑：一会话一自动化；一次性自动化可能派发即失败（手动核验替代）。

---

## 5. Git 状态

- 本地分支 `deploy/v0.2.2-container` = 远端 `github` 同名分支（`36b17df`），包含本工程 4 个新提交：
  `932fd11`(quant-base 四修复) → `ecda897`(easy_tdx 入库+tdx_gateway 归档) → `d795a63`(services 切源) → `36b17df`(frontend+ops)。
- **main = 上游合并分支，禁止覆盖**；后续同步上游 = `git fetch github main` → rebase/merge 到部署分支。
- 推送凭证：PAT 经 `http.extraheader` 单次传递，不落 config；本机 api.github.com 有网络层反代（API 探测结果不可信，git 传输正常）。
- ⚠️ 用户 PAT 已在对话中暴露过，应已撤销；下次用新 token。

---

## 6. 快速上手三件事

1. **跑测试**：`cd backend && PYTHONUTF8=1 .venv/Scripts/python -m pytest tests -q`
2. **看数据健康**：`cat /opt/tickflow-stock-panel/data/reconciliation/latest.json`（在 CT100）
3. **看模拟盘**：`GET :3018/api/paper/overview`
