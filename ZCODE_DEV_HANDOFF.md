# ZCODE 全量移交文档 — easy_tdx 数据源切换工程

> 移交时间：2026-09-04 凌晨 | 移交方：WorkBuddy 会话 | 接收方：ZCode AI 平台
> 工程目标：**弃用 VM102 数据中台（10.0.10.14 TdxW 网关），全量切换到 easy-tdx（本机 serve 实例池直连公网通达信服务器）**

---

## 0. 一句话状态

**✅ 切换已全部完成（2026-09-04 凌晨，ZCode 会话执行，详见 §9）。** easy_tdx 已接入生产（preferences 5 个 key），历史分钟库已 ×100 迁移为股（9.39 亿行），前端/后端单位换算点已改，盘中同步脚本已重写并部署，tickflow-dev 已重启加载。VM102 于 09-03 23:42（北京）起网络层死亡，按冷备保留。剩余：首个交易日盘中盯 MIN_1（已布 09:40 自动化检查）+ 二期增强（§5 任务 7）。

## 1. 决策背景（用户指令链）

1. 2026-09-03 晚：评估 github.com/handsomejustin/easy_tdx（v1.30.3，1089 star，自研 TDX 二进制协议非 pytdx 封装），实测性能达标（6 连接 124-133 sym/s、批量报价 ~1700 sym/s、全市场分钟增量推算 ~59s），评估报告：`D:\Workbuddy工作空间\量化\easy_tdx_eval\easy_tdx替换VM102评估报告.md`。
2. 用户拍板：**直接引进、立即弃用 VM102**；多服务器轮询实时切换最优；**分钟K vol 单位完全按新源（股）**；easy-tdx 以 serve Web 服务形态常驻 CT100，端口按源库默认 **8000**。
3. 用户截图（easy-tdx Web UI 服务器设置页，47/52 可达）确认要求：服务器池可视化管理 + 轮询切换。

## 2. 新架构（已部署）

```
tickflow 后端 (CT100 :3018)
  └─ EasyTdxProvider 插件 (app/plugins/easy_tdx/)
       └─ HTTP → 本机 easy-tdx serve 实例池 (7 个 systemd 服务)
            ├─ easy-tdx-main.service      :8000  UI 实例（用户可访问 http://10.0.10.25:8000 看服务器设置页）
            └─ easy-tdx-worker@8001..8006 :8001-8006  纯 API worker (--no-ui)
                 ├─ MAC 大池: bars/minute（47+ 台候选，from_best_host 自动选优）
                 └─ quotes 子池: realtime/depth5（仅 4 台白名单机支持标准协议五档命令）
                      → 公网通达信行情服务器 (52 台候选)
```

**关键事实（已实测）**：
- 52 台候选服务器中**仅 4 台支持标准协议 `get_security_quotes`（五档）**：`180.153.18.170 / 115.238.56.198 / 115.238.90.165 / 218.75.126.9`。其余 43 台返回空行（云服务器节点禁用该命令），5 台连不通。白名单硬编码在 provider 的 `DEFAULT_QUOTES_HOSTS`，可用环境变量 `EASY_TDX_QUOTES_HOSTS` 覆盖。
- provider 周期（300s）检查各实例 `current_host`，不在白名单的实例会被 `POST /server/switch` 热切换到空闲白名单机（无需重启）。8002 已手动切换到 `115.238.90.165`（用户截图中的当前服务器）。
- serve 内部是**单连接共享客户端**（`app.state.tdx_client` + `mac_client`，`_io_lock` 串行）→ 并发靠 7 实例 × 每实例 1 在途请求（provider `fan_out` 按实例分组扇出）。
- `/server/switch` 只切标准协议 client；**MAC client 的 host 在启动时 from_best_host 定死，无运行时切换 API**。MAC host 劣化时的换机手段 = `systemctl restart easy-tdx-worker@N`（重启重新测速选优）。二期可加自动 restart 策略。

## 3. 已完成清单（全部有验证证据）

| # | 项 | 证据 |
|---|---|---|
| 1 | CT100 后端 venv 安装 `easy-tdx==1.30.3`（含 pandas 降级 3.0.3→2.3.3，后端主用 polars 无影响） | uv pip install 成功，import 版本验证 |
| 2 | 7 个 serve 实例 systemd 部署（开机自启 + 崩溃 5s 重启） | `ss -tlnp` 8000-8006 全监听 |
| 3 | 插件三件套 `backend/app/plugins/easy_tdx/{provider.py, plugin.yaml, __init__.py}`（本地副本与 CT100 已同步，MD5 一致的部署版） | 六端点冒烟通过 |
| 4 | quotes 白名单 4 台扫描确认 | 扫描脚本输出（43 空 + 5 拒） |
| 5 | `/server/switch` 热切换验证 | 8002 → 115.238.90.165 ok |
| 6 | QFQ 复权有效性验证 | 600519 05-28：NONE=1275.98 vs QFQ=1247.96（差 28.02 元分红） |
| 7 | 六端点冒烟（详见 §4 对拍数据） | smoke 输出存证 |

**开发中修掉的 4 个 bug（血泪，勿重蹈）**：
1. **死锁**：`_snapshot_ports` 持 `threading.Lock` 时调 `_refresh_quotes_ports()`（内部再取同一锁）→ `get_realtime` 首次请求永久挂起（冒烟 1 小时无输出的根因；stdout 管道缓冲掩盖了卡点位置）。修复：锁外同步扫描 + `RLock` 双保险。
2. **日线 date=null**：easy-tdx `/bars` 日期是 ISO 带时间 `'2026-09-03T00:00:00'`，polars `str.cast(Date)` 只认 `'YYYY-MM-DD'` → 全 null，连带 adj_factor join 落空（0 行）。修复：DAY 分支 `row["date"][:10]` 切片。
3. **adj_factor 噪声**：easy_tdx QFQ 按分舍入 → 非事件日 ratio ±0.03% 浮动，1e-9 阈值下茅台 180 天混入 74 个伪事件。修复：阈值提到 0.001（真实分红/送转 ≥0.1%）。剩余边缘：0.1%-0.2% 小额事件可能漏检，**精度不如 tdx_gateway 的 ForwardFactor 边界法**；二期用 `/xdxr` 事件表精确定位（已实测可用：茅台 45 事件含 fenhong 值）。
4. **financial 日期**：`updated_date` 是 `'20260815'` 紧凑格式 → `_iso_date()` 规范化。

## 4. 单位契约总表（最重要，所有换算都在 provider 边界完成）

实测对拍基准：600519 09-03 全天（easy_tdx vs 现库/VM102 双源）。

| 数据集 | 字段 | easy_tdx 原生 | 输出契约（入库） | 处理 |
|---|---|---|---|---|
| **minute**（新契约，用户决策） | volume | **股**（600519 15:00 bar vol=18400） | **股** | 透传 |
| minute | amount | 元（23899392） | 元 | 透传 |
| daily 股票/ETF | vol | 股（1774765） | 手（17747.65） | **/100** |
| daily 股票/ETF | amount | 元 | 元 | 透传 |
| daily 指数 | vol | 手量纲（496990176，与现库同） | 手 | **透传**（注意：指数在 easy_tdx 原生就是"手"，与股票不同！） |
| realtime | vol | 手（17747） | 手 | 透传（与日K契约一致，实时覆写当日日线安全） |
| realtime | amount | 元 | 元 | 透传 |
| depth5 | 档位量 | 手（bid_vol1=4） | 手 | 透传（前端 Depth5Panel ×100 显示股，无需改） |
| financial | zong_guben/liutong_guben | 万股（125008.15625） | 股 | ×1e4 |

⚠️ **历史分钟库（kline_minute / kline_etf_minute 全部分区）现库 volume=手，新数据=股 → 必须迁移 ×100，否则成交量类指标在同一列内单位断层（差 100 倍）。** 这是切换前的硬性前置（见 §5 任务 3）。

指数 symbol 无需转换（`000001.SH` 上证指数直接映射 `(SH, "000001")`，TDX 协议层即如此；平安银行是 `000001.SZ` 不同市场无歧义）。

## 5. 未完成任务（按顺序执行）

### 任务 1：最后一轮双源对拍（VM102 还在线，切走前唯一窗口）
对拍 6-8 只标的（含 ETF 510300/159915、指数、1 只封板股）的 daily/minute/realtime/depth5，VM102(tdx_gateway) vs easy_tdx。重点：ETF 日K vol /100 是否成立（目前只对拍了股票+指数）、封板股 depth5 零占位语义。写对拍脚本跑一次即可。

### 任务 2：切换 preferences（接入生产）
`/opt/tickflow-stock-panel/data/user_data/preferences.json` 六个 key 全部 `tdx_gateway` → `easy_tdx`：
`daily_data_provider / adj_factor_provider / minute_data_provider / realtime_data_provider / depth5_data_provider / financial_data_provider`。
然后 `systemctl restart tickflow-dev`，观察日志无 provider 加载错误，设置页插件列表出现 "easy-tdx 通达信协议直连"。
**⚠️ 顺序：必须在任务 3（历史迁移）完成后切，或切换当天立即补迁移**——否则当晚分钟同步写入的（股）与历史（手）断层。

### 任务 3：历史分钟库 volume ×100 迁移（手→股）
- 范围：`data/kline_minute/date=*/part.parquet`（2024-01 起 ~400+ 分区，每分区 7271 只全市场）+ `data/kline_etf_minute/date=*/`（少量分区）
- 操作：逐分区读 → `volume × 100` → 原子写回（tmp 文件 + rename）。幂等性风险：**重复执行会 ×200**！迁移脚本必须落一个标记文件（如 `data/.minute_vol_unit_migrated`），或按"迁移前后抽样对拍"防重。
- 验证：迁移后抽 3 个分区 3 只标的，volume 与 VM102 时代 /100 前的原始值（=现值×100）对拍；前端分时图均价线无 100 倍跳变。
- 体量：~50GB parquet 重写，CT100 NVMe 预计 20-40 分钟，盘后执行。

### 任务 4：前端分时均价线适配（一行）
`frontend/src/lib/intraday-chart.ts:16`：`volume += row.volume * 100` → `volume += row.volume`（分钟 volume 已是股）。前端 :3011 vite 热更新，推源码即生效。
**不要动**：Depth5Panel.tsx / TickTransactionsPanel.tsx（盘口/分笔量仍是手）、EChartsCandlestick.tsx 的 turnoverRate（日线 volume 仍是手）。

### 任务 5：盘中分钟同步脚本适配
`scripts/minute_intraday_sync.py`（CT100，systemd timer `minute-pool.timer` 每 2 分钟）目前**直连 VM102 `/v1/tickdata`**，需改为调 easy_tdx provider 的 `get_minute`（或直接 HTTP `GET /api/v1/bars?category=MIN_1&count=240`）。
⚠️ **盘中 MIN_1 实时性未验证**（盘后实测含当日全天，但"盘中 14:58 时能否拉到 14:57 bar"需首个交易日盘中确认）。若盘中不可见，兜底路径：`GET /api/v1/minute`（当日分时 240 点）。**切换后第一个交易日上午必须盯此链路。**

### 任务 6：VM102 下线（不删服务）
- preferences 切走后 VM102 自然无流量；观察 1-2 天无回切需求后：
- CT100 上与 VM102 相关的 timer/服务检查一遍（minute-pool.timer 已在任务 5 改造）
- VM102 侧（10.0.10.14）TDX-Gateway 计划任务、TdxW-AutoStart **保留不删**（回切兜底，成本为零）
- 项目记忆/文档中标注 VM102 = 冷备

### 任务 7（二期，非阻塞）
- financial 历史多期：接 `/sina/financial-report`（新浪三表，easy-tdx serve 已代理）；当前只有最新单期快照（`latest_only=False` 会抛错，属预期）
- adj_factor 精确化：用 `/xdxr` 事件表替代阈值法
- serve 池 MAC host 自动 restart 策略（当前手动）
- easy-tdx 独有能力接入：逐笔成交 `/transaction`、分时 `/minute`、集合竞价 `/mac/auction`、异动 `/mac/unusual`（面板分笔面板可直接受益）

## 9. 2026-09-04 凌晨执行记录（ZCode 会话，全部完成并验证）

### 现场与本文档早前状态的差异（重要）
1. **preferences 已全部切换**（本文档 §0 原记"仍是 tdx_gateway"已过时）：daily/minute/realtime/depth5/adj_factor = `easy_tdx`；**financial = `fuyao` 保持不动**（fuyao 有历史多期，强于 easy_tdx 单期快照）。
2. **tickflow-dev 当时未重启**：preferences 切了但内存里还是 tdx_gateway，而 **VM102 已于 09-03 23:42（北京）起网络层死亡**（ping/TCP 全不通）→ 实时链路当时处于报错状态，重启后恢复。
3. 任务 1 的"活体双源对拍"窗口已关闭（VM102 死），改为 **parquet 历史库（VM102 时代数据）vs easy_tdx 现拉** 对拍，结论等价且直接覆盖迁移前置验证。

### 执行清单（顺序即依赖）
| 步骤 | 结果 |
|---|---|
| 冒烟：7 实例全 active、六端点全通、09-03 分区完整到 15:00（600519 收盘 1298.88 与 easy_tdx realtime 一致） | ✅ |
| 对拍：股票/ETF 日K vol ratio=1.000000（**ETF /100 成立**）；指数透传成立；股票/ETF 分钟 ratio=0.0100（**×100 迁移方向与范围确认**） | ✅ |
| 迁移：`scripts/migrate_minute_vol_x100.py`，720 分区 938,576,188 行，179s；抽样 600519 尾盘 184.0→18400.0 精确 ×100；幂等三重防护（总标记 `data/.minute_vol_unit_migrated` + 逐分区进度日志断点续跑 + 内容级探测防 ×200） | ✅ |
| 同步脚本重写：`minute_intraday_sync.py`（每2分钟增量）+ `t0_minute_backfill.py`（09:33/16:30/20:30）直连本机 serve 池 `/api/v1/bars`，volume 股透传（**不再 /100**）；7 worker 分组扇出每实例 1 在途；canary/时段门控/30% 失败熔断/原子写全保留；旧版留 `.vm102bak`；gate 测试通过 | ✅ |
| 单位换算点清查（grep 全库）：前端 `intraday-chart.ts` ×100→×1（分时均价，两个分时组件共用）；**后端 `strategy/intraday_signals.py:95` 同类 ×100 文档漏记，已修**（分钟累计均价）；scoring.py vwap_bias 是日K口径正确保留；盘口/分笔/日K ×100 正确保留 | ✅ |
| tickflow-dev 重启：easy_tdx 注册成功、无 tdx_gateway 报错；`/api/kline/minute` 实测 600519 返回股量级（11:15 bar=1600 股） | ✅ |

### 新发现/遗留
- **ETF 分钟库（kline_etf_minute）09-01~09-03 断供**（最新分区 08-31；由每日 UTC 16:00=北京 00:00 的 daily_pipeline 喂养）。kline_sync 增量模式从 last_dt 自愈：**下一轮 00:00 自动回补 09-01 起缺口（走 easy_tdx=股）**，次日验证 `data/kline_etf_minute/date=2026-09-04/` 是否生成。
- 老 VM102 同步链路写过 ~63.4 万行 denormal 垃圾 volume（无成交分钟，如 5.88e-41），集中在 09-01~09-03 分区，×100 后仍≈0 无害；新同步脚本不再产生。
- **VM102 = 冷备**：网络不通、无流量，无下线动作可做；VM102 侧 TdxW-AutoStart 等保留原样（任务 6 的"观察 1-2 天"已无对象）。
- 盯盘：09:40 上午盘一次性自动化检查已建；**13:35 下午盘检查因"单会话单自动化"平台限制无法在本会话创建**，需用户新建或下午按本节清单手动跑一遍。
- 脚本与对拍产物存档：本地 `ops_easytdx_20260904/`（smoke/compare/migrate/新同步脚本/vm102bak）。

## 6. 运维手册

```bash
# SSH 路径（必须经 PVE 跳板，专用密钥）
ssh -i ~/.ssh/id_ed25519_10.0.10.5_pve root@10.0.10.5 "pct exec 100 -- <cmd>"

# serve 池状态
systemctl status easy-tdx-main easy-tdx-worker@8001 ... 
journalctl -u easy-tdx-worker@8001 -f          # 实例日志
curl http://10.0.10.25:8000/                    # Web UI（服务器设置页 = 用户截图页面）

# 服务器池管理（任一实例）
curl http://10.0.10.25:8001/api/v1/server/hosts                 # 候选列表 + 当前
curl -X POST http://10.0.10.25:8001/api/v1/server/test          # 全池测速
curl -X POST http://10.0.10.25:8001/api/v1/server/switch -H 'Content-Type: application/json' -d '{"host":"115.238.90.165"}'

# provider 插件
/opt/tickflow-stock-panel/backend/app/plugins/easy_tdx/         # CT100 部署路径
本地副本: D:\Workbuddy工作空间\2026-08-02-15-04-38\tickflow-stock-panel\backend\app\plugins\easy_tdx\
环境变量: EASY_TDX_WORKER_PORTS(默认8000-8006) / EASY_TDX_QUOTES_HOSTS(4台白名单) / EASY_TDX_BASE_URL

# 冒烟（CT100，60s 超时保护）
cd /opt/tickflow-stock-panel/backend && timeout 60 .venv/bin/python -u -c "
import sys; sys.path.insert(0, '.')
from app.plugins.easy_tdx.provider import EasyTdxProvider, availability
print(availability())"
```

**已知坑（项目级，长期有效）**：
- pct exec 复合命令引号会被吃 → 一律写脚本文件 push 进容器再 bash 执行
- CT100 无 curl → 用后端 venv python urllib 探测
- CT100 时区 UTC，journalctl 时间 +8 = 北京时间
- 同一文件多处 Edit 不能并行调用（互相覆盖丢改动，本次实测两次）
- uv pip 安装走默认源极慢（16 分钟）→ 加 `--index-url https://pypi.tuna.tsinghua.edu.cn/simple`

## 7. 关键文件索引

| 文件 | 说明 |
|---|---|
| `backend/app/plugins/easy_tdx/provider.py` | provider 全部实现（~700 行，含完整单位契约注释） |
| `backend/app/plugins/easy_tdx/plugin.yaml` | 插件清单（datasets: 六数据集） |
| `/etc/systemd/system/easy-tdx-main.service` | 8000 UI 实例单元 |
| `/etc/systemd/system/easy-tdx-worker@.service` | 8001-8006 模板单元 |
| `D:\Workbuddy工作空间\量化\easy_tdx_eval\easy_tdx替换VM102评估报告.md` | 切换决策依据 |
| `D:\Workbuddy工作空间\量化\easy_tdx_eval\stress_test.py` | 压测脚本（T0-T7 可复跑） |
| `/tmp/smoke_provider.py`（CT100） | 六端点冒烟脚本（重跑加 timeout 保护） |
| 旧链路参考 | `backend/app/plugins/tdx_gateway/provider.py`（单位换算与 depth5 校验逻辑的权威实现，已尽量同构移植） |

## 8. 接手即办（First 30 Minutes）

1. 读本文档 §4 单位契约表 + §5 任务清单
2. CT100 冒烟一遍 provider（§6 命令）确认六端点仍通
3. 按任务 1→2/3（同日完成）→4→5 顺序推进；任务 5 后第一个交易日盘中盯分钟链路
4. VM102 在对拍完成前不要动

## 10. 2026-09-04 上午故障修复 + fuyao/stocksdk 回退链路（ZCode 会话 2）

> 用户报告：实时行情开关打不开、同步失败。三连根因，全部修复并验证；实时开关已开启（enabled=true, full_market, HTTP 200）。

### 根因链（按发现顺序）
1. **easy_tdx `_Cfg` config shim 缺 `datasets`**：`provider.py` 里 `type("_Cfg",())` 只造了 name/display_name，而 custom loader 的 `provider_has_dataset()` 读 `config.datasets`（fuyao/stocksdk 都有带 datasets 的 shim，新插件漏了）。裸调点：`kline_sync.py:175/343/684`（daily/adj/minute 同步）与 `quote_service.py:649`（实时路由，在 try 之外）。后果：完整性修复任务 5 秒失败 `'_Cfg' object has no attribute 'datasets'`（昨晚北京 00:39 起共 8 个失败任务）→ 历史完整性门禁（settings.py PUT realtime-quotes 的 409 分支）永久锁死开关。修复：shim 补 `datasets: dict.fromkeys(_DATASETS)`（与 plugin.yaml 六数据集一致）+ `path: None`。
2. **指数/ETF 日K硬连 TickFlow**：`index_sync.sync_and_persist_index_daily / sync_and_persist_etf_daily` 直接调 `kline_sync.sync_daily_batch`（TickFlow klines.batch，**不看 preferences**），而 capability 门禁按 easy_tdx 放行 → 切源后指数/ETF 日K一直静默拉空（指数日K 0 行、kline_index_daily 缺 09-03 分区 = 门禁报"2026-09-03 数据缺失"的来源；股票日K正常是因为 `sync_and_persist_daily_batch` 有 provider 分支）。修复：新增 `index_sync._fetch_daily_chunk` 按日K偏好路由 easy_tdx/TickFlow，**指数 chunk 必须 asset_type="index"**（保 vol 透传契约）。修复后修复任务回补：指数 6742 行、ETF 19600 行。
3. **MAC 板块指数 timestamp → quote_ts 快照误判**：980xxx 深交所板块指数的 /bars 行带 `timestamp`（=bar 北京零点），`normalize_daily` 将其映射为 quote_ts → 完整性扫描按"早于 15:00 收盘线"判"盘中快照"（门禁第二层 409）。修复：`_daily_one` 对 DAY 行 `row.pop("timestamp")`（日K契约本无 quote_ts）；存量 09-03 分区 5 行已外科清洗（quote_ts 置 null）。⚠️ 980xxx 目前 serve 池拉不到（MAC host 相关，00:00 时还能拉到、08:00 后为空），后续观察。

### 新功能：easy_tdx → fuyao/stocksdk 回退（用户指令）
- 触发 = 池级失败（EasyTdxError）**或整批空结果**；部分标的缺失（停牌/稀疏）不触发。实现于 EasyTdxProvider 各 get_* 外层（`_fallback_chain` 懒加载+负缓存）。
- 顺序：daily/adj_factor → fuyao→stocksdk；**minute → 仅 stocksdk**（fuyao 未声明 minute；**×100 手→股** 已实测 15:00 bar 184→18400）；**realtime → 仅 fuyao**（全市场无参契约，拉回后按请求 symbols 过滤；stocksdk realtime 返回形状未验证故不用）；**depth5/financial 无回退**（无第三方源/偏好独立路由）。
- 量纲：fuyao/stocksdk 日K=手（与契约一致直用，实测 600519 09-03 = 17748 手）。
- 排障开关：`EASY_TDX_DISABLE_FALLBACK` 非空即整体禁用。回退发生时 logger.error/warning 有"回退源"字样，可 grep。
- ⚠️ fuyao 在裸 shell/错误 cwd 下报"缺少 API Key"是 secrets 解析路径假象，生产进程内正常。

### ⚠️ 并发会话冲突（重要！）
修复期间**另一个会话在并行开发分笔成交功能**（`get_transactions/_txn_paged` + API 层 `/transactions`，属 §7 二期项）。期间发生：本地 provider.py 双方交替修改、CT100 provider.py 被对方覆盖（我的修复被冲掉过一次）甚至短暂删除、对方推送触发的 uvicorn reload 杀死过我运行中的修复任务（"后端重启,任务中断"）。
**现状已收敛**：CT100 与本地工作区 provider.py 同步为融合版（md5 `eca02461492939d7d50f4fa48067c198`，含 datasets shim + fuyao/stocksdk 回退 + timestamp strip + 对方的 get_transactions）。**任一方再部署前必须以本地工作区当前版本为准（或先 pull CT100 增量），不要用陈旧副本覆盖推送。**

### 自动化绑定状态
- 本会话已绑 09:40 上午盘一次性检查（§9 清单）；13:35 下午盘检查因"单会话单自动化"平台限制无法在本会话创建 → 用户需在 ZCode 界面新建，或下午手动跑 §9。
- `minute-pool.timer` 正常（每 2 分钟）。盘前 `intraday/status` 的 symbol_count=0/quote_age=null 属正常，开盘后观察。

## 11. 09-04 盘后核验（ZCode 会话 1，23:03-23:20）

> 用户问"当日定时任务似乎失败了"。核验结论：**CT100 侧全部定时任务成功、数据健康；失败的是 ZCode 平台侧盯盘自动化（09:40 那条派发即失败，runCount=0，非 CT100 问题）**。白天 §10 所述"8 个完整性任务失败"已由会话 2 当日修复。

| 项 | 结果 |
|---|---|
| minute-pool 盘中增量 | ✅ 122/122 轮 canary ok + written，failed=0，abort=0；收盘分区 1,735,629 行 / 7,244 只 / max(dt)=15:00 |
| 分钟单位（股） | ✅ 600519 15:00 volume=38400（股）；全天垃圾 tiny 行=0 |
| t0-minute 3 次 | ✅ 09:33 实跑（33,330 行）；16:30/20:30 幂等 skip（分区已 >100 万行） |
| ETF 分钟缺口 09-01~09-03 | ✅ 22:41 已补齐（§9 预估"次日管道自愈"提前实现），四天分区全为股（510300 尾盘 915 万股量级） |
| 我方两处改动存活 | ✅ intraday_signals.py 与 intraday-chart.ts 本地/CT100 md5 一致，×100 移除仍在 |
| 文件同步状态 | ✅ provider.py 本地=CT100（md5 `fa23d263f9f6b99c9509c7265d557948`，较 §10 记录又演进但两侧一致） |

**当晚日志噪音定界**：唯一一次用户可见 500（23:00:32 `/api/intraday/stream`）= 并发会话存 `live_tick_service.py` 触发 uvicorn 热重载、杀掉在途 SSE，一次性；158 条 provider WARNING（09:30-12:36 集中 worker :8002，`security_quotes` 响应解析失败/非法 market 值）= quotes 子池单实例劣化，cooldown+重试自愈，12:36 后消失。

**自动化处置**：删除失败的 09:40 检查，重建为 **00:45（09-05）一次性管道验收**——今晚 00:00 是首个 easy_tdx 全量 daily_pipeline（且 §10 的回退/路由新代码首次过夜跑），比下周一盘中检查更紧急；下周一（09-07）09:40 盘中检查需用户新开会话创建。
