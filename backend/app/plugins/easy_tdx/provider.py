"""easy-tdx 数据源插件: CT100 本机 easy-tdx serve 实例池 (通达信协议直连).

架构 (2026-09-03 起, 替代 VM102 tdx_gateway):
    tickflow 后端 → EasyTdxProvider (本插件) → 本机 easy-tdx serve 实例池
    → 公网通达信行情服务器 (52 台候选, 自动测速选优).

serve 实例池 (systemd):
    - easy-tdx-main.service      :8000  UI 实例 (用户可在 Web UI 查看服务器设置页)
    - easy-tdx-worker@8001..8006 :8001-8006  纯 API worker (--no-ui)

两个子池:
    - MAC 大池 (bars/minute/历史分笔): MAC 协议, 47+ 台服务器可用, 每实例 1 条连接,
      provider 按"每实例同时 1 个在途请求"分组扇出 (协议层单连接串行).
    - quotes 子池 (realtime/depth5/当日分笔): 标准协议 get_security_quotes 五档批量
      (80 只/批). ⚠️ 52 台候选中仅 4 台支持该命令 (实测 2026-09-04):
      180.153.18.170 / 115.238.56.198 / 115.238.90.165 / 218.75.126.9.
      provider 周期检查实例 current_host, 白名单机有覆盖缺口时自动 POST
      /server/switch 把空闲 worker 热切换过去 (无人工干预自愈).

分笔 (transaction, 2026-09-04 接管 tdx_gateway 的分时成交数据源):
    - 当日端点 GET /transaction: start=0 为最新尾页, start 增大向更老翻页, 800 条/页.
      ⚠️ 与五档同白名单 — 仅上述 4 台返回最近交易日数据, 其余主机返回 0 行.
    - 历史端点 GET /transaction/history?date=YYYYMMDD: 47 台全支持, 实测可回溯
      ≥30 天 (2026-09-04 对 20260805 验证) — 历史分笔不再依赖 g4tic/VM102.
    - 协议时间精度分钟级; 当日端点 datetime 由 serve 按今天拼接, 跨日数据日期
      字段不可信 (调用方 tick_transactions 用成交量匹配判定真实交易日).

单位契约 (与 parquet 仓库对齐, 实测 2026-09-03, 600519 双源对拍):
    - 分钟K (新契约, 用户决策"完全按新源"): volume=股, amount=元, 均透传.
      ⚠️ 历史分钟库 (tdx_gateway 时代) volume=手, 已由迁移脚本 ×100 统一为股.
    - 日K 股票: volume=手 (easy_tdx 原生股 → /100), amount=元 (透传).
    - 日K 指数: volume/amount 与现库同量纲, 全部透传.
    - realtime: volume=手, amount=元 (透传, 与日K契约一致, 实时覆写当日日线安全).
    - depth5: 档位量=手 (透传, 前端 Depth5Panel 按 1手=100股 换算展示).
    - financial: zong_guben/liutong_guben 原生即股 (2026-09-04 晚复核探针
      600519=1250081562.5 / 000001=19405918750, 均为股), 透传不再 ×1e4.
"""
from __future__ import annotations

import logging
import os
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any

import polars as pl

from app.data_providers.base import AssetType
from app.data_providers.normalizer import normalize_daily
from app.tickflow.rate_limits import chunked

logger = logging.getLogger(__name__)

DEFAULT_PORTS = ",".join(str(p) for p in range(8000, 8031))  # 31 实例 (2026-09-04 扩容, 分散 21 台上游)
DEFAULT_QUOTES_HOSTS = "180.153.18.170,115.238.56.198,115.238.90.165,218.75.126.9"
_BARS_BATCH_SYMBOLS = 40  # 日K扇出分组大小 (仅影响 on_chunk_done 粒度)
_REALTIME_BATCH = 80      # 标准协议 quotes 单批上限 (实测 80 只/次)
_FIN_CONCURRENCY = 6
_SINA_CONCURRENCY = 31    # 新浪 f10 通道无 TDX 锁 (serve 侧直连新浪), 可高并发.
                          # 31 = 实例池大小, "每实例 1 在途" 是实测最优形态
                          # (2026-09-05 probe: 24并发≈48req/s, 31并发≈54-64req/s,
                          # 62 并发触发新浪限流崩塌到 4req/s — 勿超实例数).
_BARS_PAGE = 800          # /bars 单请求 count 上限 (easy-tdx serve 硬限)
_TXN_PAGE = 800           # /transaction 单请求 count 上限 (同 serve 硬限)
_TXN_CONC_WINDOW = 5      # 分笔乐观并发翻页窗口 (页数/轮, ≤ 最小可用实例数)
_TXN_MAX_PAGES = 300      # 分笔翻页护栏 (300 页 = 24 万笔)
_MINUTE_BARS_PER_DAY = 242
_DEPTH_TOLERANCE = 0.005  # 最优档与现价最大可信偏差 0.5%
_QUOTES_ALIGN_MIN_INTERVAL = 600.0  # 白名单对齐动作最小间隔 (秒)

_MINUTE_COLUMNS = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]

_DATASETS = ("daily", "adj_factor", "minute", "realtime", "depth5", "financial", "tick", "full_minute")

# /finance (pytdx get_finance_info, 最新单期快照) → TickFlow 财务列映射.
# 仅收录语义明确、已对 600519 探针验证的字段; EPS/ROE/利润率等 37 字段中
# 不存在, 相关列输出 null — 历史多期二期接新浪三表 (/sina/financial-report).
# ⚠️ zong_guben/liutong_guben 原生单位为股 (2026-09-04 晚复核: 600519
# =1250081562.5 / 000001=19405918750, 与真实总股本一致), 直接透传.
_FINANCE_MAP: dict[str, list[tuple[str, str, float]]] = {
    "metrics": [
        ("meigujing_zichan", "bps", 1.0),                  # 每股净资产(元)
        ("zong_zichan", "total_assets", 1.0),              # 资产总计(元)
        ("liudong_zichan", "total_current_assets", 1.0),   # 流动资产合计(元)
        ("cunhuo", "inventory", 1.0),                      # 存货(元)
        ("jingying_xianjinliu", "net_operating_cash_flow", 1.0),  # 经营现金流净额(元)
        ("zong_xianjinliu", "net_cash_change", 1.0),       # 现金净增加额(元)
    ],
    "balance_sheet": [
        ("zong_zichan", "total_assets", 1.0),
        ("liudong_zichan", "total_current_assets", 1.0),
        ("guding_zichan", "fixed_assets", 1.0),
        ("wuxing_zichan", "intangible_assets", 1.0),
        ("cunhuo", "inventory", 1.0),
        ("liudong_fuzhai", "total_current_liabilities", 1.0),
        ("jing_zichan", "total_equity", 1.0),
    ],
    "shares": [
        ("zong_guben", "total_shares", 1.0),               # 股 (原生即股, 透传)
        ("liutong_guben", "float_shares", 1.0),            # 股 (原生即股, 透传)
    ],
}


class EasyTdxError(RuntimeError):
    """A transparent easy-tdx/serve error; callers must not turn it into fake rows."""


def _worker_ports() -> list[int]:
    raw = os.getenv("EASY_TDX_WORKER_PORTS", DEFAULT_PORTS)
    ports = []
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            ports.append(int(part))
    return ports or [int(p) for p in DEFAULT_PORTS.split(",")]


def _quotes_hosts() -> list[str]:
    raw = os.getenv("EASY_TDX_QUOTES_HOSTS", DEFAULT_QUOTES_HOSTS)
    return [h.strip() for h in raw.split(",") if h.strip()]


def _base_url(port: int) -> str:
    """serve 实例池基址. 仅允许本机环回 (serve 与后端同机部署, 无远程场景)."""
    import urllib.parse

    base = os.getenv("EASY_TDX_BASE_URL", "http://127.0.0.1").rstrip("/")
    parsed = urllib.parse.urlparse(base)
    if parsed.scheme != "http" or (parsed.hostname or "") not in ("127.0.0.1", "localhost"):
        raise EasyTdxError(f"EASY_TDX_BASE_URL 必须指向本机环回: {base!r}")
    return f"{base}:{port}/api/v1"


def availability() -> tuple[bool, str]:
    """插件 check: 本机 serve 实例池至少一个可达."""
    try:
        _Pool.get().ping()
    except EasyTdxError as exc:
        return False, str(exc)
    return True, "ok"


def _split_symbol(symbol: str) -> tuple[str, str]:
    """'600519.SH' → ('SH', '600519'); 支持 BJ (北交所)."""
    code, _, market = symbol.partition(".")
    market = market.upper() or ("SH" if code.startswith(("6", "5")) else "SZ")
    return market, code


# TDX 标准 quotes 协议 market 编号 (响应行回填 symbol 时的映射同源).
_QUOTE_MARKET_ID = {"SZ": 0, "SH": 1, "BJ": 2}


def _filter_quoted_rows(rows: list[dict[str, Any]], batch: list[str]) -> list[dict[str, Any]]:
    """只保留请求过的 (market, code) 响应行.

    上游对不认识的代码不报错而是回垃圾行 (实测 970070.SZ/932000.SH →
    market=1 code=600839 price=0), 价格非零的串线帧也会在此被挡掉,
    避免污染股票/指数记录流.
    """
    allowed = {(_QUOTE_MARKET_ID.get(m, 0), c) for m, c in (_split_symbol(s) for s in batch)}
    return [
        row for row in rows
        if (int(row.get("market") or 0), str(row.get("code") or "")) in allowed
    ]


# 新浪财报三表 (serve /sina/financial-report, 独立于 TDX 行情服务器) → canonical 列映射.
# canonical 列与 fuyao provider 完全一致 (financial_sync 按 symbol+period_end 合并,
# 两源数据可共存互不覆盖). 数值新浪原生为元/浮点, 直接透传; `_同比` 列不收.
# announce_date: 新浪 f10 无公告日期字段 → None (fuyao 有真实公告日, 合并时以其为准).
_SINA_REPORT_TYPE: dict[str, str] = {"income": "lrb", "balance_sheet": "fzb", "cash_flow": "llb"}
_SINA_HISTORY_PERIODS = 8  # 与 fuyao _FINANCIAL_HISTORY_PERIODS 对齐 (最近 8 期季报)

# metrics 派生 (2026-09-04): 只收无口径争议的列 —
#   同比 (新浪原生 _同比列), 毛利率 ((营收-营业成本)/营收), 资产负债率 (负债/资产).
# roe/roa/net_margin/operating_cash_to_revenue/inventory_turnover 涉及平均余额/TTM
# 口径, 不派生 (留空 → _merge_report_history 由 fuyao 旧行补齐, 不混口径).
_SINA_LRB = "lrb"
_SINA_FZB = "fzb"
_SINA_FIELD_MAP: dict[str, dict[str, str]] = {
    "income": {
        "营业总收入": "revenue",
        "营业收入": "revenue_bank",  # 银行/保险无营业总收入, 用营业收入回退
        "营业总收入_同比": "revenue_yoy",  # 新浪原生同比 (0.013 = +1.3%)
        "归属于母公司所有者的净利润_同比": "net_income_yoy",
        "营业成本": "operating_cost",
        "销售费用": "selling_expense",
        "管理费用": "admin_expense",
        "研发费用": "rd_expense",
        "营业利润": "operating_profit",
        "财务费用": "financial_expense",
        "利润总额": "total_profit",
        "所得税费用": "income_tax",
        "净利润": "net_income",
        "归属于母公司所有者的净利润": "net_income_attributable",
        "基本每股收益": "basic_eps",
    },
    "balance_sheet": {
        "货币资金": "cash_and_equivalents",
        "应收账款": "accounts_receivable",
        "存货": "inventory",
        "流动资产合计": "total_current_assets",
        "资产总计": "total_assets",
        "负债合计": "total_liabilities",
        "归属于母公司股东权益合计": "equity_attributable",
        "所有者权益(或股东权益)合计": "total_equity",
        "未分配利润": "retained_earnings",
    },
    "cash_flow": {
        "经营活动产生的现金流量净额": "net_operating_cash_flow",
        "投资活动产生的现金流量净额": "net_investing_cash_flow",
        "筹资活动产生的现金流量净额": "net_financing_cash_flow",
        "购建固定资产、无形资产和其他长期资产所支付的现金": "capex",
        "现金及现金等价物净增加额": "net_cash_change",
    },
}


def _number(value: Any) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _iso_date(value: Any) -> str | None:
    """'20260815' / '2026-08-15T..' → '2026-08-15'; 非法输入返回 None."""
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    text = str(value or "")[:10]
    return text if len(text) == 10 and text[4] == "-" else None


def _quote_levels(row: dict[str, Any], side: str) -> list[float]:
    """bid1..5 / ask1..5 原序, 保留零占位 (封死一侧全 0, depth_service 依赖)."""
    out: list[float] = []
    for i in range(1, 6):
        number = _number(row.get(f"{side}{i}"))
        out.append(float(number) if number is not None and number > 0 else 0.0)
    return out


def _quote_volumes(row: dict[str, Any], side: str) -> list[int]:
    out: list[int] = []
    for i in range(1, 6):
        price = _number(row.get(f"{side}{i}"))
        if price is None or price <= 0:
            out.append(0)
            continue
        vol = _number(row.get(f"{side}_vol{i}"))
        out.append(int(vol) if vol is not None else 0)
    return out


class _Pool:
    """serve 实例池: round-robin 分发 + 冷却 + quotes 白名单子池路由."""

    _instance: "_Pool | None" = None
    _instance_lock = threading.Lock()

    @classmethod
    def get(cls) -> "_Pool":
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = _Pool()
            return cls._instance

    def __init__(self) -> None:
        self._ports = _worker_ports()
        self._rr = 0
        self._cooldown: dict[int, float] = {}
        self._lock = threading.RLock()  # 可重入: 防御嵌套加锁路径 (见 _snapshot_ports 注释)
        # quotes 子池: {port: current_host}, 周期刷新
        self._quotes_ports: list[int] = []
        self._quotes_refreshed_at = 0.0
        self._quotes_hosts = _quotes_hosts()
        self._last_align_at = 0.0  # 白名单对齐动作节流
        # 每端口在途锁 (见 _request_one docstring): 防同实例并发响应串线
        self._port_locks: dict[int, threading.Lock] = {}
        # 失败自愈重连进行中标记 (见 _heal_worker)
        self._healing: dict[int, bool] = {}

    # ---- 基础请求 ----
    def _http_raw(self, port: int, method: str, path: str, body: dict | None, timeout: float) -> dict:
        """无锁裸 HTTP (仅供 sina=True 通道 — serve 侧直连新浪不经 TDX 连接,
        无串线风险; TDX 数据路径必须走 _request_one 的双层锁)."""
        import http.client
        import urllib.parse

        base = os.getenv("EASY_TDX_BASE_URL", "http://127.0.0.1")
        parsed = urllib.parse.urlparse(base)
        host = parsed.hostname or "127.0.0.1"
        if parsed.scheme != "http" or host not in ("127.0.0.1", "localhost"):
            raise EasyTdxError(f"EASY_TDX_BASE_URL 必须指向本机环回: {base!r}")
        data = _json_dumps(body) if body is not None else None
        headers = {"Accept": "application/json", "Connection": "close"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        conn = http.client.HTTPConnection(host, port, timeout=timeout)
        try:
            conn.request(method, f"/api/v1{path}", body=data, headers=headers)
            resp = conn.getresponse()
            status, raw = resp.status, resp.read().decode("utf-8")
        except (OSError, TimeoutError, http.client.HTTPException) as exc:
            raise EasyTdxError(f"easy-tdx serve :{port} unavailable: {exc}") from exc
        finally:
            conn.close()
        if status >= 400:
            raise EasyTdxError(f"easy-tdx serve :{port} HTTP {status}: {raw[:200]}")
        payload = json_loads(raw)
        if not isinstance(payload, dict):
            raise EasyTdxError(f"easy-tdx serve :{port} returned non-object")
        return payload

    def _request_one(self, port: int, method: str, path: str, body: dict | None, timeout: float) -> dict:
        """请求本机 serve 实例. 连接目标固定为环回地址 (EASY_TDX_BASE_URL 仅允许
        http://127.0.0.1|localhost), path 由服务端代码拼接, 无外部输入.
        双层每端口在途锁: serve 单 TDX 连接串行, 并发同实例会响应串线
        (2026-09-04 两次事故: ①进程内分笔翻页并发撞端口 ②归档回补进程 vs
        后端 API 跨进程撞端口, A 标的收到 B 的 /bars 数据). 线程锁保进程内,
        bind 互斥锁保跨进程 (后端/回补脚本/EOD job 全部经此路径).
        """
        import urllib.parse

        base = os.getenv("EASY_TDX_BASE_URL", "http://127.0.0.1")
        parsed = urllib.parse.urlparse(base)
        host = parsed.hostname or "127.0.0.1"
        if parsed.scheme != "http" or host not in ("127.0.0.1", "localhost"):
            raise EasyTdxError(f"EASY_TDX_BASE_URL 必须指向本机环回: {base!r}")
        with self._port_lock(port), _flock_port(port):
            payload = self._http_raw(port, method, path, body, timeout)
        return payload

    def _port_lock(self, port: int) -> threading.Lock:
        with self._lock:
            lock = self._port_locks.get(port)
            if lock is None:
                lock = threading.Lock()
                self._port_locks[port] = lock
            return lock

    def request(
        self,
        method: str,
        path: str,
        body: dict | None = None,
        *,
        timeout: float = 20.0,
        quotes_only: bool = False,
        port: int | None = None,
        sina: bool = False,
    ) -> dict:
        """分发请求; 单实例失败 → 冷却 60s + 强制重连清流 + 换实例重试.

        quotes_only: 只路由到 current_host ∈ 白名单的实例 (五档命令 43/52 台
        返回空行, 见模块 docstring). port: 指定实例 (fan-out 场景由分组决定).
        sina=True: 新浪 f10 端点专用通道 — serve 侧 SinaClient 直连新浪,
        **不经过 TDX 连接** → 无响应串线风险 → 绕过端口在途锁/bind 锁,
        可任意高并发轮询全部实例 (财务拉取提速用).
        """
        if sina:
            ports = self._snapshot_ports(quotes_only=False)
            last_error: Exception | None = None
            for _ in range(len(ports) or 1):
                candidate = self._next_port(ports, False)
                if candidate is None:
                    break
                try:
                    return self._http_raw(candidate, method, path, body, timeout)
                except EasyTdxError as exc:
                    last_error = exc
            raise EasyTdxError(f"sina request failed on all workers: {last_error}")
        if port is not None:
            try:
                return self._request_one(port, method, path, body, timeout)
            except EasyTdxError:
                self._heal_worker(port)
                raise
        ports = self._snapshot_ports(quotes_only)
        last_error: Exception | None = None
        for _ in range(len(ports) or 1):
            candidate = self._next_port(ports, quotes_only)
            if candidate is None:
                break
            try:
                return self._request_one(candidate, method, path, body, timeout)
            except EasyTdxError as exc:
                last_error = exc
                with self._lock:
                    self._cooldown[candidate] = time.monotonic() + 60.0
                logger.warning("easy-tdx worker :%s failed (cooldown 60s): %s", candidate, exc)
                self._heal_worker(candidate)
        raise EasyTdxError(f"all easy-tdx workers failed: {last_error}")

    def _heal_worker(self, port: int) -> None:
        """失败后强制该 worker 同主机重连, 丢弃可能孤儿化的 TDX 响应帧.

        机制: 我方超时/5xx 放弃请求后, serve 的持久 TDX 连接里残留上一命令
        的响应, 后续请求读到错位帧 (表现为 500 "数据不足" 或混入其他标的
        的行, 2026-09-04 全市场归档两次污染根因). reconnect_to 同主机也会
        重建连接 → 流归零. HTTP 管理面不经过 TDX 数据路径, 失步时也可用.
        """
        with self._lock:
            if self._healing.get(port):
                return
            self._healing[port] = True
        try:
            try:
                hosts = self._request_one(port, "GET", "/server/hosts", None, 8.0)
                host = hosts.get("current_host")
                if not host:
                    return
                self._request_one(port, "POST", "/server/switch", {"host": host}, 15.0)
                logger.warning("easy-tdx heal: worker :%s reconnected to %s (清孤儿 TDX 流)", port, host)
            except EasyTdxError:
                pass  # heal 失败不阻塞主流程, 冷却期间下轮再试
        finally:
            with self._lock:
                self._healing[port] = False

    def _snapshot_ports(self, quotes_only: bool) -> list[int]:
        now = time.monotonic()
        never_scanned = False
        with self._lock:
            alive = [p for p in self._ports if self._cooldown.get(p, 0) <= now]
            if not quotes_only:
                return alive
            stale = now - self._quotes_refreshed_at > 300
            dying = bool(self._quotes_ports) and not set(self._quotes_ports) & set(alive)
            never_scanned = not self._quotes_ports and self._quotes_refreshed_at == 0.0
            if stale or dying or never_scanned:
                self._quotes_refreshed_at = now
            quoted = [p for p in self._quotes_ports if p in alive]
        if never_scanned:
            # ⚠️ 必须在锁外同步扫描: _refresh_quotes_ports 内部会再次加锁,
            # 在非重入 Lock 内嵌套调用 = 死锁 (2026-09-04 冒烟事故根因:
            # get_realtime 首次 quotes 请求永久挂起).
            self._refresh_quotes_ports()
            with self._lock:
                alive = [p for p in self._ports if self._cooldown.get(p, 0) <= time.monotonic()]
                quoted = [p for p in self._quotes_ports if p in alive]
        return quoted or alive  # 白名单机全冷却时退化为全池 (上层按空行重试)

    def _next_port(self, ports: list[int], quotes_only: bool) -> int | None:
        if not ports:
            return None
        with self._lock:
            self._rr = (self._rr + 1) % max(len(ports), 1)
        return ports[self._rr % len(ports)]

    def _refresh_quotes_ports(self) -> None:
        """扫描各实例 current_host, 维护 quotes 白名单子池; 缺口时触发对齐."""
        host_of: dict[int, str] = {}
        for port in self._ports:
            try:
                payload = self._request_one(port, "GET", "/server/hosts", None, 8.0)
                host = payload.get("current_host")
                if host:
                    host_of[port] = str(host)
            except EasyTdxError:
                continue
        self._maybe_align(host_of)
        qualified = [p for p in self._ports if host_of.get(p) in set(self._quotes_hosts)]
        if qualified:
            with self._lock:
                self._quotes_ports = qualified
            logger.info("easy-tdx quotes-capable workers: %s", qualified)
        else:
            logger.warning(
                "no easy-tdx worker currently on a quotes-capable host %s — "
                "depth5/realtime/today-ticks will degrade until realigned",
                self._quotes_hosts,
            )

    def _maybe_align(self, host_of: dict[int, str]) -> None:
        """白名单机覆盖缺口自愈: 把不在白名单的空闲 worker 热切换到缺口机.

        只动 worker (main 实例留给用户 UI), 每台缺口机分配一个 worker;
        已有实例的白名单机不重复占用, 多余 worker 保持原主机 (MAC 大池多样性).
        """
        if os.getenv("EASY_TDX_AUTO_ALIGN", "1") != "1":
            return
        now = time.monotonic()
        with self._lock:
            if now - self._last_align_at < _QUOTES_ALIGN_MIN_INTERVAL:
                return
            self._last_align_at = now
        covered = {h for h in host_of.values() if h in set(self._quotes_hosts)}
        missing = [h for h in self._quotes_hosts if h not in covered]
        if not missing:
            return
        main_port = min(self._ports)
        idle_workers = [p for p in self._ports if p != main_port and host_of.get(p) not in set(self._quotes_hosts)]
        for port, host in zip(idle_workers, missing):
            try:
                self._request_one(port, "POST", "/server/switch", {"host": host}, 15.0)
                host_of[port] = host
                logger.info("easy-tdx align: worker :%s switched → %s (quotes/ticks whitelist)", port, host)
            except EasyTdxError as exc:
                logger.warning("easy-tdx align worker :%s → %s failed: %s", port, host, exc)

    def ports_for(self, quotes_only: bool) -> list[int]:
        """公开端口快照 (分笔翻页并发按实例轮转用)."""
        return self._snapshot_ports(quotes_only)

    def ping(self) -> None:
        self.request("GET", "/market/session", timeout=6.0)

    # ---- 分组扇出 (每实例同时 1 个在途请求) ----
    def fan_out(
        self,
        items: list[Any],
        worker: Callable[[Any, int], Any],
        *,
        concurrency: int | None = None,
        quotes_only: bool = False,
        on_progress: Callable[[int, int], None] | None = None,
    ) -> list[Any]:
        """把 items 按 worker 实例数分组, 组内串行、组间并行.

        worker(item, port) 负责单 item 的完整取数 (含翻页). 每组绑定固定
        port → 每实例同时只有 1 个在途 TDX 请求 (单连接 IO 串行, 并发无收益).

        quotes_only=True: 分组只绑定 quotes 白名单子池实例 — 五档/当日分笔命令
        在非白名单主机返回空行 (不是错误), 批次发过去只会静默丢数据.
        """
        ports = self._snapshot_ports(quotes_only)
        n = max(1, min(len(ports) if concurrency is None else concurrency, len(items) or 1))
        groups: list[list[tuple[int, Any]]] = [[] for _ in range(n)]
        for i, item in enumerate(items):
            groups[i % n].append((ports[i % len(ports)], item))
        done = [0]
        done_lock = threading.Lock()

        def run_group(group: list[tuple[int, Any]]) -> list[Any]:
            out = []
            for port, item in group:
                out.append(worker(item, port))
                if on_progress is not None:
                    with done_lock:
                        done[0] += 1
                        on_progress(done[0], len(items))
            return out

        with ThreadPoolExecutor(max_workers=n) as executor:
            frames = list(executor.map(run_group, groups))
        return [row for group in frames for row in group]


def _json_dumps(body: dict) -> bytes:
    import json

    return json.dumps(body).encode("utf-8")


class _flock_port:
    """跨进程每端口互斥 — 本机回环 bind 抢占锁 (无锁文件, 进程死亡自动释放).

    持有方式 = 成功 bind 127.0.0.1:(18000+serve端口) 且不 listen; 其他进程/
    线程 bind 同端口即 EADDRINUSE, 轮询等待. serve 端口 8000-8006 → 锁端口
    18000-18006, 与现有服务不冲突. 后端/回补脚本/EOD job 等所有本机调用方
    共用, 保证任一时刻每 serve 实例至多 1 个在途请求 (serve 单 TDX 连接
    并发会响应串线, 见 _request_one docstring).
    """

    def __init__(self, port: int, wait_timeout: float = 30.0) -> None:
        import socket

        self._lock_port = 18000 + int(port)
        self._sock: Any = None
        self._socket_mod = socket
        self._deadline = time.monotonic() + wait_timeout

    def __enter__(self) -> "_flock_port":
        while True:
            sock = self._socket_mod.socket(self._socket_mod.AF_INET, self._socket_mod.SOCK_STREAM)
            try:
                sock.bind(("127.0.0.1", self._lock_port))
                sock.settimeout(30)
                self._sock = sock  # bind 成功 = 持锁 (不 listen, 不收发)
                return self
            except OSError:
                sock.close()
                if time.monotonic() > self._deadline:
                    return self  # 等锁超时: 降级为无锁放行 (不永久卡死请求)
                time.sleep(0.02)

    def __exit__(self, *exc: Any) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            finally:
                self._sock = None


def json_loads(text: str) -> Any:
    """orjson 优先 (Rust, 大 JSON 解析 ~5-10x 且释放 GIL — 分钟K/分笔大响应
    的客户端解析瓶颈 2026-09-04 实测 stdlib json ~40k 行/s 封顶), 缺失时回退 stdlib."""
    try:
        import orjson

        return orjson.loads(text)
    except ImportError:
        import json

        return json.loads(text)


# 与 plugin.yaml datasets 一致; custom loader 的 provider_has_dataset 读
# config.datasets 做能力识别, 缺失会让 kline_sync/quote_service 直接 AttributeError
# tick = 分时成交 (tick_transactions/tick_archive 链路); full_minute = 本地 T+0
# 全量分钟管道 (t0_minute_backfill + minute_intraday_sync), 2026-09-04 已接管.
_DATASETS = ("daily", "adj_factor", "minute", "realtime", "depth5", "financial", "tick", "full_minute")

# 回退源 (2026-09-04 用户指令): easy-tdx 池无数据才调 fuyao/stocksdk。
# 触发 = 池级失败 (EasyTdxError) 或整批空结果; 部分标的缺失属停牌/稀疏, 不触发。
# depth5 无第三方源, financial 偏好独立路由, 均不在本层。
# 量纲: fuyao/stocksdk 日K 已按仓库契约(手); stocksdk 分钟=手, 仓库契约=股 → ×100。
_FALLBACK_ORDER: dict[str, tuple[str, ...]] = {
    "daily": ("fuyao", "stocksdk"),
    "adj_factor": ("fuyao", "stocksdk"),
    "minute": ("stocksdk",),
    "realtime": ("fuyao",),
}


class EasyTdxProvider:
    name = "easy_tdx"
    builtin = True
    realtime_scope = "full_market_symbols"

    def __init__(self) -> None:
        self.config = type(
            "_Cfg",
            (),
            {
                "name": "easy_tdx",
                "display_name": "easy-tdx 通达信协议直连",
                "datasets": dict.fromkeys(_DATASETS),
                "path": None,
            },
        )()
        self._pool = _Pool.get()
        self._fallback_cache: dict[str, Any] = {}

    def close(self) -> None:
        pass

    def _fallback_chain(self, dataset: str) -> list[tuple[str, Any]]:
        """dataset 的回退 provider 列表。懒加载 + 负缓存 (初始化失败记 False 不重试);
        设 EASY_TDX_DISABLE_FALLBACK 可整体关闭 (排障时区分主备故障用)。"""
        if os.getenv("EASY_TDX_DISABLE_FALLBACK"):
            return []
        out: list[tuple[str, Any]] = []
        for name in _FALLBACK_ORDER.get(dataset, ()):
            inst = self._fallback_cache.get(name)
            if inst is None:
                try:
                    if name == "fuyao":
                        from app.plugins.fuyao.provider import FuyaoProvider
                        inst = FuyaoProvider()
                    else:
                        from app.plugins.stocksdk.provider import StockSDKProvider
                        inst = StockSDKProvider()
                except Exception as exc:  # noqa: BLE001
                    logger.warning("easy-tdx 回退源 %s 初始化失败: %s", name, exc)
                    inst = False
                self._fallback_cache[name] = inst
            if inst:
                out.append((name, inst))
        return out

    # ------------------------------------------------------------------
    # 日K
    # ------------------------------------------------------------------
    def get_daily(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        if not symbols:
            return pl.DataFrame()
        try:
            df = self._daily_easytdx(symbols, start_time, end_time, asset_type, on_chunk_done)
        except EasyTdxError as exc:
            logger.error("easy-tdx 日K池无数据(%s), 尝试回退源", exc)
            df = pl.DataFrame()
        if not df.is_empty():
            return df
        for name, provider in self._fallback_chain("daily"):
            try:
                df = provider.get_daily(symbols, start_time, end_time, asset_type, on_chunk_done)
            except Exception as exc:  # noqa: BLE001
                logger.warning("easy-tdx 日K回退源 %s 失败: %s", name, exc)
                continue
            if df is not None and not df.is_empty():
                logger.warning("easy-tdx 日K不可用, 回退源 %s 提供 %d 行", name, df.height)
                return df
        return pl.DataFrame()

    def _daily_easytdx(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        frames = self._pool.fan_out(
            list(symbols),
            lambda symbol, port: self._daily_one(symbol, start_time, end_time, asset_type, port),
            on_progress=on_chunk_done,
        )
        frames = [f for f in frames if not f.is_empty()]
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    def _daily_one(
        self,
        symbol: str,
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType,
        port: int,
    ) -> pl.DataFrame:
        rows = self._fetch_bars_paged(symbol, "DAY", start_time, end_time, adjust="NONE", port=port)
        for row in rows:
            # easy-tdx /bars 的 bar 级 timestamp (=bar 的北京零点) 不分资产类型都会带
            # (板块指数 980xxx / 普通指数 / ETF 均实测), 会被 normalize_daily 当成
            # quote_ts, 完整性扫描按"早于收盘线"误判盘中快照 → 实时开关被门禁
            # 锁死; 日K契约本就无 quote_ts, 一律剥除。
            row.pop("timestamp", None)
        frame = normalize_daily(rows, default_symbol=symbol, source=self.name)
        if frame.is_empty():
            return frame
        if asset_type == "index":
            # 指数 vol 与 tdx_gateway 时代同量纲 (双源对拍 000001.SH), 全透传.
            return frame
        # 股票/ETF: easy_tdx 日K vol=股 → 仓库契约手 (/100); amount 元透传.
        if "volume" in frame.columns:
            frame = frame.with_columns((pl.col("volume") / 100.0).alias("volume"))
        return frame

    def _fetch_bars_paged(
        self,
        symbol: str,
        category: str,
        start_time: datetime | None,
        end_time: datetime | None,
        *,
        adjust: str,
        port: int,
        bars_per_day: int = 1,
    ) -> list[dict]:
        """MAC /bars 翻页: start=0 最新, start 增大向更老翻页, 直到覆盖窗口."""
        market, code = _split_symbol(symbol)
        if start_time is None:
            need = _BARS_PAGE
        else:
            days = max((end_time or datetime.now()) - start_time, timedelta(days=1)).days + 1
            need = min(days * bars_per_day + 10, 200_000)
        rows: list[dict] = []
        offset = 0
        oldest: str | None = None
        start_key = start_time.strftime("%Y-%m-%d") if start_time else None
        while offset < need:
            count = min(_BARS_PAGE, need - offset)
            payload = self._pool.request(
                "GET",
                f"/bars?market={market}&code={code}&category={category}"
                f"&start={offset}&count={count}&adjust={adjust}",
                port=port,
                timeout=30.0,
            )
            data = payload.get("data") or []
            if not data:
                break
            for row in data:
                # easy-tdx /bars 日期为 ISO 带时间 ('2026-09-03T00:00:00');
                # polars str.cast(Date) 只认 'YYYY-MM-DD', 不切片会全 null
                # (2026-09-04 冒烟: 日线 date=null + adj_factor join 落空同源).
                if category == "DAY" and isinstance(row.get("date"), str):
                    row["date"] = row["date"][:10]
                rows.append(row)
            oldest = str(data[-1].get("date") or data[-1].get("datetime") or "")[:10]
            offset += count
            if len(data) < count:  # 服务器历史见底
                break
            if start_key and oldest and oldest <= start_key:
                break
        return rows

    # ------------------------------------------------------------------
    # 除权因子 (事件法: raw vs QFQ 收盘价比值变化定位事件)
    # ------------------------------------------------------------------
    def get_adj_factors(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        schema = {"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64}
        if not symbols or asset_type != "stock":
            return pl.DataFrame(schema=schema)
        try:
            df = self._adj_easytdx(symbols, start_time, end_time, asset_type, on_chunk_done)
        except EasyTdxError as exc:
            logger.error("easy-tdx 除权池无数据(%s), 尝试回退源", exc)
            df = pl.DataFrame(schema=schema)
        if not df.is_empty():
            return df
        for name, provider in self._fallback_chain("adj_factor"):
            try:
                df = provider.get_adj_factors(symbols, start_time, end_time, asset_type, on_chunk_done)
            except Exception as exc:  # noqa: BLE001
                logger.warning("easy-tdx 除权回退源 %s 失败: %s", name, exc)
                continue
            if df is not None and not df.is_empty():
                logger.warning("easy-tdx 除权不可用, 回退源 %s 提供 %d 行", name, df.height)
                return df
        return pl.DataFrame(schema=schema)

    def _adj_easytdx(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        schema = {"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64}
        if not symbols or asset_type != "stock":
            return pl.DataFrame(schema=schema)
        context_start = start_time - timedelta(days=14) if start_time else None
        frames = self._pool.fan_out(
            list(symbols),
            lambda symbol, port: self._adj_one(symbol, context_start, end_time, start_time, port),
            on_progress=on_chunk_done,
        )
        frames = [f for f in frames if not f.is_empty()]
        return pl.concat(frames).sort(["symbol", "trade_date"]) if frames else pl.DataFrame(schema=schema)

    def _adj_one(
        self,
        symbol: str,
        context_start: datetime | None,
        end_time: datetime | None,
        start_time: datetime | None,
        port: int,
    ) -> pl.DataFrame:
        raw_rows = self._fetch_bars_paged(symbol, "DAY", context_start, end_time, adjust="NONE", port=port)
        qfq_rows = self._fetch_bars_paged(symbol, "DAY", context_start, end_time, adjust="QFQ", port=port)
        if not raw_rows or not qfq_rows:
            return pl.DataFrame()
        raw = normalize_daily(raw_rows, default_symbol=symbol, source=self.name)
        front = normalize_daily(qfq_rows, default_symbol=symbol, source=self.name)
        if raw.is_empty() or front.is_empty():
            return pl.DataFrame()
        factors = (
            raw.select("date", pl.col("close").alias("raw_close"))
            .join(front.select("date", pl.col("close").alias("front_close")), on="date", how="inner")
            .filter((pl.col("raw_close") > 0) & (pl.col("front_close") > 0))
            .sort("date")
            .with_columns((pl.col("front_close") / pl.col("raw_close")).alias("_ratio"))
            .with_columns((pl.col("_ratio") / pl.col("_ratio").shift(1)).alias("ex_factor"))
            .filter(
                pl.col("ex_factor").is_finite()
                & (pl.col("ex_factor") > 0)
                # easy_tdx QFQ 价格按分舍入 → 非事件日 ratio 有 ±0.03% 噪声
                # (2026-09-04 实测: 茅台 180 天混入 74 个 ~1.000x 伪事件).
                # 真实分红/送转事件 ≥0.1%, 取 0.001 阈值隔离噪声带.
                & ((pl.col("ex_factor") - 1.0).abs() > 0.001)
            )
        )
        if start_time is not None:
            factors = factors.filter(pl.col("date") >= start_time.date())
        return factors.select(
            pl.lit(symbol).alias("symbol"),
            pl.col("date").alias("trade_date"),
            "ex_factor",
        )

    # ------------------------------------------------------------------
    # 分钟K (新契约: volume=股, amount=元, 透传)
    # ------------------------------------------------------------------
    def get_minute(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType = "stock",
        freq: str = "1m",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        if not symbols:
            return pl.DataFrame()
        try:
            df = self._minute_easytdx(symbols, start_time, end_time, asset_type, freq, on_chunk_done)
        except EasyTdxError as exc:
            logger.error("easy-tdx 分钟池无数据(%s), 尝试回退源", exc)
            df = pl.DataFrame()
        if not df.is_empty():
            return df
        for name, provider in self._fallback_chain("minute"):
            try:
                df = provider.get_minute(symbols, start_time, end_time, asset_type, freq, on_chunk_done)
            except Exception as exc:  # noqa: BLE001
                logger.warning("easy-tdx 分钟回退源 %s 失败: %s", name, exc)
                continue
            if df is not None and not df.is_empty():
                if "volume" in df.columns:
                    # stocksdk 分钟=手, 仓库分钟契约=股 (2026-09-03 决策) → ×100
                    df = df.with_columns((pl.col("volume") * 100.0).alias("volume"))
                logger.warning("easy-tdx 分钟不可用, 回退源 %s 提供 %d 行", name, df.height)
                return df
        return pl.DataFrame()

    def _minute_easytdx(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType = "stock",
        freq: str = "1m",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        category = _minute_category(freq)
        if category is None:
            raise EasyTdxError(f"easy-tdx does not support minute frequency {freq!r}")
        frames = self._pool.fan_out(
            list(symbols),
            lambda symbol, port: self._minute_one(symbol, category, start_time, end_time, port),
            on_progress=on_chunk_done,
        )
        frames = [f for f in frames if not f.is_empty()]
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    def _minute_one(
        self,
        symbol: str,
        category: str,
        start_time: datetime | None,
        end_time: datetime | None,
        port: int,
    ) -> pl.DataFrame:
        rows = self._fetch_bars_paged(symbol, category, start_time, end_time, adjust="NONE", port=port, bars_per_day=_MINUTE_BARS_PER_DAY)
        if not rows:
            return pl.DataFrame()
        frame = pl.DataFrame(rows)
        if "datetime" not in frame.columns:
            return pl.DataFrame()
        frame = frame.with_columns(
            pl.col("datetime").cast(pl.String).str.to_datetime(strict=False).alias("datetime"),
            pl.lit(symbol).alias("symbol"),
        )
        for column in ("open", "high", "low", "close", "vol", "amount"):
            if column in frame.columns:
                frame = frame.with_columns(pl.col(column).cast(pl.Float64, strict=False))
        if "vol" in frame.columns:
            # 分钟新契约 (用户决策 2026-09-03): volume=股 — easy_tdx 原生口径, 不换算.
            frame = frame.rename({"vol": "volume"})
        keep = [c for c in _MINUTE_COLUMNS if c in frame.columns]
        frame = frame.select(keep)
        # 窗口裁剪 (翻页按根数, 需按时间收紧)
        if not frame.is_empty():
            if start_time is not None:
                start_naive = start_time.replace(tzinfo=None) if start_time.tzinfo else start_time
                frame = frame.filter(pl.col("datetime") >= start_naive)
            if end_time is not None:
                end_naive = end_time.replace(tzinfo=None) if end_time.tzinfo else end_time
                frame = frame.filter(pl.col("datetime") <= end_naive)
        return frame

    # ------------------------------------------------------------------
    # 实时快照 (标准协议 quotes, 白名单子池, 80 只/批)
    # ------------------------------------------------------------------
    def get_realtime(
        self,
        universes: list[str] | None = None,
        symbols: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        if universes:
            raise EasyTdxError("easy-tdx realtime supports explicit symbols only, not whole-market universes")
        if not symbols:
            raise EasyTdxError("easy-tdx realtime requires explicit symbols")
        try:
            records = self._realtime_easytdx(symbols)
        except EasyTdxError as exc:
            logger.error("easy-tdx 实时池无数据(%s), 尝试回退源", exc)
            records = []
        if records:
            return records
        wanted = set(symbols)
        for name, provider in self._fallback_chain("realtime"):
            try:
                rows = provider.get_realtime()
            except Exception as exc:  # noqa: BLE001
                logger.warning("easy-tdx 实时回退源 %s 失败: %s", name, exc)
                continue
            filtered = [r for r in (rows or []) if r.get("symbol") in wanted]
            if filtered:
                logger.warning(
                    "easy-tdx 实时不可用, 回退源 %s 提供 %d/%d 只", name, len(filtered), len(wanted),
                )
                return filtered
        return []

    def _realtime_easytdx(
        self,
        symbols: list[str],
    ) -> list[dict[str, Any]]:
        batches = list(chunked(list(symbols), _REALTIME_BATCH))
        results: list[list[dict]] = self._pool.fan_out(
            batches, lambda batch, _port: self._realtime_batch(batch), quotes_only=True,
        )
        return [record for batch in results for record in batch]

    def _quotes_post(self, stocks: list[dict[str, str]]) -> list[dict[str, Any]]:
        """POST /quotes; 空批次包换实例重试一次.

        上游对部分批次会间歇性静默返回空包 (2026-09-08 收盘定版轮实测每轮
        15-30% 批次空包, 深证成指/创业板指因此整日缺席缓存) — request()
        轮转到下一实例即可取回; 仍空则接受 (纯停牌/不支持代码的批次合法为空,
        盘前维护窗口整批空也只多花一次请求).
        """
        payload = self._pool.request(
            "POST", "/quotes", {"stocks": stocks}, timeout=20.0, quotes_only=True,
        )
        rows = payload.get("data") or []
        if not rows:
            payload = self._pool.request(
                "POST", "/quotes", {"stocks": stocks}, timeout=20.0, quotes_only=True,
            )
            rows = payload.get("data") or []
        return rows

    def _realtime_batch(self, batch: list[str]) -> list[dict[str, Any]]:
        stocks = [{"market": m, "code": c} for m, c in (_split_symbol(s) for s in batch)]
        rows = _filter_quoted_rows(self._quotes_post(stocks), batch)
        records: list[dict[str, Any]] = []
        for row in rows:
            code = str(row.get("code") or "")
            market = int(row.get("market") or 0)
            if not code:
                continue
            symbol = f"{code}.{'SH' if market == 1 else ('BJ' if market == 2 else 'SZ')}"
            last_price = _number(row.get("price"))
            prev_close = _number(row.get("pre_close"))
            # price=0 且无买一 (停牌/真无行情) → 丢弃, 与 tdx_gateway Now=0 守卫同语义。
            # 集合竞价阶段 (09:15-09:25) price=0 但买一价已挂 → 保留该行,
            # 用买一作虚拟撮合价, 否则看板竞价期间股票榜整体消失 (2026-09-07
            # 竞价看板停在上个交易日的根因)。
            session = "normal"
            if not last_price:
                bid1 = _number(row.get("bid1"))
                if not bid1 or prev_close in (None, 0):
                    continue
                last_price = bid1
                session = "auction"
            elif prev_close in (None, 0):
                continue
            change_amount = last_price - prev_close
            records.append({
                "symbol": symbol,
                "last_price": last_price,
                "prev_close": prev_close,
                "open": _number(row.get("open")),
                "high": _number(row.get("high")),
                "low": _number(row.get("low")),
                "volume": _number(row.get("vol")),   # 手 — 与日K契约一致
                "amount": _number(row.get("amount")),  # 元
                "change_amount": change_amount,
                "change_pct": change_amount / prev_close,
                "session": session,
            })
        return records

    # ------------------------------------------------------------------
    # 五档盘口 (与 realtime 同端点, 白名单子池)
    # ------------------------------------------------------------------
    def get_depth5(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        fetched_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        batches = list(chunked(list(symbols), _REALTIME_BATCH))
        results: list[list[dict]] = self._pool.fan_out(
            batches, lambda batch, _port: self._depth_batch(batch), quotes_only=True,
        )
        for row in [r for batch in results for r in batch]:
            code = str(row.get("code") or "")
            market = int(row.get("market") or 0)
            if not code:
                continue
            symbol = f"{code}.{'SH' if market == 1 else ('BJ' if market == 2 else 'SZ')}"
            book = self._book_from_quote(row, fetched_ms)
            if book is not None:
                result[symbol] = book
        return result

    def _depth_batch(self, batch: list[str]) -> list[dict[str, Any]]:
        stocks = [{"market": m, "code": c} for m, c in (_split_symbol(s) for s in batch)]
        return _filter_quoted_rows(self._quotes_post(stocks), batch)

    def _book_from_quote(self, row: dict[str, Any], fetched_ms: int) -> dict[str, Any] | None:
        """标准协议 quotes → depth5 契约; 最优正数档与现价偏差 >0.5% 判陈旧丢弃.

        与 tdx_gateway._book_from_quote 同语义 (零占位保留, 封板股 asks=[0,...]
        合法); 公共行情 quotes 无 TdxW 刮削的陈旧缓存问题, 结构校验可省略.
        """
        bid_prices = _quote_levels(row, "bid")
        ask_prices = _quote_levels(row, "ask")
        if not any(bid_prices) and not any(ask_prices):
            return None
        price = _number(row.get("price")) or 0
        if price > 0:
            tol = price * _DEPTH_TOLERANCE
            bid1 = next((p for p in bid_prices if p > 0), 0.0)
            ask1 = next((p for p in ask_prices if p > 0), 0.0)
            if bid1 and abs(bid1 - price) > tol:
                return None
            if ask1 and abs(ask1 - price) > tol:
                return None
        return {
            "ask_prices": ask_prices,
            "ask_volumes": _quote_volumes(row, "ask"),
            "bid_prices": bid_prices,
            "bid_volumes": _quote_volumes(row, "bid"),
            "timestamp": fetched_ms,
        }

    # ------------------------------------------------------------------
    # 分笔成交 (当日端点走白名单子池; 历史端点全池; 乐观并发翻页)
    # ------------------------------------------------------------------
    def get_transactions(self, symbol: str, trade_date: str | None = None) -> dict[str, Any]:
        """单标的分笔成交 (TDX 分笔协议, 分钟级, 含买卖方向).

        trade_date=None → 当日端点 /transaction (仅白名单子池有数据, 含最近
        交易日的跨日数据; datetime 日期字段按今天拼接, 不可信).
        trade_date=YYYY-MM-DD → /transaction/history (47 台全池, ≥30 天回溯,
        datetime 日期字段可信).

        返回 {"rows": [{datetime, price, vol, buyorsell}, ...], "paged_via": ...};
        rows 为时间升序 (start=0 是最新尾页, 翻页向更老, 拼接时逆序还原).
        """
        market, code = _split_symbol(symbol)
        if trade_date:
            date_int = int(trade_date.replace("-", ""))
            rows = self._txn_paged(
                f"/transaction/history?market={market}&code={code}&date={date_int}",
                quotes_only=False,
            )
            return {"rows": rows, "paged_via": "history"}
        rows = self._txn_paged(
            f"/transaction?market={market}&code={code}", quotes_only=True,
        )
        return {"rows": rows, "paged_via": "today"}

    def fetch_today_tail(self, symbol: str, count: int = _TXN_PAGE) -> list[dict[str, Any]]:
        """当日分笔尾页 (start=0 最新 count 条, 单请求) — 实时轮询增量合并用.

        走 quotes 白名单子池. 返回 serve 原始行 (datetime/price/vol/buyorsell).
        """
        market, code = _split_symbol(symbol)
        payload = self._pool.request(
            "GET", f"/transaction?market={market}&code={code}&start=0&count={count}",
            quotes_only=True, timeout=15.0,
        )
        return payload.get("data") or []

    def _txn_paged(self, base_path: str, *, quotes_only: bool) -> list[dict[str, Any]]:
        """乐观并发翻页: 每轮并发拉 _TXN_CONC_WINDOW 页, 见底即截断丢弃其后页.

        见底两种信号: 短页 (len<_TXN_PAGE) 或页请求失败 — serve 对超出数据
        末尾的 start 返回 HTTP 500 (TDX 协议解析空 body 报错, 而非返回空列表),
        故失败页视为越界, 保留已成功的更近页. 真网络抖动由 fetch 内换实例
        重试一次兜底.
        """
        ports = self._pool.ports_for(quotes_only)
        if not ports:
            raise EasyTdxError("no easy-tdx worker available for transaction paging")

        def fetch(offset: int) -> list[dict[str, Any]]:
            primary = ports[(offset // _TXN_PAGE) % len(ports)]
            last_error: Exception | None = None
            for port in (primary, ports[(offset // _TXN_PAGE + 1) % len(ports)]):
                try:
                    payload = self._pool.request(
                        "GET", f"{base_path}&start={offset}&count={_TXN_PAGE}",
                        port=port, timeout=20.0,
                    )
                    return payload.get("data") or []
                except EasyTdxError as exc:
                    last_error = exc
            raise EasyTdxError(f"transaction page start={offset} failed: {last_error}")

        pages: dict[int, list[dict[str, Any]]] = {0: fetch(0)}
        window = 1
        while window < _TXN_MAX_PAGES and len(pages[0]) == _TXN_PAGE:
            offsets = list(range(window, min(window + _TXN_CONC_WINDOW, _TXN_MAX_PAGES)))
            with ThreadPoolExecutor(max_workers=len(offsets)) as executor:
                future_of = {off: executor.submit(fetch, off * _TXN_PAGE) for off in offsets}
                bottom: int | None = None
                for off in offsets:
                    try:
                        pages[off] = future_of[off].result()
                    except EasyTdxError:
                        bottom = off - 1  # 越界见底: 该页及其后丢弃
                        break
                    if len(pages[off]) < _TXN_PAGE:
                        bottom = off
                        break
            if bottom is not None:
                for page in [p for p in pages if p > bottom]:
                    del pages[page]  # 见底后乐观多拉的页丢弃
                break
            window += _TXN_CONC_WINDOW
        # 页 k 覆盖更老区间, 页内升序 → 逆页序拼接 = 全天升序
        return [row for page in sorted(pages, reverse=True) for row in pages[page]]

    # ------------------------------------------------------------------
    # 财务 (最新单期快照; 历史多期二期接新浪三表)
    # ------------------------------------------------------------------
    def get_financials(
        self,
        table: str,
        symbols: list[str],
        *,
        latest_only: bool = True,
    ) -> pl.DataFrame:
        """财务表: 三表走新浪 f10 (多期), metrics/shares 走 /finance 单期快照.

        - income/balance_sheet/cash_flow: serve /sina/financial-report (新浪直连,
          独立于 TDX 行情服务器), latest_only=False 取最近 8 期季报 (2026-09-04 起,
          此前"仅最新单期"的限制解除). announce_date 无源数据 → None.
        - metrics/shares: /finance (pytdx get_finance_info) 最新单期, 见 _FINANCE_MAP.
        """
        if table in _SINA_REPORT_TYPE:
            if not symbols:
                return pl.DataFrame()
            report_type = _SINA_REPORT_TYPE[table]
            field_map = _SINA_FIELD_MAP[table]
            num = 1 if latest_only else _SINA_HISTORY_PERIODS
            rows = self._sina_f10_many(symbols, report_type, field_map, num)
            if not rows:
                return pl.DataFrame()
            return pl.DataFrame(rows).sort(["symbol", "period_end"])

        if table == "metrics":
            # 新浪派生: 同比 (原生) + 毛利率 + 资产负债率; roe 等留空由 fuyao 旧行补齐
            if not symbols:
                return pl.DataFrame()
            num = 1 if latest_only else _SINA_HISTORY_PERIODS
            lrb = self._sina_f10_many(symbols, _SINA_LRB, _SINA_FIELD_MAP["income"], num)
            fzb = self._sina_f10_many(symbols, _SINA_FZB, {
                "资产总计": "total_assets", "负债合计": "total_liabilities",
            }, num)
            return self._derive_metrics(lrb, fzb)

        mapping = _FINANCE_MAP.get(table)
        if mapping is None:
            raise EasyTdxError(
                f"easy-tdx financial table {table!r} not mapped "
                f"(available: {sorted(_SINA_REPORT_TYPE)} + {sorted(_FINANCE_MAP)})"
            )
        if not latest_only:
            raise EasyTdxError(
                "easy-tdx /finance provides latest snapshot only; "
                "history is available for income/balance_sheet/cash_flow via sina f10"
            )
        if not symbols:
            return pl.DataFrame()
        records = self._pool.fan_out(
            list(symbols), lambda symbol, port: self._finance_one(symbol, mapping, port),
            concurrency=_FIN_CONCURRENCY,
        )
        records = [r for r in records if r]
        if not records:
            return pl.DataFrame()
        frame = pl.DataFrame(records)
        return frame.sort(["symbol", "period_end"]).unique(subset=["symbol"], keep="last")

    def _sina_f10_many(
        self,
        symbols: list[str],
        report_type: str,
        field_map: dict[str, str],
        num: int,
    ) -> list[dict[str, Any]]:
        """多标的新浪 f10 平铺并发 (不经过 fan_out — sina 通道无 TDX 锁,
        组内串行是 fan_out 语义, 会把吞吐压到端口数; 平铺 32 线程打满 RTT)."""
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=_SINA_CONCURRENCY) as executor:
            batches = list(executor.map(
                lambda s: self._sina_f10_one(s, report_type, field_map, num), symbols))
        return [r for batch in batches if batch for r in batch]

    def _sina_metrics_fzb(self, symbol: str, num: int) -> list[dict[str, Any]]:
        """metrics 派生用轻量 fzb 行 (报告期/资产总计/负债合计)。"""
        rows = self._sina_f10_one(symbol, _SINA_FZB, {
            "资产总计": "total_assets", "负债合计": "total_liabilities",
        }, num)
        return rows

    def _derive_metrics(self, lrb_rows: list[dict[str, Any]],
                        fzb_rows: list[dict[str, Any]]) -> pl.DataFrame:
        """lrb + fzb → metrics 行 (canonical 列子集, 无口径争议项)."""
        by_key_fzb = {(r["symbol"], r["period_end"]): r for r in fzb_rows}
        out: list[dict[str, Any]] = []
        for r in lrb_rows:
            key = (r["symbol"], r["period_end"])
            m: dict[str, Any] = {
                "symbol": r["symbol"], "period_end": r["period_end"],
                "announce_date": r.get("announce_date"),
                "eps_basic": r.get("basic_eps"),
                "revenue_yoy": r.get("revenue_yoy"),
                "net_income_yoy": r.get("net_income_yoy"),
            }
            revenue = r.get("revenue")
            cost = r.get("operating_cost")
            if revenue and cost is not None and revenue != 0:
                m["gross_margin"] = (revenue - cost) / revenue
            f = by_key_fzb.get(key)
            if f and f.get("total_assets") and f.get("total_liabilities") is not None:
                m["debt_to_asset_ratio"] = f["total_liabilities"] / f["total_assets"]
            # 至少有一个有效指标才输出
            if any(m.get(c) is not None for c in
                   ("eps_basic", "revenue_yoy", "net_income_yoy",
                    "gross_margin", "debt_to_asset_ratio")):
                out.append(m)
        if not out:
            return pl.DataFrame()
        return pl.DataFrame(out).sort(["symbol", "period_end"])

    def _sina_f10_one(
        self,
        symbol: str,
        report_type: str,
        field_map: dict[str, str],
        num: int,
    ) -> list[dict[str, Any]]:
        """单标的新浪 f10 三表 → canonical 行 (最新期在前, 全部返回).

        sina=True 通道: serve 侧 SinaClient 直连新浪, 不经 TDX 连接 →
        绕过端口锁, 高并发安全 (财务全市场拉取提速用).
        """
        _, code = _split_symbol(symbol)
        try:
            payload = self._pool.request(
                "GET",
                f"/sina/financial-report?code={code}&type={report_type}&num={num}",
                timeout=30.0,
                sina=True,
            )
        except EasyTdxError as exc:
            logger.warning("easy-tdx sina f10 %s %s failed: %s", symbol, report_type, exc)
            return []
        rows = payload.get("data") or []
        out: list[dict[str, Any]] = []
        for r in rows:
            period = _iso_date(r.get("报告期"))
            if period is None:
                continue
            row: dict[str, Any] = {
                "symbol": symbol,
                "period_end": period,
                "announce_date": None,  # 新浪 f10 无公告日期
            }
            for src, dst in field_map.items():
                value = _number(r.get(src))
                if value is not None:
                    if dst == "revenue_bank":
                        # 银行/保险回退科目: 仅当标准科目 (营业总收入) 缺失时采用
                        if row.get("revenue") is None:
                            row["revenue"] = value
                        continue
                    row[dst] = value
            out.append(row)
        return out

    def _finance_one(self, symbol: str, mapping: list[tuple[str, str, float]], port: int) -> dict[str, Any] | None:
        market, code = _split_symbol(symbol)
        try:
            payload = self._pool.request(
                "GET", f"/finance?market={market}&code={code}", port=port, timeout=15.0,
            )
        except EasyTdxError as exc:
            logger.warning("easy-tdx finance %s failed: %s", symbol, exc)
            return None
        rows = payload.get("data") or []
        if not rows:
            return None
        row = rows[0]
        updated = _iso_date(row.get("updated_date"))
        record: dict[str, Any] = {
            "symbol": symbol,
            "period_end": updated,
            "announce_date": updated,
        }
        for src, target, scale in mapping:
            value = _number(row.get(src))
            if value is not None:
                record[target] = value * scale
        return record

    # ------------------------------------------------------------------
    # 测试钩子
    # ------------------------------------------------------------------
    def test_dataset(self, dataset: str, symbols: list[str] | None = None) -> dict[str, Any]:
        symbol_list = symbols or ["600519.SH"]
        if dataset == "daily":
            frame = self.get_daily(symbol_list, None, None)
        elif dataset == "adj_factor":
            frame = self.get_adj_factors(symbol_list, datetime.now() - timedelta(days=90), None)
        elif dataset == "minute":
            frame = self.get_minute(symbol_list, None, None)
        elif dataset == "realtime":
            records = self.get_realtime(symbols=symbol_list)
            return {"provider": self.name, "dataset": dataset, "rows": len(records),
                    "columns": list(records[0]) if records else [],
                    "preview": records[:5]}
        elif dataset == "depth5":
            depth = self.get_depth5(symbol_list)
            return {"provider": self.name, "dataset": dataset, "rows": len(depth),
                    "columns": list(next(iter(depth.values()))) if depth else [],
                    "preview": [{"symbol": s, **v} for s, v in list(depth.items())[:5]]}
        elif dataset == "transactions":
            result = self.get_transactions(symbol_list[0])
            return {"provider": self.name, "dataset": dataset, "rows": len(result["rows"]),
                    "columns": list(result["rows"][0]) if result["rows"] else [],
                    "paged_via": result["paged_via"],
                    "preview": result["rows"][:3]}
        elif dataset == "financial":
            frame = self.get_financials("metrics", symbol_list, latest_only=True)
            return {"provider": self.name, "dataset": dataset, "rows": frame.height,
                    "columns": frame.columns,
                    "preview": frame.head(5).to_dicts() if not frame.is_empty() else []}
        elif dataset in ("income", "balance_sheet", "cash_flow"):
            frame = self.get_financials(dataset, symbol_list, latest_only=False)
            return {"provider": self.name, "dataset": dataset, "rows": frame.height,
                    "columns": frame.columns,
                    "preview": frame.head(8).to_dicts() if not frame.is_empty() else []}
        else:
            raise ValueError(f"easy-tdx does not support dataset {dataset!r}")
        return {"provider": self.name, "dataset": dataset, "rows": frame.height,
                "columns": frame.columns,
                "preview": frame.head(5).to_dicts() if not frame.is_empty() else []}


def _minute_category(freq: str) -> str | None:
    return {"1m": "MIN_1", "5m": "MIN_5", "15m": "MIN_15", "30m": "MIN_30", "60m": "MIN_60"}.get(
        str(freq).lower()
    )
