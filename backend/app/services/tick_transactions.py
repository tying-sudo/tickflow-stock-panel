"""真实 tick 级分笔成交 (两个数据源, 全部为真实成交记录, 严禁模拟/随机/占位)。

数据源 A — 通达信分笔协议 (经 TDX LAN Gateway pytdx 通道):
  - 仅最近交易日可用 (公共 TDX 行情服务器不提供历史分笔)
  - 时间精度为分钟级 (HH:MM); 含方向 (买/卖/中性)
  - 包含集合竞价段 (09:15-09:25) 与深市盘后定价交易段 (15:05-15:30)

数据源 B — 通达信官方 g4tic 全市场分笔打包 (普及版会员数据通道):
  - URL: tdx.com.cn/products/data/data/g4tic/{YYYYMMDD}.zip (~100MB/日)
  - VM102 本地已存 2025-07-01..2026-05-18 共 212 个交易日; 其余日期可后台补下
  - 时间为 Δt 计数×校准单位的推断值, 秒级粒度 (与官方 1 分钟K聚合同源, 跨标的校验误差<1%)
  - 无买卖方向字段; 与 1 分钟K完全同源 → 与日K一致性极高

日期路由: 无 date / 最近交易日 → 数据源 A; 其他日期 → 数据源 B (未下载时报 404 并
支持后台补下载)。量纲统一: 成交量=手, 金额=元, 价格=元。
"""
from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from collections import defaultdict

logger = logging.getLogger(__name__)

_TICK_PAGE_COUNT = 40000  # 桥接内部按 ~1800/页自动翻页, 覆盖全天 (活跃股可达 1万+ 笔)

# buyorsell → 方向标签 (TDX 分笔协议)
_DIRECTION_LABELS = {
    0: "buy",       # 买盘
    1: "sell",      # 卖盘
    2: "neutral",   # 中性盘
    5: "after_hours",  # 盘后定价成交 (深市 15:05-15:30)
    8: "auction",   # 集合竞价探测单/虚拟成交 (volume=0)
}


class TickUnavailable(RuntimeError):
    """tick 数据源无法提供所请求的数据 (如实上报, 不做任何数据代偿)。"""

    def __init__(self, message: str, code: str = "unavailable"):
        super().__init__(message)
        self.code = code


def _gateway_request(path: str, payload: dict, timeout: float = 120) -> dict:
    from app.plugins.tdx_gateway.provider import gateway_url, get_api_key

    token = get_api_key()
    if not token:
        raise TickUnavailable("TDX_GATEWAY_TOKEN 未配置")
    req = urllib.request.Request(
        gateway_url() + path,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        try:
            detail = json.loads(body)
        except (ValueError, TypeError):
            detail = {"detail": body[:200]}
        # 网关业务错误 (404 g4_not_downloaded 等) → 带错误码上抛
        inner = detail if isinstance(detail, dict) and "error" in detail else None
        if inner:
            raise TickUnavailable(
                str(detail.get("hint") or detail.get("detail") or detail.get("error")),
                code=str(detail.get("error")),
            ) from e
        raise TickUnavailable(f"tick 网关 HTTP {e.code}: {body[:200]}") from e


# ── 数据源 A: TDX 分笔协议 (最近交易日, 分钟级, 含方向) ──────────────────

def fetch_raw_ticks(symbol: str, trade_date: str | None = None) -> list[dict]:
    """拉取分笔原始 rows (time/price/volume/num/buyorsell)。空列表 = 数据源无数据。"""
    payload: dict = {"symbols": [symbol], "kind": "ticks", "count": _TICK_PAGE_COUNT}
    if trade_date:
        payload["date"] = trade_date.replace("-", "")
    data = _gateway_request("/v1/tickdata", payload)
    rows = (data.get("rows") or {}).get(symbol) or []
    # 桥接层错误标记 (不再静默吞异常)
    bad = [r for r in rows if isinstance(r, dict) and "error" in r]
    if bad and len(bad) == len(rows):
        raise TickUnavailable(f"分笔通道错误: {bad[0].get('error')}")
    return [r for r in rows if isinstance(r, dict) and "error" not in r]


def fetch_g4_ticks(symbol: str, trade_date: str) -> tuple[list[dict], dict]:
    """g4tic 历史分笔。返回 (ticks, meta); 分笔包未下载 → TickUnavailable(code=g4_not_downloaded)。"""
    data = _gateway_request("/v1/tickdata", {
        "symbols": [symbol], "kind": "ticks_g4", "date": trade_date.replace("-", ""),
    }, timeout=300)
    rows = (data.get("rows") or {}).get(symbol) or []
    meta = data.get("meta") or {}
    if not rows:
        raise TickUnavailable(
            f"{trade_date} 的 g4tic 分笔包中无 {symbol} 的记录",
            code="g4_symbol_missing",
        )
    return rows, meta


def g4_download_start(symbol_date: str) -> dict:
    return _gateway_request("/v1/g4dl", {"date": symbol_date, "action": "start"})


def g4_download_status(symbol_date: str) -> dict:
    return _gateway_request("/v1/g4dl", {"date": symbol_date, "action": "status"})


# ── 日K一致性核对 ─────────────────────────────────────────────────────────

def _daily_row(repo, symbol: str, day: str) -> dict | None:
    try:
        row = repo.execute_one(
            "SELECT volume, amount, high, low, close FROM kline_daily "
            "WHERE symbol = ? AND date = ?",
            [symbol, day],
        )
    except Exception:  # noqa: BLE001
        return None
    if not row:
        return None
    return {"volume": float(row[0] or 0), "amount": float(row[1] or 0),
            "high": float(row[2] or 0), "low": float(row[3] or 0)}


def _pick_tick_date(repo, symbol: str, ticks: list[dict], requested: str | None) -> str | None:
    """用成交量匹配判定分笔所属交易日 (TDX 分笔协议不带日期)。"""
    tick_vol = sum(float(t.get("volume") or 0) for t in ticks)
    if tick_vol <= 0:
        return None

    from app.market_time import cn_now, cn_today

    candidates: list[str] = []
    today = cn_today().isoformat()
    now = cn_now()
    if now.hour >= 9 and now.minute >= 15 and now.weekday() < 5:
        candidates.append(today)
    try:
        latest = repo.latest_daily_date()
        if latest is not None:
            candidates.append(str(latest))
    except Exception:  # noqa: BLE001
        pass
    if requested:
        candidates.insert(0, requested)
    seen: set[str] = set()
    candidates = [c for c in candidates if not (c in seen or seen.add(c))]

    for day in candidates:
        d = _daily_row(repo, symbol, day)
        if d and d["volume"] > 0:
            diff = abs(tick_vol - d["volume"]) / d["volume"]
            if diff < 0.05:
                return day
    for day in candidates:
        if _daily_row(repo, symbol, day):
            return day
    return None


def _latest_session_date(repo) -> str:
    """最近交易日 (ISO)。当天为工作日且不早于日K最新日 → 当天。"""
    from app.market_time import cn_today

    today = cn_today().isoformat()
    try:
        latest = repo.latest_daily_date()
        latest_str = str(latest) if latest is not None else ""
    except Exception:  # noqa: BLE001
        latest_str = ""
    if cn_today().weekday() < 5 and today >= latest_str:
        return today
    return latest_str or today


# ── 分钟聚合 ─────────────────────────────────────────────────────────────

def _aggregate_minutes(ticks: list[dict]) -> list[dict]:
    """分笔 → 分钟成交量聚合 (量纲: 手; 金额=价×量×100 元)。附时段标记。"""
    agg: dict[str, dict] = {}
    for t in ticks:
        time_str = str(t.get("time") or "")
        if len(time_str) < 4:
            continue
        minute = time_str[:5]
        vol = float(t.get("volume") or 0)
        price = float(t.get("price") or 0)
        slot = agg.setdefault(minute, {
            "minute": minute, "volume": 0.0, "buy_volume": 0.0,
            "sell_volume": 0.0, "amount": 0.0, "segment": "continuous",
        })
        slot["volume"] += vol
        slot["amount"] += price * vol * 100
        direction = _DIRECTION_LABELS.get(t.get("buyorsell"), "other")
        if direction == "buy":
            slot["buy_volume"] += vol
        elif direction == "sell":
            slot["sell_volume"] += vol
        if minute < "09:30":
            slot["segment"] = "auction"        # 集合竞价 (09:15-09:25)
        elif minute > "15:00":
            slot["segment"] = "after_hours"    # 盘后定价交易 (深市 15:05-15:30)
    return [agg[k] for k in sorted(agg)]


def _consistency_check(repo, symbol: str, tick_date: str | None,
                       ticks: list[dict]) -> dict:
    tick_vol = sum(float(t.get("volume") or 0) for t in ticks)
    prices = [float(t["price"]) for t in ticks if float(t.get("volume") or 0) > 0]
    tick_high = max(prices) if prices else None
    tick_low = min(prices) if prices else None
    consistency: dict = {"checked": False}
    if tick_date:
        d = _daily_row(repo, symbol, tick_date)
        if d and d["volume"] > 0:
            consistency = {
                "checked": True,
                "tick_volume": tick_vol,
                "daily_volume": d["volume"],
                "volume_diff_pct": round(abs(tick_vol - d["volume"]) / d["volume"] * 100, 3),
                "tick_high": tick_high,
                "tick_low": tick_low,
                "daily_high": d["high"],
                "daily_low": d["low"],
                "volume_mismatch": abs(tick_vol - d["volume"]) / d["volume"] >= 0.05,
                "note": "分笔含集合竞价与盘后定价段, 与日K成交量差异主要来自盘后定价",
            }
    return consistency


def _normalize_tdx_ticks(raw: list[dict]) -> list[dict]:
    ticks = []
    for t in raw:
        price = t.get("price")
        vol = t.get("volume")
        try:
            price = float(price) if price is not None else None
            vol = float(vol) if vol is not None else None
        except (TypeError, ValueError):
            continue
        if price is None or vol is None:
            continue
        bos = t.get("buyorsell")
        ticks.append({
            "time": str(t.get("time") or ""),
            "price": price,
            "volume": vol,                                    # 手
            "num": int(t.get("num") or 0),                    # 笔数
            "direction": _DIRECTION_LABELS.get(bos, "other"),
        })
    return ticks


# ── 组装入口 ─────────────────────────────────────────────────────────────

def build_transactions(repo, symbol: str, requested_date: str | None,
                       stock_name: str | None = None) -> dict:
    """分笔成交完整响应。所有数据均为真实成交记录, 拿不到即抛 TickUnavailable。"""
    if requested_date is None:
        return _build_tdx(repo, symbol, None, stock_name)
    try:
        return _build_tdx(repo, symbol, requested_date, stock_name)
    except TickUnavailable as tdx_err:
        try:
            return _build_g4(repo, symbol, requested_date, stock_name)
        except TickUnavailable as g4_err:
            # 两源都拿不到: 报 g4 的错误 (带可操作的下载指引错误码)
            raise g4_err from tdx_err


def _build_tdx(repo, symbol: str, requested_date: str | None,
               stock_name: str | None) -> dict:
    """数据源 A: TDX 分笔协议 (最近交易日, 分钟级, 含方向)。"""
    raw = fetch_raw_ticks(symbol, requested_date)
    if not raw and requested_date:
        # 公共服务器 history 分笔不可用; date 为最近交易日时, 当日分笔即该日数据
        raw = fetch_raw_ticks(symbol, None)
    if not raw:
        if requested_date:
            raise TickUnavailable(
                f"数据源不提供 {requested_date} 的分笔记录 (通达信公共行情仅保留最近交易日分笔)",
                code="tdx_no_history",
            )
        raise TickUnavailable("行情服务器未返回分笔数据 (可能非交易时段)")

    tick_date = _pick_tick_date(repo, symbol, raw, requested_date)
    if requested_date and tick_date and requested_date != tick_date:
        raise TickUnavailable(
            f"所请求日期 {requested_date} 与分笔数据所属日 {tick_date} 不符; "
            "分笔数据源仅提供最近交易日",
            code="tdx_date_mismatch",
        )
    ticks = _normalize_tdx_ticks(raw)
    minute_volumes = _aggregate_minutes(ticks)
    segments = {
        "auction": any(m["segment"] == "auction" for m in minute_volumes),
        "after_hours": any(m["segment"] == "after_hours" for m in minute_volumes),
    }
    return {
        "symbol": symbol,
        "name": stock_name,
        "date": tick_date,
        "requested_date": requested_date,
        "source": "tdx_gateway_ticks",
        "precision": "minute",
        "tick_count": len(ticks),
        "ticks": ticks,
        "minute_volumes": minute_volumes,
        "segments": segments,
        "consistency": _consistency_check(repo, symbol, tick_date, ticks),
    }


def _build_g4(repo, symbol: str, requested_date: str, stock_name: str | None) -> dict:
    """数据源 B: g4tic 历史分笔 (秒级推断时间, 无方向; 与官方 1 分钟K同源)。"""
    raw, meta = fetch_g4_ticks(symbol, requested_date)
    ticks = []
    for t in raw:
        try:
            price = float(t.get("price"))
            vol = float(t.get("volume") or 0)
        except (TypeError, ValueError):
            continue
        ticks.append({
            "time": str(t.get("time") or "") or "盘后",
            "price": price,
            "volume": vol,
            "num": 0,
            "direction": "other",  # g4tic 打包无买卖方向字段
        })
    minute_volumes = _aggregate_minutes(ticks)
    consistency = _consistency_check(repo, symbol, requested_date, ticks)
    consistency["note"] = (
        "g4tic 与官方 1 分钟K同源聚合, 量价与日K应完全一致; "
        "时间为 Δt 计数校准推断 (秒级粒度), 非交易所原始毫秒戳"
    )
    return {
        "symbol": symbol,
        "name": stock_name,
        "date": requested_date,
        "requested_date": requested_date,
        "source": "tdx_g4tic_pack",
        "precision": "second_inferred",
        "tick_count": len(ticks),
        "ticks": ticks,
        "minute_volumes": minute_volumes,
        "segments": {"auction": False, "after_hours": False},
        "consistency": consistency,
    }
