"""真实 tick 级分笔成交 (easy-tdx + 本地归档, 全部为真实成交记录, 严禁模拟/随机/占位)。

数据源 — 通达信分笔协议 (经 easy-tdx serve 实例池, 2026-09-04 起替代 VM102 TdxW 网关):
  - 当日端点: 最近交易日可用 (仅白名单 4 台主机有数据, provider quotes 子池路由)
  - 历史端点: /transaction/history 可回溯 ≥30 天 (实测 2026-09-04), 全池并发
  - 时间精度为分钟级 (HH:MM); 含方向 (买/卖/中性); 无成交笔数字段 (num=0)
  - 包含集合竞价段 (09:15-09:25) 与深市盘后定价交易段 (15:05-15:30)

本地归档 — tick_archive (盘后全市场落盘, 2026-09-04 起): 读取归档优先, 永久保留;
30 天前的未归档日期如实 404 (g4tic/VM102 通道已随 tdx_gateway 源一并移除, 2026-09-05)。

日期路由: 无 date / 最近交易日 → 当日端点; 其他日期 → 历史端点。
量纲统一: 成交量=手, 金额=元, 价格=元。
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

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


def normalize_live_rows(rows: list[dict]) -> list[dict]:
    """easy-tdx serve 分笔行 {datetime, price, vol, buyorsell} → 旧契约字段。

    当日端点 datetime 日期按今天拼接不可信, 但 HH:MM 时间部分可信;
    真实交易日由 _pick_tick_date 成交量匹配判定。direction 字符串同步给出,
    供归档落盘直接携带方向 (upsert/EOD 路径不经过 _normalize_tdx_ticks)。
    """
    out: list[dict] = []
    for r in rows:
        dt = str(r.get("datetime") or "")
        bos = r.get("buyorsell")
        out.append({
            "time": dt[11:16] if len(dt) >= 16 else "",
            "price": r.get("price"),
            "volume": r.get("vol"),        # 手
            "num": 0,                      # easy_tdx 协议解析无笔数字段
            "buyorsell": bos,
            "direction": _DIRECTION_LABELS.get(bos, "other"),
        })
    return out


def fetch_raw_ticks(symbol: str, trade_date: str | None = None) -> list[dict]:
    """拉取分笔原始 rows (time/price/volume/num/buyorsell)。空列表 = 数据源无数据。

    经 easy-tdx serve 实例池: trade_date=None → 当日端点 (白名单子池);
    trade_date=历史日 → 历史端点 (全池)。
    """
    from app.plugins.easy_tdx.provider import EasyTdxError, EasyTdxProvider

    try:
        result = EasyTdxProvider().get_transactions(symbol, trade_date)
    except EasyTdxError as exc:
        raise TickUnavailable(f"easy-tdx 分笔通道错误: {exc}") from exc
    return normalize_live_rows(result["rows"])


# ── g4tic 历史分笔通道已移除 (2026-09-05, tdx_gateway/VM102 源下线) ──
# 历史分笔由 easy-tdx /transaction/history (≥30 天) + 本地归档 (tick_archive,
# 盘后全市场落盘永久保留) 承接; 30 天前未归档日期如实 404。


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
        raw_dir = t.get("buyorsell")
        if raw_dir is None:  # 归档回读: 只有 direction 字符串
            raw_dir = t.get("direction")
        if isinstance(raw_dir, int):
            direction = _DIRECTION_LABELS.get(raw_dir, "other")
        else:
            direction = raw_dir if raw_dir in ("buy", "sell") else "other"
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
    """分笔成交完整响应。所有数据均为真实成交记录, 拿不到即抛 TickUnavailable。

    读取优先级: 本地归档 (盘后落盘) → easy-tdx live (当日/历史端点, ≥30 天)。
    live 成功且满足归档时机 (历史日 / 盘后) 时顺手懒缓存。
    """
    from app.services import tick_archive

    archived = tick_archive.load_symbol(repo, symbol, requested_date)
    if archived:
        day = requested_date or tick_archive._cn_today_str()
        return _build_from_archive(repo, symbol, day, archived, stock_name)

    if requested_date is None:
        try:
            result = _build_tdx(repo, symbol, None, stock_name)
        except TickUnavailable:
            result = None
        live_date = result.get("date") if result else None
        if result and live_date:
            # 归属明确: 盘中当日 → 返回 live; 盘后 (≥15:35) 顺手归档当日
            # (EOD job 失联兜底); 盘前维护窗口 live_date 可能误归属昨日, 不归档
            if live_date == tick_archive._cn_today_str():
                tick_archive.lazy_archive_if_due(repo, symbol, live_date, result.get("ticks") or [])
            return result
        # live 空/维护窗口垃圾 (无法归属交易日, 如 09:09 清空后残留几条竞价)
        # → 回退本地归档最近一日
        dates = tick_archive.archived_dates(repo)
        for day in reversed(dates):
            rows = tick_archive.load_symbol(repo, symbol, day)
            if rows:
                return _build_from_archive(repo, symbol, day, rows, stock_name)
        if result:
            return result
        raise TickUnavailable("行情服务器未返回分笔数据, 且本地无归档 (可能非交易时段)")

    result = _build_tdx(repo, symbol, requested_date, stock_name)
    tick_archive.lazy_archive_if_due(repo, symbol, requested_date, result.get("ticks") or [])
    return result


def build_response_from_ticks(repo, symbol: str, day: str | None, ticks: list[dict],
                              stock_name: str | None = None,
                              source: str = "local_archive",
                              requested_date: str | None = None) -> dict:
    """归一化 ticks (time/price/volume/num/direction) → 完整响应契约。"""
    minute_volumes = _aggregate_minutes(ticks)
    segments = {
        "auction": any(m["segment"] == "auction" for m in minute_volumes),
        "after_hours": any(m["segment"] == "after_hours" for m in minute_volumes),
    }
    return {
        "symbol": symbol,
        "name": stock_name,
        "date": day,
        "requested_date": requested_date,
        "source": source,
        "precision": "minute",
        "tick_count": len(ticks),
        "ticks": ticks,
        "minute_volumes": minute_volumes,
        "segments": segments,
        "consistency": _consistency_check(repo, symbol, day, ticks),
    }


def _build_from_archive(repo, symbol: str, day: str, rows: list[dict],
                        stock_name: str | None) -> dict:
    """本地归档 → 完整响应 (ticks 已是归一化契约字段, direction 为字符串)。"""
    ticks = [{
        "time": r.get("time") or "",
        "price": float(r.get("price") or 0),
        "volume": float(r.get("volume") or 0),
        "num": int(r.get("num") or 0),
        "direction": r.get("direction") or "other",
    } for r in rows]
    return build_response_from_ticks(repo, symbol, day, ticks, stock_name,
                                     source="local_archive", requested_date=day)


def _build_tdx(repo, symbol: str, requested_date: str | None,
               stock_name: str | None) -> dict:
    """数据源 A: TDX 分笔协议 (easy-tdx serve 实例池, 分钟级, 含方向)。

    requested_date=None → 当日端点; 给定日期 → 历史端点 (≥30 天), 空时回退当日端点
    (requested_date 恰为最近交易日的场景)。
    """
    raw = fetch_raw_ticks(symbol, requested_date)
    if not raw and requested_date:
        # 历史端点无数据时回退当日端点 (date 为最近交易日时, 当日分笔即该日数据)
        raw = fetch_raw_ticks(symbol, None)
    if not raw:
        if requested_date:
            raise TickUnavailable(
                f"数据源不提供 {requested_date} 的分笔记录 "
                "(通达信公共行情历史分笔仅保留约 30 天)",
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
        "source": "easy_tdx_ticks",
        "precision": "minute",
        "tick_count": len(ticks),
        "ticks": ticks,
        "minute_volumes": minute_volumes,
        "segments": segments,
        "consistency": _consistency_check(repo, symbol, tick_date, ticks),
    }


