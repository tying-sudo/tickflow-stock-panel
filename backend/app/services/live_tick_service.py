"""盘中实时分笔+五档缓存服务 — 0.5s 节流服务器池轮询.

前端面板已按 500ms 轮询 /api/kline/transactions 与 /api/intraday/depth5;
后端若每次全量 live 翻页 (分笔 2s+/次) 会把实例池打垮。本服务按 symbol 缓存:
  - 首次: 全天分笔分页拉取 (盘中当日 live / 非盘中走归档回退链)
  - 之后每 ≥0.5s: 分笔只拉尾页 (start=0 最新 800 条) 增量合并 + 五档单请求
  - 非连续竞价时段: 分笔停刷, 五档 5s 一次 (交易所残留盘口)
  - 盘中日期归属: 拉取时刻为工作日 ≥09:15 → 当日 (修复今日 live 被标成昨日)
  - 10 分钟无访问驱逐缓存
"""
from __future__ import annotations

import threading
import time

logger = __import__("logging").getLogger(__name__)

_TAIL_MIN_INTERVAL = 0.5    # 分笔尾页刷新最小间隔 (秒)
_DEPTH_MIN_INTERVAL = 0.5   # 五档刷新最小间隔 (连续竞价时段)
_DEPTH_IDLE_INTERVAL = 5.0  # 非连续竞价时段五档间隔 (残留盘口)
_ENTRY_TTL = 600.0          # 缓存条目无访问驱逐 (秒)


def _key(t: dict) -> tuple:
    return (t.get("time"), round(float(t.get("price") or 0), 4),
            round(float(t.get("volume") or 0), 3), t.get("num"), t.get("buyorsell"))


def _merge_tail(cached: list[dict], tail: list[dict]) -> list[dict] | None:
    """尾页增量合并: 找 cached 后缀与 tail 前缀的最大重叠, 追加余下新行.

    返回 None = 重叠断裂 (如开盘瞬间增量 >800 条) 或前缀不符, 调用方全量重拉.
    """
    if not cached:
        return tail
    if not tail:
        return cached
    if len(tail) > len(cached):
        # 当日总量不足一页期: tail 即全天, 且应以 cached 为前缀
        if [_key(t) for t in tail[:len(cached)]] == [_key(t) for t in cached]:
            return tail
        return None
    ck = [_key(t) for t in cached[-len(tail):]]
    tk = [_key(t) for t in tail]
    first = tk[0]
    n = len(tk)
    for k in range(n, 0, -1):
        if ck[len(ck) - k] == first and ck[len(ck) - k:] == tk[:k]:
            return cached + tail[k:]
    return None


def _trading_started_now() -> bool:
    """工作日且北京 09:15-17:00 — 当日端点持有完整当日数据.

    端点在晚间会被 serve 侧逐步清理 (实测 22:47 只剩 411/4200 笔), 夜间
    必须走归档链; 收盘后 2 小时缓冲覆盖深市盘后定价 (15:30) 与写盘延迟。
    """
    from datetime import time as _time

    from app.market_time import cn_now

    now = cn_now()
    return (now.weekday() < 5
            and _time(9, 15) <= now.time() <= _time(17, 0))


def _continuous_trading() -> bool:
    from app.services.depth_service import DepthService

    return DepthService._is_continuous_trading()


class _Entry:
    __slots__ = ("lock", "day", "ticks", "depth", "loaded",
                 "last_tail", "last_depth", "last_access")

    def __init__(self, symbol: str) -> None:
        self.lock = threading.Lock()
        self.day: str | None = None
        self.ticks: list[dict] = []
        self.depth: dict | None = None
        self.loaded = False
        self.last_tail = 0.0
        self.last_depth = 0.0
        self.last_access = time.monotonic()
        _ = symbol


class LiveTickService:
    """单例. 前端 0.5s 轮询 → 命中缓存秒回; 对实例池至多 0.5s 一次/类/symbol."""

    _instance: "LiveTickService | None" = None
    _instance_lock = threading.Lock()

    @classmethod
    def get(cls) -> "LiveTickService":
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = LiveTickService()
            return cls._instance

    def __init__(self) -> None:
        self._entries: dict[str, _Entry] = {}
        self._lock = threading.Lock()

    def _entry(self, symbol: str) -> _Entry:
        now = time.monotonic()
        with self._lock:
            # 顺手驱逐过期条目 (条目数小, O(n) 可接受)
            dead = [s for s, e in self._entries.items() if now - e.last_access > _ENTRY_TTL]
            for s in dead:
                del self._entries[s]
            e = self._entries.get(symbol)
            if e is None:
                e = _Entry(symbol)
                self._entries[symbol] = e
            return e

    # ---- 分笔 ----------------------------------------------------------

    def transactions(self, repo, symbol: str, stock_name: str | None = None,
                     _light: bool = False) -> dict:
        """当日分笔快照 (缓存 + 0.5s 尾页增量). 非盘中走既有归档回退链.

        _light=True (尾部增量轮询用): 响应省略 minute_volumes/segments/consistency
        重计算 — 前端明细面板只用 ticks/date/source/tick_count, 万笔级列表
        0.5s 轮询下省掉每轮 ~10ms 的聚合开销.
        """
        from app.plugins.easy_tdx.provider import EasyTdxProvider
        from app.services import tick_transactions

        e = self._entry(symbol)
        with e.lock:
            e.last_access = time.monotonic()
            provider = EasyTdxProvider()
            if not e.loaded:
                if _trading_started_now():
                    raw = provider.get_transactions(symbol, None).get("rows") or []
                    if raw:
                        from app.market_time import cn_today

                        e.ticks = tick_transactions.normalize_live_rows(raw)
                        e.day = cn_today().isoformat()
                        e.loaded = True
                if not e.loaded:
                    # 非盘中/维护窗口: 既有链 (归档优先 + 成交量匹配归属)
                    r = tick_transactions.build_transactions(repo, symbol, None, stock_name)
                    e.day, e.ticks = r.get("date"), r.get("ticks") or []
                    e.loaded = True
                    return r
            elif _continuous_trading() and time.monotonic() - e.last_tail >= _TAIL_MIN_INTERVAL:
                e.last_tail = time.monotonic()
                try:
                    tail_raw = provider.fetch_today_tail(symbol)
                    merged = _merge_tail(e.ticks, tick_transactions.normalize_live_rows(tail_raw))
                    if merged is None:
                        # 重叠断裂 (增量>800/拍) → 全量重拉
                        raw = provider.get_transactions(symbol, None).get("rows") or []
                        if raw:
                            e.ticks = tick_transactions.normalize_live_rows(raw)
                    elif merged is not e.ticks:
                        e.ticks = merged
                except Exception as exc:  # noqa: BLE001
                    logger.debug("live tail refresh %s failed: %s", symbol, exc)
            if _light:
                return {
                    "symbol": symbol, "name": stock_name, "date": e.day,
                    "requested_date": None, "source": "easy_tdx_live",
                    "precision": "minute", "tick_count": len(e.ticks),
                    "ticks": e.ticks, "minute_volumes": [],
                    "segments": {"auction": False, "after_hours": False},
                    "consistency": {"checked": False},
                }
            return tick_transactions.build_response_from_ticks(
                repo, symbol, e.day, e.ticks, stock_name,
                source="easy_tdx_live", requested_date=None,
            )

    def transactions_tail(self, repo, symbol: str, after: int,
                          date_hint: str | None, stock_name: str | None = None) -> dict:
        """尾部增量: 返回 {date, tick_count, full, appended}.

        前端 0.5s 轮询消抖 (2026-09-05): 无新成交时响应 ~百字节, 前端不动状态
        → 列表不重渲染、滚动位置不重置。date_hint/after 与服务端不符 (跨日/
        缓存重建/计数漂移) → full=True 携带全量 ticks 让客户端重置。
        """
        snap = self.transactions(repo, symbol, stock_name, _light=True)
        ticks = snap.get("ticks") or []
        day = snap.get("date")
        count = len(ticks)
        if day != date_hint or after < 0 or after > count:
            return {"symbol": symbol, "date": day, "source": snap.get("source"),
                    "tick_count": count, "full": True, "appended": ticks}
        return {"symbol": symbol, "date": day, "source": snap.get("source"),
                "tick_count": count, "full": False, "appended": ticks[after:]}

    # ---- 五档 ----------------------------------------------------------

    def depth5(self, symbol: str) -> dict | None:
        """五档快照 (缓存 + 0.5s/5s 节流). 失败保留旧值, 供调用方回退."""
        from app.plugins.easy_tdx.provider import EasyTdxProvider

        e = self._entry(symbol)
        with e.lock:
            e.last_access = time.monotonic()
            interval = _DEPTH_MIN_INTERVAL if _continuous_trading() else _DEPTH_IDLE_INTERVAL
            if e.depth is None or time.monotonic() - e.last_depth >= interval:
                try:
                    e.depth = EasyTdxProvider().get_depth5([symbol]).get(symbol)
                    e.last_depth = time.monotonic()
                except Exception as exc:  # noqa: BLE001
                    logger.debug("live depth refresh %s failed: %s", symbol, exc)
            return e.depth
