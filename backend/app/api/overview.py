"""市场总览聚合 API。"""
from __future__ import annotations

import math
import re
import threading
import time
from datetime import date
from typing import Any

import polars as pl
from fastapi import APIRouter, Request

from app.services.ext_data import ExtConfig, ExtConfigStore
from app.services.screener import ScreenerService

router = APIRouter(prefix="/api/overview", tags=["overview"])

_CACHE_TTL_LATEST = 5.0      # 最新日: 实时轮询期间频繁失效, 短 TTL 防陈旧
_CACHE_TTL_HISTORICAL = 600.0  # 历史日: 快照不可变, 长缓存让切日期零重建(数据/配置变更走 invalidate)
_CACHE_MAX_ENTRIES = 8       # 近期查看过的日期各留一份, 日历来回切换即时响应
# 缓存跨线程读写锁: market_overview 在 FastAPI 线程池读, invalidate 在数据刷新线程清,
# 无锁会读到撕裂/过期状态。用模块级 Lock 守护 check-then-set 与 clear。
_cache: dict[str, tuple[float, dict]] = {}
_cache_lock = threading.Lock()


def invalidate_overview_cache() -> None:
    """清空总览聚合结果缓存。

    清除数据后调用, 避免看板在 TTL 窗口内继续返回旧的聚合结果。
    """
    with _cache_lock:
        _cache.clear()


CORE_INDEX_NAMES = {
    "000001.SH": "上证指数",
    "399001.SZ": "深证成指",
    "399006.SZ": "创业板指",
    "000680.SH": "科创综指",
}
CORE_INDEX_SYMBOLS = tuple(CORE_INDEX_NAMES.keys())

_DIMENSION_SEP = re.compile(r"[、,，;；|/\s]+")


def _dimension_field(config: ExtConfig, kind: str) -> str | None:
    candidates = ["概念", "concept", "theme"] if kind == "concept" else ["行业", "industry", "sector"]
    for candidate in candidates:
        needle = candidate.lower()
        for field in config.fields:
            haystack = f"{field.name} {field.label}".lower()
            if needle in haystack:
                return field.name
    return None


def _ext_files(data_dir, config: ExtConfig) -> list[str]:
    base = data_dir / "ext_data" / config.id
    if config.mode == "timeseries":
        root = base / "timeseries"
        return [str(p) for p in sorted(root.rglob("*.parquet")) if p.is_file()]
    return [str(p) for p in sorted(base.glob("*.parquet")) if p.is_file()]


def _read_ext_rows(data_dir, config: ExtConfig, dimension_field: str) -> list[dict]:
    files = _ext_files(data_dir, config)
    if not files:
        return []
    try:
        df = pl.read_parquet(files, hive_partitioning=True)
    except TypeError:
        try:
            df = pl.read_parquet(files)
        except Exception:  # noqa: BLE001
            return []
    except Exception:  # noqa: BLE001
        return []
    if df.is_empty() or dimension_field not in df.columns:
        return []

    if config.mode == "timeseries" and "date" in df.columns:
        latest = df.get_column("date").max()
        if latest is not None:
            df = df.filter(pl.col("date") == latest)

    symbol_cols = ["symbol", "code", "股票代码", "代码"]
    for mapping in (config.symbol_map, config.code_map):
        if isinstance(mapping, dict) and mapping.get("type") == "mapped" and mapping.get("col"):
            symbol_cols.append(str(mapping["col"]))
    cols = []
    for col in [dimension_field, *symbol_cols]:
        if col in df.columns and col not in cols:
            cols.append(col)
    return df.select(cols).to_dicts()


def _dimension_values(raw: Any) -> list[str]:
    if raw is None:
        return []
    values = [v.strip() for v in _DIMENSION_SEP.split(str(raw).strip()) if v.strip()]
    return values


def _symbol_keys(row: dict, config: ExtConfig) -> list[str]:
    fields = ["symbol", "code", "股票代码", "代码"]
    for mapping in (config.symbol_map, config.code_map):
        if isinstance(mapping, dict) and mapping.get("type") == "mapped" and mapping.get("col"):
            fields.append(str(mapping["col"]))

    keys: list[str] = []
    for field in fields:
        raw = row.get(field)
        if raw is None:
            continue
        text = str(raw).strip().upper()
        if not text:
            continue
        keys.append(text)
        if "." in text:
            keys.append(text.split(".", 1)[0])
    return keys


def _dimension_rank(rows: list[dict], request: Request, kind: str, limit: int = 5, level: int | None = None) -> dict:
    if not rows:
        return {"leading": [], "lagging": []}

    quote_map: dict[str, dict] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        quote_map[symbol] = row
        quote_map[symbol.split(".", 1)[0]] = row

    store = ExtConfigStore(request.app.state.repo.store.data_dir)
    groups: dict[str, dict[str, dict]] = {}
    for config in store.load_all():
        field = _dimension_field(config, kind)
        if not field:
            continue
        for ext_row in _read_ext_rows(request.app.state.repo.store.data_dir, config, field):
            quote = None
            for key in _symbol_keys(ext_row, config):
                quote = quote_map.get(key)
                if quote:
                    break
            if not quote:
                continue
            symbol = str(quote.get("symbol") or "")
            for value in _dimension_values(ext_row.get(field)):
                # 行业按 "-" 拆分级: "银行-银行-股份制银行" → level=2 取"银行"(二级)
                if level is not None and "-" in value:
                    parts = value.split("-")
                    value = parts[level - 1] if level <= len(parts) else parts[-1]
                groups.setdefault(value, {})[symbol] = quote

    items = []
    for name, by_symbol in groups.items():
        stocks = list(by_symbol.values())
        changes = [_finite(s.get("change_pct")) for s in stocks]
        changes = [v for v in changes if v is not None]
        if not changes:
            continue
        leader = max(stocks, key=lambda s: _finite(s.get("change_pct")) or -999)
        items.append({
            "name": name,
            "count": len(stocks),
            "avg_pct": sum(changes) / len(changes),
            "up_count": sum(1 for v in changes if v > 0),
            "down_count": sum(1 for v in changes if v < 0),
            "amount": sum(_finite(s.get("amount")) or 0 for s in stocks),
            "leader": {
                "symbol": leader.get("symbol"),
                "name": leader.get("name"),
                "change_pct": _finite(leader.get("change_pct")),
            },
        })

    leading = sorted(items, key=lambda x: x["avg_pct"], reverse=True)[:limit]
    lagging = sorted(items, key=lambda x: x["avg_pct"])[:limit]
    return {"leading": leading, "lagging": lagging}


def _finite(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _board(symbol: str) -> str:
    if symbol.endswith(".BJ"):
        return "北交所"
    if symbol.startswith(("300", "301")):
        return "创业板"
    if symbol.startswith(("688", "689")):
        return "科创板"
    if symbol.endswith(".SH"):
        return "沪主板"
    if symbol.endswith(".SZ"):
        return "深主板"
    return "其他"


def _score(value: float, low: float, high: float) -> int:
    if high <= low:
        return 50
    return max(0, min(100, round((value - low) / (high - low) * 100)))


def _quote_status(request: Request) -> dict:
    qs = getattr(request.app.state, "quote_service", None)
    if not qs:
        return {"enabled": False, "running": False, "quote_age_ms": None, "is_trading_hours": False}
    return qs.status()


def _index_quotes(request: Request, as_of: date | None = None, symbols: list[str] | None = None) -> list[dict]:
    """指数行情 — 委托 market_overview_builder.index_quotes (与看板装配同源)。"""
    from app.services.market_overview_builder import index_quotes as _builder_index_quotes
    return _builder_index_quotes(
        getattr(request.app.state, "repo", None),
        getattr(request.app.state, "quote_service", None),
        as_of,
        symbols,
    )


def _top_rows(rows: list[dict], key: str, descending: bool, limit: int = 8) -> list[dict]:
    filtered = [r for r in rows if _finite(r.get(key)) is not None]
    filtered.sort(key=lambda r: _finite(r.get(key)) or 0, reverse=descending)
    return [
        {
            "symbol": r.get("symbol"),
            "name": r.get("name"),
            "close": _finite(r.get("close")),
            "change_pct": _finite(r.get("change_pct")),
            "amount": _finite(r.get("amount")),
            "turnover_rate": _finite(r.get("turnover_rate")),
            "board": _board(str(r.get("symbol") or "")),
        }
        for r in filtered[:limit]
    ]


def _pct_band_rows(values: list[float]) -> list[dict]:
    bands = [
        ("<-5%", None, -0.05),
        ("-5~-3%", -0.05, -0.03),
        ("-3~-1%", -0.03, -0.01),
        ("-1~0%", -0.01, 0),
        ("0~1%", 0, 0.01),
        ("1~3%", 0.01, 0.03),
        ("3~5%", 0.03, 0.05),
        (">5%", 0.05, None),
    ]
    total = len(values) or 1
    out = []
    for label, low, high in bands:
        count = 0
        for v in values:
            if low is None and v < high:
                count += 1
            elif high is None and v >= low:
                count += 1
            elif low is not None and high is not None and low <= v < high:
                count += 1
        out.append({"label": label, "count": count, "pct": count / total * 100})
    return out


def _dashboard_index_symbols() -> list[str]:
    """看板指数卡片偏好列表; 空 = 回退默认四大核心指数。"""
    from app.services import preferences
    try:
        return preferences.get_dashboard_index_symbols()
    except Exception:  # noqa: BLE001
        return []


def _build_overview(request: Request, as_of: date | None = None, index_symbols: list[str] | None = None) -> dict:
    """装配市场总览(委托给 services.market_overview_builder,保持行为一致)。

    逻辑已抽离至 build_market_overview,以解耦对 Request 的依赖,
    使大盘复盘等无 Request 的调用方可复用同一装配逻辑。
    """
    from app.services.market_overview_builder import build_market_overview
    return build_market_overview(
        repo=request.app.state.repo,
        quote_service=getattr(request.app.state, "quote_service", None),
        depth_service=getattr(request.app.state, "depth_service", None),
        as_of=as_of,
        index_symbols=index_symbols,
    )


@router.get("/market")
def market_overview(request: Request, as_of: date | None = None):
    """总览页单次请求聚合数据，避免前端拉全市场明细后再计算。

    缓存按 日期+指数列表 各留一份(上限 _CACHE_MAX_ENTRIES): 最新日 TTL 5s;
    历史日快照不可变, TTL 600s, 数据清除/刷新/扩展配置/看板指数变更均会
    调 invalidate_overview_cache 即时失效。
    """
    now = time.time()
    index_symbols = _dashboard_index_symbols()
    base_key = as_of.isoformat() if as_of else "latest"
    # 指数列表参与装配结果 → 必须进缓存键 (保存端点另会清缓存, 双保险)
    symbols_key = ",".join(index_symbols) if index_symbols else "core"
    cache_key = f"{base_key}@{symbols_key}"
    ttl = _CACHE_TTL_LATEST if as_of is None else _CACHE_TTL_HISTORICAL
    # 读缓存持锁, 避免与 invalidate 的 clear 竞态读到撕裂状态
    with _cache_lock:
        hit = _cache.get(cache_key)
        if hit is not None and (now - hit[0]) < ttl:
            return hit[1]
    # 装配在锁外进行 (耗时), 允许并发未命中时各自构建, 不长时间持锁串行化请求
    data = _build_overview(request, as_of, index_symbols or None)
    with _cache_lock:
        _cache[cache_key] = (now, data)
        if len(_cache) > _CACHE_MAX_ENTRIES:
            for k in sorted(_cache, key=lambda k: _cache[k][0])[:-_CACHE_MAX_ENTRIES]:
                _cache.pop(k, None)
    return data
