"""市场总览数据装配(与 HTTP Request 解耦)。

本模块由 `app.api.overview._build_overview` 抽离而来,目的是让「大盘复盘」
等无 Request 的调用方(定时任务、复盘服务)也能复用同一套聚合逻辑。

行为与原 `_build_overview` 完全一致,仅把对 `request.app.state.{repo,
quote_service,depth_service}` 的依赖改为显式参数。

公共入口:
    build_market_overview(repo, quote_service, depth_service, as_of)
"""
from __future__ import annotations

import math
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from typing import Any

import polars as pl

from app.services.ext_data import ExtConfig, ExtConfigStore
from app.services.screener import ScreenerService

# ================================================================
# 常量(与 overview.py 保持同步;复盘复盘仅 A 股核心指数)
# ================================================================

CORE_INDEX_NAMES = {
    "000001.SH": "上证指数",
    "399001.SZ": "深证成指",
    "399006.SZ": "创业板指",
    "000680.SH": "科创综指",
}
CORE_INDEX_SYMBOLS = tuple(CORE_INDEX_NAMES.keys())

_DIMENSION_SEP = re.compile(r"[、,，;；|/\s]+")


# ================================================================
# 通用工具
# ================================================================

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


_ETF_CODE_PREFIXES = ("50", "51", "52", "56", "57", "58", "59", "15", "16", "17", "18")


def _is_etf(symbol: str) -> bool:
    """场内 ETF/基金代码判定 (与 watchlist add-etf-group 同口径)。"""
    return symbol.startswith(_ETF_CODE_PREFIXES)


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


# ================================================================
# 指数行情(实时 quote_service 优先,回退 kline_index_daily SQL)
# ================================================================

def _index_name_map(repo) -> dict[str, str]:
    """指数代码 → 名称: 核心四指数 + 本地指数表 (看板自定义指数名称解析)。"""
    names = dict(CORE_INDEX_NAMES)
    if repo is not None:
        try:
            df = repo.get_index_instruments()
            if not df.is_empty() and "symbol" in df.columns and "name" in df.columns:
                for sym, nm in zip(df["symbol"].to_list(), df["name"].to_list()):
                    if sym and nm:
                        names.setdefault(str(sym), str(nm))
        except Exception:  # noqa: BLE001
            pass
    return names


def _quote_status(quote_service) -> dict:
    qs = quote_service
    if not qs:
        return {"enabled": False, "running": False, "quote_age_ms": None, "is_trading_hours": False}
    return qs.status()


def index_quotes(repo, quote_service, as_of: date | None = None, symbols: list[str] | None = None) -> list[dict]:
    """指数行情: 实时缓存优先, 实时缺席的指数按 symbol 回退日K最近收盘。

    symbols 为看板指数卡片列表 (偏好 dashboard_index_symbols, 可含用户自选指数),
    None/空 = 默认四大核心指数。

    逐 symbol 兜底 (2026-09-08): 实时缓存每轮整包替换, 上游批次级空包会让
    个别指数缺席 (曾见深证成指/创业板指整卡 '--'); 97/98 开头国证新指数
    TDX 无实时源。两类缺口都用 kline_index_daily 最近收盘补齐, 行带
    quote_date 供前端对非当日数据弱化展示。
    """
    symbols = [s for s in (symbols or list(CORE_INDEX_SYMBOLS)) if s]
    names = _index_name_map(repo)
    rows: list[dict] = []
    if quote_service and as_of is None:
        df = quote_service.get_index_quotes(symbols)
        if not df.is_empty():
            rows = df.to_dicts()

    cached = {r.get("symbol") for r in rows}
    missing = [s for s in symbols if s not in cached]
    if missing and repo:
        placeholders = ", ".join("?" for _ in missing)
        floor = (as_of or date.today()) - timedelta(days=30)
        try:
            db_rows = repo.execute_all(
                f"""
                WITH ranked AS (
                    SELECT symbol, date, close,
                           row_number() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
                    FROM kline_index_daily
                    WHERE symbol IN ({placeholders})
                      AND (? IS NULL OR date <= ?)
                      AND date >= ?
                ), latest AS (
                    SELECT symbol,
                           max(CASE WHEN rn = 1 THEN date END) AS date,
                           max(CASE WHEN rn = 1 THEN close END) AS last_price,
                           max(CASE WHEN rn = 2 THEN close END) AS prev_close
                    FROM ranked
                    WHERE rn <= 2
                    GROUP BY symbol
                )
                SELECT symbol, date, last_price, prev_close
                FROM latest
                """,
                [*missing, as_of, as_of, floor],
            )
        except Exception:  # noqa: BLE001
            db_rows = []
        for symbol, dt, last_price, prev_close in db_rows:
            change_amount = None
            change_pct = None
            lp = _finite(last_price)
            pc = _finite(prev_close)
            if lp is not None and pc not in (None, 0):
                change_amount = lp - pc
                change_pct = change_amount / pc * 100
            rows.append({
                "symbol": symbol,
                "name": names.get(symbol),
                "date": str(dt) if dt else None,
                "last_price": lp,
                "close": lp,
                "prev_close": pc,
                "change_amount": change_amount,
                "change_pct": change_pct,
            })

    by_symbol = {r.get("symbol"): r for r in rows}
    out = []
    for symbol in symbols:
        r = by_symbol.get(symbol, {"symbol": symbol})
        out.append({
            "symbol": symbol,
            "name": r.get("name") or names.get(symbol) or symbol,
            "last_price": _finite(r.get("last_price") if r.get("last_price") is not None else r.get("close")),
            "change_pct": _finite(r.get("change_pct")),
            "change_amount": _finite(r.get("change_amount")),
            "quote_date": r.get("date"),
        })
    return out


# ================================================================
# 扩展数据(行业 / 概念)维度聚合
# ================================================================

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
    values = [
        v.strip()
        for v in _DIMENSION_SEP.split(str(raw).strip())
        if v.strip() and v.strip().casefold() not in {"nan", "none", "null"}
    ]
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


def _ext_targets(configs: list[ExtConfig]) -> dict[str, list[tuple[ExtConfig, str]]]:
    """按维度收集需要读取的 (config, field) 对。

    概念/行业可能命中同一配置的不同字段; 调用方按 (config.id, field)
    去重读取, 避免同一扩展文件被两个维度各读一遍(旧实现串行读两遍)。
    """
    targets: dict[str, list[tuple[ExtConfig, str]]] = {"concept": [], "industry": []}
    for config in configs:
        for kind in targets:
            field = _dimension_field(config, kind)
            if field:
                targets[kind].append((config, field))
    return targets


def _rank_from_ext_items(
    rows: list[dict],
    ext_items: list[tuple[ExtConfig, str, list[dict]]],
    kind: str,
    limit: int = 5,
    level: int | None = None,
) -> dict:
    """从预读的扩展行聚合维度排名 — 读盘已在并行阶段完成, 此处纯内存。

    ext_items: [(config, field, ext_rows)], 聚合口径与旧 `_dimension_rank`
    (每个 kind 现场读盘) 完全一致, 仅把读盘挪到并行阶段并跨维度去重。
    """
    if not rows:
        return {"leading": [], "lagging": []}

    quote_map: dict[str, dict] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        quote_map[symbol] = row
        quote_map[symbol.split(".", 1)[0]] = row

    groups: dict[str, dict[str, dict]] = {}
    group_source: dict[str, str] = {}  # 组名 → 首个命中的扩展字段 "configId.field" (看板成分股弹窗用)
    for config, field, ext_rows in ext_items:
        source_field = f"{config.id}.{field}"
        for ext_row in ext_rows:
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
                group_source.setdefault(value, source_field)

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
            "source_field": group_source.get(name),
            "leader": {
                "symbol": leader.get("symbol"),
                "name": leader.get("name"),
                "change_pct": _finite(leader.get("change_pct")),
            },
        })

    leading = sorted(items, key=lambda x: x["avg_pct"], reverse=True)[:limit]
    lagging = sorted(items, key=lambda x: x["avg_pct"])[:limit]
    return {"leading": leading, "lagging": lagging}


# ================================================================
# Top 行 / 涨跌幅分桶
# ================================================================

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


# ================================================================
# 主装配入口
# ================================================================

def build_market_overview(
    repo,
    quote_service=None,
    depth_service=None,
    as_of: date | None = None,
    index_symbols: list[str] | None = None,
) -> dict:
    """装配市场总览(与原 overview._build_overview 行为一致)。

    Args:
        repo: KlineRepository(必填)。
        quote_service: QuoteService(可选;实时指数行情来源)。
        depth_service: DepthService(可选;五档封板修正)。
        as_of: 指定日期,None 则取最新有数据日。
        index_symbols: 看板指数卡片列表,None/空 = 默认四大核心指数。
    """
    index_symbols = [s for s in (index_symbols or list(CORE_INDEX_SYMBOLS)) if s]
    svc = ScreenerService(repo)
    # 调用方未指定日期时视为"最新"请求: 指数行情走实时缓存 (quote_service),
    # 其余装配仍以解析出的真实日期为准。显式指定日期(历史复盘)时才回退数据库。
    explicit_as_of = as_of is not None
    as_of = as_of or svc.latest_date()
    status = _quote_status(quote_service)

    if not as_of:
        return {
            "as_of": None,
            "quote_status": status,
            "indices": index_quotes(repo, quote_service, None if not explicit_as_of else as_of, index_symbols),
            "breadth": {"total": 0, "up": 0, "down": 0, "flat": 0, "up_pct": 0, "down_pct": 0},
            "amount": {"total": 0, "avg": 0},
            "boards": [],
            "limit": {"limit_up": 0, "broken": 0, "failed": 0, "limit_down": 0, "max_boards": 0, "tiers": []},
            "distribution": [],
            "trend": {"above_ma5": 0, "above_ma20": 0, "above_ma60": 0, "above_ma5_pct": 0, "above_ma20_pct": 0, "above_ma60_pct": 0, "new_high": 0, "new_low": 0},
            "activity": {"avg_turnover": 0, "high_turnover": 0, "high_vol_ratio": 0, "vol_ratio": 1},
            "radar": [],
            "emotion": {"score": 50, "label": "暂无"},
            "top_gainers": [],
            "top_losers": [],
            "turnover_leaders": [],
            "active_leaders": [],
            "concept_rank": {"leading": [], "lagging": []},
            "industry_rank": {"leading": [], "lagging": []},
        }

    # ── 并行阶段: 互不依赖的 IO 一次并发, 总耗时≈最慢一路而非四路累加 ──
    # 看板切历史交易日慢的主因是串行装配: enriched 行加载(历史日可能触发
    # 指标重算) + 指数查询 + 五档封板修正 + 扩展数据(概念/行业各读一遍盘)。
    # 线程安全: repo.execute_all 自带锁+独立 cursor, polars 读路径无共享写,
    # depth get_sealed_map 为内存/落盘缓存只读。
    data_dir = repo.store.data_dir
    ext_targets = _ext_targets(ExtConfigStore(data_dir).load_all())

    with ThreadPoolExecutor(max_workers=8) as pool:
        fut_rows = pool.submit(svc._load_enriched_for_date, as_of)
        fut_indices = pool.submit(
            index_quotes, repo, quote_service, None if not explicit_as_of else as_of, index_symbols,
        )
        # 同一 (config, field) 只读一次盘, 概念/行业维度共享同一份结果
        fut_ext: dict[tuple[str, str], Any] = {}
        for pairs in ext_targets.values():
            for config, field in pairs:
                fut_ext.setdefault(
                    (config.id, field),
                    pool.submit(_read_ext_rows, data_dir, config, field),
                )
        fut_seal_up = (
            pool.submit(depth_service.get_sealed_map, as_of, False) if depth_service else None
        )
        fut_seal_dn = (
            pool.submit(depth_service.get_sealed_map, as_of, True) if depth_service else None
        )

        df = fut_rows.result()
        indices = fut_indices.result()
        concept_ext = [(c, f, fut_ext[(c.id, f)].result()) for c, f in ext_targets["concept"]]
        industry_ext = [(c, f, fut_ext[(c.id, f)].result()) for c, f in ext_targets["industry"]]
        up_map = fut_seal_up.result() if fut_seal_up else {}
        down_map = fut_seal_dn.result() if fut_seal_dn else {}

    if df.is_empty():
        rows: list[dict] = []
    else:
        cols = [
            "symbol", "name", "close", "change_pct", "amount", "turnover_rate", "volume",
            "vol_ratio_5d", "consecutive_limit_ups", "signal_limit_up", "signal_broken_limit_up", "signal_limit_down",
            "ma5", "ma20", "ma60", "high_60d", "low_60d", "signal_n_day_high", "signal_n_day_low",
        ]
        df = df.select([c for c in cols if c in df.columns])
        rows = df.to_dicts()

    # 过滤真停牌（volume=0 且 change_pct=0），保留有涨跌幅的浮点误差股以对齐同花顺口径
    if rows and "volume" in rows[0]:
        rows = [r for r in rows
                if (_finite(r.get("volume")) or 0) > 0
                or (_finite(r.get("change_pct")) or 0) != 0]

    total = len(rows)
    up = sum(1 for r in rows if (_finite(r.get("change_pct")) or 0) > 0)
    down = sum(1 for r in rows if (_finite(r.get("change_pct")) or 0) < 0)
    flat = max(0, total - up - down)
    up_pct = up / total * 100 if total else 0
    down_pct = down / total * 100 if total else 0

    amounts = [_finite(r.get("amount")) or 0 for r in rows]
    total_amount = sum(amounts)
    avg_amount = total_amount / total if total else 0

    pct_values = [_finite(r.get("change_pct")) for r in rows]
    pct_values = [v for v in pct_values if v is not None]
    avg_pct = sum(pct_values) / len(pct_values) if pct_values else 0
    median_pct = sorted(pct_values)[len(pct_values) // 2] if pct_values else 0
    strong_up = sum(1 for v in pct_values if v >= 0.03)
    strong_down = sum(1 for v in pct_values if v <= -0.03)

    limit_up = sum(1 for r in rows if bool(r.get("signal_limit_up")) or (_finite(r.get("consecutive_limit_ups")) or 0) > 0)
    broken = sum(1 for r in rows if bool(r.get("signal_broken_limit_up")))
    limit_down = sum(1 for r in rows if bool(r.get("signal_limit_down")))
    max_boards = max([int(_finite(r.get("consecutive_limit_ups")) or 0) for r in rows], default=0)

    # 五档 sealed 修正: 假涨停/假跌停不计入(需 Pro+ depth5.batch 能力)
    # (sealed map 已在并行阶段读取)
    sealed_ready = False
    fake_up = 0
    fake_down = 0
    if depth_service:
        sealed_ready = bool(up_map or down_map) and depth_service.is_sealed_ready(as_of)
        if up_map:
            fake_up = sum(1 for v in up_map.values() if v.get("sealed") is False)
        if down_map:
            fake_down = sum(1 for v in down_map.values() if v.get("sealed") is False)
    if sealed_ready:
        limit_up = max(0, limit_up - fake_up)
        limit_down = max(0, limit_down - fake_down)

    seal_rate = limit_up / (limit_up + broken) * 100 if (limit_up + broken) > 0 else 0

    def above_ma_count(ma_key: str) -> int:
        return sum(1 for r in rows if (_finite(r.get("close")) is not None and _finite(r.get(ma_key)) is not None and (_finite(r.get("close")) or 0) >= (_finite(r.get(ma_key)) or 0)))

    above_ma5 = above_ma_count("ma5")
    above_ma20 = above_ma_count("ma20")
    above_ma60 = above_ma_count("ma60")
    new_high = sum(1 for r in rows if bool(r.get("signal_n_day_high")) or (_finite(r.get("close")) is not None and _finite(r.get("high_60d")) is not None and (_finite(r.get("close")) or 0) >= (_finite(r.get("high_60d")) or 0)))
    new_low = sum(1 for r in rows if bool(r.get("signal_n_day_low")) or (_finite(r.get("close")) is not None and _finite(r.get("low_60d")) is not None and (_finite(r.get("close")) or 0) <= (_finite(r.get("low_60d")) or 0)))

    turnovers = [_finite(r.get("turnover_rate")) for r in rows]
    turnovers = [v for v in turnovers if v is not None]
    avg_turnover = sum(turnovers) / len(turnovers) if turnovers else 0
    high_turnover = sum(1 for v in turnovers if v >= 5)

    boards_map: dict[str, dict] = {}
    for r in rows:
        b = _board(str(r.get("symbol") or ""))
        item = boards_map.setdefault(b, {"board": b, "count": 0, "up": 0, "down": 0, "amount": 0.0})
        item["count"] += 1
        change = _finite(r.get("change_pct")) or 0
        if change > 0:
            item["up"] += 1
        elif change < 0:
            item["down"] += 1
        item["amount"] += _finite(r.get("amount")) or 0
    boards = sorted(boards_map.values(), key=lambda x: x["amount"], reverse=True)
    for b in boards:
        count = b["count"] or 1
        b["up_pct"] = b["up"] / count * 100

    tiers_map: dict[int, int] = {}
    tiers_stocks: dict[int, list] = {}
    for r in rows:
        n = int(_finite(r.get("consecutive_limit_ups")) or 0)
        if n > 0:
            tiers_map[n] = tiers_map.get(n, 0) + 1
            sym = str(r.get("symbol") or "")
            if sym:
                tiers_stocks.setdefault(n, []).append({
                    "symbol": sym,
                    "name": r.get("name") or "",
                    "amount": _finite(r.get("amount")) or 0.0,
                })
    tiers = [
        {
            "boards": k,
            "count": v,
            "stocks": sorted(tiers_stocks.get(k, []), key=lambda x: x["amount"], reverse=True)[:5],
        }
        for k, v in sorted(tiers_map.items(), key=lambda item: -item[0])
    ]

    index_changes = [_finite(r.get("change_pct")) for r in indices]
    index_changes = [v for v in index_changes if v is not None]
    avg_index_pct = sum(index_changes) / len(index_changes) if index_changes else 0
    vol_ratios = [_finite(r.get("vol_ratio_5d")) for r in rows]
    vol_ratios = [v for v in vol_ratios if v is not None]
    avg_vol_ratio = sum(vol_ratios) / len(vol_ratios) if vol_ratios else 1
    high_vol_ratio = sum(1 for v in vol_ratios if v >= 1.5)

    concept_rank = _rank_from_ext_items(rows, concept_ext, "concept")
    industry_rank = _rank_from_ext_items(rows, industry_ext, "industry", level=2)

    strong_diff_pct = (strong_up - strong_down) / total * 100 if total else 0
    high_vol_pct = high_vol_ratio / total * 100 if total else 0
    strong_down_pct = strong_down / total * 100 if total else 0
    tier2_count = sum(t["count"] for t in tiers if t["boards"] >= 2)
    mainline_items = [*concept_rank["leading"][:3], *industry_rank["leading"][:3]]
    mainline_avg = max([_finite(item.get("avg_pct")) or 0 for item in mainline_items], default=0)
    mainline_cover_pct = max([(_finite(item.get("count")) or 0) / total * 100 for item in mainline_items], default=0) if total else 0
    mainline_score = round(_score(mainline_avg, -0.005, 0.03) * 0.65 + _score(mainline_cover_pct, 1, 12) * 0.35) if mainline_items else 50

    radar = [
        {"key": "index", "label": "指数", "value": _score(avg_index_pct, -2.5, 2.5)},
        {"key": "profit", "label": "赚钱", "value": round(_score(up_pct, 20, 80) * 0.45 + _score(avg_pct, -0.02, 0.02) * 0.25 + _score(median_pct, -0.02, 0.02) * 0.20 + _score(strong_diff_pct, -8, 8) * 0.10)},
        {"key": "money", "label": "量能", "value": round(_score(avg_vol_ratio, 0.6, 1.8) * 0.70 + _score(high_vol_pct, 2, 12) * 0.30)},
        {"key": "speculation", "label": "投机", "value": round(_score(limit_up, 5, 90) * 0.25 + _score(seal_rate, 30, 85) * 0.35 + _score(max_boards, 1, 8) * 0.25 + _score(tier2_count, 0, 30) * 0.15)},
        {"key": "resilience", "label": "抗跌", "value": 100 - round(_score(down_pct, 20, 80) * 0.55 + _score(strong_down_pct, 1, 12) * 0.45)},
        {"key": "mainline", "label": "主线", "value": mainline_score},
    ]
    emotion_score = round(sum(r["value"] for r in radar) / len(radar)) if radar else 50
    if emotion_score >= 70:
        emotion_label = "强势"
    elif emotion_score >= 55:
        emotion_label = "偏暖"
    elif emotion_score >= 45:
        emotion_label = "震荡"
    elif emotion_score >= 30:
        emotion_label = "偏冷"
    else:
        emotion_label = "冰点"

    return _json_safe({
        "as_of": str(as_of),
        "quote_status": status,
        "indices": indices,
        "breadth": {
            "total": total,
            "up": up,
            "down": down,
            "flat": flat,
            "up_pct": up_pct,
            "down_pct": down_pct,
            "avg_pct": avg_pct,
            "median_pct": median_pct,
            "strong_up": strong_up,
            "strong_down": strong_down,
        },
        "amount": {"total": total_amount, "avg": avg_amount},
        "boards": boards,
        "limit": {"limit_up": limit_up, "broken": broken, "failed": 0, "limit_down": limit_down, "max_boards": max_boards, "seal_rate": seal_rate, "tiers": tiers, "sealed_ready": sealed_ready, "fake_up": fake_up, "fake_down": fake_down},
        "distribution": _pct_band_rows(pct_values),
        "trend": {
            "above_ma5": above_ma5,
            "above_ma20": above_ma20,
            "above_ma60": above_ma60,
            "above_ma5_pct": above_ma5 / total * 100 if total else 0,
            "above_ma20_pct": above_ma20 / total * 100 if total else 0,
            "above_ma60_pct": above_ma60 / total * 100 if total else 0,
            "new_high": new_high,
            "new_low": new_low,
        },
        "activity": {
            "avg_turnover": avg_turnover,
            "high_turnover": high_turnover,
            "high_vol_ratio": high_vol_pct,
            "vol_ratio": avg_vol_ratio,
        },
        "radar": radar,
        "emotion": {"score": emotion_score, "label": emotion_label},
        "top_gainers": _top_rows(rows, "change_pct", True),
        "top_losers": _top_rows(rows, "change_pct", False),
        "turnover_leaders": _top_rows(
            [r for r in rows if not _is_etf(str(r.get("symbol") or ""))],
            "amount", True,
        ),
        "active_leaders": _top_rows(rows, "turnover_rate", True),
        "concept_rank": concept_rank,
        "industry_rank": industry_rank,
    })
