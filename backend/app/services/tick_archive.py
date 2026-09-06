"""分笔成交盘后落盘归档 — 当日分笔记录本地保留 (parquet)。

背景 (2026-09-04): 通达信公共行情服务器的当日分笔在盘前维护窗口被清空
(09:09 实测返回 0 行), 历史分笔仅保留约 30 天, 且 VM102 g4tic 打包通道
已随 VM102 网络死亡废弃。分时成交面板要求任意时刻可看最近交易日明细 →
盘后把当日分笔全量落盘, 读取时归档优先、live 兜底。

存储: {data_dir}/transactions/date=YYYY-MM-DD/part.parquet
      列: symbol, time(HH:MM), price, volume(手), num, direction
写入: EOD job (15:35, 深市盘后定价 15:30 结束后) 全量归档自选股;
      另有懒缓存 — 盘后/历史日 live 拉取成功后顺手 upsert 单 symbol。
读取: tick_transactions.build_transactions 归档优先。
"""
from __future__ import annotations

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

# 归档写入互斥 (懒缓存 upsert 与 EOD 全量写可能并发触达同一 parquet)
_write_lock = threading.Lock()

_TICK_SCHEMA = {
    "symbol": pl.Utf8,
    "time": pl.Utf8,
    "price": pl.Float64,
    "volume": pl.Float64,
    "num": pl.Int64,
    "direction": pl.Utf8,
}

# 懒缓存写盘条件: 北京时间 >= 该时刻 (深市盘后定价 15:30 结束 + 缓冲)
_LAZY_ARCHIVE_AFTER = (15, 35)


def archive_dir(repo, date_str: str) -> Path:
    return repo.store.data_dir / "transactions" / f"date={date_str}" / "part.parquet"


def _cn_today_str() -> str:
    from app.market_time import cn_today

    return cn_today().isoformat()


def _past_lazy_archive_time() -> bool:
    from app.market_time import cn_now

    now = cn_now()
    return (now.hour, now.minute) >= _LAZY_ARCHIVE_AFTER


def load_symbol(repo, symbol: str, date_str: str | None) -> list[dict] | None:
    """归档中该 symbol 的分笔行; 未归档返回 None (调用方走 live)。"""
    if not repo:
        return None
    path = archive_dir(repo, date_str or _cn_today_str())
    if not path.exists():
        return None
    try:
        df = pl.read_parquet(path)
        rows = df.filter(pl.col("symbol") == symbol).sort("time").to_dicts()
        return rows or None
    except Exception as e:  # noqa: BLE001
        logger.warning("tick archive %s/%s 读取失败: %s", date_str, symbol, e)
        return None


def archived_dates(repo) -> list[str]:
    if not repo:
        return []
    root = repo.store.data_dir / "transactions"
    if not root.exists():
        return []
    return sorted(p.name.removeprefix("date=") for p in root.iterdir() if p.is_dir())


def _write_day_df(repo, date_str: str, df: pl.DataFrame) -> int:
    """覆盖写某日归档 (原子替换), df 已是 TICK_SCHEMA。"""
    path = archive_dir(repo, date_str)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.write_parquet(tmp)
    os.replace(tmp, path)
    return df.height


def _write_day(repo, date_str: str, rows: list[dict]) -> int:
    """覆盖写某日归档 (原子替换)。rows 为全日归一化 ticks (可含 symbol/buyorsell)。

    显式按 schema 整形: 多余键 (buyorsell) 丢弃, 缺失键 (direction) 补 other —
    polars 对 dict 行的 schema 外键行为随版本有差异, 不依赖其容错。
    """
    shaped = [{
        "symbol": r.get("symbol"),
        "time": r.get("time") or "",
        "price": float(r.get("price") or 0),
        "volume": float(r.get("volume") or 0),
        "num": int(r.get("num") or 0),
        "direction": r.get("direction") or "other",
    } for r in rows]
    return _write_day_df(repo, date_str, pl.DataFrame(shaped, schema=_TICK_SCHEMA))


def upsert_symbol(repo, symbol: str, date_str: str, ticks: list[dict]) -> None:
    """懒缓存: 单 symbol 分笔合并进某日归档 (EOD 全量写会覆盖, 幂等)。"""
    if not repo or not date_str or not ticks:
        return
    rows = [{
        "symbol": symbol,
        "time": str(t.get("time") or ""),
        "price": float(t.get("price") or 0),
        "volume": float(t.get("volume") or 0),
        "num": int(t.get("num") or 0),
        "direction": str(t.get("direction") or "other"),
    } for t in ticks]
    with _write_lock:
        path = archive_dir(repo, date_str)
        if path.exists():
            try:
                existing = pl.read_parquet(path).filter(pl.col("symbol") != symbol)
            except Exception as e:  # noqa: BLE001
                # 读/滤失败就中止 upsert — 当作空表会把旧全量行与新行拼出重复 symbol
                logger.warning("tick lazy upsert %s/%s 中止 (现有归档不可读): %s",
                               date_str, symbol, e)
                return
        merged = pl.concat([existing, pl.DataFrame(rows, schema=_TICK_SCHEMA)], how="vertical_relaxed")
        tmp = path.with_name(path.name + ".tmp")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        merged.write_parquet(tmp)
        os.replace(tmp, path)


def _fetch_symbol_day(symbol: str, date_str: str | None) -> tuple[str, list[dict]]:
    """live 拉取单 symbol 分笔 (归一化 ticks); 失败/空返回 (symbol, [])。"""
    from app.plugins.easy_tdx.provider import EasyTdxProvider

    try:
        result = EasyTdxProvider().get_transactions(symbol, date_str)
    except Exception as e:  # noqa: BLE001
        logger.warning("tick archive live %s (%s) 失败: %s", symbol, date_str, e)
        return symbol, []
    from app.services import tick_transactions

    return symbol, tick_transactions.normalize_live_rows(result["rows"])


def _frame_of(symbol: str, ticks: list[dict]) -> pl.DataFrame:
    """单 symbol ticks → TICK_SCHEMA DataFrame (显式整形, 多余键丢弃)。"""
    rows = [{
        "symbol": symbol,
        "time": t.get("time") or "",
        "price": float(t.get("price") or 0),
        "volume": float(t.get("volume") or 0),
        "num": int(t.get("num") or 0),
        "direction": t.get("direction") or "other",
    } for t in ticks]
    return pl.DataFrame(rows, schema=_TICK_SCHEMA) if rows else pl.DataFrame(schema=_TICK_SCHEMA)


def full_market_symbols(repo) -> list[str]:
    """全市场标的全集: enriched 内存缓存 (股票+ETF, ~7000+ 只)。"""
    if repo:
        enriched, _ = repo.get_enriched_latest()
        if not enriched.is_empty() and "symbol" in enriched.columns:
            return enriched["symbol"].unique().to_list()
    return watchlist_symbols()


def archive_day(repo, symbols: list[str], date_str: str | None = None,
                concurrency: int = 7, on_progress=None) -> dict:
    """全量归档: 拉取并落盘, 返回统计。

    date_str=None → 当日端点 (最近交易日, EOD job 场景), 归属日由成交量
    匹配判定; date_str 给定 → 历史端点 (≥30 天), 归属日即该日。
    幂等: 同日重复执行覆盖写。全市场 (~7200 只) 约 20-30 分钟;
    ticks 以 polars DataFrame 累积 (27M 行 ≈ 1.2GB), 不落 dict 大对象。
    """
    stats = {"date": date_str, "requested": len(symbols), "archived": 0,
             "empty": 0, "failed": 0}
    if date_str:
        frames: list[pl.DataFrame] = []
        done = 0
        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
            for symbol, ticks in executor.map(lambda s: _fetch_symbol_day(s, date_str), symbols):
                done += 1
                if ticks:
                    frames.append(_frame_of(symbol, ticks))
                    stats["archived"] += 1
                else:
                    stats["empty"] += 1
                if on_progress and done % 500 == 0:
                    on_progress(done, len(symbols))
        stats["date"] = date_str
        df = pl.concat(frames, how="vertical_relaxed") if frames else pl.DataFrame(schema=_TICK_SCHEMA)
        stats["rows"] = _write_day_df(repo, date_str, df)
        return stats

    # 当日端点: 按 symbol 用成交量匹配判定真实交易日 (跨日语义), 按日分组落盘
    from app.services.tick_transactions import _pick_tick_date

    by_day: dict[str, list[pl.DataFrame]] = {}
    done = 0
    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
        for symbol, ticks in executor.map(lambda s: _fetch_symbol_day(s, None), symbols):
            done += 1
            if ticks:
                day = _pick_tick_date(repo, symbol, ticks, None) or _cn_today_str()
                by_day.setdefault(day, []).append(_frame_of(symbol, ticks))
                stats["archived"] += 1
            else:
                stats["empty"] += 1
            if on_progress and done % 500 == 0:
                on_progress(done, len(symbols))
    stats["rows"] = 0
    for day, fr in by_day.items():
        df = pl.concat(fr, how="vertical_relaxed")
        stats["rows"] += _write_day_df(repo, day, df)
        stats["date"] = day
    return stats


def watchlist_symbols() -> list[str]:
    from app.services import watchlist

    return [row["symbol"] for row in watchlist.list_symbols()]


def archive_recent_eod(repo) -> dict:
    """EOD job 入口: 归档全市场 (enriched 全集 ~7000+ 只) 的当日分笔。

    并发自适应 serve 实例池 (worker 流数 ≈ 2×端口数, 上限 32; 每端口在途
    锁保证不串线). 全市场耗时 ≈ 15 端口 2-3 分钟 / 7 端口 6 分钟;
    enriched 缓存为空时退化为自选股。
    """
    from app.plugins.easy_tdx.provider import EasyTdxProvider

    symbols = full_market_symbols(repo)
    if not symbols:
        logger.info("tick archive EOD: 标的全集为空, 跳过")
        return {"skipped": "no_symbols"}

    try:
        n_ports = len(EasyTdxProvider().ports_for(False))
    except Exception:  # noqa: BLE001
        n_ports = 7
    concurrency = max(7, min(2 * n_ports, 32))

    def _progress(done: int, total: int) -> None:
        logger.info("tick archive EOD progress: %d/%d", done, total)

    started = time.monotonic()
    stats = archive_day(repo, symbols, None, concurrency=concurrency, on_progress=_progress)
    stats["elapsed_s"] = round(time.monotonic() - started, 1)
    stats["concurrency"] = concurrency
    logger.info("tick archive EOD: %s", stats)
    return stats


def lazy_archive_if_due(repo, symbol: str, tick_date: str | None,
                        ticks: list[dict]) -> None:
    """live 拉取成功后的懒缓存: 历史日随时归档; 当日仅盘后落盘时段归档。"""
    if not repo or not ticks:
        return
    if tick_date is None:
        return
    if tick_date == _cn_today_str() and not _past_lazy_archive_time():
        return  # 盘中: EOD job 会全量落盘, 不写半日数据
    try:
        upsert_symbol(repo, symbol, tick_date, ticks)
    except Exception as e:  # noqa: BLE001
        logger.debug("tick lazy archive failed %s/%s: %s", tick_date, symbol, e)
