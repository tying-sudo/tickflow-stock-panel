"""实时指数缓存轮间合并测试 (2026-09-08 深证成指/创业板指整晚 '--' 修复)。

背景: 全市场快照上游批次级空包, 每轮指数数量随机残缺 (实测单日 119~555 波动);
_index_quotes_cache 整包覆盖把收盘最后一轮的残缺名单固化整晚。修复: 仅缓存
日期=今日时按 symbol 轮间合并 (本轮缺席保留上一轮), 跨日首轮从零替换。
"""

from __future__ import annotations

import polars as pl

from app.services.quote_service import QuoteService


def _make_qs(cache: pl.DataFrame | None = None, cache_date: str | None = None) -> QuoteService:
    qs = QuoteService.__new__(QuoteService)
    qs._index_quotes_cache = cache
    qs._index_quotes_date = cache_date
    return qs


def _rows(*symbols: str) -> pl.DataFrame:
    return pl.DataFrame({
        "symbol": list(symbols),
        "last_price": [100.0 + i for i in range(len(symbols))],
        "change_pct": [1.0] * len(symbols),
    })


def test_same_day_merge_keeps_missing_symbols():
    """同日轮间合并: 本轮缺席的 399001 保留上一轮行, 本轮在场的用新值。"""
    qs = _make_qs(cache=_rows("000001.SH", "399001.SZ"), cache_date="2026-09-08")
    merged = qs._merge_index_cache(_rows("000001.SH"), "2026-09-08")
    assert set(merged["symbol"].to_list()) == {"000001.SH", "399001.SZ"}
    assert merged.filter(pl.col("symbol") == "000001.SH")["last_price"][0] == 100.0
    assert merged.filter(pl.col("symbol") == "399001.SZ")["last_price"][0] == 101.0


def test_cross_day_first_round_replaces_from_zero():
    """跨日首轮: 旧日收盘不混入新交易日, 整包替换。"""
    qs = _make_qs(cache=_rows("000001.SH", "399001.SZ"), cache_date="2026-09-07")
    merged = qs._merge_index_cache(_rows("000001.SH"), "2026-09-08")
    assert merged["symbol"].to_list() == ["000001.SH"]


def test_empty_round_same_day_keeps_previous_cache():
    """同日整轮指数空包 (批次全败): 保留上一轮, 不把缓存清空。"""
    qs = _make_qs(cache=_rows("000001.SH", "399001.SZ"), cache_date="2026-09-08")
    merged = qs._merge_index_cache(pl.DataFrame(), "2026-09-08")
    assert set(merged["symbol"].to_list()) == {"000001.SH", "399001.SZ"}


def test_empty_round_cross_day_resets_to_empty():
    qs = _make_qs(cache=_rows("000001.SH"), cache_date="2026-09-07")
    assert qs._merge_index_cache(pl.DataFrame(), "2026-09-08").is_empty()


def test_no_previous_cache_replaces():
    qs = _make_qs(cache=None, cache_date=None)
    merged = qs._merge_index_cache(_rows("000001.SH"), "2026-09-08")
    assert merged["symbol"].to_list() == ["000001.SH"]


def test_schema_drift_falls_back_to_replace():
    """合并失败 (如 dtype 漂移) 回退整包替换, 不抛异常阻断行情轮。"""
    prev = pl.DataFrame({"symbol": ["399001.SZ"], "last_price": ["not-float"], "change_pct": [1.0]})
    qs = _make_qs(cache=prev, cache_date="2026-09-08")
    merged = qs._merge_index_cache(_rows("000001.SH"), "2026-09-08")
    assert merged["symbol"].to_list() == ["000001.SH"]
