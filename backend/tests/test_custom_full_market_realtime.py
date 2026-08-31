"""_custom_full_market_realtime 双契约适配测试 (2026-08-31 全市场拉取断裂修复)。

背景: 上游 v0.2.2 调用点只有无参 get_realtime() (YAML 源契约), 与 tdx_gateway
插件的显式 symbols 契约不兼容 → realtime_data_provider=tdx_gateway 时全市场
实时拉取全程失败。修复: 按 realtime_pull 偏好本地展开 symbols 优先显式契约。
"""

from __future__ import annotations

import polars as pl
import pytest

from app.services.quote_service import QuoteService


class _FakeRepo:
    def get_instruments(self):
        return pl.DataFrame({"symbol": ["000001.SZ", "600519.SH"]})

    def get_etf_instruments(self):
        return pl.DataFrame({"symbol": ["510300.SH"]})

    def get_index_symbol_set(self):
        return {"000300.SH", "000905.SH"}


class _SymbolsProvider:
    """tdx_gateway 式契约: 显式 symbols。"""

    def __init__(self):
        self.got_symbols = None

    def get_realtime(self, symbols=None):
        if symbols is None:
            return [{"symbol": "BARE_CALL"}]
        self.got_symbols = list(symbols)
        return [{"symbol": s} for s in symbols]


class _BareProvider:
    """YAML GenericHTTPProvider 式契约: 无参全市场。"""

    def get_realtime(self):
        return [{"symbol": "BARE_ONLY"}]


def _make_qs(monkeypatch, provider, *, stock=True, etf=True, index=True, index_mode="core"):
    qs = QuoteService.__new__(QuoteService)
    qs._repo = _FakeRepo()
    qs._app_state = None
    monkeypatch.setattr("app.data_providers.custom.get_provider", lambda name: provider)
    monkeypatch.setattr("app.services.preferences.get_realtime_pull_stock", lambda: stock)
    monkeypatch.setattr("app.services.preferences.get_realtime_pull_etf", lambda: etf)
    monkeypatch.setattr("app.services.preferences.get_realtime_pull_index", lambda: index)
    monkeypatch.setattr("app.services.preferences.get_realtime_index_symbols", lambda: None)
    monkeypatch.setattr("app.services.preferences.get_realtime_index_mode", lambda: index_mode)
    return qs


def test_symbols_contract_receives_expanded_universe(monkeypatch):
    """显式契约: 按偏好展开 股票+ETF+核心指数, 去重保序。"""
    provider = _SymbolsProvider()
    qs = _make_qs(monkeypatch, provider)
    records = qs._custom_full_market_realtime("tdx_gateway")
    assert provider.got_symbols == ["000001.SZ", "600519.SH", "510300.SH"] + sorted(
        set(QuoteService.CORE_INDEX_SYMBOLS)
    )
    assert len(records) == len(provider.got_symbols)


def test_symbols_contract_mode_all_uses_full_index_set(monkeypatch):
    provider = _SymbolsProvider()
    qs = _make_qs(monkeypatch, provider, index_mode="all")
    qs._custom_full_market_realtime("tdx_gateway")
    assert set(provider.got_symbols) >= {"000300.SH", "000905.SH"}


def test_bare_contract_falls_back_on_typeerror(monkeypatch):
    """无 symbols 形参的实现 (YAML 源) → TypeError 回退无参调用。"""
    provider = _BareProvider()
    qs = _make_qs(monkeypatch, provider)
    records = qs._custom_full_market_realtime("some_yaml_source")
    assert records == [{"symbol": "BARE_ONLY"}]


def test_all_pull_prefs_off_skips_expansion(monkeypatch):
    """三类拉取全关 → 不展开 symbols, 直接无参调用 (维持上游语义)。"""
    provider = _SymbolsProvider()
    qs = _make_qs(monkeypatch, provider, stock=False, etf=False, index=False)
    records = qs._custom_full_market_realtime("tdx_gateway")
    assert records == [{"symbol": "BARE_CALL"}]
    assert provider.got_symbols is None


@pytest.mark.parametrize("name", ["tdx_gateway", "some_yaml_source"])
def test_provider_name_is_passthrough(monkeypatch, name):
    provider = _SymbolsProvider()
    qs = _make_qs(monkeypatch, provider)
    seen = {}
    monkeypatch.setattr(
        "app.data_providers.custom.get_provider", lambda n: seen.setdefault(n, provider)
    )
    qs._custom_full_market_realtime(name)
    assert list(seen) == [name]
