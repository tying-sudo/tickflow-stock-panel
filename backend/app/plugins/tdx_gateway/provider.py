"""Tick Stock Panel provider for the Windows-hosted TDX LAN gateway.

This module intentionally has no dependency on TDX's local Python runtime.
The Windows gateway is the only component that calls TDX at 127.0.0.1:17709;
this provider only consumes its narrow, authenticated HTTP contract.
"""
from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import polars as pl

from app.data_providers.base import AssetType
from app.data_providers.normalizer import normalize_daily
from app.tickflow.rate_limits import chunked

logger = logging.getLogger(__name__)

API_KEY_ENV = "TDX_GATEWAY_TOKEN"
SECRETS_FIELD = "tdx_gateway_token"
DEFAULT_GATEWAY_URL = "http://10.0.10.14:18709"
_DATASETS = ("daily", "adj_factor", "minute", "realtime", "depth5", "financial")
_BATCH = 40
_REALTIME_BATCH = 50
_REALTIME_HTTP_WORKERS = 6
_MINUTE_COLUMNS = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
# Intraday 1-minute bars come from the gateway's pytdx sidecar (/v1/tickdata).
# Only small batches (the single-symbol 分时补拉 path) may use it; the
# post-close full-market sync keeps the verified /v1/kline minute channel.
_TICKDATA_MAX_SYMBOLS = 8

# 财务数据: 通达信专业财务 FN 字段 → TickFlow 各财务表目标列。
# 依据通达信官方《获取专业财务数据》字段字典核对 (2026-08-28); 未标注「万元」的
# 经典字段按通达信惯例为「元」, 单位与个别比率口径需以实盘探针(600519/000001)复核,
# 严禁把口径不明的字段硬编码进映射。
_FIN_BATCH = 20  # 受控批量: 财务单请求标的数上限 (网关侧同值)
_FINANCIAL_FIELDS: dict[str, list[tuple[str, str]]] = {
    "metrics": [
        ("FN1", "eps_basic"),               # 基本每股收益(元/股)
        ("FN501", "eps_diluted"),           # 稀释每股收益(元)
        ("FN4", "bps"),                     # 每股净资产(元/股)
        ("FN7", "ocfps"),                   # 每股经营现金流量(元/股)
        ("FN6", "roe"),                     # 净资产收益率(%)
        ("FN281", "roe_diluted"),           # 加权净资产收益率(%)
        ("FN200", "roa"),                   # 总资产净利率(%)
        ("FN202", "gross_margin"),          # 销售毛利率(%)
        ("FN199", "net_margin"),            # 销售净利率(%)
        ("FN210", "debt_to_asset_ratio"),   # 资产负债率(%)
        ("FN183", "revenue_yoy"),           # 营业收入增长率(%)
        ("FN184", "net_income_yoy"),        # 净利润增长率(%)
        # FN223 经营现金/营收已移除: 分子 FN107 为累计、分母 FN230 为单季, 口径混杂必失真。
        ("FN173", "inventory_turnover"),    # 存货周转率(次)
    ],
    "income": [
        # 口径核验 (2026-08-28, 600519 全历史序列):
        #   FN230 营收 / FN231 营业利润 / FN232 归母净利 / FN233 扣非净利 = 【单季】口径
        #   FN134 净利润 = 仅年报/中报有值且口径异常(Q1/Q3=0)
        #   FN1/FN501 每股收益 = 累计口径 ✓
        # 利润表金额字段与累计财务报表不符, 严禁直接填入(会误导前端与财务因子)。
        # 仅保留累计口径的 EPS。
        ("FN1", "basic_eps"),               # 基本每股收益(元/股, 累计)
        ("FN501", "diluted_eps"),           # 稀释每股收益(元, 累计)
    ],
    "balance_sheet": [
        ("FN40", "total_assets"),           # 资产总计(元)
        ("FN21", "total_current_assets"),   # 流动资产合计(元)
        ("FN39", "total_non_current_assets"),  # 非流动资产合计(元)
        ("FN8", "cash_and_equivalents"),    # 货币资金(元)
        ("FN11", "accounts_receivable"),    # 应收账款(元)
        ("FN17", "inventory"),              # 存货(元)
        ("FN27", "fixed_assets"),           # 固定资产(元)
        ("FN33", "intangible_assets"),      # 无形资产(元)
        ("FN35", "goodwill"),               # 商誉(元)
        ("FN63", "total_liabilities"),      # 负债合计(元)
        ("FN54", "total_current_liabilities"),  # 流动负债合计(元)
        ("FN62", "total_non_current_liabilities"),  # 非流动负债合计(元)
        ("FN41", "short_term_borrowing"),   # 短期借款(元)
        ("FN55", "long_term_borrowing"),    # 长期借款(元)
        ("FN44", "accounts_payable"),       # 应付账款(元)
        ("FN72", "total_equity"),           # 所有者权益合计(元)
        ("FN271", "equity_attributable"),   # 归母所有者权益(元)
        ("FN68", "retained_earnings"),      # 未分配利润(元)
        ("FN69", "minority_interest"),      # 少数股东权益(元)
    ],
    "cash_flow": [
        ("FN107", "net_operating_cash_flow"),  # 经营活动现金流净额(元)
        ("FN119", "net_investing_cash_flow"),  # 投资活动现金流净额(元)
        ("FN128", "net_financing_cash_flow"),  # 筹资活动现金流净额(元)
        ("FN114", "capex"),                 # 购建固定/无形/长期资产支付的现金(元)
        ("FN131", "net_cash_change"),       # 现金及现金等价物净增加额(元)
    ],
    "shares": [
        ("FN238", "total_shares"),          # 总股本(股)
        ("FN239", "float_shares"),          # 已上市流通A股(股)
    ],
}
_FIN_ALLOWED = frozenset(fn for mapping in _FINANCIAL_FIELDS.values() for fn, _ in mapping)
_FIN_HISTORY_START_DAYS = 6 * 365  # 全量历史: 约 6 年报告期
_FIN_LATEST_START_DAYS = 2 * 365   # 仅最新一期: 2 年窗口足够覆盖最新报告期


def _window_covers_today(end_time: datetime | None) -> bool:
    """True when the requested window may include today (Beijing calendar)."""
    if end_time is None:
        return True
    tz = timezone(timedelta(hours=8))
    today = datetime.now(tz).date()
    end_date = end_time.astimezone(tz).date() if end_time.tzinfo else end_time.date()
    return end_date >= today


class TdxGatewayError(RuntimeError):
    """A transparent gateway/TDX error; callers must not turn it into fake rows."""


def get_api_key() -> str:
    from app import secrets_store

    return secrets_store.get_env_backed_secret(SECRETS_FIELD, API_KEY_ENV)


def gateway_url() -> str:
    return os.getenv("TDX_GATEWAY_URL", DEFAULT_GATEWAY_URL).rstrip("/")


def availability() -> tuple[bool, str]:
    if get_api_key():
        return True, "ok"
    return False, f"{API_KEY_ENV} is not configured"


def probe_api_key(api_key: str) -> tuple[bool, str]:
    try:
        _GatewayClient(api_key).health()
    except TdxGatewayError as exc:
        return False, str(exc)
    return True, "ok"


@dataclass
class _TdxGatewayConfig:
    name: str = "tdx_gateway"
    display_name: str = "TDX LAN Gateway"
    datasets: dict = field(default_factory=lambda: dict.fromkeys(_DATASETS))
    path: None = None
    builtin: bool = True


class _GatewayClient:
    def __init__(self, token: str, timeout: float = 30.0) -> None:
        self._token = token
        self._timeout = timeout

    def health(self) -> dict[str, Any]:
        return self._request("GET", "/v1/health")

    def kline(
        self,
        symbols: list[str],
        period: str,
        start_time: datetime | None,
        end_time: datetime | None,
        *,
        adjust: str = "none",
    ) -> dict[str, list[dict[str, Any]]]:
        body = {
            "symbols": symbols,
            "period": period,
            "start": _format_time(start_time, period),
            "end": _format_time(end_time, period),
            "adjust": adjust,
        }
        payload = self._request("POST", "/v1/kline", body)
        rows = payload.get("rows")
        if not isinstance(rows, dict):
            raise TdxGatewayError("TDX gateway returned no rows map")
        return {str(symbol): value for symbol, value in rows.items() if isinstance(value, list)}

    def realtime(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        payload = self._request("POST", "/v1/realtime", {"symbols": symbols})
        rows = payload.get("rows")
        if not isinstance(rows, dict):
            raise TdxGatewayError("TDX gateway returned no realtime rows map")
        return {str(symbol): value for symbol, value in rows.items() if isinstance(value, dict)}

    def tickdata(self, symbols: list[str], *, kind: str = "bars", count: int = 240) -> dict[str, list[dict[str, Any]]]:
        """Recent intraday bars/ticks via the gateway's pytdx sidecar.

        Serves TODAY even during continuous trading, which /v1/kline cannot
        (TDX Quant 盘中仅支持日K).  Older gateways reject the path and the
        caller falls back to the legacy minute channel.
        """
        payload = self._request(
            "POST", "/v1/tickdata", {"symbols": symbols, "kind": kind, "count": count},
        )
        rows = payload.get("rows")
        if not isinstance(rows, dict):
            raise TdxGatewayError("TDX gateway returned no tickdata rows map")
        return {str(symbol): value for symbol, value in rows.items() if isinstance(value, list)}

    def financials(
        self,
        symbols: list[str],
        table_list: list[str],
        *,
        start_time: str | None = None,
        end_time: str | None = None,
        report_type: str = "tag_time",
    ) -> dict[str, list[dict[str, Any]]]:
        """Fetch professional financials (whitelisted FN fields) via the gateway.

        线上网关契约 (2026-08-28 实测): 请求参数名为 ``fields`` (1..32 个),
        响应为 TDX 原始结构 ``{"data": {"Value": {symbol: {field: [..], announce_time: [..], tag_time: [..]}}}}``
        (列式, 每列一个报告期)。此处把列式转成记录列表 [{field: value}, ...],
        并兼容 ``rows`` / ``result`` 两种记录式响应, 保证 Linux 侧解析不随网关版本漂移。
        """
        body = {
            "symbols": symbols,
            "fields": list(table_list),
            "start_time": start_time,
            "end_time": end_time,
            "report_type": report_type,
        }
        payload = self._request("POST", "/v1/financials", body, timeout=90.0)
        if not isinstance(payload, dict):
            raise TdxGatewayError("TDX gateway returned a non-object financial response")
        value: Any = None
        data = payload.get("data")
        if isinstance(data, dict):
            value = data.get("Value")
        if not isinstance(value, dict):
            value = payload.get("rows")
        if not isinstance(value, dict):
            value = payload.get("result")
        if not isinstance(value, dict):
            raise TdxGatewayError("TDX gateway returned no financial Value map")
        result: dict[str, list[dict[str, Any]]] = {}
        for symbol in symbols:
            columns = value.get(symbol)
            result[str(symbol)] = _columnar_financial_records(columns)
        return result

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = Request(
            f"{gateway_url()}{path}",
            data=data,
            method=method,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self._token}",
            },
        )
        try:
            with urlopen(request, timeout=timeout or self._timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:300]
            raise TdxGatewayError(f"TDX gateway HTTP {exc.code}: {detail}") from exc
        except (URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise TdxGatewayError(f"TDX gateway is unavailable: {exc}") from exc
        if not isinstance(payload, dict):
            raise TdxGatewayError("TDX gateway returned a non-object response")
        if payload.get("error"):
            raise TdxGatewayError(f"TDX gateway: {payload['error']}")
        return payload


class TdxGatewayProvider:
    name = "tdx_gateway"
    builtin = True
    # TDX Quant's snapshot API is one symbol per call.  The gateway fans those
    # calls out with a fixed global limit, while QuoteService supplies the
    # configured full-market universe as explicit symbols.
    realtime_scope = "full_market_symbols"

    def __init__(self) -> None:
        self.config = _TdxGatewayConfig()

    def close(self) -> None:
        pass

    def _client(self) -> _GatewayClient:
        token = get_api_key()
        if not token:
            raise TdxGatewayError(f"{API_KEY_ENV} is not configured")
        return _GatewayClient(token)

    def get_daily(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        return self._get_kline(
            symbols, "1d", start_time, end_time, on_chunk_done,
            minute=False, adjust="none",
        )

    def get_adj_factors(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        """Derive event factors from TDX's prices, never ``ForwardFactor``.

        TDX describes ForwardFactor as an event-day value carried forward to
        the next event and explicitly says it cannot directly calculate qfq
        prices. We use its changes only to *locate* an event. Comparing
        ``front`` and ``none`` daily closes then yields the event multiplier
        that Tick Stock Panel expects.
        """
        schema = {"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64}
        if not symbols or asset_type != "stock":
            return pl.DataFrame(schema=schema)

        # Include a leading context so an event on the requested first day is
        # compared with the immediately preceding trading session.
        context_start = start_time - timedelta(days=14) if start_time else None
        frames: list[pl.DataFrame] = []
        parts = chunked(symbols, _BATCH)
        for index, part in enumerate(parts):
            client = self._client()
            raw_rows = client.kline(part, "1d", context_start, end_time, adjust="none")
            front_rows = client.kline(part, "1d", context_start, end_time, adjust="front")
            for symbol in part:
                event_dates = _forward_factor_event_dates(raw_rows.get(symbol, []))
                if not event_dates:
                    continue
                raw = normalize_daily(raw_rows.get(symbol, []), default_symbol=symbol, source=self.name)
                front = normalize_daily(front_rows.get(symbol, []), default_symbol=symbol, source=self.name)
                if raw.is_empty() or front.is_empty():
                    continue
                factors = (
                    raw.select("date", pl.col("close").alias("raw_close"))
                    .join(front.select("date", pl.col("close").alias("front_close")), on="date", how="inner")
                    .filter((pl.col("raw_close") > 0) & (pl.col("front_close") > 0))
                    .sort("date")
                    .with_columns([
                        (pl.col("front_close") / pl.col("raw_close")).alias("_ratio"),
                    ])
                    .with_columns([
                        (pl.col("_ratio") / pl.col("_ratio").shift(1)).alias("ex_factor"),
                    ])
                    # Cash dividends are a stable price offset before the
                    # event, so the ratio alone moves slightly every day.
                    # ForwardFactor change dates distinguish those rounding
                    # fluctuations from genuine corporate actions.
                    .filter(
                        pl.col("ex_factor").is_finite()
                        & (pl.col("ex_factor") > 0)
                        # 丢弃精确 1.0 的伪事件: forward_factor 变化但价格比率不变,
                        # 无实际复权效果, 写入因子表只会污染数据。
                        & ((pl.col("ex_factor") - 1.0).abs() > 1e-9)
                        & pl.col("date").is_in(event_dates)
                    )
                )
                if start_time is not None:
                    factors = factors.filter(pl.col("date") >= start_time.date())
                if not factors.is_empty():
                    frames.append(factors.select(
                        pl.lit(symbol).alias("symbol"),
                        pl.col("date").alias("trade_date"),
                        "ex_factor",
                    ))
            if on_chunk_done:
                on_chunk_done(index + 1, len(parts))

        return pl.concat(frames).sort(["symbol", "trade_date"]) if frames else pl.DataFrame(schema=schema)

    def get_minute(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType = "stock",
        freq: str = "1m",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        period = str(freq).lower()
        if period not in {"1m", "5m", "15m", "30m", "60m"}:
            raise TdxGatewayError(f"TDX gateway does not support minute frequency {freq!r}")
        now = datetime.now()
        win_end = end_time or now
        win_start = start_time or (win_end - timedelta(days=1))
        multi_day = (win_end - win_start) > timedelta(days=1)
        small_batch = len(symbols) <= _TICKDATA_MAX_SYMBOLS
        covers_today = _window_covers_today(end_time)
        # 单日窗口 (分时图当日补拉): tickdata 只回最近 ~240 分钟, 正好覆盖当天,
        # 命中即返回。多日窗口 (个股历史补齐) tickdata 只有当天一根腿, 不能短路 —
        # 否则历史日永远补不上 (实测 2026-08-28: days=20 补齐只写入当天)。
        if period == "1m" and small_batch and covers_today and not multi_day:
            try:
                frame = self._get_tickdata_minute(symbols, on_chunk_done)
                if not frame.is_empty():
                    frame = _clip_minute_window(frame, start_time, end_time)
                    if not frame.is_empty():
                        return frame
            except TdxGatewayError as exc:
                logger.info("tickdata minute unavailable, falling back to kline channel: %s", exc)
        frame = self._get_kline(
            symbols, period, start_time, end_time, on_chunk_done,
            minute=True, adjust="none",
        )
        if multi_day and period == "1m" and small_batch and covers_today:
            # 多日窗口: kline 通道盘中只回历史 (当日分钟拿不到), 用 tickdata
            # 把今天补上; (symbol, datetime) 冲突时 tickdata 更新, 保留 tickdata。
            try:
                today_frame = self._get_tickdata_minute(symbols, on_chunk_done)
                if not today_frame.is_empty():
                    today_frame = _clip_minute_window(today_frame, start_time, end_time)
                    if not today_frame.is_empty():
                        combined = (
                            pl.concat([frame, today_frame], how="diagonal_relaxed")
                            if not frame.is_empty() else today_frame
                        )
                        frame = combined.sort(["symbol", "datetime"]).unique(
                            subset=["symbol", "datetime"], keep="last", maintain_order=True,
                        )
            except TdxGatewayError as exc:
                logger.info("tickdata today top-up failed for multi-day window: %s", exc)
        return frame

    def _get_tickdata_minute(
        self,
        symbols: list[str],
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        frames: list[pl.DataFrame] = []
        parts = list(chunked(symbols, _REALTIME_BATCH))
        total = len(parts)
        for index, part in enumerate(parts):
            rows_by_symbol = self._client().tickdata(part, kind="bars", count=240)
            for symbol, rows in rows_by_symbol.items():
                # tickdata 通道: amount 已是元, volume 股→手 (见 _minute_frame 注释)
                frame = _minute_frame(rows, symbol, volume_scale=0.01)
                if not frame.is_empty():
                    frames.append(frame)
            if on_chunk_done:
                on_chunk_done(index + 1, total)
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    def get_realtime(
        self,
        universes: list[str] | None = None,
        symbols: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        if universes:
            raise TdxGatewayError("TDX realtime supports explicit symbols only, not whole-market universes")
        if not symbols:
            raise TdxGatewayError("TDX realtime requires an explicit watchlist or monitor symbol")

        records: list[dict[str, Any]] = []
        parts_list = list(chunked(symbols, _REALTIME_BATCH))
        with ThreadPoolExecutor(max_workers=min(_REALTIME_HTTP_WORKERS, len(parts_list))) as executor:
            futures = [executor.submit(self._realtime_snapshot_tolerant, part) for part in parts_list]
            snapshots_by_part = [future.result() for future in futures]
        for part, snapshots in zip(parts_list, snapshots_by_part, strict=True):
            for symbol in part:
                snapshot = snapshots.get(symbol)
                if not snapshot:
                    continue
                last_price = _number(snapshot.get("Now"))
                prev_close = _number(snapshot.get("LastClose"))
                # not last_price 同时拦截 None 与 0.0: 盘前/周末快照可能返回 Now=0.00,
                # 放行会产出 0 价记录并把涨跌额算成 -prev_close (实测 2026-08-30)。
                if not last_price or prev_close in (None, 0):
                    logger.warning("TDX realtime snapshot for %s lacks valid Now/LastClose; skipping it", symbol)
                    continue
                change_amount = last_price - prev_close
                # TDX 快照 Amount 单位为万元 → 元 (日K契约 amount=元)。
                # 2026-08-31 实测: 未换算的快照金额经实时覆写流入当日 kline_daily
                # 分区 (000001.SZ amount=97322 万元口径), 策略 amount_min 按元
                # 过滤全市场 0 通过 → 策略页整页 0 命中。Volume 快照单位=手,
                # 与日K契约一致, 不换算。
                amount_wan = _number(snapshot.get("Amount"))
                records.append({
                    "symbol": symbol,
                    "last_price": last_price,
                    "prev_close": prev_close,
                    "open": _number(snapshot.get("Open")),
                    "high": _number(snapshot.get("Max")),
                    "low": _number(snapshot.get("Min")),
                    "volume": _number(snapshot.get("Volume")),
                    "amount": amount_wan * 1e4 if amount_wan is not None else None,
                    "change_amount": change_amount,
                    "change_pct": change_amount / prev_close,
                })
        return records

    def _realtime_snapshot_tolerant(self, part: list[str]) -> dict[str, dict[str, Any]]:
        """单批快照, 批内个别坏标的 (清盘/停牌 ETF 等, TDX 缺 Now/LastClose) 会让
        网关整批 502 —— 二分重试把坏标的隔离到单只并跳过, 其余标的照常返回。
        没有这层容错, 一个坏 ETF 就会让全市场行情轮询整体失败 (2026-08-30 实测:
        512413.SH/512423.SH/516443.SH/588533.SH 毒化 11/146 批 → 全市场 0 条行情)。
        """
        try:
            return self._client().realtime(part)
        except TdxGatewayError as exc:
            if len(part) == 1:
                logger.warning("TDX realtime 快照不可用, 已跳过 %s: %s", part[0], exc)
                return {}
            mid = len(part) // 2
            with ThreadPoolExecutor(max_workers=2) as executor:
                left = executor.submit(self._realtime_snapshot_tolerant, part[:mid]).result()
                right = executor.submit(self._realtime_snapshot_tolerant, part[mid:]).result()
            merged: dict[str, dict[str, Any]] = dict(left)
            merged.update(right)
            return merged

    def get_depth5(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        """Map TDX's read-only snapshot book to the existing depth5 contract."""
        result: dict[str, dict[str, Any]] = {}
        fetched_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        for part in chunked(symbols, _BATCH):
            # 与 get_realtime 同源风险: 批内坏标的会让网关整批 502, 同样二分容错
            snapshots = self._realtime_snapshot_tolerant(list(part))
            for symbol in part:
                snapshot = snapshots.get(symbol)
                if not snapshot:
                    continue
                if _book_inconsistent_with_last(snapshot):
                    # TdxW 有时会在同一份快照里给出与 Now 自相矛盾的陈旧盘口
                    # (如 Now=31.28 而 bid1=31.08)。宁可空档也不展示误导数据。
                    logger.warning(
                        "depth5 %s: book contradicts Now=%s (stale TdxW book) — dropping levels",
                        symbol,
                        snapshot.get("Now"),
                    )
                    result[symbol] = {
                        "ask_prices": [],
                        "ask_volumes": [],
                        "bid_prices": [],
                        "bid_volumes": [],
                        "timestamp": fetched_ms,
                    }
                    continue
                result[symbol] = {
                    "ask_prices": _depth_levels(snapshot.get("Sellp")),
                    "ask_volumes": _depth_levels(snapshot.get("Sellv"), integer=True),
                    "bid_prices": _depth_levels(snapshot.get("Buyp")),
                    "bid_volumes": _depth_levels(snapshot.get("Buyv"), integer=True),
                    "timestamp": fetched_ms,
                }
        return result

    def get_financials(
        self,
        table: str,
        symbols: list[str],
        *,
        latest_only: bool = True,
    ) -> pl.DataFrame:
        """Professional financials via the gateway's whitelisted FN fields.

        Returns records with ``symbol`` / ``period_end`` / ``announce_date`` plus
        the table's mapped columns (frontend schema in StockFinancialDetail).
        ``latest_only`` keeps the newest report period per symbol (the sync
        history path refreshes existing symbols with latest and backfills new
        symbols with the full history).
        """
        mapping = _FINANCIAL_FIELDS.get(table)
        if mapping is None:
            raise TdxGatewayError(f"TDX gateway does not support financial table {table!r}")
        if not symbols:
            return pl.DataFrame()
        field_names = [fn for fn, _ in mapping]
        start_days = _FIN_LATEST_START_DAYS if latest_only else _FIN_HISTORY_START_DAYS
        start_time = (datetime.now() - timedelta(days=start_days)).strftime("%Y%m%d")
        records: list[dict[str, Any]] = []
        for part in chunked(symbols, _FIN_BATCH):
            rows_by_symbol = self._client().financials(
                part, field_names, start_time=start_time, report_type="tag_time",
            )
            for symbol in part:
                for row in rows_by_symbol.get(symbol) or []:
                    period_end = _fin_date(row.get("tag_time"))
                    if period_end is None:
                        continue
                    record: dict[str, Any] = {
                        "symbol": symbol,
                        "period_end": period_end,
                        "announce_date": _fin_date(row.get("announce_time")),
                    }
                    for fn, target in mapping:
                        value = _number(row.get(fn))
                        if value is not None:
                            record[target] = value
                    records.append(record)
        if not records:
            return pl.DataFrame()
        frame = pl.DataFrame(records)
        if latest_only:
            frame = frame.sort(["symbol", "period_end"]).unique(
                subset=["symbol"], keep="last",
            )
        return frame

    def _get_kline(
        self,
        symbols: list[str],
        period: str,
        start_time: datetime | None,
        end_time: datetime | None,
        on_chunk_done: Callable[[int, int], None] | None,
        *,
        minute: bool,
        adjust: str,
    ) -> pl.DataFrame:
        if not symbols:
            return pl.DataFrame()
        frames: list[pl.DataFrame] = []
        parts = chunked(symbols, _BATCH)
        for index, part in enumerate(parts):
            rows_by_symbol = self._client().kline(
                part, period, start_time, end_time, adjust=adjust,
            )
            for symbol, rows in rows_by_symbol.items():
                if minute:
                    # /v1/kline 分钟通道: amount 万元→元, volume 股→手 (见 _minute_frame 注释)
                    frame = _minute_frame(rows, symbol, amount_scale=1e4, volume_scale=0.01)
                else:
                    frame = _daily_frame(rows, symbol)
                if not frame.is_empty():
                    frames.append(frame)
            if on_chunk_done:
                on_chunk_done(index + 1, len(parts))
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    def test_dataset(self, dataset: str, symbols: list[str] | None = None) -> dict[str, Any]:
        symbol_list = symbols or ["600519.SH"]
        if dataset == "daily":
            frame = self.get_daily(symbol_list, None, None)
        elif dataset == "adj_factor":
            frame = self.get_adj_factors(symbol_list, None, None)
        elif dataset == "minute":
            frame = self.get_minute(symbol_list, None, None)
        elif dataset == "realtime":
            records = self.get_realtime(symbols=symbol_list)
            return {
                "provider": self.name,
                "dataset": dataset,
                "rows": len(records),
                "columns": list(records[0]) if records else [],
                "preview": records[:5],
            }
        elif dataset == "depth5":
            depth = self.get_depth5(symbol_list)
            return {
                "provider": self.name,
                "dataset": dataset,
                "rows": len(depth),
                "columns": list(next(iter(depth.values()))) if depth else [],
                "preview": [{"symbol": symbol, **row} for symbol, row in list(depth.items())[:5]],
            }
        elif dataset == "financial":
            frame = self.get_financials("metrics", symbol_list, latest_only=True)
            return {
                "provider": self.name,
                "dataset": dataset,
                "rows": frame.height,
                "columns": frame.columns,
                "preview": frame.head(5).to_dicts() if not frame.is_empty() else [],
            }
        else:
            raise ValueError(f"TDX gateway does not support dataset {dataset!r}")
        return {
            "provider": self.name,
            "dataset": dataset,
            "rows": frame.height,
            "columns": frame.columns,
            "preview": frame.head(5).to_dicts() if not frame.is_empty() else [],
        }


def _format_time(value: datetime | None, period: str) -> str | None:
    if value is None:
        return None
    return value.strftime("%Y%m%d %H:%M:%S" if period.endswith("m") else "%Y%m%d")


def _clip_minute_window(
    frame: pl.DataFrame,
    start_time: datetime | None,
    end_time: datetime | None,
) -> pl.DataFrame:
    """把分钟 frame 裁剪到调用方窗口 (tickdata 会带回窗口外的最近 bar)。"""
    if frame.is_empty() or "datetime" not in frame.columns:
        return frame
    start_naive = (
        start_time.replace(tzinfo=None)
        if (start_time is not None and start_time.tzinfo)
        else start_time
    )
    end_naive = (
        end_time.replace(tzinfo=None)
        if (end_time is not None and end_time.tzinfo)
        else end_time
    )
    if start_naive is not None:
        frame = frame.filter(pl.col("datetime") >= start_naive)
    if end_naive is not None:
        frame = frame.filter(pl.col("datetime") <= end_naive)
    return frame


def _minute_frame(
    rows: list[dict[str, Any]],
    symbol: str,
    *,
    amount_scale: float = 1.0,
    volume_scale: float = 1.0,
) -> pl.DataFrame:
    """TDX 分钟行 → 分钟 DataFrame, 归一到上游分钟库契约: amount 元 + volume 手。

    实测 (2026-08-28, amount/(volume×close) ≈ 价格交叉验证) 两个通道原始单位:
      - /v1/kline 分钟通道 (盘后全市场同步走这里): amount 万元, volume 股
      - /v1/tickdata 通道 (盘中分时补拉走这里):    amount 元,   volume 股
    上游契约 (tick-stock-panel 前端 computeIntradayAverage 按 volume×100 折算
    均价, 回测 minute_fill 亦假设手): amount 元, volume 手。因此两个通道调用方
    都传 volume_scale=0.01 (股→手), kline 通道另传 amount_scale=1e4 (万元→元)。
    """
    if not rows:
        return pl.DataFrame()
    frame = pl.DataFrame(rows)
    if "datetime" not in frame.columns and "time" in frame.columns:
        frame = frame.rename({"time": "datetime"})
    if "datetime" not in frame.columns:
        return pl.DataFrame()
    frame = frame.with_columns(
        pl.col("datetime").cast(pl.String).str.to_datetime(strict=False).alias("datetime"),
        pl.lit(symbol).alias("symbol"),
    )
    for column in ("open", "high", "low", "close", "volume", "amount"):
        if column in frame.columns:
            frame = frame.with_columns(pl.col(column).cast(pl.Float64, strict=False))
    if amount_scale != 1.0 and "amount" in frame.columns:
        frame = frame.with_columns((pl.col("amount") * amount_scale).alias("amount"))
    if volume_scale != 1.0 and "volume" in frame.columns:
        frame = frame.with_columns((pl.col("volume") * volume_scale).alias("volume"))
    keep = [column for column in _MINUTE_COLUMNS if column in frame.columns]
    return frame.select(keep) if "datetime" in keep else pl.DataFrame()


def _daily_frame(rows: list[dict[str, Any]], symbol: str) -> pl.DataFrame:
    """Normalize TDX daily bars to TickFlow's storage units.

    TDX Quant reports daily ``Volume`` in shares, while the existing
    TickFlow turnover formula stores daily volume in lots (1 lot = 100
    shares).  Convert exactly once at this provider boundary so every core
    consumer keeps its established contract.

    ``Amount`` is likewise converted: TDX Quant reports daily amount in
    万元 while the established TickFlow contract (策略 basic_filter 的
    amount_min/amount_max、前端成交额格式化) is 元.  实测 2026-08-28
    (600354 当日成交 8.7 亿元, 存值 86976.75 = 万元)。漏转会让
    amount_min=2000万 的过滤在万元口径下等于 2 万亿, 全市场无一通过。
    """
    frame = normalize_daily(rows, default_symbol=symbol, source=TdxGatewayProvider.name)
    if frame.is_empty():
        return frame
    if "volume" in frame.columns:
        frame = frame.with_columns((pl.col("volume") / 100.0).alias("volume"))
    if "amount" in frame.columns:
        frame = frame.with_columns((pl.col("amount") * 1e4).alias("amount"))
    return frame


def _number(value: Any) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _fin_date(value: Any) -> str | None:
    """Normalize TDX YYYYMMDD (announce_time/tag_time) to an ISO date string."""
    digits = "".join(char for char in str(value) if char.isdigit())
    if len(digits) < 8:
        return None
    year, month, day = digits[:4], digits[4:6], digits[6:8]
    if month in ("", "00") or day in ("", "00"):
        return None
    return f"{year}-{month}-{day}"


def _columnar_financial_records(columns) -> list[dict[str, Any]]:
    """TDX 列式财务响应 → 记录列表 (每行一个报告期)。

    ``{field: [v, ...], "announce_time": [..], "tag_time": [..]}`` 转成
    ``[{field: v, "announce_time": .., "tag_time": ..}, ...]``。
    列长不一致时按最长列对齐, 缺失位补 None (容忍上游缺字段)。
    """
    if not isinstance(columns, dict):
        return []
    series = {key: value for key, value in columns.items() if isinstance(value, list)}
    if not series:
        return []
    size = max(len(value) for value in series.values())
    return [
        {key: (value[index] if index < len(value) else None) for key, value in series.items()}
        for index in range(size)
    ]


# 盘口档位上限: TdxW Quant 当前快照只提供 5 档 (2026-08-31 实测, 字段集固定
# 26 键无 L2 十档字段); 若未来网关返回十档, 透传全量由前端自适应渲染。
_DEPTH_MAX_LEVELS = 10


def _depth_levels(value: Any, *, integer: bool = False) -> list[float | int]:
    """Normalize one TDX level array (up to 10 levels), preserving source order.

    No zero-padding: callers zip/filter on price > 0, so the returned length
    is exactly the number of levels the source actually provided (5 today,
    10 if L2 ever becomes available on the Quant interface).
    """
    values = value if isinstance(value, list) else []
    result: list[float | int] = []
    for item in values[:_DEPTH_MAX_LEVELS]:
        number = _number(item)
        result.append(int(number) if integer and number is not None else (number or 0.0))
    return result


# 盘口与现价的最大可信偏差。真实盘口的最优档必然紧贴最新成交价
# (同一快照内原子产生, 偏差仅来自档位刷新时序, 通常 ≤1 tick)。
_DEPTH_TOLERANCE = 0.005  # 0.5%


def _first_positive_level(value: Any) -> float:
    """Best non-zero price of one side, 0.0 when the side is empty (涨停/跌停)."""
    for item in value if isinstance(value, list) else []:
        number = _number(item)
        if number is not None and number > 0:
            return float(number)
    return 0.0


def _book_inconsistent_with_last(snapshot: dict[str, Any]) -> bool:
    """Detect the stale-TdxW-book failure: levels that contradict this very
    snapshot's ``Now`` (observed 2026-08-31: Now=31.28 while bid1=31.08).

    A genuine book brackets the last price — bid1 ≤ Now ≤ ask1 (one side may
    be empty at a limit). Any best level deviating from ``Now`` by more than
    the tolerance marks the whole book as stale and untrustworthy.
    """
    try:
        now = float(snapshot.get("Now") or 0)
    except (TypeError, ValueError):
        now = 0.0
    if now <= 0:
        return False  # 无现价可对照 (周末 Now=0 等边界), 不做校验
    tol = now * _DEPTH_TOLERANCE
    bid1 = _first_positive_level(snapshot.get("Buyp"))
    ask1 = _first_positive_level(snapshot.get("Sellp"))
    if bid1 and abs(bid1 - now) > tol:
        return True
    if ask1 and abs(ask1 - now) > tol:
        return True
    return False


def _forward_factor_event_dates(rows: list[dict[str, Any]]) -> list[datetime.date]:
    """Return dates where TDX's carried ForwardFactor actually changes.

    The value itself is deliberately not written as an event multiplier. It is
    an authoritative event boundary only; prices are used for the multiplier.
    """
    previous: float | None = None
    dates: list[datetime.date] = []
    for row in rows:
        factor = _number(row.get("forward_factor"))
        value = row.get("date")
        if factor is None or value is None:
            continue
        try:
            trade_date = datetime.fromisoformat(str(value)).date()
        except ValueError:
            continue
        if previous is not None and abs(factor - previous) > 1e-12:
            dates.append(trade_date)
        previous = factor
    return dates
