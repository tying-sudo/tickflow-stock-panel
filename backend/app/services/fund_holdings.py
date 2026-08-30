"""天天基金 F10 持仓明细 → ETF 成分股（基金披露的股票持仓）。

用于「添加 ETF → 以 ETF 名建分组 → 成分股入组」。ETF 无公开的成分股
直连接口，以其最新报告期的股票投资明细为成分股口径：半年报/年报披露
全部持仓，季报仅 top10。数据源为东方财富天天基金 F10 的 FundArchivesDatas
AJAX 接口（type=jjcc），按基金代码（=ETF 代码）直查，无需 ETF→指数映射。

响应壳: ``var apidata={ content:"<html>",arryear:[..],curyear:..};``
content 内含多个报告期盒（boxitem），每盒 = 截止日期 + 一张持仓表。
页面对单期显示有截断（LoadMore），实测传 month=6/12 可取到半年报/年报的
完整披露表；因此按多个 (year, month) 组合拉取，取持仓数最多的一期。
"""
from __future__ import annotations

import logging
import re
import urllib.request
from datetime import datetime

logger = logging.getLogger(__name__)

_URL = (
    "https://fundf10.eastmoney.com/FundArchivesDatas.aspx"
    "?type=jjcc&code={code}&topline=400&year={year}&month={month}"
)
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://fundf10.eastmoney.com/",
}
_TIMEOUT = 15
# 优先半年报/年报（全持仓），季报兜底（top10）
_MONTHS = (6, 12, 3, 9)
_DATE_RE = re.compile(r">(\d{4}-\d{2}-\d{2})</font>")
_CODE_RE = re.compile(r"unify/r/([0-9])\.([0-9]{6})")


class FundHoldingsError(RuntimeError):
    """成分股获取失败（上游不可达/无持仓数据），调用方不得造数。"""


def _http_get(url: str) -> str:
    request = urllib.request.Request(url, headers=_HEADERS)
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT) as response:
            return response.read().decode("utf-8", errors="replace")
    except (OSError, ValueError) as exc:
        raise FundHoldingsError(f"天天基金接口不可达: {exc}") from exc


def _symbol_suffix(market: str, code: str) -> str:
    """EM 行情链接市场位 1=沪 0=深；北交所代码(43/83/87/92)挂 0 下。"""
    if market == "1":
        return f"{code}.SH"
    if code.startswith(("43", "83", "87", "92")):
        return f"{code}.BJ"
    return f"{code}.SZ"


def _parse_periods(body: str) -> list[dict]:
    """解析响应壳里全部报告期盒 → [{fund_name, report_date, holdings}]。"""
    if 'content:"' not in body:
        return []
    content = body.split('content:"', 1)[1].rsplit('"', 1)[0]
    fund_name_m = re.search(r"<a title='([^']+)'", content)
    fund_name = fund_name_m.group(1) if fund_name_m else None
    periods: list[dict] = []
    for box in content.split("<div class='boxitem")[1:]:
        date_m = _DATE_RE.search(box)
        if not date_m:
            continue
        holdings: list[dict] = []
        seen: set[str] = set()
        for tr in box.split("<tr>")[1:]:
            link = _CODE_RE.search(tr)
            if not link:
                continue
            symbol = _symbol_suffix(link.group(1), link.group(2))
            if symbol in seen:
                continue
            texts = re.findall(r"unify/r/[0-9]\.[0-9]{6}'>([^<]+)</a>", tr)
            pct_m = re.search(r"(\d+(?:\.\d+)?)%", tr)
            seen.add(symbol)
            holdings.append({
                "symbol": symbol,
                "name": texts[1] if len(texts) > 1 else None,
                "pct_nav": float(pct_m.group(1)) / 100 if pct_m else None,
            })
        if holdings:
            periods.append({
                "fund_name": fund_name,
                "report_date": date_m.group(1),
                "holdings": holdings,
            })
    return periods


def fetch_etf_holdings(symbol: str) -> dict:
    """ETF 代码 → {fund_name, report_date, holdings: [{symbol,name,pct_nav}]}。

    取披露持仓数最多的报告期（并列取最新）。fund_code = symbol 点号前段。
    """
    code = symbol.split(".")[0]
    if not code.isdigit() or len(code) != 6:
        raise FundHoldingsError(f"无效的基金/ETF 代码: {symbol}")
    year = datetime.now().year
    best: dict | None = None
    for yr in (year, year - 1):
        for month in _MONTHS:
            url = _URL.format(code=code, year=yr, month=month)
            try:
                body = _http_get(url)
            except FundHoldingsError as exc:
                logger.warning("fund holdings fetch failed (%s): %s", symbol, exc)
                continue
            for period in _parse_periods(body):
                key = (len(period["holdings"]), period["report_date"])
                if best is None or key > (len(best["holdings"]), best["report_date"]):
                    best = period
    if not best or not best["holdings"]:
        raise FundHoldingsError(f"{symbol} 未取到任何持仓披露数据")
    return best


# ================================================================
# 场外基金 (pingzhongdata): 搜索代码 + 前十大重仓
# ================================================================

_SEARCH_URL = "https://fundsuggest.eastmoney.com/FundSearch/api/FundSearchAPI.ashx"
_NAME_RE = re.compile(r'var\s+fS_name\s*=\s*"([^"]*)"')
_STOCK_CODES_RE = re.compile(r"var\s+stockCodesNew\s*=\s*(\[[^\]]*\])")
_MARKET_PREFIX = {"1": "SH", "0": "SZ"}


def search_fund_code_by_name(name: str) -> str | None:
    """基金名称 → 场外基金代码 (搜索结果中精确同名者, 无匹配返回 None)。"""
    import json as _json
    import urllib.parse

    q = (name or "").strip()
    if not q:
        return None
    url = _SEARCH_URL + "?" + urllib.parse.urlencode(
        {"m": "1", "key": q, "pageindex": "0", "pagesize": "20"}
    )
    try:
        body = _http_get(url)
    except FundHoldingsError as exc:
        logger.warning("fund search failed (%s): %s", name, exc)
        return None
    try:
        datas = _json.loads(body).get("Datas") or []
    except ValueError:
        return None
    for d in datas:
        if d.get("CATEGORY") != 700:
            continue
        if str(d.get("NAME") or "").strip() == q:
            code = str(d.get("CODE") or "")
            return code if re.fullmatch(r"\d{6}", code) else None
    return None


def fetch_fund_holdings(code: str) -> dict:
    """场外基金代码 → {fund_name, report_date:"", holdings:[{symbol,name,pct_nav}]}。

    pingzhongdata 的 stockCodesNew 即基金最新一期前十大重仓 (接口不含披露期,
    report_date 置空)。与 ETF 路径 (F10 全量披露) 结构对齐, 便于同步器统一处理。
    """
    code = str(code or "").strip()
    if not re.fullmatch(r"\d{6}", code):
        raise FundHoldingsError(f"非法场外基金代码: {code}")
    url = "https://fund.eastmoney.com/pingzhongdata/{}.js".format(code)
    body = _http_get(url)
    import json as _json

    name_m = _NAME_RE.search(body)
    codes_m = _STOCK_CODES_RE.search(body)
    fund_name = name_m.group(1).strip() if name_m else ""
    symbols: list[str] = []
    if codes_m:
        try:
            raw_codes = _json.loads(codes_m.group(1))
        except (ValueError, TypeError):
            raw_codes = []
        for raw in raw_codes:
            s = str(raw).strip()
            if "." not in s:
                continue
            market, code6 = s.split(".", 1)
            exchange = _MARKET_PREFIX.get(market)
            if exchange and len(code6) == 6 and code6.isdigit():
                symbols.append(f"{code6}.{exchange}")
    symbols = list(dict.fromkeys(symbols))
    if not symbols:
        raise FundHoldingsError(f"{code} 未取到持仓披露数据 (stockCodesNew 为空)")
    return {
        "fund_name": fund_name,
        "report_date": "",
        "holdings": [{"symbol": s, "name": None, "pct_nav": None} for s in symbols],
    }
