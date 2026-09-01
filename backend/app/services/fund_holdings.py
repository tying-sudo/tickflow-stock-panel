"""基金/ETF 成分股多数据源合成。

用于「添加 ETF/基金 → 以其名建分组 → 成分股入组」。单一数据源口径不足:
基金披露持仓滞后（季报仅 top10）、ETF 跟踪指数的官方成分才是"成分股"
的权威口径。故结合两源交叉确定:

1. **基金披露持仓** — 东方财富天天基金 F10 ``FundArchivesDatas`` (type=jjcc)，
   各报告期股票投资明细（半年报/年报全量、季报 top10），按基金代码直查；
2. **跟踪指数官方成分** — 基金概况页(jbgk)取"跟踪标的"指数名 → 东方财富
   搜索联想换指数代码 → 中证指数官网 closeweight 月度权重表 (.xls,
   polars/fastexcel/calamine 解析，零新增依赖)。

合并策略 (:func:`resolve_holdings`): 指数成分做骨架（官方、最新月度、
全量权重），披露持仓中不在指数内的标的（主动偏离/打新等）追加在后，
每只标的带 ``sources`` 来源标记。

响应壳: ``var apidata={ content:"<html>",arryear:[..],curyear:..};``
content 内含多个报告期盒（boxitem），每盒 = 截止日期 + 一张持仓表。
页面对单期显示有截断（LoadMore），实测传 month=6/12 可取到半年报/年报的
完整披露表；因此按多个 (year, month) 组合拉取，取持仓数最多的一期。
"""
from __future__ import annotations

import io
import json
import logging
import re
import urllib.parse
import urllib.request
from datetime import datetime

import polars as pl

logger = logging.getLogger(__name__)

_URL = (
    "https://fundf10.eastmoney.com/FundArchivesDatas.aspx"
    "?type=jjcc&code={code}&topline=400&year={year}&month={month}"
)
_SEARCH_URL = "https://fundsuggest.eastmoney.com/FundSearch/api/FundSearchAPI.ashx?m=1&key={code}"
_JBGK_URL = "https://fundf10.eastmoney.com/jbgk_{code}.html"
_INDEX_SUGGEST_URL = (
    "https://searchapi.eastmoney.com/api/suggest/get"
    "?input={kw}&type=14&token=D43BF722C8E33BDC906FB84D85E326E8&count=8"
)
_INDEX_CLOSEWEIGHT_URL = (
    "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file"
    "/autofile/closeweight/{code}closeweight.xls"
)
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://fund.eastmoney.com/",
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


def search_fund(code: str) -> dict:
    """天天基金搜索: 6 位基金/ETF 代码 → {code, name, fund_type}。

    供自选页搜索框联想 (场外基金不在 instruments 维表内, instrument
    搜索搜不到)。单次请求 ~0.3s, 比拉持仓表轻得多; 建组时再走
    fetch_etf_holdings 拿全量成分股。
    """
    if not code.isdigit() or len(code) != 6:
        raise FundHoldingsError(f"无效的基金代码: {code}")
    body = _http_get(_SEARCH_URL.format(code=code))
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise FundHoldingsError(f"基金搜索响应异常: {exc}") from exc
    for item in payload.get("Datas") or []:
        # CATEGORY 700 = 基金; 代码精确匹配 (搜索可能返回前缀相近项)
        if item.get("CATEGORY") != 700:
            continue
        if str(item.get("CODE")) != code and str(item.get("BACKCODE")) != code:
            continue
        return {
            "code": code,
            "name": str(item.get("NAME") or "").strip() or code,
            "fund_type": item.get("FundBaseInfo", {}).get("FTYPE"),
        }
    raise FundHoldingsError(f"基金 {code} 未找到 (天天基金搜索无结果)")


def search_fund_suggest(q: str, limit: int = 8) -> list[dict]:
    """天天基金模糊联想: 代码片段或名称关键词 → [{code, name, fund_type}]。

    与 search_fund 共用 FundSearchAPI (key=关键词本身即模糊匹配),
    区别仅在不做精确匹配, 返回 CATEGORY=700 (基金) 的前 limit 条。
    供基金自选页搜索框联想: 输入部分代码 ("0133") 或名称片段 ("芯片") 均可。
    """
    import urllib.parse

    key = (q or "").strip()
    if len(key) < 2:
        return []
    url = _FUND_SEARCH_BASE + "?" + urllib.parse.urlencode(
        {"m": "1", "key": key, "pageindex": "0", "pagesize": str(limit * 3)}
    )
    body = _http_get(url)
    try:
        datas = json.loads(body).get("Datas") or []
    except (json.JSONDecodeError, ValueError) as exc:
        raise FundHoldingsError(f"基金联想响应异常: {exc}") from exc
    out: list[dict] = []
    for item in datas:
        # CATEGORY 700 = 基金 (股票/板块等其他类别跳过)
        if item.get("CATEGORY") != 700:
            continue
        code = str(item.get("CODE") or "")
        if not re.fullmatch(r"\d{6}", code):
            continue
        base = item.get("FundBaseInfo")
        ftype = base.get("FTYPE") if isinstance(base, dict) else None
        out.append({
            "code": code,
            "name": str(item.get("NAME") or "").strip() or code,
            "fund_type": ftype,
        })
        if len(out) >= limit:
            break
    return out


# ---------------------------------------------------------------------------
# 数据源 2: ETF 跟踪指数的官方成分 (jbgk 跟踪标的 → 指数代码 → 中证权重表)
# ---------------------------------------------------------------------------

_TRACKED_RE = re.compile(r"跟踪标的</th><td[^>]*>([^<]+)</td>")
_EXCHANGE_SUFFIX = (
    ("深圳", ".SZ"), ("上海", ".SH"), ("北京", ".BJ"),
)


def _http_bytes(url: str) -> bytes:
    request = urllib.request.Request(url, headers=_HEADERS)
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT) as response:
            return response.read()
    except (OSError, ValueError) as exc:
        raise FundHoldingsError(f"下载失败: {exc}") from exc


def fetch_tracked_index_name(fund_code: str) -> str | None:
    """基金概况页 (jbgk) → '跟踪标的' 指数名；非指数基金返回 None。"""
    body = _http_get(_JBGK_URL.format(code=fund_code))
    m = _TRACKED_RE.search(body)
    if not m:
        return None
    name = m.group(1).strip()
    return name or None


def _index_name_candidates(name: str) -> list[str]:
    """跟踪标的全名 → 搜索候选 (去交易所前缀 / 去 主题·指数 后缀 / 去板块前缀)。"""
    seen: set[str] = set()
    out: list[str] = []

    def add(value: str) -> None:
        value = value.strip()
        if value and value not in seen:
            seen.add(value)
            out.append(value)

    current = name.strip()
    add(current)
    current = re.sub(r"^(上证|中证|深证|国证|沪深|深圳)", "", current)
    add(current)
    current = re.sub(r"(主题)?指数$", "", current)
    add(current)
    current = re.sub(r"^(科创板|创业板)", "", current)
    add(current)
    return out


def search_index_code(index_name: str) -> str | None:
    """指数名 → 指数代码（东方财富搜索联想, SecurityTypeName 含'指数'）。"""
    for kw in _index_name_candidates(index_name):
        try:
            payload = json.loads(_http_get(_INDEX_SUGGEST_URL.format(kw=urllib.parse.quote(kw))))
        except (FundHoldingsError, json.JSONDecodeError) as exc:
            logger.info("index suggest failed for %r: %s", kw, exc)
            continue
        for item in (payload.get("QuotationCodeTable") or {}).get("Data") or []:
            if "指数" not in str(item.get("SecurityTypeName") or ""):
                continue
            code = str(item.get("Code") or "")
            if re.fullmatch(r"\d{6}", code):
                return code
    return None


def _normalize_closeweight(df: pl.DataFrame) -> tuple[str | None, list[dict]]:
    """中证 closeweight 表 → (index_date, [{symbol, name, weight}])。

    仅保留 A 股交易所成分（深/上/北交所）；港美等标的无行情, 跳过。
    """
    def _col(*keywords: str) -> str | None:
        for column in df.columns:
            if all(k in column for k in keywords):
                return column
        return None

    col_code = _col("成份券代码") or _col("Constituent Code")
    col_name = _col("成份券名称")
    col_exch = _col("交易所")
    col_weight = _col("权重")
    col_date = _col("日期")
    if col_code is None or col_exch is None or col_weight is None:
        raise FundHoldingsError("closeweight 表缺少必要列")
    index_date = None
    if col_date is not None and df.height:
        raw_date = str(df[col_date][0])
        digits = re.sub(r"\D", "", raw_date)
        if len(digits) >= 8:
            index_date = f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    holdings: list[dict] = []
    for row in df.iter_rows(named=True):
        exchange = str(row.get(col_exch) or "")
        suffix = next((s for prefix, s in _EXCHANGE_SUFFIX if prefix in exchange), None)
        if suffix is None:
            continue
        code = row.get(col_code)
        code = f"{int(code):06d}" if isinstance(code, (int, float)) else str(code or "").strip()
        if not re.fullmatch(r"\d{6}", code):
            continue
        weight = row.get(col_weight)
        holdings.append({
            "symbol": code + suffix,
            "name": (str(row.get(col_name)).strip() if row.get(col_name) is not None else None),
            "weight": float(weight) if weight is not None else None,
        })
    return index_date, holdings


def _cnindex_suffix(code: str) -> str | None:
    """国证名单代码段 → A 股后缀 (仅保留股票)。"""
    if code.startswith(("600", "601", "603", "605", "688", "689")):
        return ".SH"
    if code.startswith(("000", "001", "002", "003", "300", "301")):
        return ".SZ"
    if code.startswith(("43", "83", "87", "92")):
        return ".BJ"
    return None


_CNINDEX_URL = (
    "https://www.cnindex.com.cn/sample-detail/detail"
    "?indexcode={code}&dateStr={ym}&pageNum=1&rows=1000"
)


def _fetch_cnindex_constituents(index_code: str) -> dict:
    """S2: 国证指数官网月度成分名单 → {index_code, index_name, index_date, holdings}。

    覆盖国证/深证系列 (399xxx/980xxx); 当月名单缺失自动回退上月。
    """
    from datetime import date
    today = date.today()
    if today.month == 1:
        prev = date(today.year - 1, 12, 1)
    else:
        prev = date(today.year, today.month - 1, 1)
    last_err = ""
    for ym in (today.strftime("%Y-%m"), prev.strftime("%Y-%m")):
        try:
            payload = json.loads(_http_get(_CNINDEX_URL.format(code=index_code, ym=ym)))
        except (FundHoldingsError, ValueError) as exc:
            last_err = str(exc)
            continue
        rows = (payload.get("data") or {}).get("rows") or []
        holdings = []
        for row in rows:
            code6 = str(row.get("seccode") or row.get("code") or row.get("secsCode") or "").strip()
            if not re.fullmatch(r"\d{6}", code6):
                continue
            suffix = _cnindex_suffix(code6)
            if suffix is None:
                continue
            try:
                weight = float(str(row.get("weight")).strip())
            except (TypeError, ValueError):
                weight = None
            holdings.append({
                "symbol": code6 + suffix,
                "name": (str(row.get("secname") or row.get("name") or row.get("secName") or "").strip() or None),
                "weight": weight,
            })
        if holdings:
            return {
                "index_code": index_code,
                "index_name": None,
                "index_date": ym,
                "holdings": holdings,
                "source": "cnindex",
            }
        last_err = f"{ym} 名单为空"
    raise FundHoldingsError(f"国证指数 {index_code} 成分不可得: {last_err}")


def _fetch_csindex_constituents(index_code: str) -> dict:
    """S1: 中证指数官网月度权重表 → {..., source: "csindex"}。"""
    raw = _http_bytes(_INDEX_CLOSEWEIGHT_URL.format(code=index_code))
    try:
        sheets = pl.read_excel(io.BytesIO(raw), sheet_id=0)
    except Exception as exc:  # noqa: BLE001
        raise FundHoldingsError(f"closeweight 获取/解析失败: {exc}") from exc
    df = next(iter(sheets.values())) if isinstance(sheets, dict) else sheets
    index_date, holdings = _normalize_closeweight(df)
    if not holdings:
        raise FundHoldingsError(f"指数 {index_code} 成分表为空")
    return {
        "index_code": index_code,
        "index_name": None,
        "index_date": index_date,
        "holdings": holdings,
        "source": "csindex",
    }


def _ths_thscode_candidates(index_code: str) -> list[str]:
    """指数代码 → 同花顺 thscode 候选 (主后缀优先, 失败试备选)。"""
    code = index_code.upper().split(".")[0]
    if index_code.upper().endswith(".TI"):
        return [index_code.upper()]
    if code.startswith(("399", "980")):
        return [f"{code}.SZ", f"{code}.SH"]
    return [f"{code}.SH", f"{code}.SZ"]


def fetch_ths_constituents(index_code: str) -> dict:
    """S2.5: 同花顺(扶摇 REST)指数/THS 板块当前成分 → {..., source: "ths"}。

    权威性低于官方月度文件 (无权重明细/无历史快照/调样日可能有延迟),
    定位: 官方源失败时的兜底 + 官方成分的交叉验证源; THS 自有板块
    (.TI) 是官方源不覆盖的唯一成分口径, 此时为该板块的最权威来源。
    """
    from app.plugins.fuyao.client import FuyaoClient, FuyaoError
    from app.plugins.fuyao import provider as fuyao_provider

    key = fuyao_provider.get_api_key()
    if not key:
        raise FundHoldingsError("未配置 FUYAO_API_KEY (同花顺源不可用)")
    last: Exception | None = None
    for thscode in _ths_thscode_candidates(index_code):
        cli = FuyaoClient(key)
        try:
            items = cli.index_constituents(thscode)
        except FuyaoError as exc:
            last = exc
            continue
        finally:
            cli.close()
        holdings = []
        for it in items:
            ths = str(it.get("thscode") or "")
            code6 = ths.split(".")[0]
            if not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", ths):
                continue
            if code6.startswith(("900", "200")):  # B 股
                continue
            holdings.append({"symbol": ths, "name": it.get("name"), "weight": None})
        if holdings:
            return {
                "index_code": index_code,
                "index_name": None,
                "index_date": None,
                "holdings": holdings,
                "source": "ths",
            }
        last = FundHoldingsError(f"同花顺 {thscode} 成分为空")
    raise last or FundHoldingsError("同花顺成分不可得")


def fetch_index_constituents(index_code: str) -> dict:
    """S1→S2→S2.5 串联: 中证官网 → 国证官网 → 同花顺(扶摇)。

    按代码段排主源顺序 (399/980 国证段优先 cnindex), 任一成功即返回,
    全部失败抛最后一个错误。.TI 结尾 (THS 自有板块) 直走同花顺。
    → {index_code, index_name, index_date, holdings, source}。
    """
    if index_code.upper().endswith(".TI"):
        return fetch_ths_constituents(index_code)
    if index_code.startswith(("399", "980")):
        builders = [_fetch_cnindex_constituents]
    else:
        builders = [_fetch_csindex_constituents, _fetch_cnindex_constituents]
    builders.append(fetch_ths_constituents)  # S2.5 总兜底
    last: Exception | None = None
    for fn in builders:
        try:
            return fn(index_code)
        except FundHoldingsError as exc:
            last = exc
    raise last or FundHoldingsError(f"指数 {index_code} 成分不可得")


def resolve_holdings(symbol: str) -> dict:
    """多数据源合成成分股（对外主入口）。

    返回 {fund_name, report_date, index|None, holdings: [{symbol, name,
    weight, pct_nav, sources}]}。

    口径：**ETF 严格跟踪官方指数** —— 有指数成分时 holdings 仅保留指数
    成分（披露持仓只用于标注双源与占比，指数外的披露标的如主动偏离/
    打新/已调出成分不入组）；无指数成分（场外基金 / 指数解析失败）时
    回退披露持仓。
    """
    disclosed = fetch_etf_holdings(symbol)
    index_info: dict | None = None
    code = symbol.split(".")[0]
    try:
        tracked = fetch_tracked_index_name(code)
        if tracked:
            index_code = search_index_code(tracked)
            if index_code:
                index_info = fetch_index_constituents(index_code)
                index_info["index_name"] = tracked
    except FundHoldingsError as exc:
        logger.info("index constituent source unavailable for %s: %s", symbol, exc)
    except Exception as exc:  # noqa: BLE001  解析意外错误不阻断披露持仓
        logger.warning("index constituent resolution failed for %s: %s", symbol, exc)

    merged: dict[str, dict] = {}
    if index_info:
        for h in index_info["holdings"]:
            merged[h["symbol"]] = {
                "symbol": h["symbol"], "name": h["name"],
                "weight": h["weight"], "pct_nav": None, "sources": ["index"],
            }
    if index_info is None:
        # 无官方指数成分 (场外基金 / 解析失败): 披露持仓兜底
        for h in disclosed["holdings"]:
            merged[h["symbol"]] = {
                "symbol": h["symbol"], "name": h["name"],
                "weight": None, "pct_nav": h["pct_nav"], "sources": ["disclosed"],
            }
    else:
        # 严格跟踪: 披露持仓只用于给指数成分标注双源与净值占比
        for h in disclosed["holdings"]:
            existing = merged.get(h["symbol"])
            if existing is not None:
                existing["sources"].append("disclosed")
                existing["pct_nav"] = h["pct_nav"]
                if not existing.get("name") and h.get("name"):
                    existing["name"] = h["name"]
    items = sorted(
        merged.values(),
        key=lambda h: (
            h["weight"] is None,
            -(h["weight"] or 0.0),
            h["pct_nav"] is None,
            -(h["pct_nav"] or 0.0),
        ),
    )
    return {
        "fund_name": disclosed["fund_name"],
        "report_date": disclosed["report_date"],
        "index": (
            {k: index_info[k] for k in ("index_code", "index_name", "index_date")}
            if index_info else None
        ),
        "holdings": items,
    }


# ================================================================
# 场外基金 (pingzhongdata): 搜索代码 + 前十大重仓 (S3 兜底口径之一)
# 2026-08-31 恢复 — 多源重写时被误删, etf_group_sync/回填仍在引用
# ================================================================

_NAME_RE = re.compile(r'var\s+fS_name\s*=\s*"([^"]*)"')
_STOCK_CODES_RE = re.compile(r"var\s+stockCodesNew\s*=\s*(\[[^\]]*\])")
_MARKET_PREFIX = {"1": "SH", "0": "SZ"}
_FUND_SEARCH_BASE = "https://fundsuggest.eastmoney.com/FundSearch/api/FundSearchAPI.ashx"


def search_fund_code_by_name(name: str) -> str | None:
    """基金名称 → 场外基金代码 (搜索结果中精确同名者, 无匹配返回 None)。"""
    import urllib.parse

    q = (name or "").strip()
    if not q:
        return None
    url = _FUND_SEARCH_BASE + "?" + urllib.parse.urlencode(
        {"m": "1", "key": q, "pageindex": "0", "pagesize": "20"}
    )
    try:
        body = _http_get(url)
    except FundHoldingsError as exc:
        logger.warning("fund search failed (%s): %s", name, exc)
        return None
    try:
        datas = json.loads(body).get("Datas") or []
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
    report_date 置空)。与 ETF 路径 (F10 全量披露) 结构对齐, 便于统一处理。
    """
    code = str(code or "").strip()
    if not re.fullmatch(r"\d{6}", code):
        raise FundHoldingsError(f"非法场外基金代码: {code}")
    url = "https://fund.eastmoney.com/pingzhongdata/{}.js".format(code)
    body = _http_get(url)
    name_m = _NAME_RE.search(body)
    codes_m = _STOCK_CODES_RE.search(body)
    fund_name = name_m.group(1).strip() if name_m else ""
    symbols: list[str] = []
    if codes_m:
        try:
            raw_codes = json.loads(codes_m.group(1))
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
