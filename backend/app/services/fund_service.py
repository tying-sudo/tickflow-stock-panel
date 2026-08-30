"""基金数据服务（东方财富公开接口）。

- 基金模糊搜索: fundsuggest.eastmoney.com FundSearchAPI.ashx（按代码/名称/拼音首字母）
- 基金持仓:    fund.eastmoney.com/pingzhongdata/{code}.js
  其中 var fS_name = "基金名"; var stockCodesNew = ["1.600519", "0.000333", ...]
  为前十大重仓股代码，前缀 1=沪 0=深，转成本项目 symbol 格式 "600519.SH" / "000333.SZ"。
"""
from __future__ import annotations

import json
import logging
import re

import httpx

logger = logging.getLogger(__name__)

_SEARCH_URL = "https://fundsuggest.eastmoney.com/FundSearch/api/FundSearchAPI.ashx"
_PINGZHONG_URL = "https://fund.eastmoney.com/pingzhongdata/{code}.js"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Referer": "https://fund.eastmoney.com/",
}

_CATEGORY_FUND = 700  # Datas[].CATEGORY == 700 为基金

_NAME_RE = re.compile(r'var\s+fS_name\s*=\s*"([^"]*)"')
_STOCK_CODES_RE = re.compile(r"var\s+stockCodesNew\s*=\s*(\[[^\]]*\])")

_MARKET_PREFIX = {"1": "SH", "0": "SZ"}


def search_funds(q: str, limit: int = 10) -> list[dict]:
    """按代码/名称/拼音首字母模糊搜索基金，返回 [{code, name, fund_type, company, manager}]。"""
    resp = httpx.get(
        _SEARCH_URL,
        params={"m": "1", "key": q.strip(), "pageindex": "0", "pagesize": str(max(limit, 1) * 3)},
        headers=_HEADERS,
        timeout=8.0,
    )
    resp.raise_for_status()
    datas = resp.json().get("Datas") or []
    results: list[dict] = []
    for d in datas:
        if d.get("CATEGORY") != _CATEGORY_FUND:
            continue
        base = d.get("FundBaseInfo") or {}
        results.append(
            {
                "code": str(d.get("CODE") or ""),
                "name": str(d.get("NAME") or ""),
                "fund_type": base.get("FTYPE"),
                "company": base.get("JJGS"),
                "manager": base.get("JJJL"),
            }
        )
        if len(results) >= limit:
            break
    return results


def parse_holdings_js(text: str) -> tuple[str, list[str]]:
    """解析 pingzhongdata JS 文本 → (基金名, [symbol...])。"""
    name_m = _NAME_RE.search(text)
    codes_m = _STOCK_CODES_RE.search(text)
    name = name_m.group(1).strip() if name_m else ""
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
            market, code = s.split(".", 1)
            exchange = _MARKET_PREFIX.get(market)
            if exchange and len(code) == 6 and code.isdigit():
                symbols.append(f"{code}.{exchange}")
    return name, symbols


def get_fund_profile(code: str) -> dict:
    """拉取基金名称与前十大重仓股。返回 {code, name, holdings: [symbol...]}。"""
    code = code.strip()
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError(f"非法基金代码: {code}")
    resp = httpx.get(_PINGZHONG_URL.format(code=code), headers=_HEADERS, timeout=10.0)
    resp.raise_for_status()
    name, holdings = parse_holdings_js(resp.text)
    if not name:
        raise ValueError(f"未解析到基金信息 (代码 {code} 可能不存在或非场外基金)")
    return {"code": code, "name": name, "holdings": holdings}
