"""指数补充源: 腾讯 qt/ifzq 实时行情·中文名 + 日K·分时, 新浪 hq 兜底。

背景 (2026-09-08): 57 只指数 (97/98 国证新段、395 深市统计段、93/0008xx-0009xx
中证系) 在 TDX 体系没有实时行情 (/quotes 对这些代码返回垃圾行), 东财 push2
全系对机房 IP 封闭。实测:
- 腾讯 qt.gtimg.cn 覆盖 42/57 (21 只中证系 0008xx-0009xx + 21 只国证 97/98),
  全部带真实价格且**返回中文名** (创业板人工智能/机器人产业/中证200...),
  volume 量纲=手 与 TDX 指数契约一致 (399001 对拍: 收盘 13703.21 / vol
  644275504 手)。这是实时补充的主通道, 也是维表中文名的权威来源。
- 新浪 hq.sinajs.cn 全量格式覆盖国证 97/98 段 21 只 (名字是英文短名
  CNTAI50/CNIROBOT, 价格可用) → fetch_realtime 里作腾讯断线时的次级兜底。
- 395xxx 深市成交统计段 (主板A/债券现货/总成交...) 任何源都无"价格"
  (纯统计指数), 日K 由 TDX /bars 正常提供 → 维持日K兜底显示, 不算死源。
- 932000 中证2000 腾讯/新浪均无 → 唯一残留, 维持日K兜底 (最后已知 09-02)。

本模块是 easy_tdx 的**补充通道** (supplement), 只服务 easy_tdx 拿不到的
指数, 不参与能力路由/偏好选择:
- fetch_realtime  → quote_service 实时轮询缺失指数补齐 (records 契约同
  provider.get_realtime, volume=手)
- fetch_cn_names  → sync_index_instruments 维表中文名修正 (英文短名全覆盖)
- fetch_daily_many → index_sync._fetch_daily_chunk 日K缺口补齐 (DAILY_COLS
  契约, 走 normalize_daily, volume=手; 腾讯为主新浪为备)
- fetch_minute    → /api/index/minute 分时回退 (CANONICAL_MINUTE_COLS 契约,
  腾讯累计量差分为分钟量; 腾讯只供最新交易日)

安全边界: 出站请求仅允许下方白名单内的 https 域名 (行情端点为固定常量,
不接受任何外部输入拼接主机名), URL 主机不在白名单直接拒绝。
"""
from __future__ import annotations

import json
import logging
import time
import urllib.request
from datetime import date, datetime
from urllib.parse import urlparse

import polars as pl

from app.data_providers.normalizer import normalize_daily

logger = logging.getLogger(__name__)

_SINA_TIMEOUT = 8.0
_TENCENT_TIMEOUT = 10.0
_HTTP_RETRIES = 2
# 新浪全量行情字段 (指数): 0名称 1今开 2昨收 3最新 4最高 5最低 6卖价 7买价
# 8成交量(股) 9成交额(元) ... 30日期 31时间。vol/100 → 手 (腾讯对拍校准)。
_SINA_MIN_FIELDS = 32
# 腾讯 qt.gtimg.cn 完整行情字段 (指数, 实测 46 段): 0未知 1名称 2代码 3最新
# 4昨收 5今开 6成交量(手) 7-30五档/买卖盘 31空 32时间戳 33涨跌额 34涨跌幅%
# 35最高 36最低 37 "价/量/额(元)" 38成交量(手) 39成交额(万) 40换手 41市盈
_TENCENT_MIN_FIELDS = 38

# 出站域名白名单 (本模块唯一允许访问的主机, 全部为公开行情端点)
_ALLOWED_HOSTS = frozenset({
    "hq.sinajs.cn",
    "quotes.sina.cn",
    "qt.gtimg.cn",
    "web.ifzq.gtimg.cn",
})


def _http_text(url: str, *, headers: dict | None = None, encoding: str = "utf-8", timeout: float) -> str:
    parsed = urlparse(url)
    if parsed.scheme != "https" or (parsed.hostname or "") not in _ALLOWED_HOSTS:
        raise ValueError(f"host not allowed: {url!r}")
    last_exc: Exception | None = None
    for attempt in range(_HTTP_RETRIES):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", **(headers or {})},
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode(encoding, "replace")
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt + 1 < _HTTP_RETRIES:
                time.sleep(0.5 * (attempt + 1))
    raise last_exc if last_exc else RuntimeError("http request failed")


def _sina_code(symbol: str) -> str:
    code, _, mkt = symbol.partition(".")
    return ("sh" if mkt.upper() == "SH" else "sz") + code


def _from_symbol(symbol: str) -> str:
    code, _, mkt = symbol.partition(".")
    return ("sh" if mkt.upper() == "SH" else "sz") + code


def _num(value: str) -> float | None:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if v != 0.0 else None  # 新浪指数 6/7 档固定 0.000, 一律视无效


# ==================================================================
# 实时行情 (腾讯 qt 为主, 新浪 hq 兜底)
# ==================================================================

def _tencent_realtime(symbols: list[str]) -> list[dict]:
    """腾讯 qt.gtimg.cn 批量实时 → records (vol=手, 名称含中文)。"""
    codes = ",".join(_from_symbol(s) for s in symbols)
    body = _http_text(f"https://qt.gtimg.cn/q={codes}", encoding="gbk", timeout=_TENCENT_TIMEOUT)
    by_code = {_from_symbol(s): s for s in symbols}
    records: list[dict] = []
    for line in body.split(";"):
        line = line.strip()
        if not line or "~" not in line:
            continue
        sym = line.split("=", 1)[0].replace("v_", "").strip()
        symbol = by_code.get(sym)
        if not symbol:
            continue
        fields = line.split("=", 1)[1].replace('"', "").split("~")
        if len(fields) < _TENCENT_MIN_FIELDS:
            continue
        last = _num(fields[3])
        prev = _num(fields[4])
        if last is None or prev in (None, 0):
            continue  # 无行情/统计指数 (395xxx), 交日K兜底
        # 交易时段外高/低/开可能为 0, 交由 _build_daily 的 close 填充守卫
        vol = _num(fields[6])
        amount: float | None = None
        try:
            amount = float(str(fields[37]).split("/")[2]) if "/" in fields[37] else None
        except (TypeError, ValueError, IndexError):
            amount = None
        change_amount = last - prev
        records.append({
            "symbol": symbol,
            "last_price": last,
            "prev_close": prev,
            "open": _num(fields[5]),
            "high": _num(fields[35]),
            "low": _num(fields[36]),
            "volume": vol,            # 手, 与 TDX 契约一致
            "amount": amount,         # 元 (无则 None)
            "change_amount": change_amount,
            "change_pct": change_amount / prev,
            "session": "normal",
        })
    return records


def _sina_full_realtime(symbols: list[str]) -> list[dict]:
    """新浪 hq.sinajs.cn 全量格式 → records (国证 97/98 段; vol 股→手)。

    只作腾讯通道失灵时的次级兜底: 主场失败抛异常 → 此兜底成功也不算失败。
    """
    codes = ",".join(_sina_code(s) for s in symbols)
    body = _http_text(
        f"https://hq.sinajs.cn/list={codes}",
        headers={"Referer": "https://finance.sina.com.cn"},
        encoding="gbk",
        timeout=_SINA_TIMEOUT,
    )
    by_code = {_sina_code(s): s for s in symbols}
    records: list[dict] = []
    for line in body.splitlines():
        if '="' not in line:
            continue
        var, payload = line.split('="', 1)
        payload = payload.rstrip('";')
        code = var.replace("var hq_str_", "").strip()
        symbol = by_code.get(code)
        if not symbol or not payload:
            continue
        fields = payload.split(",")
        if len(fields) < _SINA_MIN_FIELDS:
            continue
        last = _num(fields[3])
        prev = _num(fields[2])
        if last is None or prev in (None, 0):
            continue
        vol = _num(fields[8])
        change_amount = last - prev
        records.append({
            "symbol": symbol,
            "last_price": last,
            "prev_close": prev,
            "open": _num(fields[1]),
            "high": _num(fields[4]),
            "low": _num(fields[5]),
            "volume": vol / 100.0 if vol else None,  # 股 → 手
            "amount": _num(fields[9]),
            "change_amount": change_amount,
            "change_pct": change_amount / prev,
            "session": "normal",
        })
    return records


def fetch_realtime(symbols: list[str]) -> list[dict]:
    """指数实时补充 → records (契约同 easy_tdx provider.get_realtime)。

    腾讯 qt.gtimg.cn 为唯一主通道 (42/57 带中文名); 失败时新浪全量格式兜底
    (21 只国证段)。仍缺的 (395xxx 统计段/932000) 交日K兜底显示。
    """
    if not symbols:
        return []
    try:
        records = _tencent_realtime(symbols)
    except Exception as exc:  # noqa: BLE001
        logger.warning("补充源实时(腾讯)失败, 走新浪兜底: %s", exc)
        records = []
    if records:
        return records
    return _sina_full_realtime(symbols)


def fetch_cn_names(symbols: list[str]) -> dict[str, str]:
    """腾讯批量取中文指数名 → {}。

    上游维表对 97/98 国证段给英文短名 (CNTAI50/CNIROBOT/CNTHKD...), 腾讯
    返回中文全称 (创业板人工智能/机器人产业/创业板指(港币)(CNH)...);
    395xxx 统计段腾讯只给代码名 (395001), 跳过。失效 as-is (留给上游名)。
    """
    if not symbols:
        return {}
    codes = ",".join(_from_symbol(s) for s in symbols)
    try:
        body = _http_text(f"https://qt.gtimg.cn/q={codes}", encoding="gbk", timeout=_TENCENT_TIMEOUT)
    except Exception as exc:  # noqa: BLE001
        logger.warning("指数中文名补充(腾讯)失败: %s", exc)
        return {}
    by_code = {_from_symbol(s): s for s in symbols}
    out: dict[str, str] = {}
    for line in body.split(";"):
        line = line.strip()
        if not line or "~" not in line:
            continue
        sym = line.split("=", 1)[0].replace("v_", "").strip()
        symbol = by_code.get(sym)
        if not symbol:
            continue
        fields = line.split("=", 1)[1].replace('"', "").split("~")
        if len(fields) < 2:
            continue
        name = fields[1].strip()
        if not name or any(ch.isdigit() for ch in name) and not any("\u4e00" <= ch <= "\u9fff" for ch in name):
            continue  # 纯代码名 (395001) 或空名, 无用
        if any("\u4e00" <= ch <= "\u9fff" for ch in name):
            out[symbol] = name
    return out


# ==================================================================
# 日 K (腾讯 ifzq 为主, 新浪为备)
# ==================================================================

def _tencent_daily(symbol: str, count: int) -> list[dict]:
    code = _from_symbol(symbol)
    body = _http_text(
        f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={code},day,,,{count},qfq",
        timeout=_TENCENT_TIMEOUT,
    )
    node = (json.loads(body).get("data") or {}).get(code) or {}
    klines = node.get("qfqday") or node.get("day") or []
    rows: list[dict] = []
    for k in klines:
        # [date, open, close, high, low, volume(手)] — 无 amount 字段
        try:
            rows.append({
                "symbol": symbol,
                "date": str(k[0]),
                "open": float(k[1]),
                "close": float(k[2]),
                "high": float(k[3]),
                "low": float(k[4]),
                "volume": float(k[5]) if len(k) > 5 else None,
                "amount": None,
            })
        except (TypeError, ValueError, IndexError):
            continue
    return rows


def _sina_daily(symbol: str, count: int) -> list[dict]:
    body = _http_text(
        "https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData"
        f"?symbol={_sina_code(symbol)}&scale=240&ma=no&datalen={count}",
        timeout=_TENCENT_TIMEOUT,
    )
    rows: list[dict] = []
    try:
        items = json.loads(body)
    except ValueError:
        return []
    for k in items or []:
        try:
            vol = float(k.get("volume") or 0)
            rows.append({
                "symbol": symbol,
                "date": str(k.get("day"))[:10],
                "open": float(k.get("open")),
                "close": float(k.get("close")),
                "high": float(k.get("high")),
                "low": float(k.get("low")),
                "volume": vol / 100.0 if vol else None,  # 股 → 手
                "amount": None,
            })
        except (TypeError, ValueError):
            continue
    return rows


def fetch_daily_many(
    symbols: list[str],
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> pl.DataFrame:
    """批量拉指数日K → DAILY_COLS 契约 (normalize_daily 收口, vol=手)。

    逐只请求 (腾讯为主、新浪为备), 失败只记 debug 不中断; 窗口过滤在本地做。
    """
    if not symbols:
        return pl.DataFrame()
    days = 400
    if start_time is not None:
        end = end_time or datetime.now()
        days = max((end - start_time).days + 5, 10)
    count = min(days, 800)
    frames: list[pl.DataFrame] = []
    for symbol in symbols:
        rows: list[dict] = []
        try:
            rows = _tencent_daily(symbol, count)
        except Exception as exc:  # noqa: BLE001
            logger.debug("em daily(tencent) %s failed: %s", symbol, exc)
        if not rows:
            try:
                rows = _sina_daily(symbol, count)
            except Exception as exc:  # noqa: BLE001
                logger.debug("em daily(sina) %s failed: %s", symbol, exc)
        if not rows:
            continue
        if start_time is not None:
            floor = start_time.strftime("%Y-%m-%d")
            rows = [r for r in rows if str(r["date"]) >= floor]
        frame = normalize_daily(rows, default_symbol=symbol)
        if not frame.is_empty():
            frames.append(frame)
    if not frames:
        return pl.DataFrame()
    df = pl.concat(frames, how="diagonal_relaxed")
    if end_time is not None:
        ceiling = end_time.date()
        df = df.filter(pl.col("date") <= ceiling)
    return df


# ==================================================================
# 分时 (腾讯 minute/query, 只供最新交易日)
# ==================================================================

def fetch_minute(symbol: str, trade_date: date) -> pl.DataFrame:
    """腾讯分时 → CANONICAL_MINUTE_COLS (datetime=北京墙钟 naive)。

    行格式 'HHMM 价 累计量(手) 累计额(元)': 分钟量/额 = 累计差分;
    分时线只有成交价, open/high/low 用该分钟价填充。
    """
    code = _from_symbol(symbol)
    body = _http_text(
        f"https://web.ifzq.gtimg.cn/appstock/app/minute/query?code={code}",
        timeout=_TENCENT_TIMEOUT,
    )
    try:
        node = (((json.loads(body).get("data") or {}).get(code) or {}).get("data") or {})
    except ValueError:
        return pl.DataFrame()
    raw_date = str(node.get("date") or "")
    rows_raw = node.get("data") or []
    if not raw_date or not rows_raw:
        return pl.DataFrame()
    day = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
    if day != str(trade_date):
        return pl.DataFrame()  # 腾讯只回最新交易日

    out: list[dict] = []
    prev_vol = 0.0
    prev_amt = 0.0
    for line in rows_raw:
        parts = str(line).split(" ")
        if len(parts) < 3:
            continue
        hhmm = parts[0]
        try:
            price = float(parts[1])
            cum_vol = float(parts[2])
            cum_amt = float(parts[3]) if len(parts) > 3 else 0.0
        except ValueError:
            continue
        out.append({
            "symbol": symbol,
            "datetime": f"{day} {hhmm[:2]}:{hhmm[2:4]}:00",
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": max(cum_vol - prev_vol, 0.0),
            "amount": max(cum_amt - prev_amt, 0.0),
        })
        prev_vol, prev_amt = cum_vol, cum_amt
    if not out:
        return pl.DataFrame()
    return pl.DataFrame(out)


def availability() -> tuple[bool, str]:
    """冒烟探针: 新浪实时通道是否可达 (399001 对照)。"""
    try:
        rows = fetch_realtime(["399001.SZ"])
    except Exception as exc:  # noqa: BLE001
        return False, f"sina hq unreachable: {exc}"
    if not rows:
        return False, "sina hq empty response"
    return True, f"ok, 399001={rows[0]['last_price']}"
