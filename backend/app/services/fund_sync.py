"""场外基金数据同步服务 (天天基金/东方财富公开接口, 免登录)。

数据流:
  1. 基金维表: fund.eastmoney.com/js/fundcode_search.js
     → 全量场外基金列表 (约2.7万只, 含代码/名称/类型)
  2. 净值快照: fund.eastmoney.com/data/rankhandler.aspx (基金排行数据接口)
     → 全市场最新单位/累计净值 (约2万只有净值), 每次分页拉全量
     每日快照落盘一份 (date=YYYY-MM-DD), 日积月累形成净值历史。

落盘:
  data/funds/fund_list.parquet              基金维表
  data/funds/nav/date=YYYY-MM-DD/*.parquet  每日净值快照

卡片统计: 基金总数 / 有净值基金数 / 净值披露日期范围 / 已积累快照天数。
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from datetime import date
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
_FUND_LIST_URL = "http://fund.eastmoney.com/js/fundcode_search.js"
_NAV_PAGE_URL = ("http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all"
                 "&rs=&gs=0&sc=zzf&st=desc&pi={pi}&pn={pn}&dx=1&_={ts}")
_NAV_PAGE_SIZE = 10000
_NAV_MAX_PAGES = 10
_PAGE_SLEEP_S = 1.0  # 温和限速, 避免触发风控


def _http_get(url: str, referer: str, timeout: float = 60) -> str:
    import urllib.request

    req = urllib.request.Request(url, headers={
        "User-Agent": _UA,
        "Referer": referer,
        "Accept": "*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "ignore")


# ── 基金维表 ─────────────────────────────────────────────────────────────

def fetch_fund_list() -> list[dict]:
    """全量场外基金列表 → [{code, pinyin, name, fund_type}]。"""
    txt = _http_get(_FUND_LIST_URL, "http://fund.eastmoney.com/")
    start, end = txt.find("["), txt.rfind("]")
    if start < 0 or end <= start:
        raise RuntimeError("fundcode_search.js 解析失败")
    arr = json.loads(txt[start:end + 1])
    rows: list[dict] = []
    for item in arr:
        if not isinstance(item, (list, tuple)) or not item:
            continue
        code = str(item[0]).strip()
        if not code:
            continue
        rows.append({
            "code": code,
            "pinyin": str(item[1]) if len(item) > 1 else "",
            "name": str(item[2]) if len(item) > 2 else code,
            "fund_type": str(item[3]) if len(item) > 3 else "",
        })
    return rows


# ── 净值快照 ─────────────────────────────────────────────────────────────

def _extract_datas_array(txt: str) -> tuple[list, int | None]:
    """从 `var rankData = {datas:[...],allRecords:N,...}` 中括号匹配提取 datas。

    rankData 是 JS 对象字面量(键不带引号), 不能直接 json.loads,
    但 datas 数组本身是合法 JSON 字符串数组。
    """
    i = txt.find("datas:")
    if i < 0:
        return [], None
    start = txt.find("[", i)
    if start < 0:
        return [], None
    depth, end, in_str, esc = 0, -1, False, False
    for j in range(start, len(txt)):
        c = txt[j]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
            continue
        if c == '"':
            in_str = True
        elif c == "[":
            depth += 1
        elif c == "]":
            depth -= 1
            if depth == 0:
                end = j + 1
                break
    if end < 0:
        return [], None
    rows = json.loads(txt[start:end])
    m = re.search(r"allRecords:\s*(\d+)", txt)
    return rows, (int(m.group(1)) if m else None)


def fetch_nav_snapshot() -> list[dict]:
    """全市场最新净值快照 → [{code, name, nav_date, unit_nav, acc_nav, change_pct}]。"""
    out: list[dict] = []
    total: int | None = None
    for pi in range(1, _NAV_MAX_PAGES + 1):
        url = _NAV_PAGE_URL.format(pi=pi, pn=_NAV_PAGE_SIZE, ts=int(time.time() * 1000))
        txt = _http_get(url, "http://fund.eastmoney.com/data/fundranking.html")
        raw, total = _extract_datas_array(txt)
        if not raw:
            break
        for item in raw:
            parts = item.split(",") if isinstance(item, str) else item
            if not isinstance(parts, (list, tuple)) or len(parts) < 6:
                continue
            code = str(parts[0]).strip()
            nav_date = str(parts[3]).strip() if parts[3] else None
            if not code or not nav_date or nav_date == "":
                continue
            out.append({
                "code": code,
                "name": str(parts[1]),
                "nav_date": nav_date,
                "unit_nav": _to_float(parts[4]),
                "acc_nav": _to_float(parts[5]),
                "change_pct": _to_float(parts[6]),
            })
        done = total is not None and len(out) >= total
        if done or len(raw) < _NAV_PAGE_SIZE:
            break
        time.sleep(_PAGE_SLEEP_S)
    if total is not None and len(out) < total:
        logger.warning("基金净值快照不完整: %d/%d", len(out), total)
    return out


def _to_float(v) -> float | None:
    try:
        s = str(v).strip().replace("%", "")
        if s in ("", "-", "--"):
            return None
        return float(s)
    except (TypeError, ValueError):
        return None


# ── 落盘 ─────────────────────────────────────────────────────────────────

def _atomic_write_parquet(df: pl.DataFrame, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".parquet.tmp")
    df.write_parquet(tmp)
    os.replace(tmp, target)


def sync_all(data_dir: Path) -> dict:
    """同步基金维表 + 全市场净值快照(阻塞)。返回摘要 dict。"""
    base = Path(data_dir) / "funds"
    started = time.time()

    funds = fetch_fund_list()
    fl_df = pl.DataFrame(funds)
    _atomic_write_parquet(fl_df, base / "fund_list.parquet")

    type_map = {f["code"]: f["fund_type"] for f in funds}
    snap = fetch_nav_snapshot()
    for row in snap:
        row["fund_type"] = type_map.get(row["code"], "")
    nav_df = pl.DataFrame(snap)

    snapshot_day = date.today().isoformat()
    part_dir = base / "nav" / f"date={snapshot_day}"
    part_dir.mkdir(parents=True, exist_ok=True)
    tmp_name = part_dir / "part-0000.parquet.tmp"
    nav_df.write_parquet(tmp_name)
    # 清掉同分区旧文件(重跑), 保留唯一 part
    for f in part_dir.glob("*.parquet"):
        f.unlink()
    os.replace(tmp_name, part_dir / "part-0000.parquet")

    summary = {
        "fund_count": len(funds),
        "nav_rows": len(snap),
        "snapshot_date": snapshot_day,
        "nav_date_min": str(nav_df["nav_date"].min()) if snap else None,
        "nav_date_max": str(nav_df["nav_date"].max()) if snap else None,
        "elapsed_s": round(time.time() - started, 1),
    }
    logger.info("基金数据同步完成: %s", summary)
    return summary


# ── 统计聚合 (数据画像卡片) ───────────────────────────────────────────────

def aggregate_funds(data_dir: Path) -> dict | None:
    """基金卡片统计。无任何数据时返回 None。"""
    base = Path(data_dir) / "funds"
    out: dict = {}

    fl_path = base / "fund_list.parquet"
    if fl_path.exists():
        try:
            df = pl.read_parquet(fl_path)
            out["rows"] = df.height          # 基金总数(维表) → 卡片大数字
            out["fund_count"] = df.height
        except Exception as e:  # noqa: BLE001
            logger.warning("fund_list.parquet 读取失败: %s", e)

    nav_root = base / "nav"
    parts = sorted(nav_root.glob("date=*")) if nav_root.exists() else []
    if parts:
        out["trading_days"] = len(parts)     # 已积累快照天数
        latest_files = sorted(parts[-1].glob("*.parquet"))
        if latest_files:
            try:
                ndf = pl.read_parquet(latest_files[0])
                out["symbols_covered"] = ndf.height   # 有净值基金数
                if ndf.height:
                    out["earliest_date"] = str(ndf["nav_date"].min())
                    out["latest_date"] = str(ndf["nav_date"].max())
            except Exception as e:  # noqa: BLE001
                logger.warning("净值快照读取失败: %s", e)

    return out or None


# ── 后台任务状态 ─────────────────────────────────────────────────────────

_state_lock = threading.Lock()
_state: dict = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "error": None,
    "last_summary": None,
}
_thread: threading.Thread | None = None


def get_status(data_dir: Path | None = None) -> dict:
    with _state_lock:
        status = dict(_state)
    if data_dir is not None:
        status["stats"] = aggregate_funds(data_dir)
    return status


def _finish(summary: dict | None, error: str | None) -> None:
    from datetime import datetime

    with _state_lock:
        _state["running"] = False
        _state["finished_at"] = datetime.now().isoformat(timespec="seconds")
        _state["error"] = error
        if summary:
            _state["last_summary"] = summary
    # 同步完成后失效数据画像缓存(funds 表)
    try:
        from app.api.data import invalidate_data_cache
        invalidate_data_cache("funds")
    except Exception:  # noqa: BLE001
        pass


def _run() -> None:
    from app.config import settings

    try:
        summary = sync_all(settings.data_dir)
        _finish(summary, None)
    except Exception as e:  # noqa: BLE001
        logger.exception("基金数据同步失败")
        _finish(None, str(e))


def start_sync(data_dir: Path | None = None) -> dict:
    """后台线程启动同步; 已在运行时返回 False。"""
    global _thread
    with _state_lock:
        if _state["running"]:
            return {"started": False, "running": True}
        from datetime import datetime

        _state["running"] = True
        _state["started_at"] = datetime.now().isoformat(timespec="seconds")
        _state["error"] = None
    _thread = threading.Thread(target=_run, name="fund-sync", daemon=True)
    _thread.start()
    return {"started": True, "running": True}
