"""最新资讯二开扩展 — 多源财经快讯聚合页。

能力(按 docs/secondary-development.md L2 路由注册接入, 不改核心行为):
  - 抓取 6 个公开资讯源(财联社/东方财富/交易所公告/新浪财经/金十数据/同花顺),
    单源失败只记录状态, 不影响其他源与主流程。
  - 快讯按发布日(北京时间)落 parquet 分区 {data_dir}/news/items/date=YYYY-MM-DD/。
  - AI 标注(复用 app.services.ai_provider 当前配置的模型): 利好/利空/中性、
    重要标记、概念标签、涉及个股; AI 未配置时条目保持未标注, 不推送。
  - 消息推送复用监控中心已配置的飞书/企业微信 webhook, 仅推 AI 判定为重要的条目。
  - 定时抓取: 扩展自持 AsyncIOScheduler(interval 可配), shutdown 钩子里停止。

存储与状态均为扩展私有目录, 不写入核心 preferences/仓库。
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from html import unescape
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import polars as pl
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import APIRouter, HTTPException, Query

from app.extensions import (
    BACKEND_EXTENSION_API_VERSION,
    BackendExtensionRegistrar,
    ExtensionContext,
)
from app.services import preferences, webhook_adapter
from app.services.ai_provider import Message, ai_configured, generate_ai_text

logger = logging.getLogger(__name__)

EXTENSION_ID = "news.feed"
EXTENSION_API_VERSION = BACKEND_EXTENSION_API_VERSION

SH = ZoneInfo("Asia/Shanghai")

SOURCE_LABELS: dict[str, str] = {
    "cls": "财联社",
    "eastmoney": "东方财富",
    "em_news": "东财要闻",
    "exchange": "交易所公告",
    "sina": "新浪财经",
    "jin10": "金十数据",
    "ths": "同花顺",
}
PUSH_CHANNELS = {"feishu", "wecom"}
SENTIMENTS = {"positive", "negative", "neutral", "none"}

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
}

_SCHEMA = {
    "id": pl.String,
    "source": pl.String,
    "title": pl.String,
    "content": pl.String,
    "url": pl.String,
    "published_ts": pl.Int64,
    "published_at": pl.String,
    "fetched_at": pl.String,
    "sentiment": pl.String,
    "important": pl.Boolean,
    "tags_json": pl.String,
    "stocks_json": pl.String,
    "analyzed": pl.Boolean,
}

_SETTINGS_DEFAULTS: dict[str, Any] = {
    "interval_minutes": 30,
    "sources": {name: True for name in SOURCE_LABELS},
    "push_enabled": False,
    "push_channels": ["feishu"],
    "focus_concepts": ["科技"],
    "retention_days": 30,
}

# ---------------------------------------------------------------------------
# 北京时间与路径工具
# ---------------------------------------------------------------------------


def _now_bj() -> datetime:
    return datetime.now(tz=SH)


def _ts_to_bj_str(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=SH).strftime("%Y-%m-%d %H:%M:%S")


def _bj_date_of_ts(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=SH).strftime("%Y-%m-%d")


def _parse_bj_time(text: str) -> int | None:
    """把源返回的北京时间墙钟字符串转为 epoch 秒; 解析失败返回 None。"""
    text = str(text or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return int(datetime.strptime(text, fmt).replace(tzinfo=SH).timestamp())
        except ValueError:
            continue
    return None


def _news_dir(data_dir: Path) -> Path:
    return Path(data_dir) / "news"


def _items_dir(data_dir: Path) -> Path:
    return _news_dir(data_dir) / "items"


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, path)


def _atomic_write_parquet(path: Path, df: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name("part.parquet.tmp")
    df.write_parquet(tmp)
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# 设置与运行状态
# ---------------------------------------------------------------------------


def load_settings(data_dir: Path) -> dict[str, Any]:
    settings = json.loads(json.dumps(_SETTINGS_DEFAULTS))  # deep copy
    path = _news_dir(data_dir) / "settings.json"
    if path.exists():
        try:
            stored = json.loads(path.read_text(encoding="utf-8"))
            for key in _SETTINGS_DEFAULTS:
                if key in stored:
                    settings[key] = stored[key]
        except Exception as exc:
            logger.warning("news settings load failed, use defaults: %s", exc)
    return _validate_settings(settings)


def _validate_settings(settings: dict[str, Any]) -> dict[str, Any]:
    try:
        settings["interval_minutes"] = int(settings["interval_minutes"])
    except (TypeError, ValueError):
        settings["interval_minutes"] = _SETTINGS_DEFAULTS["interval_minutes"]
    settings["interval_minutes"] = max(10, min(360, settings["interval_minutes"]))

    sources = settings.get("sources")
    settings["sources"] = {
        name: bool(sources.get(name, True)) if isinstance(sources, dict) else True
        for name in SOURCE_LABELS
    }

    settings["push_enabled"] = bool(settings.get("push_enabled"))

    channels = settings.get("push_channels")
    settings["push_channels"] = [
        c for c in (channels if isinstance(channels, list) else []) if c in PUSH_CHANNELS
    ] or ["feishu"]

    focus = settings.get("focus_concepts")
    settings["focus_concepts"] = [
        str(c).strip()[:12] for c in (focus if isinstance(focus, list) else []) if str(c).strip()
    ][:10]

    try:
        settings["retention_days"] = max(7, min(365, int(settings.get("retention_days", 30))))
    except (TypeError, ValueError):
        settings["retention_days"] = 30
    return settings


def save_settings(data_dir: Path, settings: dict[str, Any]) -> dict[str, Any]:
    validated = _validate_settings(settings)
    _atomic_write_json(_news_dir(data_dir) / "settings.json", validated)
    _reschedule(validated["interval_minutes"])
    return validated


def load_state(data_dir: Path) -> dict[str, Any]:
    path = _news_dir(data_dir) / "state.json"
    if not path.exists():
        return {"last_fetch_at": None, "sources": {}, "pushed_ids": [], "last_push_at": None}
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"last_fetch_at": None, "sources": {}, "pushed_ids": [], "last_push_at": None}
    state.setdefault("sources", {})
    state.setdefault("pushed_ids", [])
    return state


def _update_state(data_dir: Path, **changes: Any) -> None:
    state = load_state(data_dir)
    state.update(changes)
    state["pushed_ids"] = state.get("pushed_ids", [])[-1000:]
    _atomic_write_json(_news_dir(data_dir) / "state.json", state)


# ---------------------------------------------------------------------------
# 条目存储 (parquet 按发布日分区)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NewsItem:
    id: str
    source: str
    title: str
    content: str
    url: str | None
    published_ts: int
    tags: list[str]
    stocks: list[dict[str, str]]
    important_hint: bool = False


def _row_of(item: NewsItem, fetched_at: str) -> dict[str, Any]:
    return {
        "id": item.id,
        "source": item.source,
        "title": item.title[:300],
        "content": item.content[:4000],
        "url": item.url or "",
        "published_ts": int(item.published_ts),
        "published_at": _ts_to_bj_str(item.published_ts),
        "fetched_at": fetched_at,
        "sentiment": "none",
        "important": bool(item.important_hint),
        "tags_json": json.dumps(item.tags[:8], ensure_ascii=False, separators=(",", ":")),
        "stocks_json": json.dumps(item.stocks[:8], ensure_ascii=False, separators=(",", ":")),
        "analyzed": False,
    }


def _partition_file(data_dir: Path, bj_date: str) -> Path:
    return _items_dir(data_dir) / f"date={bj_date}" / "part.parquet"


def _load_known_ids(data_dir: Path, days: int = 7) -> set[str]:
    ids: set[str] = set()
    today = _now_bj().date()
    for offset in range(days):
        path = _partition_file(data_dir, (today - timedelta(days=offset)).isoformat())
        if path.exists():
            try:
                ids.update(pl.read_parquet(path, columns=["id"])["id"].to_list())
            except Exception as exc:
                logger.warning("news partition id reload failed %s: %s", path, exc)
    return ids


_known_ids: set[str] = set()


def save_items(data_dir: Path, items: list[NewsItem]) -> int:
    """按发布日分区落盘, 幂等(按 id 去重), 返回新增条数。"""
    global _known_ids
    if not items:
        return 0
    fetched_at = _now_bj().strftime("%Y-%m-%d %H:%M:%S")
    by_date: dict[str, list[dict[str, Any]]] = {}
    seen_in_batch: set[str] = set()
    for item in items:
        if item.id in _known_ids or item.id in seen_in_batch:
            continue
        seen_in_batch.add(item.id)
        by_date.setdefault(_bj_date_of_ts(item.published_ts), []).append(_row_of(item, fetched_at))
    if not by_date:
        return 0

    saved = 0
    for bj_date, rows in by_date.items():
        path = _partition_file(data_dir, bj_date)
        new_df = pl.DataFrame(rows, schema=_SCHEMA)
        if path.exists():
            try:
                existing = pl.read_parquet(path)
                merged = pl.concat([existing, new_df], how="vertical")
            except Exception as exc:
                logger.warning("news partition read failed %s, rewrite: %s", path, exc)
                merged = new_df
        else:
            merged = new_df
        merged = merged.unique(subset=["id"], keep="first").sort("published_ts")
        _atomic_write_parquet(path, merged)
        saved += len(rows)
    _known_ids |= seen_in_batch
    return saved


def _partition_files_between(data_dir: Path, start: date, end: date) -> list[Path]:
    files: list[Path] = []
    current = start
    while current <= end:
        path = _partition_file(data_dir, current.isoformat())
        if path.exists():
            files.append(path)
        current += timedelta(days=1)
    return files


def _row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    def _loads(text: Any, fallback: list) -> list:
        try:
            return json.loads(text) if text else fallback
        except (TypeError, json.JSONDecodeError):
            return fallback

    return {
        "id": row["id"],
        "source": row["source"],
        "source_label": SOURCE_LABELS.get(row["source"], row["source"]),
        "title": row["title"],
        "content": row["content"],
        "url": row["url"] or None,
        "published_ts": int(row["published_ts"]),
        "published_at": row["published_at"],
        "sentiment": row["sentiment"],
        "important": bool(row["important"]),
        "tags": _loads(row.get("tags_json"), []),
        "stocks": _loads(row.get("stocks_json"), []),
        "analyzed": bool(row["analyzed"]),
    }


def query_items(
    data_dir: Path,
    *,
    start: date,
    end: date,
    source: str = "all",
    item_type: str = "all",
    keyword: str = "",
    concept: str = "",
    symbol: str = "",
    symbols: list[str] | None = None,
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    files = _partition_files_between(data_dir, start, end)
    base: pl.DataFrame | None = None
    if files:
        df = pl.read_parquet(files)
        if source != "all":
            df = df.filter(pl.col("source") == source)
        for column, value in (("title", keyword), ("content", keyword), ("tags_json", concept)):
            if value.strip():
                needle = re.escape(value.strip())
                df = df.filter(pl.col(column).str.contains(needle))
        if symbol.strip():
            df = df.filter(
                pl.col("stocks_json").str.contains(re.escape(f'"code":"{symbol.strip()}"'))
            )
        if symbols:
            pattern = "|".join(re.escape(s.strip()) for s in symbols[:500] if s.strip())
            if pattern:
                df = df.filter(pl.col("stocks_json").str.contains(pattern))
        base = df

    counts = {
        "positive": 0,
        "negative": 0,
    }
    if base is not None and base.height:
        counts["positive"] = int((base["sentiment"] == "positive").sum())
        counts["negative"] = int((base["sentiment"] == "negative").sum())
        typed = base
        if item_type == "positive":
            typed = base.filter(pl.col("sentiment") == "positive")
        elif item_type == "negative":
            typed = base.filter(pl.col("sentiment") == "negative")
        elif item_type == "important":
            typed = base.filter(pl.col("important"))
        elif item_type == "watchlist" and symbols:
            pattern = "|".join(re.escape(s.strip()) for s in symbols[:500] if s.strip())
            typed = base.filter(pl.col("stocks_json").str.contains(pattern))
    else:
        typed = pl.DataFrame(schema=_SCHEMA)

    total = typed.height
    page = max(1, page)
    page_size = max(1, min(100, page_size))
    rows = (
        typed.sort("published_ts", descending=True)
        .slice((page - 1) * page_size, page_size)
        .to_dicts()
    )
    return {
        "total": total,
        "positive": counts["positive"],
        "negative": counts["negative"],
        "page": page,
        "page_size": page_size,
        "items": [_row_to_dict(row) for row in rows],
    }


def apply_analysis(
    data_dir: Path,
    updates: dict[str, dict[str, Any]],
) -> int:
    """把 AI 标注结果合并进对应分区(按 id join, 原子替换)。"""
    if not updates:
        return 0
    upd = pl.DataFrame(
        [
            {
                "id": item_id,
                "sentiment": str(r.get("sentiment") or "none"),
                "important": bool(r.get("important")),
                "tags_json": json.dumps(
                    [str(t)[:20] for t in (r.get("tags") or [])[:8]],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                "stocks_json": json.dumps(
                    [
                        {"code": str(s.get("code", ""))[:10], "name": str(s.get("name", ""))[:20]}
                        for s in (r.get("stocks") or [])[:8]
                        if str(s.get("code", "")).strip()
                    ],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                "analyzed": True,
            }
            for item_id, r in updates.items()
        ],
        schema={
            "id": pl.String,
            "sentiment": pl.String,
            "important": pl.Boolean,
            "tags_json": pl.String,
            "stocks_json": pl.String,
            "analyzed": pl.Boolean,
        },
    )
    updated = 0
    for path in _partition_files_between(
        data_dir, _now_bj().date() - timedelta(days=8), _now_bj().date()
    ):
        try:
            df = pl.read_parquet(path)
        except Exception as exc:
            logger.warning("news partition read failed %s: %s", path, exc)
            continue
        update_ids = upd["id"].to_list()
        if not df["id"].is_in(update_ids).any():
            continue
        merged = (
            df.join(upd, on="id", how="left", suffix="_new")
            .with_columns(
                pl.coalesce(["sentiment_new", "sentiment"]).alias("sentiment"),
                pl.coalesce(["important_new", "important"]).alias("important"),
                pl.coalesce(["tags_json_new", "tags_json"]).alias("tags_json"),
                pl.coalesce(["stocks_json_new", "stocks_json"]).alias("stocks_json"),
                pl.col("analyzed") | pl.col("analyzed_new").fill_null(False),
            )
            .select(list(_SCHEMA.keys()))
        )
        _atomic_write_parquet(path, merged)
        updated += df["id"].is_in(update_ids).sum()
    return int(updated)


def cleanup_old_partitions(data_dir: Path, retention_days: int) -> int:
    removed = 0
    cutoff = (_now_bj().date() - timedelta(days=retention_days)).isoformat()
    root = _items_dir(data_dir)
    if not root.exists():
        return 0
    for child in root.iterdir():
        match = re.fullmatch(r"date=(\d{4}-\d{2}-\d{2})", child.name)
        if match and match.group(1) < cutoff:
            for sub in child.iterdir():
                sub.unlink(missing_ok=True)
            child.rmdir()
            removed += 1
    return removed


# ---------------------------------------------------------------------------
# 源抓取适配器 (公开接口, 单源失败不影响其他源)
# ---------------------------------------------------------------------------

_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    text = _TAG_RE.sub("", unescape(text or ""))
    return re.sub(r"\s+", " ", text).strip()


def _make_id(source: str, raw_id: Any, title: str, ts: int) -> str:
    if raw_id:
        return f"{source}-{raw_id}"
    digest = hashlib.sha256(f"{source}|{title}|{ts}".encode()).hexdigest()[:16]
    return f"{source}-{digest}"


def _cls_sign(params: dict[str, str]) -> str:
    """财联社 nodeapi 签名 = md5(sha1(sorted_query)) — 上游协议要求的固定算法, 非安全用途。"""
    from urllib.parse import urlencode

    qs = urlencode(dict(sorted(params.items())))
    sha1 = hashlib.sha1(qs.encode("utf-8")).hexdigest()
    return hashlib.md5(sha1.encode("utf-8")).hexdigest()


async def _fetch_cls(client: httpx.AsyncClient) -> list[NewsItem]:
    params = {
        "app": "CailianpressWeb",
        "category": "",
        "lastTime": "",
        "os": "web",
        "refresh_type": "1",
        "rn": "50",
        "subscribedColumnIds": "",
        "sv": "8.4.6",
    }
    params["sign"] = _cls_sign(params)
    resp = await client.get("https://www.cls.cn/v1/roll/get_roll_list", params=params)
    resp.raise_for_status()
    roll = ((resp.json().get("data") or {}).get("roll_data")) or []
    items: list[NewsItem] = []
    for raw in roll:
        content = _strip_html(str(raw.get("content") or ""))
        title = str(raw.get("title") or "").strip() or content[:40]
        ts = int(raw.get("ctime") or 0)
        if not content or ts <= 0:
            continue
        tags = [
            str(s.get("subject_name"))
            for s in (raw.get("subject") or [])
            if s.get("subject_name")
        ]
        stocks = [
            {"code": str(s.get("StockID")), "name": str(s.get("name") or "")}
            for s in (raw.get("stock_list") or [])
            if s.get("StockID")
        ]
        items.append(
            NewsItem(
                id=_make_id("cls", raw.get("id"), title, ts),
                source="cls",
                title=title,
                content=content,
                url=f"https://www.cls.cn/detail/{raw.get('id')}",
                published_ts=ts,
                tags=tags,
                stocks=stocks,
            )
        )
    return items


async def _fetch_sina(client: httpx.AsyncClient) -> list[NewsItem]:
    resp = await client.get(
        "https://zhibo.sina.com.cn/api/zhibo/feed",
        params={
            "page": "1",
            "page_size": "50",
            "zhibo_id": "152",
            "tag_id": "0",
            "dire": "f",
            "dpc": "1",
        },
    )
    resp.raise_for_status()
    feed = (((resp.json().get("result") or {}).get("data") or {}).get("feed")) or {}
    items: list[NewsItem] = []
    for raw in feed.get("list") or []:
        content = _strip_html(str(raw.get("rich_text") or ""))
        ts = _parse_bj_time(str(raw.get("create_time") or ""))
        if not content or ts is None:
            continue
        items.append(
            NewsItem(
                id=_make_id("sina", raw.get("id"), content[:40], ts),
                source="sina",
                title=content[:40],
                content=content,
                url=f"https://finance.sina.com.cn/7x24/?id={raw.get('id')}",
                published_ts=ts,
                tags=[],
                stocks=[],
            )
        )
    return items


async def _fetch_eastmoney(client: httpx.AsyncClient) -> list[NewsItem]:
    resp = await client.get(
        "https://np-listapi.eastmoney.com/comm/web/getFastNewsList",
        params={
            "client": "web",
            "biz": "web_724",
            "fastColumn": "102",
            "sortEnd": "",
            "pageSize": "50",
            "req_trace": str(int(_now_bj().timestamp() * 1000)),
        },
    )
    resp.raise_for_status()
    rows = ((resp.json().get("data") or {}).get("fastNewsList")) or []
    items: list[NewsItem] = []
    for raw in rows:
        title = str(raw.get("title") or "").strip()
        summary = _strip_html(str(raw.get("summary") or ""))
        ts = _parse_bj_time(str(raw.get("showTime") or ""))
        if not (title or summary) or ts is None:
            continue
        code = str(raw.get("code") or "")
        items.append(
            NewsItem(
                id=_make_id("eastmoney", code or None, title or summary[:40], ts),
                source="eastmoney",
                title=title or summary[:40],
                content=summary or title,
                url=f"https://kuaixun.eastmoney.com/{code}.html" if code else None,
                published_ts=ts,
                tags=[],
                stocks=[],
            )
        )
    return items


async def _fetch_em_news(client: httpx.AsyncClient) -> list[NewsItem]:
    """东财栏目新闻(FundTrack App 同款接口) — 要闻 350 + 财经 351 两栏目,
    带媒体名(mediaName)与 canonical url, 与 7x24 快讯(eastmoney)互补: 深度报道 vs 快讯。"""
    items: list[NewsItem] = []
    for column in ("350", "351"):
        resp = await client.get(
            "https://np-listapi.eastmoney.com/comm/web/getNewsByColumns",
            params={
                "client": "web",
                "biz": "web_news_col",
                "column": column,
                "fields": "code,showTime,title,mediaName,summary,url,uniqueUrl,Np_dst",
                "types": "1,20",
                "page_index": "1",
                "page_size": "25",
                "req_trace": str(int(_now_bj().timestamp() * 1000)),
            },
        )
        resp.raise_for_status()
        rows = ((resp.json().get("data") or {}).get("list")) or []
        for raw in rows:
            title = str(raw.get("title") or "").strip()
            summary = _strip_html(str(raw.get("summary") or ""))
            ts = _parse_bj_time(str(raw.get("showTime") or ""))
            if not title or ts is None:
                continue
            code = str(raw.get("code") or "")
            media = str(raw.get("mediaName") or "").strip()
            items.append(
                NewsItem(
                    id=_make_id("em_news", code or None, title, ts),
                    source="em_news",
                    title=title,
                    content=summary or title,
                    url=str(raw.get("uniqueUrl") or raw.get("url") or "") or None,
                    published_ts=ts,
                    tags=[media] if media else [],
                    stocks=[],
                )
            )
    return items


async def _fetch_ths(client: httpx.AsyncClient) -> list[NewsItem]:
    resp = await client.get(
        "https://news.10jqka.com.cn/tapp/news/push/stock/",
        params={"page": "1", "tag": "", "track": "website", "pagesize": "50"},
    )
    resp.raise_for_status()
    rows = ((resp.json().get("data") or {}).get("list")) or []
    items: list[NewsItem] = []
    for raw in rows:
        title = str(raw.get("title") or "").strip()
        digest = _strip_html(str(raw.get("digest") or ""))
        ts = int(raw.get("ctime") or 0)
        if not title or ts <= 0:
            continue
        items.append(
            NewsItem(
                id=_make_id("ths", raw.get("id"), title, ts),
                source="ths",
                title=title,
                content=digest or title,
                url=str(raw.get("url")) if raw.get("url") else None,
                published_ts=ts,
                tags=[str(raw.get("tag"))] if raw.get("tag") else [],
                stocks=[],
            )
        )
    return items


async def _fetch_jin10(client: httpx.AsyncClient) -> list[NewsItem]:
    resp = await client.get(
        "https://www.jin10.com/flash_newest.js",
        params={"t": str(int(_now_bj().timestamp() * 1000))},
    )
    resp.raise_for_status()
    match = re.search(r"\[.*\]", resp.text, re.S)
    if not match:
        return []
    rows = json.loads(match.group(0))
    items: list[NewsItem] = []
    for raw in rows:
        # 现行结构: {"id","time","type","important":0/1,"data":{"title","content"},...}
        # 兼容旧版顶层 content 字段
        body = raw.get("data") if isinstance(raw.get("data"), dict) else {}
        text = _strip_html(str(body.get("content") or raw.get("content") or ""))
        title = str(body.get("title") or "").strip() or text[:40]
        ts = _parse_bj_time(str(raw.get("time") or ""))
        if not text or ts is None:
            continue
        tags = [str(t) for t in (raw.get("tags") or []) if t]
        items.append(
            NewsItem(
                id=_make_id("jin10", raw.get("id"), title, ts),
                source="jin10",
                title=title,
                content=text,
                url=f"https://flash.jin10.com/detail/{raw.get('id')}",
                published_ts=ts,
                tags=tags,
                stocks=[],
                important_hint=bool(raw.get("important")),
            )
        )
    return items


async def _fetch_exchange(client: httpx.AsyncClient) -> list[NewsItem]:
    """沪深交易所公告 — 走东方财富公告聚合 API(两市直连接口已不稳定: SZSE 返回维护页 500,
    SSE queryCompanyBulletinNew 恒空), 字段含 codes(代码+简称)与公告类型分类。"""
    resp = await client.get(
        "https://np-anotice-stock.eastmoney.com/api/security/ann",
        params={
            "sr": "-1",
            "page_size": "50",
            "page_index": "1",
            "ann_type": "A",
            "client_source": "web",
            "f_node": "0",
            "s_node": "0",
        },
    )
    resp.raise_for_status()
    rows = ((resp.json().get("data") or {}).get("list")) or []
    items: list[NewsItem] = []
    for raw in rows:
        title = _strip_html(str(raw.get("title") or ""))
        ts = _parse_bj_time(str(raw.get("notice_date") or ""))
        if not title or ts is None:
            continue
        stocks = [
            {"code": str(code.get("stock_code") or ""), "name": str(code.get("short_name") or "")}
            for code in (raw.get("codes") or [])
            if code.get("stock_code")
        ][:5]
        columns = [
            str(column.get("column_name"))
            for column in (raw.get("columns") or [])
            if column.get("column_name")
        ]
        art_code = str(raw.get("art_code") or "")
        items.append(
            NewsItem(
                id=_make_id("exchange", art_code or None, title, ts),
                source="exchange",
                title=title,
                content=f"{stocks[0]['name'] if stocks else ''} {title}".strip(),
                url=f"https://data.eastmoney.com/notices/detail/{stocks[0]['code']}/{art_code}.html"
                if art_code and stocks
                else None,
                published_ts=ts,
                tags=columns[:3] or ["公告"],
                stocks=stocks,
            )
        )
    return items


_FETCHERS = {
    "cls": _fetch_cls,
    "eastmoney": _fetch_eastmoney,
    "em_news": _fetch_em_news,
    "exchange": _fetch_exchange,
    "sina": _fetch_sina,
    "jin10": _fetch_jin10,
    "ths": _fetch_ths,
}


# ---------------------------------------------------------------------------
# AI 标注
# ---------------------------------------------------------------------------

_AI_SYSTEM = (
    "你是A股资讯分析引擎。对输入的财经快讯逐条分析, 严格只输出 JSON 数组, 不要输出其他文字。"
    '每条格式: {"id": 原id, "sentiment": "positive|negative|neutral", "important": true或false, '
    '"tags": ["概念或行业标签", 最多6个], '
    '"stocks": [{"code": "6位A股代码", "name": "简称"}] 仅提取明确提到的个股, 最多5个, 没有则空数组}。'
    "positive=利好, negative=利空, neutral=中性。重要=对次日盘面或个股有明确重大影响。"
)

_ai_attempts: dict[str, int] = {}


def _parse_ai_json(raw: str) -> dict[str, dict[str, Any]]:
    text = raw.strip()
    match = re.search(r"\[.*\]", text, re.S)
    if not match:
        return {}
    results: dict[str, dict[str, Any]] = {}
    for entry in json.loads(match.group(0)):
        if not isinstance(entry, dict) or not entry.get("id"):
            continue
        sentiment = str(entry.get("sentiment") or "none")
        results[str(entry["id"])] = {
            "sentiment": sentiment if sentiment in SENTIMENTS else "none",
            "important": bool(entry.get("important")),
            "tags": [str(t) for t in (entry.get("tags") or []) if t],
            "stocks": [
                {"code": str(s.get("code", "")), "name": str(s.get("name", ""))}
                for s in (entry.get("stocks") or [])
                if isinstance(s, dict) and str(s.get("code", "")).strip()
            ],
        }
    return results


async def analyze_pending(data_dir: Path, *, batch_size: int = 10, max_items: int = 20) -> int:
    """对最近 7 天未标注条目做 AI 标注; AI 未配置时直接返回 0。"""
    if not ai_configured():
        return 0
    files = _partition_files_between(
        data_dir, _now_bj().date() - timedelta(days=7), _now_bj().date()
    )
    if not files:
        return 0
    df = pl.read_parquet(files).filter(~pl.col("analyzed"))
    if not df.height:
        return 0
    pending = (
        df.sort("published_ts", descending=True)
        .head(max_items)
        .to_dicts()
    )
    pending = [row for row in pending if _ai_attempts.get(row["id"], 0) < 3]
    analyzed = 0
    for offset in range(0, len(pending), batch_size):
        chunk = pending[offset : offset + batch_size]
        updates: dict[str, dict[str, Any]] = {}
        try:
            raw = await generate_ai_text(
                [
                    Message({"role": "system", "content": _AI_SYSTEM}),
                    Message(
                        {
                            "role": "user",
                            "content": json.dumps(
                                [
                                    {"id": row["id"], "title": row["title"], "content": row["content"][:600]}
                                    for row in chunk
                                ],
                                ensure_ascii=False,
                            ),
                        }
                    ),
                ],
                temperature=0.1,
                max_tokens=2000,
                timeout=120.0,
            )
            results = _parse_ai_json(raw)
        except Exception as exc:
            logger.warning("news AI analyze failed: %s", exc)
            results = {}
        for row in chunk:
            item_id = row["id"]
            _ai_attempts[item_id] = _ai_attempts.get(item_id, 0) + 1
            if item_id in results:
                updates[item_id] = results[item_id]
            elif _ai_attempts[item_id] >= 3:
                updates[item_id] = {"sentiment": "none", "important": False, "tags": [], "stocks": []}
        analyzed += apply_analysis(data_dir, updates)
    return analyzed


# ---------------------------------------------------------------------------
# 推送 (复用监控中心 webhook 渠道)
# ---------------------------------------------------------------------------


def _channel_configured(channel: str) -> bool:
    if channel == "feishu":
        return bool(preferences.get_feishu_webhook_url())
    if channel == "wecom":
        return bool(preferences.get_wecom_webhook_url())
    return False


def _push_once(settings: dict[str, Any], items: list[dict[str, Any]]) -> int:
    if not items:
        return 0
    lines = []
    for row in items[:5]:
        lines.append(
            f"[{SOURCE_LABELS.get(row['source'], row['source'])}] {row['published_at']}\n"
            f"{row['title']}\n{row['content'][:120]}"
        )
    title = f"资讯提醒: {len(items)}条重要"
    body = "\n\n".join(lines)
    sent = 0
    for channel in settings["push_channels"]:
        try:
            if channel == "feishu":
                url = preferences.get_feishu_webhook_url()
                ok = bool(url) and webhook_adapter.send_feishu(
                    url, title, body, preferences.get_feishu_webhook_secret()
                )
            elif channel == "wecom":
                url = preferences.get_wecom_webhook_url()
                ok = bool(url) and webhook_adapter.send_wecom(url, title, body)
            else:
                ok = False
        except Exception as exc:
            logger.warning("news push %s failed: %s", channel, exc)
            ok = False
        sent += int(ok)
    return sent


# ---------------------------------------------------------------------------
# 抓取循环与调度
# ---------------------------------------------------------------------------

_fetch_lock = asyncio.Lock()


async def run_fetch_cycle(data_dir: Path) -> dict[str, Any]:
    """抓取全部启用源 → 落盘 → AI 标注 → 推送重要条目。单飞串行, 防重叠。"""
    async with _fetch_lock:
        settings = load_settings(data_dir)
        fetched_at = _now_bj().strftime("%Y-%m-%d %H:%M:%S")
        source_status: dict[str, dict[str, Any]] = {}
        new_items: list[NewsItem] = []

        async def _run_one(name: str, fetcher) -> None:
            if not settings["sources"].get(name, True):
                return
            try:
                rows = await fetcher()
                new_items.extend(rows)
                source_status[name] = {"ok": True, "count": len(rows), "error": None, "at": fetched_at}
            except Exception as exc:
                logger.warning("news source %s fetch failed: %s", name, exc)
                source_status[name] = {"ok": False, "count": 0, "error": str(exc)[:200], "at": fetched_at}

        async with httpx.AsyncClient(
            headers=_BROWSER_HEADERS, timeout=httpx.Timeout(12.0), follow_redirects=True
        ) as client:
            await asyncio.gather(
                *(_run_one(name, _bind_fetcher(fetcher, client)) for name, fetcher in _FETCHERS.items())
            )

        saved = save_items(data_dir, new_items)
        analyzed = await analyze_pending(data_dir)

        state = load_state(data_dir)
        pushed_ids = list(state.get("pushed_ids", []))
        push_sent = 0
        if settings["push_enabled"] and saved:
            fresh = [
                row
                for row in _recent_rows(data_dir, hours=24)
                if row["id"] not in pushed_ids and row["important"] and row["analyzed"]
                and row["source"] in {i.source for i in new_items}
            ]
            if fresh:
                push_sent = await asyncio.to_thread(_push_once, settings, fresh)
                pushed_ids.extend(row["id"] for row in fresh)

        _update_state(
            data_dir,
            last_fetch_at=fetched_at,
            sources=source_status,
            pushed_ids=pushed_ids,
            last_push_at=fetched_at if push_sent else state.get("last_push_at"),
        )
        cleanup_old_partitions(data_dir, settings["retention_days"])
        return {
            "fetched_at": fetched_at,
            "new_items": saved,
            "analyzed": analyzed,
            "push_sent": push_sent,
            "sources": source_status,
        }


def _bind_fetcher(fetcher, client: httpx.AsyncClient):
    async def _call() -> list[NewsItem]:
        return await fetcher(client)

    return _call


def _recent_rows(data_dir: Path, hours: int) -> list[dict[str, Any]]:
    since = int(_now_bj().timestamp()) - hours * 3600
    files = _partition_files_between(data_dir, _now_bj().date() - timedelta(days=2), _now_bj().date())
    if not files:
        return []
    df = pl.read_parquet(files).filter(pl.col("published_ts") >= since)
    return df.sort("published_ts", descending=True).to_dicts()


_scheduler: AsyncIOScheduler | None = None
_data_dir: Path | None = None


async def _scheduled_cycle() -> None:
    if _data_dir is None:
        return
    try:
        result = await run_fetch_cycle(_data_dir)
        logger.info("news scheduled fetch: +%s new / %s analyzed", result["new_items"], result["analyzed"])
    except Exception as exc:
        logger.warning("news scheduled cycle failed: %s", exc)


def _start_scheduler() -> None:
    global _scheduler
    if _scheduler is not None or _data_dir is None:
        return
    scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")
    settings = load_settings(_data_dir)
    scheduler.add_job(
        _scheduled_cycle,
        trigger=IntervalTrigger(minutes=settings["interval_minutes"], timezone="Asia/Shanghai"),
        id="news_fetch",
        next_run_time=_now_bj().replace(tzinfo=None) + timedelta(seconds=15),
        misfire_grace_time=300,
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    scheduler.start()
    _scheduler = scheduler
    logger.info("news scheduler started, interval=%smin", settings["interval_minutes"])


def _reschedule(interval_minutes: int) -> None:
    if _scheduler is None:
        return
    _scheduler.modify_job(
        "news_fetch",
        trigger=IntervalTrigger(minutes=int(interval_minutes), timezone="Asia/Shanghai"),
    )


def get_status(data_dir: Path) -> dict[str, Any]:
    settings = load_settings(data_dir)
    state = load_state(data_dir)
    channels = [c for c in settings["push_channels"] if _channel_configured(c)]
    next_run_at = None
    if _scheduler is not None:
        job = _scheduler.get_job("news_fetch")
        if job is not None and job.next_run_time is not None:
            next_run_at = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")
    return {
        "last_fetch_at": state.get("last_fetch_at"),
        "next_run_at": next_run_at,
        "interval_minutes": settings["interval_minutes"],
        "sources_enabled": {k: v for k, v in settings["sources"].items() if v},
        "sources_status": state.get("sources", {}),
        "focus_concepts": settings["focus_concepts"],
        "push": {
            "enabled": settings["push_enabled"],
            "channels": channels,
            "configured": bool(channels),
        },
        "ai_configured": ai_configured(),
    }


# ---------------------------------------------------------------------------
# HTTP 路由
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/custom/news", tags=["custom-news"])


def _require_data_dir() -> Path:
    if _data_dir is None:
        raise HTTPException(status_code=503, detail="news extension not started")
    return _data_dir


@router.get("/items")
def list_news_items(
    range: str = Query("today", alias="range"),
    start_date: str = Query("", alias="start_date"),
    end_date: str = Query("", alias="end_date"),
    type: str = Query("all", alias="type"),
    source: str = Query("all", alias="source"),
    keyword: str = Query("", alias="keyword"),
    concept: str = Query("", alias="concept"),
    symbol: str = Query("", alias="symbol"),
    symbols: str = Query("", alias="symbols"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    data_dir = _require_data_dir()
    today = _now_bj().date()
    range_days = {"today": 0, "3d": 2, "1w": 6, "1m": 29}
    if range == "custom" and start_date and end_date:
        try:
            start = date.fromisoformat(start_date)
            end = date.fromisoformat(end_date)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="invalid custom date") from exc
    elif range in range_days:
        start = today - timedelta(days=range_days[range])
        end = today
    else:
        raise HTTPException(status_code=422, detail="invalid range")
    if start > end:
        start, end = end, start
    watchlist_symbols = [s for s in symbols.split(",") if s.strip()] if symbols else None
    return query_items(
        data_dir,
        start=start,
        end=end,
        source=source if source in SOURCE_LABELS else "all",
        item_type=type if type in {"all", "positive", "negative", "important", "watchlist"} else "all",
        keyword=keyword,
        concept=concept,
        symbol=symbol,
        symbols=watchlist_symbols,
        page=page,
        page_size=page_size,
    )


@router.post("/fetch")
async def fetch_now() -> dict[str, Any]:
    return await run_fetch_cycle(_require_data_dir())


@router.get("/settings")
def get_news_settings() -> dict[str, Any]:
    return load_settings(_require_data_dir())


@router.put("/settings")
def update_news_settings(payload: dict[str, Any]) -> dict[str, Any]:
    current = load_settings(_require_data_dir())
    current.update({k: v for k, v in payload.items() if k in _SETTINGS_DEFAULTS})
    return save_settings(_data_dir, current)


@router.get("/status")
def get_news_status() -> dict[str, Any]:
    return get_status(_require_data_dir())


# ---------------------------------------------------------------------------
# 扩展注册契约
# ---------------------------------------------------------------------------


def setup(registrar: BackendExtensionRegistrar) -> None:
    registrar.include_router(router)


def startup(context: ExtensionContext) -> None:
    global _data_dir, _known_ids
    _data_dir = Path(context.data_dir)
    try:
        _known_ids = _load_known_ids(_data_dir)
    except Exception as exc:
        logger.warning("news known ids reload failed: %s", exc)
    _start_scheduler()


def shutdown(context: ExtensionContext) -> None:
    del context
    global _scheduler
    if _scheduler is not None:
        try:
            _scheduler.shutdown(wait=False)
        except Exception as exc:
            logger.warning("news scheduler shutdown failed: %s", exc)
        _scheduler = None
        logger.info("news scheduler stopped")
