"""最新资讯二开扩展测试 — 存储幂等/过滤查询/失败隔离/AI 标注/路由契约。"""
from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import polars as pl
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.custom import news_feed
from app.extensions.contracts import BACKEND_EXTENSION_API_VERSION, ExtensionContext
from app.extensions.loader import stop_backend_extensions
from app.extensions.registry import BackendExtensionRegistry

SH = ZoneInfo("Asia/Shanghai")


@pytest.fixture(autouse=True)
def _reset_extension_globals():
    old = (news_feed._data_dir, news_feed._known_ids, news_feed._scheduler)
    news_feed._data_dir = None
    news_feed._known_ids = set()
    news_feed._scheduler = None
    news_feed._ai_attempts.clear()
    yield
    news_feed._data_dir, news_feed._known_ids, news_feed._scheduler = old


def _mk_item(source: str = "cls", title: str = "测试快讯", hours_ago: int = 1, **extra) -> news_feed.NewsItem:
    from datetime import datetime

    ts = int((datetime.now(tz=SH) - timedelta(hours=hours_ago)).timestamp())
    return news_feed.NewsItem(
        id=extra.get("id", f"{source}-{title}-{hours_ago}"),
        source=source,
        title=title,
        content=extra.get("content", f"{title}的内容"),
        url=extra.get("url"),
        published_ts=ts,
        tags=extra.get("tags", []),
        stocks=extra.get("stocks", []),
        important_hint=extra.get("important_hint", False),
    )


def _query(data_dir: Path, **overrides):
    from datetime import date as date_cls

    params = {
        "start": date_cls.today() - timedelta(days=1),
        "end": date_cls.today(),
        "source": "all",
        "item_type": "all",
    }
    params.update(overrides)
    return news_feed.query_items(data_dir, **params)


# ---------------------------------------------------------------------------
# 设置
# ---------------------------------------------------------------------------


def test_settings_defaults_and_clamp(tmp_path: Path) -> None:
    settings = news_feed.load_settings(tmp_path)
    assert settings["interval_minutes"] == 30
    assert set(settings["sources"]) == set(news_feed.SOURCE_LABELS)

    saved = news_feed.save_settings(
        tmp_path,
        {"interval_minutes": 1, "push_channels": ["feishu", "smtp", "wecom"], "focus_concepts": ["算力"]},
    )
    assert saved["interval_minutes"] == 10  # 下限钳制
    assert set(saved["push_channels"]) == {"feishu", "wecom"}
    assert news_feed.load_settings(tmp_path) == saved  # 持久化往返


# ---------------------------------------------------------------------------
# 存储
# ---------------------------------------------------------------------------


def test_save_items_is_idempotent_and_query_roundtrips(tmp_path: Path) -> None:
    item = _mk_item(tags=["算力"], stocks=[{"code": "300726", "name": "机器人"}])
    assert news_feed.save_items(tmp_path, [item]) == 1
    assert news_feed.save_items(tmp_path, [item]) == 0  # 幂等

    result = _query(tmp_path)
    assert result["total"] == 1
    row = result["items"][0]
    assert row["title"] == "测试快讯"
    assert row["source_label"] == "财联社"
    assert row["tags"] == ["算力"]
    assert row["stocks"] == [{"code": "300726", "name": "机器人"}]
    assert row["sentiment"] == "none"
    assert row["analyzed"] is False


def test_query_filters(tmp_path: Path) -> None:
    items = [
        _mk_item(source="cls", title="利好消息", tags=["算力"]),
        _mk_item(source="sina", title="普通消息"),
        _mk_item(source="jin10", title="重要数据", important_hint=True),
        _mk_item(source="ths", title="个股动态", stocks=[{"code": "600519", "name": "贵州茅台"}]),
    ]
    news_feed.save_items(tmp_path, items)
    # 模拟 AI 标注第一条(仅情绪, 不改重要标记)
    news_feed.apply_analysis(
        tmp_path,
        {items[0].id: {"sentiment": "positive", "important": False, "tags": ["算力"], "stocks": []}},
    )

    assert _query(tmp_path, source="sina")["total"] == 1
    assert _query(tmp_path, item_type="positive")["total"] == 1
    assert _query(tmp_path, item_type="important")["total"] == 1
    assert _query(tmp_path, item_type="watchlist", symbols=["600519"])["total"] == 1
    assert _query(tmp_path, keyword="利好")["total"] == 1
    assert _query(tmp_path, concept="算力")["total"] == 1
    assert _query(tmp_path, symbol="600519")["total"] == 1
    assert _query(tmp_path, symbol="000001")["total"] == 0

    counts = _query(tmp_path)
    assert (counts["positive"], counts["negative"]) == (1, 0)

    page = _query(tmp_path, item_type="all")
    assert page["page_size"] == 20


def test_apply_analysis_updates_partition_rows(tmp_path: Path) -> None:
    items = [_mk_item(id="cls-1", title="一"), _mk_item(id="cls-2", title="二")]
    news_feed.save_items(tmp_path, items)
    updated = news_feed.apply_analysis(
        tmp_path,
        {"cls-1": {"sentiment": "negative", "important": False, "tags": ["锂矿"], "stocks": [{"code": "002460", "name": "赣锋锂业"}]}},
    )
    assert updated == 1
    rows = {row["id"]: row for row in _query(tmp_path)["items"]}
    assert rows["cls-1"]["sentiment"] == "negative"
    assert rows["cls-1"]["tags"] == ["锂矿"]
    assert rows["cls-1"]["analyzed"] is True
    assert rows["cls-2"]["sentiment"] == "none"  # 未标注条目不受影响


def test_cleanup_old_partitions(tmp_path: Path) -> None:
    items = [_mk_item(id="old-1", hours_ago=24 * 60), _mk_item(id="new-1", hours_ago=1)]
    news_feed.save_items(tmp_path, items)
    removed = news_feed.cleanup_old_partitions(tmp_path, retention_days=30)
    assert removed >= 1
    assert _query(tmp_path, source="cls")["total"] == 1


# ---------------------------------------------------------------------------
# AI 标注
# ---------------------------------------------------------------------------


def test_parse_ai_json_tolerates_fences_and_bad_values() -> None:
    raw = '```json\n[{"id": "cls-1", "sentiment": "利空", "important": 1, "tags": ["算力", ""], "stocks": [{"code": "300726", "name": "机器人"}, {"code": ""}]}]\n```'
    results = news_feed._parse_ai_json(raw)
    assert results["cls-1"]["sentiment"] == "none"  # 非法值回退
    assert results["cls-1"]["important"] is True
    assert results["cls-1"]["tags"] == ["算力"]
    assert results["cls-1"]["stocks"] == [{"code": "300726", "name": "机器人"}]


def test_analyze_pending_applies_updates_and_caps_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(news_feed, "ai_configured", lambda: True)
    monkeypatch.setattr(news_feed, "generate_ai_text", _failing_ai)
    items = [_mk_item(id="cls-1", title="反复失败")]
    news_feed.save_items(tmp_path, items)

    for _ in range(3):
        assert await_analyze(tmp_path) in {0, 1}
    rows = _query(tmp_path)["items"]
    assert rows[0]["analyzed"] is True  # 3 次失败后置为已分析, 不再无限重试
    assert rows[0]["sentiment"] == "none"


async def _failing_ai(messages, **kwargs) -> str:
    raise RuntimeError("ai down")


def await_analyze(data_dir: Path) -> int:
    import asyncio

    return asyncio.run(news_feed.analyze_pending(data_dir))


def test_analyze_pending_skips_when_ai_not_configured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(news_feed, "ai_configured", lambda: False)
    news_feed.save_items(tmp_path, [_mk_item(id="cls-1")])
    assert await_analyze(tmp_path) == 0
    assert _query(tmp_path)["items"][0]["analyzed"] is False


# ---------------------------------------------------------------------------
# 抓取循环: 源失败隔离
# ---------------------------------------------------------------------------


async def _ok_fetcher(client: httpx.AsyncClient) -> list[news_feed.NewsItem]:
    return [_mk_item(source="cls", id="cls-ok", title="成功源")]


async def _bad_fetcher(client: httpx.AsyncClient) -> list[news_feed.NewsItem]:
    raise RuntimeError("源挂了")


def test_fetch_cycle_isolates_source_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(news_feed, "_FETCHERS", {"cls": _ok_fetcher, "sina": _bad_fetcher})
    monkeypatch.setattr(news_feed, "ai_configured", lambda: False)

    import asyncio

    result = asyncio.run(news_feed.run_fetch_cycle(tmp_path))

    assert result["new_items"] == 1
    assert result["sources"]["cls"]["ok"] is True
    assert result["sources"]["sina"]["ok"] is False
    assert "源挂了" in result["sources"]["sina"]["error"]
    state = news_feed.load_state(tmp_path)
    assert state["last_fetch_at"] is not None
    assert _query(tmp_path)["total"] == 1


# ---------------------------------------------------------------------------
# 源适配器解析
# ---------------------------------------------------------------------------


def _mock_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_fetch_cls_parses_roll_data() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "v1/roll/get_roll_list" in str(request.url)
        assert "sign" in str(request.url)
        return httpx.Response(
            200,
            json={
                "data": {
                    "roll_data": [
                        {
                            "id": 1,
                            "title": "",
                            "content": "<p>液冷订单交付 <b>超预期</b></p>",
                            "ctime": 1757157120,
                            "subject": [{"subject_name": "液冷"}],
                            "stock_list": [{"StockID": "300024", "name": "机器人"}],
                        }
                    ]
                }
            },
        )

    items = await news_feed._fetch_cls(_mock_client(handler))
    assert len(items) == 1
    assert items[0].title.startswith("液冷订单交付")
    assert items[0].tags == ["液冷"]
    assert items[0].stocks == [{"code": "300024", "name": "机器人"}]


async def test_fetch_jin10_parses_flash_js() -> None:
    # 现行结构: data 嵌套 title/content
    payload = json.dumps(
        [
            {
                "id": "20260907103446075800",
                "time": "2026-09-07 10:34:46",
                "type": 0,
                "important": 1,
                "data": {"title": "美联储利率决议", "content": "美联储公布利率决议"},
            },
            {
                "id": "20260907103446075801",
                "time": "2026-09-07 10:30:00",
                "type": 0,
                "content": "旧版顶层字段",
            },
        ],
        ensure_ascii=False,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=f"var newest = {payload};")

    items = await news_feed._fetch_jin10(_mock_client(handler))
    assert len(items) == 2
    assert items[0].title == "美联储利率决议"
    assert items[0].content == "美联储公布利率决议"
    assert items[0].important_hint is True
    assert items[1].content == "旧版顶层字段"  # 旧结构兼容


async def test_fetch_em_news_parses_columns() -> None:
    """东财栏目新闻(FundTrack App 同款接口): 要闻 350 + 财经 351 双栏目聚合。"""

    def handler(request: httpx.Request) -> httpx.Response:
        column = request.url.params.get("column")
        rows = [
            {
                "code": "202609073866536236",
                "showTime": "2026-09-07 12:42:06",
                "title": "增材制造正在打印全行业",
                "mediaName": "上观新闻",
                "summary": "<p>深度报道正文</p>",
                "url": "http://finance.eastmoney.com/news/1350,202609073866536236.html",
                "uniqueUrl": "http://finance.eastmoney.com/a/202609073866536236.html",
                "Np_dst": "CMS",
            }
        ]
        return httpx.Response(200, json={"data": {"list": rows if column == "350" else []}})

    items = await news_feed._fetch_em_news(_mock_client(handler))
    assert len(items) == 1
    assert items[0].source == "em_news"
    assert items[0].id == "em_news-202609073866536236"
    assert items[0].tags == ["上观新闻"]  # 媒体名作为标签
    assert items[0].content == "深度报道正文"
    assert items[0].url == "http://finance.eastmoney.com/a/202609073866536236.html"


async def test_fetch_exchange_uses_eastmoney_aggregate() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "np-anotice-stock.eastmoney.com" in str(request.url)
        return httpx.Response(
            200,
            json={
                "data": {
                    "list": [
                        {
                            "title": "中工国际:投资者关系活动记录表",
                            "notice_date": "2026-09-07 00:00:00",
                            "art_code": "AN202609071829087233",
                            "codes": [{"stock_code": "002051", "short_name": "中工国际"}],
                            "columns": [{"column_code": "050003", "column_name": "调研活动"}],
                        }
                    ]
                }
            },
        )

    items = await news_feed._fetch_exchange(_mock_client(handler))
    assert len(items) == 1
    assert items[0].id == "exchange-AN202609071829087233"
    assert items[0].stocks == [{"code": "002051", "name": "中工国际"}]
    assert items[0].tags == ["调研活动"]
    assert "002051" in (items[0].url or "")


# ---------------------------------------------------------------------------
# 路由契约
# ---------------------------------------------------------------------------


def _app_with_client(data_dir: Path) -> TestClient:
    news_feed._data_dir = data_dir
    app = FastAPI()
    app.include_router(news_feed.router)
    return TestClient(app)


def test_router_items_settings_status(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(news_feed, "ai_configured", lambda: False)
    news_feed.save_items(tmp_path, [_mk_item(id="cls-1", title="路由测试")])
    client = _app_with_client(tmp_path)

    resp = client.get("/api/custom/news/items", params={"range": "today"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["title"] == "路由测试"

    resp = client.get("/api/custom/news/items", params={"range": "custom", "start_date": "bad", "end_date": "2026-09-01"})
    assert resp.status_code == 422

    resp = client.put("/api/custom/news/settings", json={"interval_minutes": 999, "push_enabled": True})
    assert resp.status_code == 200
    assert resp.json()["interval_minutes"] == 360
    assert resp.json()["push_enabled"] is True

    status = client.get("/api/custom/news/status").json()
    assert status["interval_minutes"] == 360
    assert status["push"]["enabled"] is True
    assert status["ai_configured"] is False


def test_router_fetch_uses_patched_sources(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(news_feed, "_FETCHERS", {"cls": _ok_fetcher})
    monkeypatch.setattr(news_feed, "ai_configured", lambda: False)
    client = _app_with_client(tmp_path)

    resp = client.post("/api/custom/news/fetch")
    assert resp.status_code == 200
    assert resp.json()["new_items"] == 1


def test_router_returns_503_before_startup(tmp_path: Path) -> None:
    client = _app_with_client(tmp_path)
    news_feed._data_dir = None
    resp = client.get("/api/custom/news/items")
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# 停机钩子
# ---------------------------------------------------------------------------


def test_stop_backend_extensions_calls_shutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    import types

    from app.extensions.registry import BackendExtensionRegistrar

    called: list[str] = []
    module = types.ModuleType("app.custom.dummy")
    module.EXTENSION_ID = "news.feed"
    module.EXTENSION_API_VERSION = BACKEND_EXTENSION_API_VERSION
    module.shutdown = lambda context: called.append(context.data_dir.name)
    monkeypatch.setattr(
        "app.extensions.loader._custom_module_names", lambda: ["app.custom.dummy"]
    )
    monkeypatch.setattr(
        "app.extensions.loader.importlib.import_module", lambda name: module
    )

    registry = BackendExtensionRegistry()
    stage = BackendExtensionRegistrar("news.feed", api_version=BACKEND_EXTENSION_API_VERSION)
    registry.register(stage)
    registry.freeze()

    context = ExtensionContext(
        api_version=1, data_dir=Path("/tmp/news"), repository=None
    )
    stop_backend_extensions(context, registry)
    assert called == ["news"]

    # 未注册的扩展不得触发 shutdown
    other = BackendExtensionRegistry()
    stage_other = BackendExtensionRegistrar("other.ext", api_version=BACKEND_EXTENSION_API_VERSION)
    other.register(stage_other)
    other.freeze()
    called.clear()
    stop_backend_extensions(context, other)
    assert called == []


def test_partition_schema_roundtrip(tmp_path: Path) -> None:
    """分区文件可被 polars 直接读取, 列与声明 schema 一致(兼容后续版本读取)。"""
    news_feed.save_items(tmp_path, [_mk_item(id="cls-x")])
    path = next((tmp_path / "news" / "items").glob("date=*/part.parquet"))
    df = pl.read_parquet(path)
    assert set(news_feed._SCHEMA.keys()) == set(df.columns)
