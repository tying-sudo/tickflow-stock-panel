"""基金 API：搜索 / 持仓查询 / 一键导入为自选分组。"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from app.services import fund_service, watchlist_groups

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/fund", tags=["fund"])


class FundImportRequest(BaseModel):
    code: str


def _attach_names(symbols: list[str], request: Request) -> list[dict]:
    """用 instruments 名称表回填成分股名称，解析不到时 name 为 None。"""
    name_map: dict[str, str] = {}
    try:
        name_map = request.app.state.repo.get_name_map(symbols)
    except Exception as e:  # noqa: BLE001
        logger.debug("fund name resolve failed: %s", e)
    return [{"symbol": s, "name": name_map.get(s)} for s in symbols]


@router.get("/search")
def fund_search(
    q: str = Query(..., min_length=1, max_length=30, description="基金代码/名称/拼音首字母"),
    limit: int = Query(10, ge=1, le=20),
):
    try:
        results = fund_service.search_funds(q, limit)
    except Exception as e:  # noqa: BLE001
        logger.warning("fund search failed for q=%s: %s", q, e)
        raise HTTPException(502, "基金搜索源不可用，请稍后重试") from e
    return {"results": results}


@router.get("/{code}/holdings")
def fund_holdings(code: str, request: Request):
    """查询基金前十大重仓股（不落库，预览用）。"""
    try:
        profile = fund_service.get_fund_profile(code)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    except Exception as e:  # noqa: BLE001
        logger.warning("fund holdings failed for %s: %s", code, e)
        raise HTTPException(502, "基金持仓源不可用，请稍后重试") from e
    return {
        "code": profile["code"],
        "name": profile["name"],
        "members": _attach_names(profile["holdings"], request),
    }


@router.post("/import")
def fund_import(req: FundImportRequest, request: Request):
    """基金 → 自选分组：基金名作为分组名，前十大重仓股作为组内成员（同名组覆盖）。"""
    try:
        profile = fund_service.get_fund_profile(req.code)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    except Exception as e:  # noqa: BLE001
        logger.warning("fund import failed for %s: %s", req.code, e)
        raise HTTPException(502, "基金持仓源不可用，请稍后重试") from e

    if not profile["holdings"]:
        raise HTTPException(400, "未获取到该基金的股票持仓，无法创建分组")

    groups = watchlist_groups.upsert_group(profile["name"], profile["holdings"])
    return {
        "group": {"name": profile["name"], "count": len(profile["holdings"])},
        "members": _attach_names(profile["holdings"], request),
        "groups": groups,
    }
