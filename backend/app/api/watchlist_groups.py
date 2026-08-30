"""自选分组 API（分组成员视图 / 手动建组）。"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services import watchlist_groups

router = APIRouter(prefix="/api/watchlist-groups", tags=["watchlist-groups"])


class GroupUpsertRequest(BaseModel):
    name: str
    symbols: list[str] = []


@router.get("")
def list_groups():
    return {"groups": watchlist_groups.list_groups()}


@router.post("")
def upsert_group(req: GroupUpsertRequest):
    try:
        groups = watchlist_groups.upsert_group(req.name, req.symbols)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    return {"groups": groups}


@router.delete("/{name}")
def delete_group(name: str):
    watchlist_groups.delete_group(name)
    return {"groups": watchlist_groups.list_groups()}


@router.delete("/{name}/members/{symbol}")
def remove_group_member(name: str, symbol: str):
    watchlist_groups.remove_member(name, symbol)
    return {"groups": watchlist_groups.list_groups()}
