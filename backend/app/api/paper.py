"""模拟盘 API — 台账总览 / 手动结算 / 重置。

功能开关由 data/paper_trading/config.json 存在与否决定 (不存在 = disabled)。
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request

from app.services import paper_trading

router = APIRouter(prefix="/api/paper", tags=["paper-trading"])


def _data_dir(request: Request) -> Path:
    return request.app.state.repo.store.data_dir


@router.get("/overview")
def paper_overview(request: Request):
    return paper_trading.overview(_data_dir(request))


@router.post("/settle")
def paper_settle(request: Request, day: str | None = None):
    target = date.fromisoformat(day) if day else None
    if target is not None and target > date.today():
        raise HTTPException(status_code=400, detail="结算日不能晚于今天")
    result = paper_trading.settle(_data_dir(request), request.app.state.repo, day=target)
    if result.get("status") == "error":
        raise HTTPException(status_code=500, detail=result.get("error"))
    return result


@router.post("/reset")
def paper_reset(request: Request):
    return paper_trading.reset(_data_dir(request))
