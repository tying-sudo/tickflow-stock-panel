"""分笔成交数据包 (g4tic) 后台下载端点。

与 api/kline.py 的 /transactions 配套:
  POST /api/kline/transactions/download         触发后台下载指定交易日分笔包
  GET  /api/kline/transactions/download-status  查询下载进度状态
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query, Request

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/kline", tags=["kline"])


def _daily_row_exists(repo, day: str) -> bool:
    iso = day[:4] + "-" + day[4:6] + "-" + day[6:]
    try:
        row = repo.execute_one(
            "SELECT 1 FROM kline_daily WHERE date = ? LIMIT 1", [iso],
        )
        return bool(row)
    except Exception:  # noqa: BLE001
        return False


def _gateway_ok() -> tuple[bool, str]:
    from app.plugins.tdx_gateway import provider as tdx_provider

    return tdx_provider.availability()


@router.post("/transactions/download")
def download_tick_pack(request: Request, body: dict):
    """后台下载指定交易日的通达信 g4tic 历史分笔包 (~100MB, 断点续传)。

    下载完成后 GET /api/kline/transactions?date=... 即可查询该日真实历史分笔。
    """
    from app.services import tick_transactions

    ok, reason = _gateway_ok()
    if not ok:
        raise HTTPException(status_code=403, detail=f"tick 数据需要 TDX 网关: {reason}")
    repo = request.app.state.repo
    raw_date = str(body.get("date") or "").strip()
    day = raw_date.replace("-", "")
    if not (day.isdigit() and len(day) == 8):
        raise HTTPException(status_code=422, detail="date must be YYYY-MM-DD")
    if not _daily_row_exists(repo, day):
        not_trading = {"code": "not_trading_day",
                       "message": raw_date + " 不是本地日K中的交易日"}
        raise HTTPException(status_code=404, detail=not_trading)
    try:
        result = tick_transactions.g4_download_start(day)
    except tick_transactions.TickUnavailable as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    return {"date": day, **result}


@router.get("/transactions/download-status")
def download_tick_status(request: Request, date: str = Query(..., description="YYYY-MM-DD")):
    """g4tic 分笔包后台下载状态。"""
    from app.services import tick_transactions

    ok, reason = _gateway_ok()
    if not ok:
        raise HTTPException(status_code=403, detail=f"tick 数据需要 TDX 网关: {reason}")
    day = date.replace("-", "")
    if not (day.isdigit() and len(day) == 8):
        raise HTTPException(status_code=422, detail="date must be YYYY-MM-DD")
    try:
        result = tick_transactions.g4_download_status(day)
    except tick_transactions.TickUnavailable as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    return {"date": day, **result}
