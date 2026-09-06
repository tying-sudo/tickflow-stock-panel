"""g4tic 分笔包下载端点已移除 (2026-09-05, tdx_gateway/VM102 源下线)。

历史分笔由 easy-tdx /transaction/history (≥30 天) + tick_archive 本地归档承接;
保留空路由避免历史 include 报错, 两个旧路径直接 410。
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/kline", tags=["kline"])

_GONE_DETAIL = "g4tic 下载通道已随 TDX LAN Gateway 源下线; 历史分笔请直接查询 /api/kline/transactions?date=..."


@router.post("/transactions/download")
def download_tick_pack_removed():
    raise HTTPException(status_code=410, detail=_GONE_DETAIL)


@router.get("/transactions/download-status")
def download_tick_status_removed():
    raise HTTPException(status_code=410, detail=_GONE_DETAIL)
