"""个股实时五档盘口 (TDX Quant snapshot book — 免费实时, 与 realtime 同源)。

GET /api/kline/depth5?symbol=000001.SZ
返回 卖1-卖5 / 买1-买5 的价格与量(手), 按价格规范化排序:
  asks 升序 (卖1最低), bids 降序 (买1最高) — 与源内顺序无关。
非交易时段返回交易所残留挂单; 停牌/无行情 404。
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query, Request

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/kline", tags=["kline"])


@router.get("/depth5")
def get_depth5(request: Request, symbol: str = Query(..., description="标的代码")):
    from app.data_providers import custom as custom_sources
    from app.plugins.tdx_gateway import provider as tdx_provider

    ok, reason = tdx_provider.availability()
    if not ok:
        raise HTTPException(status_code=503, detail=f"五档盘口需要 TDX 网关: {reason}")
    try:
        provider = custom_sources.get_provider("tdx_gateway")
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"TDX 数据源未安装: {e}") from e
    if provider is None:
        raise HTTPException(status_code=503, detail="TDX 数据源未安装")

    try:
        depth = provider.get_depth5([symbol])
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"五档获取失败: {e}") from e

    row = depth.get(symbol)
    if not row:
        raise HTTPException(status_code=404, detail="该标的暂无五档快照 (可能停牌/无行情)")

    def levels(prices, volumes, *, reverse: bool) -> list[dict]:
        pairs = []
        for pr, v in zip(list(prices or []), list(volumes or [])):
            try:
                pr_f, v_f = float(pr or 0), float(v or 0)
            except (TypeError, ValueError):
                continue
            if pr_f > 0:
                pairs.append((pr_f, v_f))
        pairs.sort(key=lambda x: x[0], reverse=reverse)
        # 档位数自适应: TdxW Quant 当前 5 档, 未来 L2 十档时自动透传 10 档。
        return [{"price": pr, "volume": v} for pr, v in pairs[:10]]

    return {
        "symbol": symbol,
        # 卖1..卖5 (升序, 卖1=最低卖价)
        "asks": levels(row.get("ask_prices"), row.get("ask_volumes"), reverse=False),
        # 买1..买5 (降序, 买1=最高买价)
        "bids": levels(row.get("bid_prices"), row.get("bid_volumes"), reverse=True),
        "timestamp": row.get("timestamp"),
    }
