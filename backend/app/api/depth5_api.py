"""个股实时五档盘口 (easy-tdx 标准协议 quotes — 免费实时, 与 realtime 同源)。

GET /api/kline/depth5?symbol=000001.SZ
返回 卖1-卖5 / 买1-买5 的价格与量(手), 按价格规范化排序:
  asks 升序 (卖1最低), bids 降序 (买1最高) — 与源内顺序无关。
非交易时段返回交易所残留挂单; 停牌/无行情 404。

数据源分流 (preferences.depth5_data_provider):
  - easy_tdx (现行默认): easy-tdx serve 实例池 quotes 白名单子池
    (tdx_gateway/VM102 源已于 2026-09-05 移除)
  - tickflow: TickFlow 付费 depth (需 Pro+)
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query, Request

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/kline", tags=["kline"])


@router.get("/depth5")
def get_depth5(request: Request, symbol: str = Query(..., description="标的代码")):
    from app.services import preferences

    provider_name = preferences.get_depth5_data_provider()
    depth: dict = {}
    if provider_name == "tickflow":
        from app.tickflow.capabilities import Cap

        capset = getattr(request.app.state, "capabilities", None)
        if capset is None or not capset.has(Cap.DEPTH5):
            raise HTTPException(status_code=403, detail="五档盘口不可用: 当前套餐无五档权限 (需 Pro+)")
        from app.tickflow.client import get_client

        try:
            depth = get_client().depth.get(symbol)
        except Exception as e:  # noqa: BLE001
            raise HTTPException(status_code=502, detail=f"五档获取失败: {e}") from e
        # tickflow depth.get 返回 {symbol, ask_prices..} 同构 dict, 透传
        row = depth if isinstance(depth, dict) else {}
    else:  # easy_tdx (默认)
        from app.plugins.easy_tdx import provider as easy_provider

        ok, reason = easy_provider.availability()
        if not ok:
            raise HTTPException(status_code=503, detail=f"五档盘口需要 easy-tdx 实例池: {reason}")
        try:
            depth = easy_provider.EasyTdxProvider().get_depth5([symbol])
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
        # 档位数自适应: 标准协议当前 5 档, 未来 L2 十档时自动透传 10 档。
        return [{"price": pr, "volume": v} for pr, v in pairs[:10]]

    return {
        "symbol": symbol,
        # 卖1..卖5 (升序, 卖1=最低卖价)
        "asks": levels(row.get("ask_prices"), row.get("ask_volumes"), reverse=False),
        # 买1..买5 (降序, 买1=最高买价)
        "bids": levels(row.get("bid_prices"), row.get("bid_volumes"), reverse=True),
        "timestamp": row.get("timestamp"),
    }
