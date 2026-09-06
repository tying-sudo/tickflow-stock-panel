"""数据对账 (reconciliation) — 日K/分钟/股本三链交叉校验, 管道每日落盘报告。

背景 (缺口修复④, 2026-09-06): 单位契约 (分钟=股 / 日K=手 / 股本=股) 靠人肉
记忆维持, 历史上踩过 ×100、×1e4 等至少四次量纲事故; 完整性核对曾靠出事后的
手工审计。本服务把三组不变量变成管道固定阶段, 异常写 data/reconciliation/
latest.json 供告警/前端消费 (失败只告警, 绝不阻断管道)。

校验项:
1. 日K 成交量 vs 分钟聚合: Σminute.volume(股) ≈ daily.volume(手)×100,
   容忍 3% (尾盘竞价分钟缺失/停牌分钟剔除的边界差)。
2. 日K 收盘价 vs 当日最后一根分钟收盘: |Δ|/close ≤ 0.1%。
3. 股本单位探针: 600519/000001 total_shares 必须落在真实量级区间
   (×1e4 中毒会变成 1e13/1e11 量级)。
"""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from app.parquet import scan_daily_parquet, scan_parquet_compat

logger = logging.getLogger(__name__)

VOLUME_TOLERANCE = 0.03
CLOSE_TOLERANCE = 0.001
SAMPLE_SIZE = 15

# 股本探针 (symbol → (下限股, 上限股)); 区间取真实总股本 ±20%
SHARE_PROBES: dict[str, tuple[float, float]] = {
    "600519.SH": (1.0e9, 1.5e9),   # 贵州茅台 ~12.5 亿股
    "000001.SZ": (1.5e10, 2.4e10), # 平安银行 ~194 亿股
}


def cross_check_daily_minute(
    daily: pl.DataFrame, minute: pl.DataFrame
) -> list[dict[str, Any]]:
    """纯函数: daily[symbol,date,volume(手),close] × minute[symbol,datetime,volume(股),close]。

    只对两表共有的 (symbol, date) 校验, 返回违例列表 (空 = 全绿)。
    """
    if daily.is_empty():
        return []
    minute = minute.with_columns(
        pl.col("datetime").cast(pl.Utf8).str.slice(0, 10).str.to_date().alias("date")
    )
    agg = minute.group_by("symbol", "date").agg(
        pl.col("volume").sum().alias("minute_volume"),
        pl.col("close").last().alias("minute_close"),
        pl.len().alias("minute_rows"),
    )
    # left join: 日K有量但分钟整日缺失的标的也要暴露 (minute_* 为 null)
    joined = daily.join(agg, on=["symbol", "date"], how="left")
    violations: list[dict[str, Any]] = []
    for row in joined.iter_rows(named=True):
        daily_volume_shares = (row["volume"] or 0.0) * 100.0
        mv = row.get("minute_volume")
        if mv is None or mv <= 0 or daily_volume_shares <= 0:
            if (row.get("minute_rows") or 0) == 0 and (row["volume"] or 0) > 0:
                violations.append({
                    "check": "minute_missing",
                    "symbol": row["symbol"], "date": str(row["date"]),
                    "detail": "日K有量但分钟无行",
                })
            continue
        ratio = mv / daily_volume_shares
        if abs(ratio - 1.0) > VOLUME_TOLERANCE:
            violations.append({
                "check": "volume_mismatch",
                "symbol": row["symbol"], "date": str(row["date"]),
                "detail": f"分钟聚合/日K×100 = {ratio:.4f} (分钟Σ={mv:.0f} vs 日K股={daily_volume_shares:.0f})",
            })
        mc = row.get("minute_close")
        dc = row.get("close")
        if mc is not None and dc is not None and dc > 0:
            if abs(mc - dc) / dc > CLOSE_TOLERANCE:
                violations.append({
                    "check": "close_mismatch",
                    "symbol": row["symbol"], "date": str(row["date"]),
                    "detail": f"分钟末收 {mc} vs 日K收 {dc}",
                })
    return violations


def check_shares_units(shares: pl.DataFrame) -> list[dict[str, Any]]:
    """纯函数: 股本表单位探针。shares[symbol,total_shares] (最新期)。"""
    if shares.is_empty() or "total_shares" not in shares.columns:
        return []
    latest = (
        shares.filter(pl.col("symbol").is_in(list(SHARE_PROBES)))
        .sort("symbol", "period_end" if "period_end" in shares.columns else "symbol")
        .group_by("symbol")
        .agg(pl.col("total_shares").last())
    )
    violations: list[dict[str, Any]] = []
    for row in latest.iter_rows(named=True):
        lo, hi = SHARE_PROBES.get(row["symbol"], (0.0, float("inf")))
        value = row["total_shares"]
        if value is None or not (lo <= value <= hi):
            violations.append({
                "check": "shares_unit_poison",
                "symbol": row["symbol"],
                "detail": f"total_shares={value} 不在 [{lo:.3g},{hi:.3g}] 股区间 (疑似 ×1e4/单位中毒)",
            })
    return violations


def run_reconciliation(data_dir: Path, *, sample_size: int = SAMPLE_SIZE) -> dict:
    """IO 包装: 抽样最近交易日, 跑三组校验, 落盘 latest.json + 追加 history。"""
    report: dict[str, Any] = {
        "ran_at": date.today().isoformat(),
        "trade_date": None,
        "sampled": 0,
        "checks": {},
        "violations": [],
        "ok": True,
    }
    daily_glob = (data_dir / "kline_daily" / "**" / "*.parquet").as_posix()
    minute_glob = (data_dir / "kline_minute" / "**" / "*.parquet").as_posix()
    try:
        daily_all = scan_daily_parquet(daily_glob)
        latest = daily_all.select(pl.col("date").max()).collect().item()
        if latest is None:
            report["checks"]["daily"] = "no-data"
            return _finish(data_dir, report)
        report["trade_date"] = str(latest)
        # 抽样: 确定性步进取样 (可复现, 无随机源)
        symbols = (
            daily_all.select(pl.col("symbol").unique())
            .collect()["symbol"].sort().to_list()
        )
        step = max(1, len(symbols) // max(1, sample_size))
        sample = symbols[::step][:sample_size]
        report["sampled"] = len(sample)
        daily = (
            daily_all.filter(
                (pl.col("date") == pl.lit(latest)) & pl.col("symbol").is_in(sample)
            )
            .select("symbol", "date", "volume", "close")
            .collect()
        )
        date_text = latest.isoformat()
        minute = (
            scan_parquet_compat(minute_glob)
            .filter(
                pl.col("symbol").is_in(sample)
                & pl.col("datetime").cast(pl.Utf8).str.starts_with(date_text)
            )
            .select("symbol", "datetime", "volume", "close")
            .collect()
        )
        report["checks"]["daily_vs_minute"] = "ran"
        report["violations"].extend(cross_check_daily_minute(daily, minute))

        shares_path = data_dir / "financials" / "shares" / "part.parquet"
        if shares_path.exists():
            schema_names = set(pl.read_parquet_schema(shares_path).names())
            cols = ["symbol", "total_shares"] + (
                ["period_end"] if "period_end" in schema_names else []
            )
            shares = pl.read_parquet(shares_path, columns=cols)
            report["checks"]["shares_units"] = "ran"
            report["violations"].extend(check_shares_units(shares))
        else:
            report["checks"]["shares_units"] = "no-table"
    except Exception as exc:  # noqa: BLE001
        report["checks"]["error"] = str(exc)[:300]
        report["ok"] = False
        logger.warning("reconciliation failed: %s", exc)
    return _finish(data_dir, report)


def _finish(data_dir: Path, report: dict) -> dict:
    report["ok"] = report["ok"] and not report["violations"]
    out = data_dir / "reconciliation"
    out.mkdir(parents=True, exist_ok=True)
    (out / "latest.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    with (out / "history.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(report, ensure_ascii=False, default=str) + "\n")
    return report
