"""对账服务测试: 日K×100 vs 分钟聚合 / 收盘价互检 / 股本单位探针 / 报告落盘。"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl

from app.services import reconciliation as rc


def _daily(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema={
        "symbol": pl.Utf8, "date": pl.Date,
        "volume": pl.Float64, "close": pl.Float64,
    })


def _minute(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema={
        "symbol": pl.Utf8, "datetime": pl.Utf8,
        "volume": pl.Float64, "close": pl.Float64,
    })


def test_volume_and_close_match_pass():
    daily = _daily([
        {"symbol": "600000.SH", "date": date(2026, 9, 4), "volume": 10000.0, "close": 10.0},
    ])
    minute = _minute([
        {"symbol": "600000.SH", "datetime": "2026-09-04T09:31:00", "volume": 400000.0, "close": 9.9},
        {"symbol": "600000.SH", "datetime": "2026-09-04T15:00:00", "volume": 600000.0, "close": 10.0},
    ])
    assert rc.cross_check_daily_minute(daily, minute) == []  # Σ100万股 vs 10000手×100


def test_volume_unit_regression_detected():
    # 分钟库若被写成"手" (少 ×100): Σ=10000 vs 日K股=1000000 → 比值 0.01 违例
    daily = _daily([
        {"symbol": "600000.SH", "date": date(2026, 9, 4), "volume": 10000.0, "close": 10.0},
    ])
    minute = _minute([
        {"symbol": "600000.SH", "datetime": "2026-09-04T15:00:00", "volume": 10000.0, "close": 10.0},
    ])
    violations = rc.cross_check_daily_minute(daily, minute)
    assert any(v["check"] == "volume_mismatch" for v in violations)


def test_close_mismatch_detected():
    daily = _daily([
        {"symbol": "600000.SH", "date": date(2026, 9, 4), "volume": 100.0, "close": 10.0},
    ])
    minute = _minute([
        {"symbol": "600000.SH", "datetime": "2026-09-04T15:00:00", "volume": 10000.0, "close": 9.0},
    ])
    violations = rc.cross_check_daily_minute(daily, minute)
    assert any(v["check"] == "close_mismatch" for v in violations)


def test_daily_volume_without_minute_rows_flagged():
    daily = _daily([
        {"symbol": "600001.SH", "date": date(2026, 9, 4), "volume": 5000.0, "close": 8.0},
    ])
    violations = rc.cross_check_daily_minute(daily, _minute([]))
    assert any(v["check"] == "minute_missing" for v in violations)


def test_shares_unit_probe():
    ok = pl.DataFrame({
        "symbol": ["600519.SH", "000001.SZ"],
        "period_end": ["2026-06-30"] * 2,
        "total_shares": [1.2500815625e9, 1.9405918750e10],
    })
    assert rc.check_shares_units(ok) == []
    poisoned = pl.DataFrame({
        "symbol": ["600519.SH"],
        "period_end": ["2026-06-30"],
        "total_shares": [1.2500815625e13],  # ×1e4 中毒形态
    })
    violations = rc.check_shares_units(poisoned)
    assert len(violations) == 1 and violations[0]["check"] == "shares_unit_poison"


def test_run_reconciliation_writes_report(tmp_path: Path):
    # 构造最小双链数据 (抽样步进会取到唯一 symbol)
    d = tmp_path / "kline_daily" / "date=2026-09-04"
    d.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["600000.SH"], "date": [date(2026, 9, 4)],
        "open": [10.0], "high": [10.2], "low": [9.8], "close": [10.0],
        "volume": [10000.0], "amount": [1.0e7],
    }).write_parquet(d / "part.parquet")
    m = tmp_path / "kline_minute" / "date=2026-09-04"
    m.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["600000.SH"] * 2,
        "datetime": ["2026-09-04T09:31:00", "2026-09-04T15:00:00"],
        "open": [10.0, 10.0], "high": [10.0, 10.0], "low": [10.0, 10.0],
        "close": [9.9, 10.0], "volume": [400000.0, 600000.0], "amount": [5e6, 5e6],
    }).write_parquet(m / "part.parquet")
    report = rc.run_reconciliation(tmp_path)
    assert report["trade_date"] == "2026-09-04"
    assert report["violations"] == []
    assert report["ok"] is True
    latest = tmp_path / "reconciliation" / "latest.json"
    assert latest.exists()
    import json
    assert json.loads(latest.read_text(encoding="utf-8"))["sampled"] == 1
    assert (tmp_path / "reconciliation" / "history.jsonl").exists()
