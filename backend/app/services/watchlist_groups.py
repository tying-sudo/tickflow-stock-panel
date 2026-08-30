"""自选分组服务。

存储:`data/user_data/watchlist_groups.parquet` (name + created_at)
     `data/user_data/watchlist_group_members.parquet` (group_name + symbol + added_at)

典型用法: 自选页搜索基金 → 基金名作为分组名、前十大重仓股作为组内成员。
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import polars as pl

from app.config import settings

logger = logging.getLogger(__name__)

_GROUPS_SCHEMA = {"name": pl.Utf8, "created_at": pl.Utf8}
_MEMBERS_SCHEMA = {"group_name": pl.Utf8, "symbol": pl.Utf8, "added_at": pl.Utf8}


def _groups_path() -> Path:
    p = settings.data_dir / "user_data" / "watchlist_groups.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _members_path() -> Path:
    p = settings.data_dir / "user_data" / "watchlist_group_members.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def _read(path: Path, schema: dict) -> pl.DataFrame:
    if path.exists():
        return pl.read_parquet(path)
    return pl.DataFrame(schema=schema)


def list_groups() -> list[dict]:
    """返回 [{name, created_at, symbols: [...]}]，按创建顺序，成员按加入顺序。"""
    gdf = _read(_groups_path(), _GROUPS_SCHEMA)
    mdf = _read(_members_path(), _MEMBERS_SCHEMA)
    members_by_group: dict[str, list[str]] = {}
    if not mdf.is_empty():
        for row in mdf.sort("added_at").to_dicts():
            members_by_group.setdefault(row["group_name"], []).append(row["symbol"])
    out: list[dict] = []
    for row in gdf.to_dicts():
        name = row["name"]
        out.append(
            {
                "name": name,
                "created_at": row["created_at"],
                "symbols": members_by_group.get(name, []),
            }
        )
    return out


def upsert_group(name: str, symbols: list[str]) -> list[dict]:
    """创建或更新分组（同名组整体替换成员，保持传入顺序）。返回最新 list_groups()。"""
    name = (name or "").strip()
    if not name:
        raise ValueError("分组名不能为空")
    symbols = [s.strip() for s in symbols if s and s.strip()]

    gdf = _read(_groups_path(), _GROUPS_SCHEMA)
    if name not in set(gdf["name"].to_list() if not gdf.is_empty() else []):
        new_row = pl.DataFrame({"name": [name], "created_at": [_now()]})
        gdf = pl.concat([gdf, new_row], how="diagonal_relaxed")
    gdf.write_parquet(_groups_path())

    mdf = _read(_members_path(), _MEMBERS_SCHEMA)
    if not mdf.is_empty():
        mdf = mdf.filter(pl.col("group_name") != name)
    if symbols:
        rows = pl.DataFrame(
            {
                "group_name": [name] * len(symbols),
                "symbol": symbols,
                "added_at": [_now()] * len(symbols),
            }
        )
        mdf = pl.concat([mdf, rows], how="diagonal_relaxed")
    mdf.write_parquet(_members_path())
    return list_groups()


def delete_group(name: str) -> None:
    gdf = _read(_groups_path(), _GROUPS_SCHEMA)
    if not gdf.is_empty():
        gdf = gdf.filter(pl.col("name") != name)
        gdf.write_parquet(_groups_path())
    mdf = _read(_members_path(), _MEMBERS_SCHEMA)
    if not mdf.is_empty():
        mdf = mdf.filter(pl.col("group_name") != name)
        mdf.write_parquet(_members_path())


def remove_member(name: str, symbol: str) -> None:
    mdf = _read(_members_path(), _MEMBERS_SCHEMA)
    if mdf.is_empty():
        return
    mdf = mdf.filter(~((pl.col("group_name") == name) & (pl.col("symbol") == symbol)))
    mdf.write_parquet(_members_path())


def group_symbols(name: str) -> list[str]:
    mdf = _read(_members_path(), _MEMBERS_SCHEMA)
    if mdf.is_empty():
        return []
    return mdf.filter(pl.col("group_name") == name).sort("added_at")["symbol"].to_list()
