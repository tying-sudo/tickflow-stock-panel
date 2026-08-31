"""ETF / 场外基金 成分股分组定期同步。

「添加 ETF → 以 ETF 名建分组 → 成分股入组」(api/watchlist.py add-etf-group) 的
后续维护: 基金新一期财报(季报/半年报/年报)披露后, 持仓成员会变化。本模块提供:

- record_source(): 导入时记录 分组→基金 映射 (kind: etf|fund)
- sync_etf_groups(): 重新拉取最新披露期持仓并同步成员
    * 新增成分股自动入组
    * 仅移除"上次同步带入、本期已退出披露"的成员 —— 用户手动添加的成员永不删除
    * ETF 本体始终保留
- repo 传入时对尚无映射的历史分组自动回填:
    1) 分组名 = ETF 名称 (repo ETF 维表) → kind=etf (F10 全量披露)
    2) 否则天天基金搜索精确同名 → kind=fund (pingzhongdata 前十大重仓)

映射存储: data/user_data/etf_group_sources.json
  { group_id: {"kind", "symbol"/"code", "report_date", "synced_members", "synced_at"} }
"""
from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime

from app.config import settings

logger = logging.getLogger(__name__)

_LOCK = threading.RLock()


def _sources_path():
    p = settings.data_dir / "user_data" / "etf_group_sources.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _read_sources() -> dict:
    p = _sources_path()
    if not p.exists():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("etf_group_sources.json 读取失败, 忽略: %s", exc)
        return {}


def _write_sources(sources: dict) -> None:
    p = _sources_path()
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(sources, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, p)


def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def record_source(group_id: str, symbol: str, report_date: str, members: list[str],
                  kind: str = "etf") -> None:
    """导入/复用分组时记录映射。members 传成分股列表(不含基金本体)。"""
    with _LOCK:
        sources = _read_sources()
        sources[group_id] = {
            "kind": kind if kind in ("etf", "fund") else "etf",
            "symbol": symbol,
            "report_date": report_date,
            "synced_members": list(dict.fromkeys(members or [])),
            "synced_at": _now(),
        }
        _write_sources(sources)


def remove_source(group_id: str) -> None:
    with _LOCK:
        sources = _read_sources()
        if group_id in sources:
            del sources[group_id]
            _write_sources(sources)


def _group_member_symbols(group_id: str) -> set[str]:
    """分组当前成员 symbol 集合 (多值 group_ids 模型)。"""
    from app.services import watchlist
    return {
        row["symbol"]
        for row in watchlist.list_symbols()
        if group_id in (row.get("group_ids") or [])
    }


def _watchlist_symbols() -> set[str]:
    from app.services import watchlist
    return {row["symbol"] for row in watchlist.list_symbols()}


def _fetch_holdings(src: dict) -> dict:
    """按 kind 分发拉取成分股 → {fund_name, report_date, members, index, status}。

    - kind=etf:  多源合成 (跟踪指数官方成分优先, 披露标注/兜底)
    - kind=fund: 场外基金走 pingzhongdata 前十大重仓 (原口径)。
      2026-08-31 回退: 曾统一走 resolve_holdings, 但主动基金半年报"全量
      披露"会给分组带出 100+ 标的, 组内等权涨跌幅严重失真 —— 全量披露
      是监管口径, 不是"重仓成分"概念。
    """
    from app.services import fund_holdings
    if src.get("kind") == "fund":
        info = fund_holdings.fetch_fund_holdings(src["symbol"])
        return {
            "fund_name": info.get("fund_name") or "",
            "report_date": info.get("report_date") or "",
            "members": [h["symbol"] for h in info["holdings"]],
            "index": None,
            "status": "degraded",
        }
    info = fund_holdings.resolve_holdings(src["symbol"])
    return {
        "fund_name": info.get("fund_name") or "",
        "report_date": info.get("report_date") or "",
        "members": [h["symbol"] for h in info["holdings"]],
        "index": info.get("index"),
        "status": "authoritative" if info.get("index") else "degraded",
    }


def _backfill_sources(repo, groups: list[dict], sources: dict) -> int:
    """历史分组回填映射: 先按 ETF 名称反查, 再按场外基金搜索精确同名。"""
    from app.services import fund_holdings

    patched = 0
    etf_by_name: dict[str, str] = {}
    if repo is not None:
        try:
            etf_inst = repo.get_etf_instruments()
            if not etf_inst.is_empty() and "name" in etf_inst.columns:
                for row in etf_inst.select(["symbol", "name"]).iter_rows(named=True):
                    if row.get("name") and row.get("symbol"):
                        etf_by_name.setdefault(str(row["name"]), str(row["symbol"]))
        except Exception as e:  # noqa: BLE001
            logger.warning("etf_group_sync 回填: ETF 维表不可用 (%s)", e)

    for group in groups:
        gid, gname = group["id"], group["name"]
        if gid in sources:
            continue
        symbol = etf_by_name.get(gname)
        if symbol:
            sources[gid] = {
                "kind": "etf", "symbol": symbol, "report_date": "",
                "synced_members": [], "synced_at": _now(),
            }
            patched += 1
            logger.info("etf_group_sync 回填映射(ETF): 分组「%s」→ %s", gname, symbol)
            continue
        code = None
        try:
            code = fund_holdings.search_fund_code_by_name(gname)
        except Exception as e:  # noqa: BLE001
            logger.warning("etf_group_sync 回填: 基金搜索失败「%s」(%s)", gname, e)
        if code:
            sources[gid] = {
                "kind": "fund", "symbol": code, "report_date": "",
                "synced_members": [], "synced_at": _now(),
            }
            patched += 1
            logger.info("etf_group_sync 回填映射(基金): 分组「%s」→ %s", gname, code)
    return patched


def sync_etf_groups(repo=None) -> dict:
    """同步全部基金/ETF 成分股分组到最新披露期。返回汇总 dict。"""
    from app.services import watchlist

    groups = watchlist.list_groups()
    with _LOCK:
        sources = _read_sources()
        backfilled = _backfill_sources(repo, groups, sources)
        if backfilled:
            _write_sources(sources)

    checked = updated = failed = skipped = 0
    added_total = removed_total = 0
    details: list[dict] = []

    for group in groups:
        gid, gname = group["id"], group["name"]
        src = sources.get(gid)
        if not src or not src.get("symbol"):
            skipped += 1
            continue
        kind = src.get("kind", "etf")
        checked += 1
        try:
            fetched = _fetch_holdings(src)
            fund_name = fetched["fund_name"]
            report_date = fetched["report_date"]
            new_constituents = fetched["members"]
        except Exception as e:  # noqa: BLE001
            failed += 1
            details.append({"group": gname, "kind": kind, "symbol": src["symbol"],
                            "error": str(e)})
            logger.warning("etf_group_sync %s(%s %s) 拉取持仓失败: %s",
                           gname, kind, src["symbol"], e)
            continue
        if not new_constituents:
            failed += 1
            details.append({"group": gname, "kind": kind, "symbol": src["symbol"],
                            "error": "holdings empty"})
            continue

        new_set = set(new_constituents)
        current = _group_member_symbols(gid)
        prev_synced = set(src.get("synced_members") or [])

        to_add = [s for s in new_constituents if s not in current]
        # 只删"上次同步带入、本期已退出披露"的成员; 用户手动加的 (不在 synced_members) 不动
        to_remove = sorted(
            s for s in prev_synced
            if s not in new_set and s in current
        )
        # 旧版本 add-etf-group 曾把 ETF 本体入组 — 本体不在披露名单内, 顺手摘除
        if src["symbol"] in current and src["symbol"] not in to_remove:
            to_remove.append(src["symbol"])
        report_changed = bool(src.get("report_date")) and src.get("report_date") != report_date

        if not to_add and not to_remove and not report_changed:
            # 数据没变也刷新 synced_members/synced_at, 让映射跟上人工调整
            with _LOCK:
                sources[gid].update({
                    "kind": kind,
                    "report_date": report_date,
                    "index_code": (fetched.get("index") or {}).get("index_code"),
                    "status": fetched["status"],
                    "synced_members": sorted(new_set),
                    "synced_at": _now(),
                })
                _write_sources(sources)
            details.append({"group": gname, "kind": kind, "symbol": src["symbol"],
                            "changed": False, "report_date": report_date,
                            "index": fetched.get("index"), "status": fetched["status"]})
            continue

        try:
            if to_add:
                existing = _watchlist_symbols()
                to_tag = [s for s in to_add if s in existing]
                to_create = [s for s in to_add if s not in existing]
                for s in to_tag:
                    watchlist.add_to_group(s, gid)
                if to_create:
                    # add_batch 逐只 insert(0), 倒序传入 → 成分股按披露顺序落在列表前部
                    watchlist.add_batch(list(reversed(to_create)), note="", group_id=gid)
            for s in to_remove:
                watchlist.remove_from_group(s, gid)
        except Exception as e:  # noqa: BLE001
            failed += 1
            details.append({"group": gname, "kind": kind, "symbol": src["symbol"],
                            "error": str(e)})
            logger.exception("etf_group_sync %s(%s) 成员更新失败: %s", gname, src["symbol"], e)
            continue

        added_total += len(to_add)
        removed_total += len(to_remove)
        updated += 1
        with _LOCK:
            sources[gid].update({
                "report_date": report_date,
                "index_code": (fetched.get("index") or {}).get("index_code"),
                "status": fetched["status"],
                "synced_members": sorted(new_set),
                "synced_at": _now(),
            })
            _write_sources(sources)
        details.append({
            "group": gname, "kind": kind, "symbol": src["symbol"], "changed": True,
            "report_date": report_date,
            "index": fetched.get("index"), "status": fetched["status"],
            "added": to_add, "removed": to_remove,
        })
        logger.info("etf_group_sync %s(%s %s): +%d / -%d (披露期 %s)",
                    gname, kind, src["symbol"], len(to_add), len(to_remove), report_date or "n/a")

    return {
        "groups": len(groups),
        "checked": checked,
        "updated": updated,
        "failed": failed,
        "skipped": skipped,
        "backfilled": backfilled,
        "added": added_total,
        "removed": removed_total,
        "details": details,
    }
