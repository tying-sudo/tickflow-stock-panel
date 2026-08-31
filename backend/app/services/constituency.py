"""成分股判定统一服务 — 多数据源、明确优先级、缺失/冲突处理。

两类判定对象:
- **基金/ETF** → "成分股" = 其跟踪指数的官方名单 (被动跟踪, 指数优先于披露持仓);
- **个股**     → "成分股属性" = 是否属于某个指数/主题, 由多源证据综合判定。

== 数据源与优先级 (指数成分获取) ==
S1 中证指数官网 closeweight 月度权重表 — 权威/全量/含权重
   覆盖中证/上证系列 (000xxx/950xxx/959xxx/H30xxx 等)
S2 国证指数官网 sample-detail 月度名单 — 权威/全量
   覆盖国证/深证系列 (399xxx/980xxx), S1 无此代码时兜底
S3 天天基金披露持仓 — 非权威兜底 (degraded)
   ETF: F10 jjcc (半年报/年报全量, 季报 top10); 场外基金: pingzhongdata 前十大
   仅当 S1/S2 均不可得时使用; 结果必须标记 degraded, 不代表官方成分。

== ETF/基金判定规则 ==
R1 基金概况页「跟踪标的」→ 指数代码 → S1/S2 官方成分 → status=authoritative
R2 R1 任一环节失败 → S3 披露持仓兜底 → status=degraded (不中断, 记录原因)
R3 S1/S2 与 S3 冲突时以官方指数为准: 指数外披露标的 (主动偏离/打新/已调出)
   仅作标注 (pct_nav), 不进入成员集合
R4 基金/ETF 本体永不为成员
实现入口: fund_holdings.resolve_holdings → etf_group_sync._fetch_holdings

== 个股判定 (judge_symbol) 证据源与权重 ==
E1 指数官方名单 (S1/S2 成分集合)                     w=1.0  authoritative
E2 行情/维表: instruments 在市校验 + tickflow 指数池
   (沪深300/中证500/上证50)                          w=0.9  authoritative(仅此三池)
E3 财务: financials/metrics 在市佐证                 w=0.4  auxiliary
   (当前表无行业/主题字段, 不能独立判定成分, 仅佐证正常在市)
E4 公告/F10 主营业务: 尚无稳定数据源                  w=0   missing(不参与判定)

== 判定规则 ==
J1 E1 命中                          → verdict=constituent   (official)
J2 E1 名单在手且未命中              → verdict=non_constituent (官方名单是否定性证据)
J3 E1 不可得而 E2 命中              → verdict=constituent   (source=pool)
J4 E1 未命中但 E2 同指数命中 (沪深300/中证500/上证50 与其官方代码对照)
                                    → status=conflict, 结论取 E1 (官方优先)
J5 E1/E2 均不可得                   → verdict=undetermined, status=unknown;
   加权分 score = Σ(w_i·hit_i)/Σ(w_i) 仅在存在可用正证据源时输出,
   绝不因单一弱证据 (如仅财务在市) 给出成分结论 — 不造数。

== 缓存 ==
指数成分按月更新 → data/user_data/constituency_cache.json, fetched_at 超过
7 天或 refresh=True 重拉; S1/S2 全部失败时回退过期缓存并标记 stale。
"""
from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime

import polars as pl

from app.config import settings

logger = logging.getLogger(__name__)

_CACHE_TTL_SECONDS = 7 * 86400
# E2 指数池与官方指数代码对照 (用于 J4 冲突检测)
_POOL_INDEX_CODES = {"CSI300": "000300", "CSI500": "000905", "SSE50": "000016"}
_W_E1 = 1.0   # 指数官方名单
_W_E2 = 0.9   # 行情/维表 (指数池)
_W_E3 = 0.4   # 财务 (佐证)


class ConstituencyError(RuntimeError):
    """成分判定无法完成 (全部相关数据源不可得)。调用方不得造数。"""


def _cache_path():
    p = settings.data_dir / "user_data" / "constituency_cache.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _read_cache() -> dict:
    try:
        raw = json.loads(_cache_path().read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except (OSError, ValueError):
        return {}


def _write_cache(cache: dict) -> None:
    p = _cache_path()
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
    tmp.replace(p)


# ---------------------------------------------------------------------------
# S1/S2: 指数官方成分 (缓存层; 实际抓取在 fund_holdings, 此处统一缓存/降级)
# ---------------------------------------------------------------------------

def fetch_index_members(index_code: str, refresh: bool = False) -> dict:
    """指数代码 → {index_code, index_date, source, members, detail, cached, stale?}。

    S1(csindex)/S2(cnindex) 在 fund_holdings.fetch_index_constituents 内部串联;
    本层负责缓存与"上游失败回退过期缓存"。
    """
    from app.services import fund_holdings

    cache = _read_cache()
    ent = cache.get(index_code)
    if ent and not refresh and time.time() - ent.get("fetched_at", 0) < _CACHE_TTL_SECONDS:
        out = dict(ent)
        out["cached"] = True
        return out
    try:
        info = fund_holdings.fetch_index_constituents(index_code)
    except Exception as exc:  # noqa: BLE001
        if ent:
            out = dict(ent)
            out["cached"] = True
            out["stale"] = True
            logger.warning("指数 %s 成分拉取失败, 回退过期缓存: %s", index_code, exc)
            return out
        raise ConstituencyError(f"指数 {index_code} 官方成分不可得: {exc}") from exc
    ent = {
        "index_code": index_code,
        "index_date": info.get("index_date") or "",
        "source": info.get("source") or "csindex",
        "members": [h["symbol"] for h in info["holdings"]],
        "detail": {
            h["symbol"]: {"name": h.get("name"), "weight": h.get("weight")}
            for h in info["holdings"]
        },
        "fetched_at": time.time(),
    }
    if ent["source"] in ("csindex", "cnindex"):
        # S2.5 交叉验证: 同花顺当前成分 vs 官方月度名单, 差异仅记录不改判定
        # (月度交界处天然可能不同 — 官方为权威口径)
        try:
            ths = fund_holdings.fetch_ths_constituents(index_code)
            official = set(ent["members"])
            ths_set = {h["symbol"] for h in ths["holdings"]}
            ent["crosscheck"] = {
                "source": "ths",
                "agree": official == ths_set,
                "official_count": len(official),
                "ths_count": len(ths_set),
                "only_official": sorted(official - ths_set)[:20],
                "only_ths": sorted(ths_set - official)[:20],
            }
        except Exception as exc:  # noqa: BLE001
            logger.info("THS 交叉验证不可用(%s): %s", index_code, exc)
    cache[index_code] = ent
    _write_cache(cache)
    out = dict(ent)
    out["cached"] = False
    return out


def _known_index_codes() -> list[str]:
    """当前可判定的指数代码 = 判定缓存 + 各分组同步记录的跟踪指数。"""
    codes = list(_read_cache().keys())
    try:
        p = settings.data_dir / "user_data" / "etf_group_sources.json"
        if p.exists():
            for src in json.loads(p.read_text(encoding="utf-8")).values():
                c = (src or {}).get("index_code")
                if c and c not in codes:
                    codes.append(c)
    except (OSError, ValueError):
        pass
    return codes


# ---------------------------------------------------------------------------
# 标的归一化
# ---------------------------------------------------------------------------

def _suffix_by_code(code: str) -> str | None:
    """A 股股票代码段 → 交易所后缀 (北交所 43/83/87/92)。"""
    if code.startswith(("600", "601", "603", "605", "688", "689")):
        return ".SH"
    if code.startswith(("000", "001", "002", "003", "300", "301")):
        return ".SZ"
    if code.startswith(("43", "83", "87", "92")):
        return ".BJ"
    return None


def _is_fund_code(code: str) -> bool:
    """场内基金 (ETF/LOF) 代码段: 沪 50-52/56-59, 深 15-18。"""
    return code.startswith(("50", "51", "52", "56", "57", "58", "59", "15", "16", "17", "18"))


# ---------------------------------------------------------------------------
# ETF/基金判定 (R1-R4)
# ---------------------------------------------------------------------------

def _judge_fund(symbol: str, refresh: bool = False) -> dict:
    from app.services import fund_holdings

    try:
        info = fund_holdings.resolve_holdings(symbol)
    except fund_holdings.FundHoldingsError as exc:
        return {
            "object": "fund", "symbol": symbol,
            "verdict": "undetermined", "status": "unknown",
            "rule": "R2 失败: S1/S2/S3 全部不可得, 不造数",
            "error": str(exc), "as_of": datetime.utcnow().isoformat(timespec="seconds"),
        }
    index = info.get("index")
    src_count = {"index": 0, "index+disclosed": 0, "disclosed_only": 0}
    for h in info["holdings"]:
        srcs = h.get("sources") or []
        if "index" in srcs and "disclosed" in srcs:
            src_count["index+disclosed"] += 1
        elif "index" in srcs:
            src_count["index"] += 1
        else:
            src_count["disclosed_only"] += 1
    return {
        "object": "fund",
        "symbol": symbol,
        "fund_name": info.get("fund_name"),
        "status": "authoritative" if index else "degraded",
        "rule": "R1 跟踪指数官方成分" if index else "R2 披露持仓兜底 (S1/S2 不可得)",
        "index": index,
        "report_date": info.get("report_date"),
        "constituents_count": len(info["holdings"]),
        "sources_summary": src_count,
        "conflict_policy": "R3 指数与披露冲突时以指数为准 (披露外标的仅标注不入组)",
        "holdings_sample": [
            {"symbol": h["symbol"], "name": h.get("name"), "weight": h.get("weight")}
            for h in info["holdings"][:20]
        ],
        "as_of": datetime.utcnow().isoformat(timespec="seconds"),
    }


# ---------------------------------------------------------------------------
# 个股判定 (E1-E4 / J1-J5)
# ---------------------------------------------------------------------------

def _ev(source: str, kind: str, weight: float, available: bool, **kw) -> dict:
    out = {"source": source, "kind": kind, "weight": weight, "available": available}
    out.update(kw)
    return out


def _check_instruments(sym: str, code: str) -> tuple[bool | None, str, dict | None]:
    """E2a: instruments 维表在市校验 → (listed, resolved_symbol, extra)。"""
    try:
        inst = pl.read_parquet(settings.data_dir / "instruments" / "instruments.parquet")
    except Exception as exc:  # noqa: BLE001
        return None, sym, {"error": str(exc)}
    row = inst.filter(pl.col("symbol") == sym)
    if row.is_empty() and "." not in sym:
        row = inst.filter(pl.col("symbol").str.starts_with(code + "."))
    if row.is_empty():
        return False, sym, None
    return True, str(row["symbol"][0]), {"name": row["name"][0]}


def _check_financial(sym: str) -> tuple[bool, str | None]:
    """E3: financials/metrics 是否有该标的近期报告 (佐证在市, 不判成分)。"""
    for cand in ("metrics.parquet", "metrics"):
        p = settings.data_dir / "financials" / cand
        if not p.exists():
            continue
        try:
            df = (
                pl.scan_parquet(p)
                .filter(pl.col("symbol") == sym)
                .select("period_end")
                .collect()
            )
        except Exception:  # noqa: BLE001
            continue
        if df.is_empty():
            return False, None
        return True, str(df["period_end"].max())
    return False, None


def judge_stock(symbol: str, index_codes: list[str] | None = None,
                refresh: bool = False) -> dict:
    """个股成分股属性判定 (E1-E4 证据收集 + J1-J5 规则)。"""
    raw = symbol.strip().upper()
    code = raw.split(".")[0]
    sym = raw if re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", raw) else code
    evidence: list[dict] = []

    # --- E2a 在市校验 (无后缀裸代码按代码段规则补后缀) ---
    if not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", sym):
        suf = _suffix_by_code(code)
        if suf:
            sym = code + suf
    listed, sym, extra = _check_instruments(sym, code)
    if extra is not None:
        evidence.append(_ev("instruments", "quote_registry", _W_E2,
                            available=True, hit=listed, **extra))
    else:
        evidence.append(_ev("instruments", "quote_registry", _W_E2,
                            available=False, hit=None, detail="instruments 维表不可读"))
    if listed is False:
        return {
            "object": "stock", "symbol": sym,
            "verdict": "undetermined", "status": "unknown",
            "rule": "J5 标的不在 A 股维表 (退市/非 A 股/代码有误), 不造数",
            "evidence": evidence,
            "as_of": datetime.utcnow().isoformat(timespec="seconds"),
        }

    # --- E1 指数官方名单 ---
    codes = index_codes if index_codes else _known_index_codes()
    if not codes:
        evidence.append(_ev("index-official", "official_list", _W_E1, available=False,
                            hit=None, detail="无可用指数名单 (未配置/未同步任何跟踪指数)"))
    index_miss: set[str] = set()
    for ic in codes:
        try:
            members = fetch_index_members(ic, refresh=refresh)
        except ConstituencyError as exc:
            evidence.append(_ev(f"index:{ic}", "official_list", _W_E1, available=False,
                                hit=None, detail=str(exc)))
            continue
        hit = sym in members["members"]
        evidence.append(_ev(
            f"index:{ic}({members.get('source')})", "official_list", _W_E1,
            available=True, hit=hit, index_date=members.get("index_date"),
            cached=members.get("cached", False), stale=members.get("stale", False),
            crosscheck=members.get("crosscheck"),
        ))
        if not hit:
            index_miss.add(ic)

    # --- E2b 指数池 (沪深300/中证500/上证50) ---
    pool_hits: dict[str, bool] = {}
    from app.tickflow.pools import get_pool
    for pid in _POOL_INDEX_CODES:
        try:
            syms = set(get_pool(pid))
        except Exception as exc:  # noqa: BLE001
            evidence.append(_ev(f"pool:{pid}", "quote_pool", _W_E2, available=False,
                                hit=None, detail=str(exc)))
            continue
        if not syms:
            evidence.append(_ev(f"pool:{pid}", "quote_pool", _W_E2, available=False,
                                hit=None, detail="指数池为空 (数据源未提供 universes)"))
            continue
        hit = sym in syms
        pool_hits[pid] = hit
        evidence.append(_ev(f"pool:{pid}", "quote_pool", _W_E2, available=True, hit=hit))

    # --- E3 财务 (佐证) / E4 公告 (缺失) ---
    fin_ok, fin_period = _check_financial(sym)
    evidence.append(_ev("financials/metrics", "financial_corroborate", _W_E3,
                        available=fin_ok, hit=fin_ok,
                        detail=("仅佐证正常在市披露, 不独立判定成分" if fin_ok else "无财务数据"),
                        latest_period=fin_period))
    evidence.append(_ev("announcements", "announcement_text", 0.0, available=False,
                        hit=None, detail="公告/F10 主营数据源未接入, 不参与判定"))

    # --- 规则 J1-J5 ---
    e1_hit = [e for e in evidence if e["kind"] == "official_list" and e.get("hit")]
    e1_avail = [e for e in evidence if e["kind"] == "official_list" and e["available"]]
    e2_hit_pools = [pid for pid, hit in pool_hits.items() if hit]

    # J4 冲突: 某指数池命中, 但其对应官方名单在手且未命中
    conflict = any(
        pid in e2_hit_pools and _POOL_INDEX_CODES[pid] in index_miss
        for pid in _POOL_INDEX_CODES
    )
    if e1_hit:
        verdict, status = "constituent", ("conflict" if conflict else "ok")
    elif e1_avail:
        verdict = "non_constituent"
        status = "conflict" if conflict else "ok"
    elif e2_hit_pools:
        verdict, status = "constituent", "ok"          # J3
    else:
        verdict, status = "undetermined", "unknown"    # J5

    # 加权分 (仅统计可用的正证据源 E1/E2; E3 佐证不计入, 见模块文档 J5)
    avail = [e for e in evidence if e["available"] and e["weight"] > 0]
    score = None
    if avail:
        score = round(
            sum(e["weight"] for e in avail if e.get("hit"))
            / sum(e["weight"] for e in avail), 3)

    return {
        "object": "stock",
        "symbol": sym,
        "listed": listed,
        "verdict": verdict,
        "status": status,
        "score": score,
        "rule": {
            "constituent": ("J1 指数官方名单命中" if e1_hit
                            else "J3 指数池命中 (官方名单不可得)"),
            "non_constituent": ("J2 官方名单在手且未命中"
                                + (" (J4 与指数池冲突, 取官方结论)" if conflict else "")),
            "undetermined": "J5 全部相关源不可得, 不造数",
        }[verdict],
        "indices": [e["source"] for e in e1_hit],
        "pools": e2_hit_pools,
        "evidence": evidence,
        "as_of": datetime.utcnow().isoformat(timespec="seconds"),
    }


def judge_symbol(symbol: str, repo=None, index_codes: list[str] | None = None,
                 refresh: bool = False) -> dict:
    """统一入口: ETF/场外基金 → 跟踪指数口径 (R1-R4); 个股 → 多源证据判定 (E/J)。"""
    raw = (symbol or "").strip().upper()
    code = raw.split(".")[0]
    if re.fullmatch(r"\d{6}", code):
        has_suffix = re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", raw) is not None
        if not has_suffix:
            # 裸 6 位代码: 场内基金代码段 → 基金路径; 否则先查 A 股维表,
            # 在市 → 个股判定, 不在 → 按场外基金处理 (维表不含场外基金)
            if _is_fund_code(code):
                return _judge_fund(code, refresh=refresh)
            listed, _sym, _extra = _check_instruments(code, code)
            if listed:
                return judge_stock(code, index_codes=index_codes, refresh=refresh)
            return _judge_fund(code, refresh=refresh)
        if _is_fund_code(code):
            return _judge_fund(raw, refresh=refresh)       # 场内 ETF/LOF
    return judge_stock(raw, index_codes=index_codes, refresh=refresh)
