"""Run on the Windows TDX VM, not on the Tick Stock Panel host.

It exposes only read-only K-line and explicit-symbol snapshot requests to the
allowed Linux IP and forwards them to the local TDX Quant HTTP process at
127.0.0.1:17709.  It deliberately has no trading endpoint and refuses unknown
callers rather than substituting another market-data source.
"""
from __future__ import annotations

import json
import os
import struct
import threading
import time
import zipfile
import zlib
from datetime import date, timedelta
import socket
import subprocess
from concurrent.futures import ThreadPoolExecutor
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import BoundedSemaphore
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

LISTEN_HOST = os.environ.get("TDX_GATEWAY_LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("TDX_GATEWAY_LISTEN_PORT", "18709"))
TOKEN = os.environ.get("TDX_GATEWAY_TOKEN", "")
ALLOWED_CLIENTS = {item.strip() for item in os.environ.get("TDX_GATEWAY_ALLOWED_CLIENTS", "10.0.10.25").split(",") if item.strip()}
TDX_URL = os.environ.get("TDX_QUANT_URL", "http://127.0.0.1:17709/")
# The public API is batch-shaped for Tick Stock Panel's provider contract, but
# TDX Quant documents one symbol per get_market_data request.  Fan out below
# rather than relying on an undocumented multi-symbol response.
MAX_SYMBOLS = 50
# TDX documents one symbol per market-data call.  Concurrency re-verified by
# the 2026-09-01 stress test (3840 direct single-symbol snapshot calls, zero
# errors): C=12/16 sustain 245-325 calls/s vs 181 at C=6, p95 latency <115ms.
# Default raised 6 -> 12 (env-overridable, hard cap 16); set
# TDX_GATEWAY_TDX_WORKERS=6 in run-gateway.cmd to roll back without redeploy.
# The global semaphore caps all concurrent HTTP requests.
TDX_WORKERS = max(1, min(int(os.environ.get("TDX_GATEWAY_TDX_WORKERS", "12")), 16))
_TDX_SLOTS = BoundedSemaphore(TDX_WORKERS)

# --- pytdx sidecar (added 2026-08-28): intraday 1-minute bars and ticks via
# the TDX quote-server protocol.  The official 17709 HTTP bridge only serves
# intraday daily K; /v1/tickdata below covers today's minutes for the panel's
# intraday chart.  Nothing here replaces the 17709 path for daily/sync data.
_TICKDATA_MAX_COUNT = 240
# ticks (分笔成交): 全天活跃个股可达数千笔, 单独放宽上限; 桥接内部 2000/页分页
_TICKDATA_MAX_TICKS = 40000
_TICKDATA_PY = r"C:\TdxGateway\pytdx_bridge.py"
_TICKDATA_PY_EXE = r"C:\TdxGateway\venv\Scripts\python.exe"
_TICKDATA_TIMEOUT = 60
_FIN_MAX_FIELDS = 32
_FIN_MAX_SYMBOLS = 20  # 与插件侧 _FIN_BATCH 对齐
PERIODS = {"1d", "1m", "5m", "15m", "30m", "60m"}


class GatewayHandler(BaseHTTPRequestHandler):
    server_version = "tdx-gateway/1"

    def do_GET(self) -> None:
        if self.path != "/v1/health":
            self._write(HTTPStatus.NOT_FOUND, {"error": "not found"})
            return
        if not self._authorized():
            return
        reachable = _tdx_reachable()
        self._write(HTTPStatus.OK if reachable else HTTPStatus.SERVICE_UNAVAILABLE, {"ok": reachable, "tdx_url": TDX_URL})

    def do_POST(self) -> None:
        if self.path not in {"/v1/kline", "/v1/realtime", "/v1/tickdata", "/v1/financials", "/v1/g4dl", "/v1/quotes"}:
            self._write(HTTPStatus.NOT_FOUND, {"error": "not found"})
            return
        if not self._authorized():
            return
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 131072:
                raise ValueError("invalid request size")
            body = json.loads(self.rfile.read(size).decode("utf-8"))
            if self.path == "/v1/g4dl":
                action = body.get("action", "status")
                g4date = str(body.get("date") or "").replace("-", "").strip()
                if not (g4date.isdigit() and len(g4date) == 8):
                    raise ValueError("date must be YYYYMMDD")
                if action == "start":
                    st = _g4_dl_status.get(g4date)
                    if st and st.get("status") == "downloading":
                        self._write(HTTPStatus.OK, {"status": "downloading", "date": g4date})
                        return
                    z = os.path.join(_G4_DIR, g4date + ".zip")
                    if os.path.exists(z) and os.path.getsize(z) > 1_000_000:
                        already = {"status": "ok", "msg": "already downloaded",
                                   "bytes": os.path.getsize(z)}
                        _g4_dl_status[g4date] = already
                        self._write(HTTPStatus.OK, {"status": "ok", "date": g4date})
                        return
                    _g4_dl_status[g4date] = {"status": "downloading", "msg": "", "bytes": 0}
                    threading.Thread(target=_g4_download, args=(g4date,),
                                     name="g4dl-" + g4date, daemon=True).start()
                    self._write(HTTPStatus.OK, {"status": "started", "date": g4date})
                    return
                st = _g4_dl_status.get(g4date)
                if st is None:
                    z = os.path.join(_G4_DIR, g4date + ".zip")
                    ok_zip = os.path.exists(z) and os.path.getsize(z) > 1_000_000
                    st = {"status": "ok" if ok_zip else "not_started", "msg": "", "bytes": 0}
                self._write(HTTPStatus.OK, {"date": g4date, "status": st.get("status"),
                                            "msg": st.get("msg", ""), "bytes": st.get("bytes", 0)})
                return
            symbols = body.get("symbols")
            if not isinstance(symbols, list) or not symbols or len(symbols) > MAX_SYMBOLS:
                raise ValueError(f"symbols must contain 1..{MAX_SYMBOLS} items")
            if self.path == "/v1/financials":
                raw_fields = body.get("fields") or body.get("table_list")
                if (
                    not isinstance(raw_fields, list)
                    or not raw_fields
                    or len(raw_fields) > _FIN_MAX_FIELDS
                ):
                    raise ValueError(f"financials fields must contain 1..{_FIN_MAX_FIELDS} FN ids")
                if len(symbols) > _FIN_MAX_SYMBOLS:
                    raise ValueError(f"financials supports at most {_FIN_MAX_SYMBOLS} symbols per request")
                clean_fields = []
                for field in raw_fields:
                    text = str(field).strip().upper()
                    if not text.startswith("FN") or not text[2:].isdigit():
                        raise ValueError(f"invalid FN field id: {field!r}")
                    number = int(text[2:])
                    if not 1 <= number <= 599:
                        raise ValueError(f"FN field id out of range: {text}")
                    clean_fields.append(text)
                # 桥契约要求 start_time/end_time/report_type 必填；调用方未给时
                # 缺省近两年至今、按报告期口径。键名兼容插件形状（start_time/
                # end_time/table_list，见 tdx_gateway provider）与早期形状
                # （start/end/fields）。report_type 白名单防呆（桥本身宽容）。
                today = date.today()
                default_start = (today - timedelta(days=730)).strftime("%Y%m%d")
                report_type = str(body.get("report_type") or "tag_time")
                if report_type not in {"tag_time", "announce_time", "report_time"}:
                    raise ValueError("report_type must be tag_time, announce_time or report_time")
                params = {
                    "stock_list": [str(s) for s in symbols],
                    "table_list": clean_fields,
                    "report_type": report_type,
                    "start_time": _tdx_date_arg(
                        body.get("start_time") or body.get("start") or default_start
                    ),
                    "end_time": _tdx_date_arg(
                        body.get("end_time") or body.get("end") or today.strftime("%Y%m%d")
                    ),
                }
                with _TDX_SLOTS:
                    payload = _call_tdx_method("get_financial_data", params)
                result = payload.get("result", payload) if isinstance(payload, dict) else payload
                self._write(HTTPStatus.OK, {"data": result})
                return

            if self.path == "/v1/tickdata":
                kind = body.get("kind", "bars")
                if kind not in {"bars", "ticks", "ticks_g4"}:
                    raise ValueError("kind must be bars, ticks or ticks_g4")
                if kind == "ticks_g4":
                    # g4tic 历史分笔: 单标的, date 必填, 时间为推断秒级
                    date_arg = str(body.get("date") or "").replace("-", "").strip()
                    if not (date_arg.isdigit() and len(date_arg) == 8):
                        raise ValueError("ticks_g4 requires date=YYYYMMDD")
                    if len(symbols) != 1:
                        raise ValueError("ticks_g4 supports exactly one symbol")
                    code = str(symbols[0]).split(".")[0].strip()
                    try:
                        result = _g4_symbol_ticks(code, date_arg)
                    except G4NotDownloaded:
                        self._write(HTTPStatus.NOT_FOUND, {
                            "error": "g4_not_downloaded",
                            "date": date_arg,
                            "hint": "pack not downloaded; POST /v1/g4dl {date, action:'start'}",
                        })
                        return
                    meta = result.get("meta") or {}
                    if meta.get("error"):
                        self._write(HTTPStatus.NOT_FOUND, {
                            "error": "g4_symbol_missing",
                            "date": date_arg, "detail": meta["error"],
                        })
                        return
                    self._write(HTTPStatus.OK, {"rows": {symbols[0]: result["ticks"]},
                                                "meta": meta})
                    return
                count = body.get("count", _TICKDATA_MAX_COUNT)
                cap = _TICKDATA_MAX_TICKS if kind == "ticks" else _TICKDATA_MAX_COUNT
                if not isinstance(count, int) or not 1 <= count <= cap:
                    raise ValueError(f"count must be 1..{cap}")
                date_arg = body.get("date")
                if date_arg is not None:
                    date_arg = str(date_arg).replace("-", "").strip()
                    if date_arg and (not date_arg.isdigit() or len(date_arg) != 8):
                        raise ValueError("date must be YYYYMMDD")
                    date_arg = date_arg or None
                rows = _tickdata_rows(symbols, kind, count, date_arg)
                self._write(HTTPStatus.OK, {"rows": rows})
                return

            if self.path == "/v1/quotes":
                # 批量五档盘口 via pytdx (公共行情服务器): TdxW 快照盘口是客户端
                # UI 缓存刮削, 只有客户端正在显示的标的才有完整档位 (2026-09-01
                # 实测), 本通道与客户端显示状态无关, 供插件侧兜底。
                rows = _quotes_rows(symbols)
                self._write(HTTPStatus.OK, {"rows": rows})
                return

            if self.path == "/v1/realtime":
                with ThreadPoolExecutor(max_workers=min(TDX_WORKERS, len(symbols))) as executor:
                    futures = {symbol: executor.submit(_fetch_snapshot, symbol) for symbol in symbols}
                    rows = {symbol: futures[symbol].result() for symbol in symbols}
                self._write(HTTPStatus.OK, {"rows": rows})
                return

            period = body.get("period")
            if period not in PERIODS:
                raise ValueError("unsupported period")
            adjust = body.get("adjust", "none")
            if adjust not in {"none", "front", "back"}:
                raise ValueError("adjust must be none, front, or back")
            with ThreadPoolExecutor(max_workers=min(TDX_WORKERS, len(symbols))) as executor:
                futures = {
                    symbol: executor.submit(
                        _fetch_symbol, symbol, period, body.get("start"), body.get("end"), adjust,
                    )
                    for symbol in symbols
                }
                # Preserve the caller's symbol order.  A failed TDX request is
                # surfaced as a 502 for the whole request; no empty rows are
                # fabricated and the sync job can report the real failure.
                rows = {symbol: futures[symbol].result() for symbol in symbols}
            self._write(HTTPStatus.OK, {"rows": rows})
        except ValueError as exc:
            self._write(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except RuntimeError as exc:
            self._write(HTTPStatus.BAD_GATEWAY, {"error": str(exc)})

    def _authorized(self) -> bool:
        if not TOKEN:
            self._write(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "TDX_GATEWAY_TOKEN is not configured"})
            return False
        if self.client_address[0] not in ALLOWED_CLIENTS:
            self._write(HTTPStatus.FORBIDDEN, {"error": "caller IP is not allowed"})
            return False
        if self.headers.get("Authorization") != f"Bearer {TOKEN}":
            self._write(HTTPStatus.UNAUTHORIZED, {"error": "invalid token"})
            return False
        return True

    def _write(self, status: HTTPStatus, payload: dict) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, _format: str, *_args) -> None:
        pass


def _tdx_reachable() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 17709), timeout=2):
            return True
    except OSError:
        return False


def _call_tdx(symbols: list[str], period: str, start: str | None, end: str | None, adjust: str):
    params = {"stock_list": symbols, "period": period, "dividend_type": adjust}
    # TDX accepts calendar dates for minute bars.  It rejects timestamps such
    # as ``YYYYMMDD HH:MM:SS``; fetch the required calendar day(s), then apply
    # the precise time bounds locally to the returned real records.
    if start:
        params["start_time"] = _tdx_date_arg(start)
    if end:
        params["end_time"] = _tdx_date_arg(end)
    return _call_tdx_method("get_market_data", params)


def _call_tdx_method(method: str, params: dict):
    request = Request(
        TDX_URL,
        data=json.dumps({"id": 1, "method": method, "params": params}).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"TDX Quant request failed: {exc}") from exc


def _fetch_symbol(symbol: str, period: str, start: str | None, end: str | None, adjust: str) -> list[dict]:
    # ThreadingHTTPServer may serve more than one panel request at a time, so
    # the bound belongs here as well as on each request's executor.
    with _TDX_SLOTS:
        result = _call_tdx([symbol], period, start, end, adjust)
    return _filter_rows(_rows_by_symbol(result, [symbol], period)[symbol], period, start, end)


def _fetch_snapshot(symbol: str) -> dict:
    """Fetch one actual TDX snapshot; callers receive a 502 on a bad reply."""
    with _TDX_SLOTS:
        payload = _call_tdx_method("get_market_snapshot", {"stock_code": symbol})
    result = payload.get("result", payload) if isinstance(payload, dict) else None
    if isinstance(result, dict) and isinstance(result.get("Value"), dict):
        result = result["Value"]
    if not isinstance(result, dict):
        raise RuntimeError("TDX Quant returned an unsupported snapshot response")
    # QuoteService requires Now and LastClose.  Refuse malformed values here
    # instead of manufacturing a price from historical K-lines.
    if "Now" not in result or "LastClose" not in result:
        raise RuntimeError("TDX Quant snapshot lacks Now or LastClose")
    return result


def _run_bridge_job(job: dict) -> dict:
    """Run the pytdx bridge subprocess and return its parsed JSON payload.

    The bridge runs in a short-lived interpreter so a slow or unreachable
    quote server can never wedge the gateway's request threads; a hard
    subprocess timeout converts any hang into a clean 502.
    """
    try:
        proc = subprocess.run(
            [_TICKDATA_PY_EXE, _TICKDATA_PY],
            input=json.dumps(job).encode("utf-8"),
            capture_output=True,
            timeout=_TICKDATA_TIMEOUT,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"bridge subprocess timed out after {_TICKDATA_TIMEOUT}s") from exc
    if proc.returncode != 0:
        detail = proc.stderr.decode("utf-8", errors="replace")[-300:]
        raise RuntimeError(f"bridge subprocess failed: {detail}")
    try:
        return json.loads(proc.stdout.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError("bridge subprocess returned invalid JSON") from exc


def _tickdata_rows(symbols: list[str], kind: str, count: int, date_arg: str | None = None) -> dict:
    """Fetch recent intraday bars/ticks via the pytdx bridge subprocess.

    date_arg (YYYYMMDD, optional): kind=ticks 时取指定交易日的分笔成交;
    缺省为当日分笔。行形状: list[dict]。
    """
    job: dict = {"symbols": symbols, "kind": kind, "count": count}
    if date_arg:
        job["date"] = date_arg
    rows = _run_bridge_job(job).get("rows")
    if not isinstance(rows, dict):
        raise RuntimeError("tickdata subprocess returned no rows map")
    return {str(key): list(value or []) for key, value in rows.items()}


def _quotes_rows(symbols: list[str]) -> dict:
    """批量五档盘口 via pytdx bridge (kind=quotes, 公共行情服务器)。

    TdxW Quant 快照的盘口是客户端 UI 缓存刮削品 — 只有客户端正在显示的标的
    才有完整档位 (2026-09-01 实测), 本通道与客户端显示状态无关。
    行形状: per-symbol dict (bid1..5/bid_vol1..5/ask1..5/ask_vol1..5/price/
    last_close), 与 tickdata 的 list-of-rows 不同, 勿混用。
    """
    rows = _run_bridge_job({"symbols": symbols, "kind": "quotes", "count": 0}).get("rows")
    if not isinstance(rows, dict):
        raise RuntimeError("quotes subprocess returned no rows map")
    return rows


# ── g4tic 历史分笔 (通达信官方全市场分笔打包; 普及版会员数据通道) ─────────
# 数据: https://www.tdx.com.cn/products/data/data/g4tic/{YYYYMMDD}.zip (~100MB)
# 已由 C:\tdx-minute-backfill\tdx_download.py 批量下载 2025-07-01..2026-05-18。
# 每笔: varint 5 元组 [Δt计数, 价格增量(分*100, signed), 量(手), signed, ?]。
# 时间为 Δt 计数×校准单位的推断值 (亚秒粒度, 跨标的校验误差<1%); 价格/量真实。
_G4_DIR = r"C:\tdx-minute-backfill\zips"
_G4_URL = "https://www.tdx.com.cn/products/data/data/g4tic/{}.zip"
_G4_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
_g4_data_cache = {}                # date -> (htc_path, bytes) (仅缓存最近一个, ~100MB)
_g4_opens_cache: dict = {}         # date -> {(MKT, code): open_yuan}
_g4_dl_status: dict = {}           # date -> {status, msg, bytes}
_g4_extract_lock = threading.Lock()

if r"C:\tdx-minute-backfill" not in __import__("sys").path:
    __import__("sys").path.insert(0, r"C:\tdx-minute-backfill")
import minute_assign as _g4ma  # noqa: E402  (parse_block/price_ticks/assign_minutes 复用)


class G4NotDownloaded(RuntimeError):
    """该日期 g4tic 分笔包尚未下载。"""


def _g4_htc_path(date_str: str):
    """确保 extracted/{date}.htc 存在; zip 缺失返回 None。"""
    htc = os.path.join(_G4_DIR, "extracted", f"{date_str}.htc")
    if os.path.exists(htc) and os.path.getsize(htc) > 50_000_000:
        return htc
    z = os.path.join(_G4_DIR, f"{date_str}.zip")
    if not os.path.exists(z) or os.path.getsize(z) < 1_000_000:
        return None
    if not _g4_extract_lock.acquire(blocking=False):
        return None  # 另一线程正在解压
    try:
        os.makedirs(os.path.dirname(htc), exist_ok=True)
        with zipfile.ZipFile(z) as zz:
            name = [n for n in zz.namelist() if n.endswith(".htc")][0]
            with zz.open(name) as src, open(htc + ".part", "wb") as dst:
                while True:
                    chunk = src.read(1 << 20)
                    if not chunk:
                        break
                    dst.write(chunk)
        os.replace(htc + ".part", htc)
        return htc
    finally:
        _g4_extract_lock.release()


def _iter_g4_blocks(data: bytes):
    """高效遍历 htc blocks (C 级 bytes.find 替代逐字节循环)。"""
    n = len(data)
    if n < 16:
        return
    _, _, _, total = struct.unpack_from("<IIII", data, 0)
    off = 16
    count = 0
    while off < n and count < total:
        s1 = data.find(b"\x00", off)
        if s1 < 0:
            return
        s2 = data.find(b"\x00", s1 + 1)
        if s2 < 0:
            return
        sym = data[s1 + 1:s2].replace(b"\x00", b"").decode("ascii", "replace")
        off = s2 + 1
        if off + 22 > n:
            return
        _date, a, b, c = struct.unpack_from("<IIII", data, off)
        off += 16
        f = struct.unpack_from("<f", data, off)[0]
        off += 4
        off += 2  # flags
        payload = b""
        if a and b:
            raw = data[off:off + b]
            off += b
            try:
                payload = zlib.decompress(raw)
            except zlib.error:
                payload = b""
        count += 1
        yield sym, a, b, c, f, payload


def _g4_tick_times(recs):
    """逐笔推断时刻 (秒, 相对 09:30)。返回 {idx: secs}; 盘后定价记录无可靠时刻。

    校准逻辑与 minute_assign.assign_minutes 一致:
    上午 u = 7200/Σf1(首条..午休); 午后独立 3 轮迭代校准; >7260s = 盘后定价丢弃。
    """
    n = len(recs)
    if n == 0:
        return {}
    f1 = [r[0] for r in recs]
    times = {}
    if n == 1:
        times[0] = 0
        return times
    lunch = max(range(1, n), key=lambda i: f1[i])
    s_m = sum(f1[1:lunch])
    u = 7200.0 / s_m if s_m > 0 else _g4ma.U_DEFAULT
    if not (_g4ma.U_LO <= u <= _g4ma.U_HI):
        u = _g4ma.U_DEFAULT
    cum = 0.0
    times[0] = 0
    for i in range(1, lunch):
        cum += f1[i] * u
        times[i] = min(int(cum), 7199)
    a_rest = f1[lunch + 1:]
    u_a = u
    for _ in range(3):
        cum = 0.0
        k = 0
        for dt in a_rest:
            cum += dt * u_a
            if cum > 7260:
                break
            k += 1
        s = sum(a_rest[:k])
        if s <= 0:
            break
        u_new = 7200.0 / s
        if abs(u_new - u_a) < 1e-9:
            u_a = u_new
            break
        u_a = u_new
    times[lunch] = 7200  # 13:00:00 首条
    cum = 0.0
    for i in range(lunch + 1, n):
        cum += f1[i] * u_a
        if cum > 7260:
            continue  # 盘后定价, 无可靠时刻
        times[i] = min(7200 + int(cum), 14400)  # 15:00 收盘竞价归 14400
    return times


def _secs_to_hhmmss(secs: int, morning: bool) -> str:
    base = 34200 + secs if morning else 46800 + (secs - 7200)
    h = base // 3600
    m = (base % 3600) // 60
    s = base % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def _ensure_g4day(date_str: str):
    """g4day 行情快照 zip (含 .cod/.md1 开盘价), 缺失时从官方地址下载。"""
    p = os.path.join(_G4_DIR, "g4day_" + date_str + ".zip")
    if os.path.exists(p) and os.path.getsize(p) > 1_000_000:
        return p
    url = "https://www.tdx.com.cn/products/data/data/g4day/" + date_str + ".zip"
    for attempt in range(8):
        try:
            from urllib.request import urlretrieve
            urlretrieve(url, p)
            if os.path.getsize(p) > 1_000_000:
                return p
        except Exception:  # noqa: BLE001
            time.sleep(3 + attempt * 2)
    return None


def _load_md1_opens(g4day_zip: str) -> dict:
    """{(MKT, 6位代码): open_yuan} — 与 vm_pipeline.load_md1_opens 相同格式。"""
    opens = {}
    with zipfile.ZipFile(g4day_zip) as zz:
        cods, mds = {}, {}
        for name in zz.namelist():
            base = os.path.basename(name)
            if base.endswith(".cod"):
                cods[base[:2]] = zz.read(name)
            elif base.endswith(".md1"):
                mds[base[:2]] = zz.read(name)
    for mkt, cod in cods.items():
        md = mds.get(mkt)
        if md is None:
            continue
        for i in range(min(len(cod) // 150, len(md) // 512)):
            sym = cod[i * 150:i * 150 + 6].decode("ascii", "replace")
            seg = md[i * 512:(i + 1) * 512]
            d = struct.unpack_from("<5d", seg, 4)
            opens[(mkt.upper(), sym)] = d[1]
    return opens


def _code_market(code: str) -> str:
    if code.startswith(("600", "601", "603", "605", "688", "689", "51", "56", "58")):
        return "SH"
    if code.startswith(("43", "83", "87", "92")):
        return "BJ"
    return "SZ"


def _g4_symbol_ticks(code: str, date_str: str) -> dict:
    """从 g4tic htc 解析单只股票全部分笔。code 为 6 位数字代码。"""
    htc = _g4_htc_path(date_str)
    if htc is None:
        raise G4NotDownloaded(date_str)
    g4day = _ensure_g4day(date_str)
    if g4day is None:
        raise RuntimeError("g4day snapshot unavailable for " + date_str)
    opens = _g4_opens_cache.get(date_str)
    if opens is None:
        opens = _load_md1_opens(g4day)
        _g4_opens_cache[date_str] = opens
    md1_open = opens.get((_code_market(code), str(code).strip()))
    if md1_open is None:
        raise RuntimeError("no md1 open for " + str(code) + " on " + date_str)
    cached = _g4_data_cache.get(date_str)
    if cached is None or cached[0] != htc:
        with open(htc, "rb") as fh:
            cached = (htc, fh.read())
        _g4_data_cache.clear()
        _g4_data_cache[date_str] = cached
    blob = cached[1]
    target = str(code).strip()
    for sym, _a, _b, _c, open_yuan, payload in _iter_g4_blocks(blob):
        if sym.strip() != target:
            continue
        if not payload:
            return {"ticks": [], "meta": {"date": date_str, "symbol": code, "total_vol": 0}}
        _hdr, recs = _g4ma.parse_block(payload)
        prices = _g4ma.price_ticks(recs, md1_open)
        times = _g4_tick_times(recs)
        ticks = []
        total_vol = 0.0
        high = 0.0
        low = 0.0
        for i, r in enumerate(recs):
            vol = r[2]
            price = prices[i] / 100.0
            t = times.get(i)
            time_str = _secs_to_hhmmss(t, t < 7200) if t is not None else ""
            if vol <= 0:
                # 竞价探测单 (量 0): 保留真实价格, 量记 0
                ticks.append({"time": time_str, "price": price, "volume": 0,
                              "num": 0, "buyorsell": None, "inferred": True})
                continue
            total_vol += vol
            high = max(high, price)
            low = min(low, price) if low > 0 else price
            ticks.append({"time": time_str, "price": price, "volume": vol,
                          "num": 0, "buyorsell": None, "inferred": True})
        return {
            "ticks": ticks,
            "meta": {"date": date_str, "symbol": code,
                     "total_vol": round(total_vol, 2), "high": high, "low": low,
                     "precision": "second_inferred"},
        }
    return {"ticks": [], "meta": {"date": date_str, "symbol": code,
                                  "error": f"symbol {code} not in g4 pack"}}


def _g4_download(date_str: str) -> None:
    """后台下载 g4tic 分笔包 (断点续传, 临时 404 重试; 复用 tdx_download 逻辑)。"""
    st = _g4_dl_status.setdefault(date_str, {"status": "downloading", "msg": "", "bytes": 0})
    if st.get("status") == "ok" and os.path.exists(os.path.join(_G4_DIR, f"{date_str}.zip")):
        return
    st.update(status="downloading", msg="", bytes=0)
    dest = os.path.join(_G4_DIR, f"{date_str}.zip")
    tmp = dest + ".part"
    try:
        for attempt in range(10):
            rc = subprocess.run(
                ["curl", "-f", "-s", "-S", "-C", "-", "-A", _G4_UA,
                 "--max-time", "2400", "-o", tmp, _G4_URL.format(date_str)],
                capture_output=True, text=True, timeout=2500,
            ).returncode
            sz = os.path.getsize(tmp) if os.path.exists(tmp) else 0
            st["bytes"] = sz
            if rc == 0 and sz > 10_000_000:
                os.replace(tmp, dest)
                st.update(status="ok", bytes=sz)
                return
            if rc == 22 and sz <= 0:
                # 真伪 404 探测: HEAD 有 content-length 则为临时 404, 重试
                head = subprocess.run(
                    ["curl", "-s", "-I", "--max-time", "20", _G4_URL.format(date_str)],
                    capture_output=True, text=True, timeout=40,
                ).stdout
                for ln in head.splitlines():
                    if ln.lower().startswith("content-length:") and int(ln.split(":", 1)[1].strip() or 0) > 0:
                        break
                else:
                    st.update(status="fail", msg="server has no pack for this date (404)")
                    return
            time.sleep(min(120, 8 + 10 * attempt))
        st.update(status="fail", msg="max attempts reached")
    except Exception as exc:  # noqa: BLE001
        st.update(status="fail", msg=str(exc))


def _tdx_date_arg(value: str) -> str:
    digits = "".join(char for char in str(value) if char.isdigit())
    return digits[:8] if len(digits) >= 8 else str(value)


def _filter_rows(rows: list[dict], period: str, start: str | None, end: str | None) -> list[dict]:
    """Apply caller time bounds after TDX minute requests have been day-bounded."""
    if not rows or (not start and not end):
        return rows
    start_key = _bound_key(start, end=False) if start else None
    end_key = _bound_key(end, end=True) if end else None
    filtered: list[dict] = []
    for row in rows:
        value = row.get("datetime") if period != "1d" else row.get("date")
        row_key = _bound_key(str(value), end=False) if value is not None else None
        if row_key is None:
            continue
        if start_key and row_key < start_key:
            continue
        if end_key and row_key > end_key:
            continue
        filtered.append(row)
    return filtered


def _bound_key(value: str, *, end: bool) -> str | None:
    digits = "".join(char for char in str(value) if char.isdigit())
    if len(digits) < 8:
        return None
    if len(digits) == 8:
        return digits + ("235959" if end else "000000")
    return (digits + "000000")[:14]


def _rows_by_symbol(payload, symbols: list[str], period: str) -> dict[str, list[dict]]:
    """Accept the documented JSON-RPC result variants, reject unknown shapes."""
    result = payload.get("result", payload) if isinstance(payload, dict) else None
    if not isinstance(result, dict):
        raise RuntimeError("TDX Quant returned an unsupported response shape")
    if isinstance(result.get("rows"), dict):
        return {symbol: list(result["rows"].get(symbol) or []) for symbol in symbols}
    if all(isinstance(result.get(symbol), list) for symbol in symbols):
        return {symbol: list(result.get(symbol) or []) for symbol in symbols}
    values = result.get("Value")
    if isinstance(values, dict):
        return {
            symbol: _tdx_columnar_rows(values.get(symbol), period)
            for symbol in symbols
        }
    raise RuntimeError(f"TDX Quant returned an unsupported response shape: {list(result)[:8]}")


def _tdx_columnar_rows(values, period: str) -> list[dict]:
    """Convert the documented TDX ``Value`` column-map to row records.

    TDX returns a dict of lists per symbol (``Date``, ``Time``, ``Open`` ...),
    rather than row objects.  We preserve the provider values and only normalize
    names/timestamps required by Tick Stock Panel's provider contract.
    """
    if not isinstance(values, dict):
        return []
    dates = values.get("Date")
    if not isinstance(dates, list):
        return []
    fields = {
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
        "Amount": "amount",
        "ForwardFactor": "forward_factor",
    }
    rows: list[dict] = []
    times = values.get("Time") if isinstance(values.get("Time"), list) else []
    for index, date_value in enumerate(dates):
        date_text = str(date_value).split(".", 1)[0]
        if not date_text or date_text == "0":
            continue
        row: dict[str, object] = {}
        date_iso = f"{date_text[:4]}-{date_text[4:6]}-{date_text[6:8]}"
        if period == "1d":
            row["date"] = date_iso
        else:
            time_value = times[index] if index < len(times) else None
            time_text = str(time_value).split(".", 1)[0] if time_value is not None else ""
            if time_text and time_text != "0":
                time_text = time_text.zfill(6)
                row["datetime"] = (
                    f"{date_iso} "
                    f"{time_text[:2]}:{time_text[2:4]}:{time_text[4:6]}"
                )
            else:
                row["datetime"] = date_iso
        for source, target in fields.items():
            column = values.get(source)
            if isinstance(column, list) and index < len(column):
                row[target] = column[index]
        rows.append(row)
    return rows


if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("TDX_GATEWAY_TOKEN must be set")
    ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), GatewayHandler).serve_forever()
