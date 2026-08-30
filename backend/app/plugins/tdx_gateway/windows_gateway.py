"""Run on the Windows TDX VM, not on the Tick Stock Panel host.

It exposes only read-only K-line and explicit-symbol snapshot requests to the
allowed Linux IP and forwards them to the local TDX Quant HTTP process at
127.0.0.1:17709.  It deliberately has no trading endpoint and refuses unknown
callers rather than substituting another market-data source.
"""
from __future__ import annotations

import json
import os
import socket
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
# TDX documents one symbol per market-data call.  Six in-flight calls were
# verified against the Windows client (all rows returned, faster than four);
# the global semaphore caps all concurrent HTTP requests.
TDX_WORKERS = max(1, min(int(os.environ.get("TDX_GATEWAY_TDX_WORKERS", "6")), 6))
_TDX_SLOTS = BoundedSemaphore(TDX_WORKERS)
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
        if self.path not in {"/v1/kline", "/v1/realtime"}:
            self._write(HTTPStatus.NOT_FOUND, {"error": "not found"})
            return
        if not self._authorized():
            return
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 131072:
                raise ValueError("invalid request size")
            body = json.loads(self.rfile.read(size).decode("utf-8"))
            symbols = body.get("symbols")
            if not isinstance(symbols, list) or not symbols or len(symbols) > MAX_SYMBOLS:
                raise ValueError(f"symbols must contain 1..{MAX_SYMBOLS} items")
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
