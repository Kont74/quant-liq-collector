"""
Module: liq_collector.py
Purpose: Record Bybit linear-perp liquidation events (public allLiquidation
         WebSocket stream). Liquidations are NOT backfillable — this collector
         IS the historical record from the day it starts.
Design:  Runs inside an hourly GitHub Actions job. Listens for RUN_SECONDS
         (default 3300s = 55 min), then exits so the job can commit. Hourly
         cron restarts it — near-continuous coverage with ~5 min/hour blind
         spots (acceptable; noted as evidence-grade metadata).
Universe: top N linear symbols by 24h turnover (fetched at start of each run)
         — bounds subscription count while covering where liquidations happen.
Output:  Appends to data/bybit_liq_YYYY-MM-DD.csv
         Columns: recv_ts_utc, exch_ts_ms, symbol, side, size, price, raw_json
         NOTE on side: per Bybit v5 docs, side is the DIRECTION OF THE
         LIQUIDATION ORDER — "Buy" = a SHORT position was liquidated (forced
         buy-back); "Sell" = a LONG was liquidated. Stored verbatim; interpret
         at analysis time. Raw JSON always preserved.
Deps:    websocket-client, requests. Python 3.10+.
"""

import csv
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
    import websocket
except ImportError:
    sys.exit("Missing dependency: pip install websocket-client requests")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("liq")

WS_URL = "wss://stream.bybit.com/v5/public/linear"
TICKERS_URL = "https://api.bybit.com/v5/market/tickers"
TOP_N = int(os.environ.get("LIQ_TOP_N", "100"))
RUN_SECONDS = int(os.environ.get("LIQ_RUN_SECONDS", "3300"))
DATA_DIR = Path("data")
CSV_COLUMNS = ["recv_ts_utc", "exch_ts_ms", "symbol", "side", "size", "price", "raw_json"]

rows_written = 0
write_lock = threading.Lock()


def top_symbols() -> list:
    """Top-N linear symbols by 24h turnover. Failure here must fail the run loudly."""
    resp = requests.get(TICKERS_URL, params={"category": "linear"}, timeout=20)
    resp.raise_for_status()
    tickers = resp.json().get("result", {}).get("list", [])
    usdt = [t for t in tickers if t.get("symbol", "").endswith("USDT")]
    ranked = sorted(usdt, key=lambda t: float(t.get("turnover24h", 0) or 0), reverse=True)
    syms = [t["symbol"] for t in ranked[:TOP_N]]
    if len(syms) < 10:
        raise RuntimeError(f"only {len(syms)} symbols ranked — tickers payload suspect")
    logger.info(f"subscribing to top {len(syms)} symbols by turnover")
    return syms


def csv_path() -> Path:
    DATA_DIR.mkdir(exist_ok=True)
    return DATA_DIR / f"bybit_liq_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.csv"


def append_rows(rows: list) -> None:
    global rows_written
    path = csv_path()
    is_new = not path.exists()
    with write_lock:
        with path.open("a", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            if is_new:
                writer.writerow(CSV_COLUMNS)
            writer.writerows(rows)
        rows_written += len(rows)


def on_message(ws, message: str) -> None:
    try:
        msg = json.loads(message)
    except json.JSONDecodeError:
        return
    if "topic" not in msg or not str(msg["topic"]).startswith("allLiquidation"):
        return
    recv = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    rows = []
    for item in msg.get("data", []) or []:
        rows.append([recv,
                     item.get("T", msg.get("ts", "")),
                     item.get("s", ""),
                     item.get("S", ""),
                     item.get("v", ""),
                     item.get("p", ""),
                     json.dumps(item, separators=(",", ":"))])
    if rows:
        append_rows(rows)


def run_session(symbols: list, deadline: float) -> None:
    """One WebSocket session: subscribe, ping-loop, consume until deadline or drop."""
    ws = websocket.WebSocket()
    ws.connect(WS_URL, timeout=15)
    for i in range(0, len(symbols), 10):                     # <=10 args per subscribe op
        chunk = symbols[i:i + 10]
        ws.send(json.dumps({"op": "subscribe",
                            "args": [f"allLiquidation.{s}" for s in chunk]}))
        time.sleep(0.1)
    logger.info("subscribed; listening")
    last_ping = time.time()
    ws.settimeout(30)
    while time.time() < deadline:
        if time.time() - last_ping > 20:
            ws.send(json.dumps({"op": "ping"}))
            last_ping = time.time()
        try:
            message = ws.recv()
            if message:
                on_message(ws, message)
        except websocket.WebSocketTimeoutException:
            continue                                          # quiet market, keep waiting
    ws.close()


def main() -> None:
    deadline = time.time() + RUN_SECONDS
    symbols = top_symbols()
    session = 0
    while time.time() < deadline - 10:
        session += 1
        try:
            run_session(symbols, deadline)
        except Exception as exc:  # noqa: BLE001 — reconnect until the window ends
            remaining = int(deadline - time.time())
            logger.warning(f"session {session} dropped ({exc}); {remaining}s left; reconnecting")
            time.sleep(min(5, max(1, remaining)))
    logger.info(f"window complete: {rows_written} liquidation rows written across "
                f"{session} session(s)")
    # Zero rows in a whole hour on 100 top perps is possible in dead-calm tape but
    # rare — worth a loud note so persistent zeros get investigated, not ignored.
    if rows_written == 0:
        logger.warning("ZERO liquidations captured this window — plausible in calm "
                       "tape; investigate if it repeats across consecutive runs")


if __name__ == "__main__":
    main()
