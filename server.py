"""
Market Research Dashboard — FastAPI Server (v3: BTC Delta + Carousel Fix)
==========================================================================
Multi-market relay with BTC delta tracking.

Endpoints:
  GET  /                        → index.html
  GET  /api/markets             → list of all tracked markets
  GET  /api/market/{slug}/history → CSV data as JSON
  WS   /ws/live-chart           → real-time multi-market price stream with BTC delta
"""

from __future__ import annotations

import asyncio
import csv
import json
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse

import market_discovery
from market_discovery import (
    MarketState,
    market_rotation_loop,
    registry,
    btc_tracker,
)
from ws_logger import market_manager, DATA_DIR

# ── Constants ────────────────────────────────────────────────────────────

HOST = "0.0.0.0"
PORT = 8765
TICK_INTERVAL = 1.0
FRONTEND_FILE = Path(__file__).parent / "index.html"


# ── WebSocket Client Manager ────────────────────────────────────────────

class ClientManager:
    def __init__(self):
        self._clients: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self._clients.add(ws)
        print(f"🖥️  Frontend client connected ({len(self._clients)} total)")

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            self._clients.discard(ws)
        print(f"🖥️  Frontend client disconnected ({len(self._clients)} total)")

    async def broadcast(self, message: dict):
        if not self._clients:
            return
        payload = json.dumps(message)
        disconnected = []
        async with self._lock:
            clients = list(self._clients)
        for client in clients:
            try:
                await client.send_text(payload)
            except Exception:
                disconnected.append(client)
        for client in disconnected:
            async with self._lock:
                self._clients.discard(client)

    @property
    def count(self) -> int:
        return len(self._clients)


client_manager = ClientManager()


# ── Helpers ────────────────────────────────────────────────────────────

def build_market_list() -> list[dict]:
    """Build ordered market list (chronological: PAST → CURRENT → NEXT)."""
    markets = registry.get_navigable()
    result = []
    for m in markets:
        d = m.to_dict()
        # Only attach price_to_beat for past and current markets.
        # Future (next) markets don't have a PTB yet — it forms at round start.
        if m.role in ("past", "current"):
            d["price_to_beat"] = btc_tracker.get_price_to_beat(m.slug)
        else:
            d["price_to_beat"] = 0  # not yet available for future
        d["current_btc"] = btc_tracker.current_btc
        result.append(d)
    return result


# ── Callbacks ────────────────────────────────────────────────────────────

async def on_new_current(market: MarketState):
    await market_manager.add_market(market)
    await client_manager.broadcast({
        "type": "MARKET_UPDATE",
        "event": "new_current",
        "markets": build_market_list(),
        "current_slug": market.slug,
    })
    print(f"📣 New current market broadcast: {market.slug}")


async def on_new_next(market: MarketState):
    await market_manager.add_market(market)
    await client_manager.broadcast({
        "type": "MARKET_UPDATE",
        "event": "new_next",
        "markets": build_market_list(),
        "next_slug": market.slug,
    })
    print(f"📣 New next market broadcast: {market.slug}")


async def on_rotation():
    await client_manager.broadcast({
        "type": "MARKET_ROTATION",
        "markets": build_market_list(),
        "current_slug": registry.current_slug,
        "next_slug": registry.next_slug,
    })
    print("📣 Market rotation broadcast")


# ── Background Relay Ticker ──────────────────────────────────────────────

async def relay_ticker():
    """
    Every second, push price + BTC delta snapshots for ALL tracked markets.
    """
    print("📡 Relay ticker started (1Hz)")
    while True:
        try:
            all_snapshots = await market_manager.get_all_snapshots()

            if all_snapshots and client_manager.count > 0:
                ticks = {}
                for slug, snap in all_snapshots.items():
                    mkt = registry.get(slug)
                    if not mkt:
                        continue
                    # Get BTC delta for this market
                    btc_snap = await btc_tracker.get_snapshot(slug)
                    # Only include price_to_beat for past/current markets
                    ptb = btc_snap["price_to_beat"] if mkt.role in ("past", "current") else 0
                    btc_delta = round(btc_snap["btc_delta"], 2) if mkt.role in ("past", "current") else 0
                    ticks[slug] = {
                        "yes_price": round(snap["yes_price"], 6),
                        "no_price": round(snap["no_price"], 6),
                        "remaining_seconds": mkt.remaining_seconds,
                        "role": mkt.role,
                        "current_btc": btc_snap["current_btc"],
                        "price_to_beat": ptb,
                        "btc_delta": btc_delta,
                        "last_changed_at": snap.get("last_update", 0),
                    }

                if ticks:
                    await client_manager.broadcast({
                        "type": "multi_price_tick",
                        "ticks": ticks,
                        "timestamp": time.time(),
                    })

            await asyncio.sleep(TICK_INTERVAL)

        except asyncio.CancelledError:
            print("🛑 Relay ticker cancelled")
            return
        except Exception as e:
            print(f"❌ Relay ticker error: {e}")
            await asyncio.sleep(TICK_INTERVAL)


# ── Lifespan ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    print("=" * 60)
    print("📊 Market Research Dashboard v3 — BTC Delta + Carousel")
    print("=" * 60)

    # Start BTC price tracking first (so we have a price for the first market)
    await btc_tracker.start()
    await asyncio.sleep(2)  # Let first BTC price fetch complete

    await market_manager.start()
    rotation_task = asyncio.create_task(
        market_rotation_loop(on_new_current, on_new_next, on_rotation)
    )
    ticker_task = asyncio.create_task(relay_ticker())

    print(f"🌐 Dashboard: http://localhost:{PORT}")
    yield

    print("🛑 Shutting down...")
    rotation_task.cancel()
    ticker_task.cancel()
    await btc_tracker.stop()
    await market_manager.stop()
    try:
        await rotation_task
    except asyncio.CancelledError:
        pass
    try:
        await ticker_task
    except asyncio.CancelledError:
        pass
    print("👋 Dashboard stopped")


# ── FastAPI App ──────────────────────────────────────────────────────────

app = FastAPI(
    title="Market Research Dashboard v3",
    description="Multi-market carousel with BTC delta tracking",
    version="3.0.0",
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
async def serve_dashboard():
    return FileResponse(FRONTEND_FILE)


@app.get("/api/markets", tags=["Markets"])
async def list_markets():
    markets = build_market_list()
    all_snaps = await market_manager.get_all_snapshots()
    for m in markets:
        snap = all_snaps.get(m["slug"], {})
        m["yes_price"] = snap.get("yes_price", 0.0)
        m["no_price"] = snap.get("no_price", 0.0)
    return {"markets": markets, "current_slug": registry.current_slug}


@app.get("/api/market/{slug}/history", tags=["Markets"])
async def get_market_history(slug: str):
    csv_path = DATA_DIR / f"{slug}.csv"
    if not csv_path.exists():
        return JSONResponse(status_code=404, content={"error": f"No data for {slug}"})

    data_points = []
    try:
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                point = {
                    "timestamp": float(row["timestamp"]),
                    "timestamp_readable": row["timestamp_readable"],
                    "yes_price": float(row["yes_price"]),
                    "no_price": float(row["no_price"]),
                }
                # New columns (may not exist in old CSVs)
                if "btc_price" in row:
                    point["btc_price"] = float(row.get("btc_price", 0) or 0)
                if "price_to_beat" in row:
                    point["price_to_beat"] = float(row.get("price_to_beat", 0) or 0)
                if "btc_delta" in row:
                    point["btc_delta"] = float(row.get("btc_delta", 0) or 0)
                data_points.append(point)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

    return {"slug": slug, "count": len(data_points), "data": data_points}


@app.websocket("/ws/live-chart")
async def live_chart_ws(ws: WebSocket):
    await client_manager.connect(ws)

    all_snaps = await market_manager.get_all_snapshots()
    markets = build_market_list()
    for m in markets:
        snap = all_snaps.get(m["slug"], {})
        m["yes_price"] = snap.get("yes_price", 0.0)
        m["no_price"] = snap.get("no_price", 0.0)

    # Get initial BTC data
    btc_snap = await btc_tracker.get_snapshot(registry.current_slug)

    try:
        await ws.send_text(json.dumps({
            "type": "init",
            "markets": markets,
            "current_slug": registry.current_slug,
            "next_slug": registry.next_slug,
            "btc": btc_snap,
        }))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        await client_manager.disconnect(ws)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host=HOST, port=PORT, log_level="info", reload=False)
