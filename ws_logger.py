"""
Market Research Dashboard — WebSocket Data Logger (v3: Multi-Market + BTC Delta)
==================================================================================
Supports multiple simultaneous market subscriptions over a single WS connection.
Each market gets its own PriceState and CSV file.
CSV now includes btc_price, price_to_beat, and btc_delta columns.

Compatible with websockets v14+/v15+ (no .closed attribute).
"""

from __future__ import annotations

import asyncio
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import websockets
import aiohttp

from market_discovery import MarketState, btc_tracker

# ── Constants ────────────────────────────────────────────────────────────

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
DATA_DIR = Path(__file__).parent / "data"

INITIAL_RECONNECT_DELAY = 1.0
MAX_RECONNECT_DELAY = 30.0
RECONNECT_BACKOFF_FACTOR = 2.0
GAMMA_PRICE_POLL_INTERVAL = 1.0  # Poll Gamma API every 1s as fallback
GAMMA_API_URL = "https://gamma-api.polymarket.com/markets"


# ── Per-Market Price State ───────────────────────────────────────────────

class PriceState:
    def __init__(self):
        self.yes_price: float = 0.0
        self.no_price: float = 0.0
        self.last_update: float = 0.0
        self.last_changed_at: float = 0.0  # Only updates when price actually changes
        self._lock = asyncio.Lock()

    async def update(self, yes_price: float | None = None, no_price: float | None = None):
        async with self._lock:
            changed = False
            if yes_price is not None and yes_price != self.yes_price:
                self.yes_price = yes_price
                changed = True
            if no_price is not None and no_price != self.no_price:
                self.no_price = no_price
                changed = True
            self.last_update = time.time()
            if changed:
                self.last_changed_at = self.last_update

    async def snapshot(self) -> dict:
        async with self._lock:
            return {
                "yes_price": self.yes_price,
                "no_price": self.no_price,
                "last_update": self.last_update,
                "last_changed_at": self.last_changed_at,
            }


# ── Per-Market CSV Logger ────────────────────────────────────────────────

class CSVLogger:
    MIN_INTERVAL = 1.0  # Write at most once per second
    MARKET_DURATION = 900  # 15 minutes in seconds

    def __init__(self, slug: str):
        self._slug = slug
        self._filepath = None
        self._file = None
        self._writer = None
        self._last_write: float = 0.0

        # Extract market_unix from slug (e.g. "btc-updown-15m-1774449000" → 1774449000)
        try:
            self._market_unix = int(slug.split("-")[-1])
        except (ValueError, IndexError):
            self._market_unix = 0
        self._window_start = float(self._market_unix)
        self._window_end = float(self._market_unix + self.MARKET_DURATION)


    def log_tick(self, yes_price: float, no_price: float,
                 btc_price: float = 0.0, price_to_beat: float = 0.0, btc_delta: float = 0.0):
        if self._writer is None:
            return
        now = time.time()
        # Only log within the market's 15-minute window
        if self._market_unix > 0 and (now < self._window_start or now >= self._window_end):
            return
        # Throttle: write at most once per second
        if now - self._last_write < self.MIN_INTERVAL:
            return
        self._last_write = now
        readable = datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self._writer.writerow([
            f"{now:.3f}", readable,
            f"{yes_price:.6f}", f"{no_price:.6f}",
            f"{btc_price:.2f}", f"{price_to_beat:.2f}", f"{btc_delta:.0f}",
        ])

    def close(self):
        if self._file:
            self._file.close()
            self._file = None
            self._writer = None


# ── Unified CSV Logger (All Markets) ──────────────────────────────────────

class UnifiedCSVLogger:
    def __init__(self):
        self._filepath = None
        self._file = None
        self._writer = None

    def log_state(self, timestamp: float, slug: str, market_unix: int, 
                  yes_price: float, no_price: float, btc_delta: float):
        if self._writer is None:
            return
        
        # Calculate seconds back from 900
        seconds_passed = int(timestamp - market_unix)
        seconds_left = max(0, 900 - seconds_passed)
        
        readable = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        spread = yes_price - no_price
        
        self._writer.writerow([
            readable,
            f"{seconds_left}",
            f"{spread:.2f}",
            f"{btc_delta:.0f}"
        ])

    def close(self):
        if self._file:
            self._file.close()
            self._file = None
            self._writer = None



# ── Per-Market Tracker ───────────────────────────────────────────────────

class MarketTracker:
    def __init__(self, market: MarketState):
        self.market = market
        self.slug = market.slug
        self.yes_token = market.yes_token_id
        self.no_token = market.no_token_id
        self.price_state = PriceState()
        self.csv_logger = CSVLogger(market.slug)

    def close(self):
        self.csv_logger.close()


# ── Multi-Market WS Manager ─────────────────────────────────────────────

def _ws_is_open(ws) -> bool:
    """Check if a websockets connection is open (compatible with v14+ and v15+)."""
    try:
        # v15+ uses protocol.state
        from websockets.protocol import State
        return ws.protocol.state is State.OPEN
    except (AttributeError, ImportError):
        pass
    try:
        # v14+ uses .state
        return ws.state.name == "OPEN"
    except AttributeError:
        pass
    # Fallback: try the old .open attribute
    return getattr(ws, "open", True)


class MultiMarketManager:
    """Single WS connection routing events to per-market MarketTrackers.
    Also runs a Gamma REST API poller as fallback for thin order books."""

    def __init__(self):
        self._ws = None
        self._running: bool = False
        self._reconnect_delay: float = INITIAL_RECONNECT_DELAY
        self._task: asyncio.Task | None = None
        self._poll_task: asyncio.Task | None = None
        self._unified_poll_task: asyncio.Task | None = None
        self._session: aiohttp.ClientSession | None = None
        self._trackers: dict[str, MarketTracker] = {}
        self._asset_to_slug: dict[str, str] = {}
        self._asset_to_side: dict[str, str] = {}
        self._all_assets: set[str] = set()
        self._lock = asyncio.Lock()
        self._ws_buffer: dict[str, dict] = {}  # Per-slug buffer for paired WS updates
        self._unified_logger = UnifiedCSVLogger()


    async def add_market(self, market: MarketState):
        async with self._lock:
            if market.slug in self._trackers:
                return
            tracker = MarketTracker(market)
            self._trackers[market.slug] = tracker
            self._asset_to_slug[market.yes_token_id] = market.slug
            self._asset_to_slug[market.no_token_id] = market.slug
            self._asset_to_side[market.yes_token_id] = "yes"
            self._asset_to_side[market.no_token_id] = "no"
            self._all_assets.add(market.yes_token_id)
            self._all_assets.add(market.no_token_id)
            print(f"📊 Tracking market: {market.slug} ({market.role})")

        if self._ws and _ws_is_open(self._ws):
            await self._send_subscription([market.yes_token_id, market.no_token_id])

    async def remove_market(self, slug: str):
        async with self._lock:
            tracker = self._trackers.pop(slug, None)
            if tracker:
                for token in [tracker.yes_token, tracker.no_token]:
                    self._asset_to_slug.pop(token, None)
                    self._asset_to_side.pop(token, None)
                    self._all_assets.discard(token)
                tracker.close()

    def get_prices(self, slug: str) -> PriceState | None:
        tracker = self._trackers.get(slug)
        return tracker.price_state if tracker else None

    async def get_all_snapshots(self) -> dict[str, dict]:
        result = {}
        async with self._lock:
            trackers = list(self._trackers.values())
        for tracker in trackers:
            result[tracker.slug] = await tracker.price_state.snapshot()
        return result

    async def start(self):
        if self._running:
            return
        self._running = True
        self._session = aiohttp.ClientSession()
        self._task = asyncio.create_task(self._run_forever())
        self._poll_task = asyncio.create_task(self._poll_gamma_prices())
        self._unified_poll_task = asyncio.create_task(self._unified_csv_poll())
        print("🔌 Multi-Market WS Manager started")

    async def stop(self):
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        if self._unified_poll_task:
            self._unified_poll_task.cancel()
            try:
                await self._unified_poll_task
            except asyncio.CancelledError:
                pass
            self._unified_poll_task = None
        if self._session:
            await self._session.close()
            self._session = None
        async with self._lock:
            for tracker in self._trackers.values():
                tracker.close()
            self._trackers.clear()
            self._asset_to_slug.clear()
            self._asset_to_side.clear()
            self._all_assets.clear()
        self._unified_logger.close()
        print("🛑 Multi-Market WS Manager stopped")

    # ── Unified CSV Price Poller ─────────────────────────────────────────

    async def _unified_csv_poll(self):
        """Poll all active trackers every 1s and write to the global unified CSV."""
        print("📡 Unified CSV poller started (1s interval)")
        while self._running:
            try:
                now = time.time()
                async with self._lock:
                    trackers = list(self._trackers.values())

                for tracker in trackers:
                    if tracker.csv_logger._market_unix > 0 and (now >= tracker.csv_logger._window_start and now < tracker.csv_logger._window_end):
                        snap = await tracker.price_state.snapshot()
                        # Only log if prices have been fetched at least once
                        if snap["yes_price"] > 0 or snap["no_price"] > 0:
                            btc_snap = await btc_tracker.get_snapshot(tracker.slug)
                            self._unified_logger.log_state(
                                now,
                                tracker.slug,
                                tracker.csv_logger._market_unix,
                                snap["yes_price"],
                                snap["no_price"],
                                btc_snap["btc_delta"]
                            )

                # Try to sleep precisely until the next second boundary
                elapsed = time.time() - now
                sleep_time = max(0.1, 1.0 - elapsed)
                await asyncio.sleep(sleep_time)

            except asyncio.CancelledError:
                print("🛑 Unified CSV poller cancelled")
                return
            except Exception as e:
                print(f"❌ Unified CSV poller error: {e}")
                await asyncio.sleep(1.0)

    # ── Gamma REST API Price Poller (fallback for thin order books) ──────

    async def _poll_gamma_prices(self):
        """Poll Gamma API every 2s for all tracked markets.
        This provides reliable prices even when the CLOB order book is empty."""
        print("📡 Gamma price poller started (2s interval)")
        while self._running:
            try:
                async with self._lock:
                    slugs = list(self._trackers.keys())

                for slug in slugs:
                    if not self._running or not self._session:
                        break
                    try:
                        async with self._session.get(
                            GAMMA_API_URL, params={"slug": slug}, timeout=aiohttp.ClientTimeout(total=5)
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                if data and isinstance(data, list) and len(data) > 0:
                                    market = data[0]
                                    yes_price = 0.0
                                    no_price = 0.0

                                    # Primary: outcomePrices array ["yes_price", "no_price"]
                                    outcome_prices = market.get("outcomePrices")
                                    if outcome_prices and isinstance(outcome_prices, list) and len(outcome_prices) >= 2:
                                        yes_price = float(outcome_prices[0])
                                        no_price = float(outcome_prices[1])

                                    # Fallback: tokens array with price field
                                    if yes_price == 0 and no_price == 0:
                                        for t in market.get("tokens", []):
                                            outcome = t.get("outcome", "").lower()
                                            price = float(t.get("price", 0))
                                            if outcome == "yes":
                                                yes_price = price
                                            elif outcome == "no":
                                                no_price = price

                                    # Fallback: bestBid/bestAsk
                                    if yes_price == 0 and no_price == 0:
                                        bb = float(market.get("bestBid", 0))
                                        ba = float(market.get("bestAsk", 0))
                                        if bb > 0 or ba > 0:
                                            yes_price = bb
                                            no_price = 1.0 - bb if bb > 0 else ba

                                    if yes_price > 0 or no_price > 0:
                                        tracker = self._trackers.get(slug)
                                        if tracker:
                                            snap = await tracker.price_state.snapshot()
                                            # Only seed prices if PriceState is empty (WS hasn't sent data yet)
                                            # Otherwise, just refresh last_update timestamp
                                            if snap["yes_price"] == 0 and snap["no_price"] == 0:
                                                await tracker.price_state.update(
                                                    yes_price=yes_price, no_price=no_price
                                                )
                                            else:
                                                # Refresh last_update without changing prices
                                                await tracker.price_state.update()
                                            # CSV log using current PriceState (WS prices)
                                            snap = await tracker.price_state.snapshot()
                                            btc_snap = await btc_tracker.get_snapshot(slug)
                                            tracker.csv_logger.log_tick(
                                                snap["yes_price"], snap["no_price"],
                                                btc_snap["current_btc"],
                                                btc_snap["price_to_beat"],
                                                btc_snap["btc_delta"],
                                            )
                    except Exception:
                        pass  # Silently skip individual market fetch errors

                await asyncio.sleep(GAMMA_PRICE_POLL_INTERVAL)

            except asyncio.CancelledError:
                print("🛑 Gamma price poller cancelled")
                return
            except Exception as e:
                print(f"❌ Gamma poller error: {e}")
                await asyncio.sleep(GAMMA_PRICE_POLL_INTERVAL)

    async def _run_forever(self):
        while self._running:
            try:
                await self._connect_and_listen()
            except Exception as e:
                if not self._running:
                    break
                print(f"❌ WS error: {type(e).__name__}: {e}")

            if not self._running:
                break

            print(f"🔄 WS reconnecting in {self._reconnect_delay:.1f}s...")
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(
                self._reconnect_delay * RECONNECT_BACKOFF_FACTOR,
                MAX_RECONNECT_DELAY,
            )

    async def _connect_and_listen(self):
        print(f"🔌 Connecting to {WS_URL}...")
        async with websockets.connect(
            WS_URL, ping_interval=20, ping_timeout=10, close_timeout=5,
        ) as ws:
            self._ws = ws
            self._reconnect_delay = INITIAL_RECONNECT_DELAY
            print("✅ WS connected")

            async with self._lock:
                all_assets = list(self._all_assets)
            if all_assets:
                await self._send_subscription(all_assets)

            async for raw_message in ws:
                if not self._running:
                    break
                await self._handle_message(raw_message)

    async def _send_subscription(self, token_ids: list[str]):
        if not self._ws or not _ws_is_open(self._ws):
            return
        msg = json.dumps({"type": "market", "assets_ids": token_ids})
        await self._ws.send(msg)
        print(f"📡 Subscribed to {len(token_ids)} asset(s)")

    async def _handle_message(self, raw: str):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return
        if isinstance(data, list):
            for item in data:
                await self._process_event(item)
        else:
            await self._process_event(data)

    async def _process_event(self, event: dict):
        event_type = event.get("event_type", "")
        # Ignore 'book' events — they carry stale best_bid from thin/empty order books
        if event_type == "price_change":
            await self._handle_price_change(event)

    async def _handle_price_change(self, event: dict):
        """Buffer price_change events per-slug; flush to PriceState when
        both yes AND no sides arrive within 500ms (atomic pair update).
        If only one side arrives, flush it after 500ms timeout."""
        changes = event.get("price_changes", [])
        for change in changes:
            asset_id = change.get("asset_id", "")
            slug = self._asset_to_slug.get(asset_id)
            if not slug:
                continue
            tracker = self._trackers.get(slug)
            if not tracker:
                continue

            best_bid = change.get("best_bid", "")
            if not best_bid:
                continue

            bid_price = float(best_bid)
            side = self._asset_to_side.get(asset_id, "")

            # Initialize per-slug buffer
            if slug not in self._ws_buffer:
                self._ws_buffer[slug] = {"yes": None, "no": None, "ts": time.time()}

            buf = self._ws_buffer[slug]

            # If buffer is stale (>500ms), flush whatever we have, then reset
            if time.time() - buf["ts"] > 0.5:
                if buf["yes"] is not None or buf["no"] is not None:
                    update_kw = {}
                    if buf["yes"] is not None:
                        update_kw["yes_price"] = buf["yes"]
                    if buf["no"] is not None:
                        update_kw["no_price"] = buf["no"]
                    await tracker.price_state.update(**update_kw)
                buf["yes"] = None
                buf["no"] = None
                buf["ts"] = time.time()

            if side == "yes":
                buf["yes"] = bid_price
            elif side == "no":
                buf["no"] = bid_price

            # Flush when both sides are present (atomic pair)
            if buf["yes"] is not None and buf["no"] is not None:
                await tracker.price_state.update(
                    yes_price=buf["yes"], no_price=buf["no"]
                )
                # CSV log
                snap = await tracker.price_state.snapshot()
                btc_snap = await btc_tracker.get_snapshot(slug)
                tracker.csv_logger.log_tick(
                    snap["yes_price"], snap["no_price"],
                    btc_snap["current_btc"], btc_snap["price_to_beat"], btc_snap["btc_delta"],
                )
                # Clear buffer for next pair
                buf["yes"] = None
                buf["no"] = None
                buf["ts"] = time.time()


market_manager = MultiMarketManager()
