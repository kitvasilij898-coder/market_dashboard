"""
Market Research Dashboard — Auto-Market Discovery (v3: Multi-Market + BTC Delta)
==================================================================================
Tracks past, current, and next 15-minute BTC markets simultaneously.
MarketRegistry maintains an ordered history of all discovered markets.
Also fetches real-time BTC/USD price from Binance for delta calculation.

Slug format: btc-updown-15m-{unix_timestamp}
Markets start at :00, :15, :30, :45 UTC boundaries.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Awaitable

import aiohttp

# ── Constants ────────────────────────────────────────────────────────────

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
BINANCE_PRICE_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
MARKET_INTERVAL = 15 * 60  # 15 minutes in seconds
ROTATION_CHECK_INTERVAL = 5
DISCOVERY_RETRY_DELAY = 10
MAX_DISCOVERY_RETRIES = 12
MAX_HISTORY = 5  # 3 past + 1 live + 1 next
BTC_POLL_INTERVAL = 2.0  # fetch BTC price every 2 seconds


# ── Data Structures ──────────────────────────────────────────────────────

@dataclass
class MarketState:
    """A single 15-minute BTC market."""
    slug: str = ""
    condition_id: str = ""
    yes_token_id: str = ""
    no_token_id: str = ""
    market_unix: int = 0
    expiry_unix: int = 0
    question: str = ""
    is_active: bool = False
    role: str = ""  # "past", "current", "next"

    @property
    def remaining_seconds(self) -> int:
        return max(0, self.expiry_unix - int(time.time()))

    @property
    def is_expired(self) -> bool:
        return time.time() >= self.expiry_unix

    def to_dict(self) -> dict:
        return {
            "slug": self.slug,
            "condition_id": self.condition_id,
            "yes_token_id": self.yes_token_id,
            "no_token_id": self.no_token_id,
            "market_unix": self.market_unix,
            "expiry_unix": self.expiry_unix,
            "question": self.question,
            "role": self.role,
            "remaining_seconds": self.remaining_seconds,
        }


# ── BTC Price Tracker ────────────────────────────────────────────────────

class BTCPriceTracker:
    """
    Tracks real-time BTC/USD and the per-market reference price ("price to beat").
    Delta = current_btc - price_to_beat
    """

    def __init__(self):
        self.current_btc: float = 0.0
        self.last_update: float = 0.0
        # Per-market price-to-beat (market slug -> snapshot BTC price at market start)
        self._price_to_beat: dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None

    async def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._poll_loop())
            print("📈 BTC price tracker started")

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _poll_loop(self):
        while True:
            try:
                await self._fetch_price()
                await asyncio.sleep(BTC_POLL_INTERVAL)
            except asyncio.CancelledError:
                return
            except Exception as e:
                print(f"⚠️  BTC price fetch error: {e}")
                await asyncio.sleep(BTC_POLL_INTERVAL * 2)

    async def _fetch_price(self):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    BINANCE_PRICE_URL,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        async with self._lock:
                            self.current_btc = float(data["price"])
                            self.last_update = time.time()
        except Exception:
            pass  # Silently continue — old price stays until next successful fetch

    async def snapshot_price_to_beat(self, slug: str, market_unix: int = 0):
        """
        Fetch the Binance 15-minute candle open price as the price-to-beat.
        This is the BTC/USD price at the exact start of the 15-minute window.
        """
        if market_unix <= 0:
            try:
                market_unix = int(slug.split("-")[-1])
            except (ValueError, IndexError):
                market_unix = 0

        if market_unix <= 0:
            return

        # Fetch from Binance klines API
        klines_url = (
            f"https://api.binance.com/api/v3/klines"
            f"?symbol=BTCUSDT&interval=15m"
            f"&startTime={market_unix * 1000}&limit=1"
        )
        for attempt in range(5):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        klines_url, timeout=aiohttp.ClientTimeout(total=5)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data and len(data) > 0:
                                open_price = float(data[0][1])
                                async with self._lock:
                                    self._price_to_beat[slug] = open_price
                                print(f"📌 Price to beat for {slug}: ${open_price:,.2f} (Binance 15m candle open)")
                                return
            except Exception as e:
                print(f"⚠️  Klines fetch attempt {attempt+1} failed: {e}")

            # Candle might not exist yet for future markets  — retry after delay
            await asyncio.sleep(3)

        # Fallback: use current price if klines unavailable
        async with self._lock:
            if self.current_btc > 0:
                self._price_to_beat[slug] = self.current_btc
                print(f"📌 Price to beat for {slug}: ${self.current_btc:,.2f} (fallback: current price)")

    def get_price_to_beat(self, slug: str) -> float:
        return self._price_to_beat.get(slug, 0.0)

    async def get_snapshot(self, slug: str = "") -> dict:
        async with self._lock:
            ptb = self._price_to_beat.get(slug, 0.0) if slug else 0.0
            delta = (self.current_btc - ptb) if ptb > 0 else 0.0
            return {
                "current_btc": self.current_btc,
                "price_to_beat": ptb,
                "btc_delta": delta,
                "last_update": self.last_update,
            }


btc_tracker = BTCPriceTracker()


# ── Market Registry ─────────────────────────────────────────────────────

class MarketRegistry:
    """Maintains an ordered list of all tracked markets."""

    def __init__(self):
        self._markets: list[MarketState] = []
        self._by_slug: dict[str, MarketState] = {}
        self._current_slug: str = ""
        self._next_slug: str = ""

    def add(self, market: MarketState):
        if market.slug in self._by_slug:
            old = self._by_slug[market.slug]
            old.condition_id = market.condition_id
            old.yes_token_id = market.yes_token_id
            old.no_token_id = market.no_token_id
            old.question = market.question
            old.role = market.role
            old.is_active = market.is_active
            return

        self._by_slug[market.slug] = market
        inserted = False
        for i, m in enumerate(self._markets):
            if market.market_unix < m.market_unix:
                self._markets.insert(i, market)
                inserted = True
                break
        if not inserted:
            self._markets.append(market)

        while len(self._markets) > MAX_HISTORY:
            oldest = self._markets.pop(0)
            del self._by_slug[oldest.slug]

    def set_current(self, slug: str):
        self._current_slug = slug
        if slug in self._by_slug:
            self._by_slug[slug].role = "current"

    def set_next(self, slug: str):
        self._next_slug = slug
        if slug in self._by_slug:
            self._by_slug[slug].role = "next"

    def mark_as_past(self, slug: str):
        if slug in self._by_slug:
            self._by_slug[slug].role = "past"

    def get(self, slug: str) -> MarketState | None:
        return self._by_slug.get(slug)

    def get_current(self) -> MarketState | None:
        return self._by_slug.get(self._current_slug)

    def get_next(self) -> MarketState | None:
        return self._by_slug.get(self._next_slug)

    def get_all(self) -> list[MarketState]:
        return list(self._markets)

    def get_navigable(self) -> list[MarketState]:
        """Return markets in chronological order: PAST (left) → CURRENT → NEXT (right)."""
        return list(self._markets)

    @property
    def current_slug(self) -> str:
        return self._current_slug

    @property
    def next_slug(self) -> str:
        return self._next_slug


registry = MarketRegistry()


# ── Core Functions ───────────────────────────────────────────────────────

def calc_current_market_unix() -> int:
    now = int(time.time())
    return now - (now % MARKET_INTERVAL)

def calc_next_market_unix() -> int:
    return calc_current_market_unix() + MARKET_INTERVAL

def calc_prev_market_unix() -> int:
    return calc_current_market_unix() - MARKET_INTERVAL

def make_slug(market_unix: int) -> str:
    return f"btc-updown-15m-{market_unix}"


async def fetch_market_info(slug: str) -> MarketState | None:
    """Query the Gamma API to resolve a market slug."""
    import json

    url = f"{GAMMA_API_BASE}/events"
    params = {"slug": slug}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    print(f"⚠️  Gamma API returned {resp.status} for '{slug}'")
                    return None
                data = await resp.json()

        if not data:
            return None

        event = None
        for item in data:
            if item.get("slug") == slug:
                event = item
                break
        if event is None:
            event = data[0]

        markets = event.get("markets", [])
        if not markets:
            return None

        market = markets[0]
        condition_id = market.get("conditionId", "")

        outcomes_raw = market.get("outcomes", "")
        clob_token_ids_raw = market.get("clobTokenIds", "")

        if isinstance(outcomes_raw, str):
            try:
                outcomes_list = json.loads(outcomes_raw)
            except (json.JSONDecodeError, TypeError):
                outcomes_list = [o.strip() for o in outcomes_raw.split(",")]
        else:
            outcomes_list = outcomes_raw

        if isinstance(clob_token_ids_raw, str):
            try:
                token_ids_list = json.loads(clob_token_ids_raw)
            except (json.JSONDecodeError, TypeError):
                token_ids_list = [t.strip() for t in clob_token_ids_raw.split(",")]
        else:
            token_ids_list = clob_token_ids_raw

        yes_token = ""
        no_token = ""
        for i, outcome_name in enumerate(outcomes_list):
            token_id = token_ids_list[i] if i < len(token_ids_list) else ""
            if outcome_name.lower() in ("yes", "up"):
                yes_token = token_id
            elif outcome_name.lower() in ("no", "down"):
                no_token = token_id

        if not yes_token and len(token_ids_list) >= 1:
            yes_token = token_ids_list[0]
        if not no_token and len(token_ids_list) >= 2:
            no_token = token_ids_list[1]

        question = market.get("question", event.get("title", ""))
        try:
            market_unix = int(slug.split("-")[-1])
        except ValueError:
            market_unix = calc_current_market_unix()

        state = MarketState(
            slug=slug,
            condition_id=condition_id,
            yes_token_id=yes_token,
            no_token_id=no_token,
            market_unix=market_unix,
            expiry_unix=market_unix + MARKET_INTERVAL,
            question=question,
            is_active=True,
        )

        print(f"✅ Market discovered: {slug}")
        print(f"   Question:     {question[:80]}")
        print(f"   Yes token:    {yes_token[:24]}...")
        print(f"   No token:     {no_token[:24]}...")
        dt_str = datetime.fromtimestamp(state.expiry_unix, tz=timezone.utc).strftime('%H:%M:%S UTC')
        print(f"   Expires:      {dt_str}")

        return state

    except aiohttp.ClientError as e:
        print(f"❌ Gamma API request failed: {type(e).__name__}: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error in fetch_market_info: {e}")
        return None


async def discover_market_with_retry(slug: str, max_retries: int = MAX_DISCOVERY_RETRIES) -> MarketState | None:
    for attempt in range(1, max_retries + 1):
        state = await fetch_market_info(slug)
        if state is not None:
            return state
        print(f"🔄 Market '{slug}' not found yet. Retry {attempt}/{max_retries}...")
        await asyncio.sleep(DISCOVERY_RETRY_DELAY)
    print(f"❌ Failed to discover market '{slug}' after {max_retries} retries.")
    return None


async def market_rotation_loop(
    on_new_current: Callable[[MarketState], Awaitable[None]],
    on_new_next: Callable[[MarketState], Awaitable[None]],
    on_rotation: Callable[[], Awaitable[None]],
) -> None:
    """Background loop that tracks current + next markets and handles rotation."""
    print("🔄 Market rotation loop started")

    while True:
        try:
            # ── Step 1: Discover current market ──────────────────────
            current_unix = calc_current_market_unix()
            current_slug = make_slug(current_unix)

            if registry.current_slug != current_slug:
                if registry.current_slug:
                    registry.mark_as_past(registry.current_slug)

                print(f"🔍 Discovering current market: {current_slug}...")
                state = await discover_market_with_retry(current_slug)
                if state:
                    state.role = "current"
                    registry.add(state)
                    registry.set_current(current_slug)
                    await on_new_current(state)
                    # Snapshot BTC price as price-to-beat for this market
                    await btc_tracker.snapshot_price_to_beat(current_slug)
                else:
                    await asyncio.sleep(ROTATION_CHECK_INTERVAL)
                    continue

            # ── Step 2: Discover next market ─────────────────────────
            next_unix = calc_next_market_unix()
            next_slug = make_slug(next_unix)

            if registry.next_slug != next_slug:
                print(f"🔍 Discovering next market: {next_slug}...")
                next_state = await discover_market_with_retry(next_slug, max_retries=3)
                if next_state:
                    next_state.role = "next"
                    registry.add(next_state)
                    registry.set_next(next_slug)
                    await on_new_next(next_state)
                    # NOTE: No price_to_beat snapshot for next markets.
                    # PTB only forms at the start of the 15-min round.

            # ── Step 3: Try to discover previous if not in registry ──
            prev_unix = calc_prev_market_unix()
            prev_slug = make_slug(prev_unix)
            if not registry.get(prev_slug):
                prev_state = await fetch_market_info(prev_slug)
                if prev_state:
                    prev_state.role = "past"
                    registry.add(prev_state)
                    # Snapshot price-to-beat for past market
                    await btc_tracker.snapshot_price_to_beat(prev_slug, prev_unix)
                    print(f"📜 Previous market loaded: {prev_slug}")

            # ── Step 4: Wait for current market to expire ────────────
            current_mkt = registry.get_current()
            while current_mkt and not current_mkt.is_expired:
                await asyncio.sleep(ROTATION_CHECK_INTERVAL)
                # Keep trying to discover next if we haven't yet
                if not registry.get_next():
                    next_unix = calc_next_market_unix()
                    next_slug = make_slug(next_unix)
                    next_state = await fetch_market_info(next_slug)
                    if next_state:
                        next_state.role = "next"
                        registry.add(next_state)
                        registry.set_next(next_slug)
                        await on_new_next(next_state)

            print(f"⏰ Market {current_slug} expired. Rotating...")
            await on_rotation()

        except asyncio.CancelledError:
            print("🛑 Market rotation loop cancelled")
            return
        except Exception as e:
            print(f"❌ Market rotation error: {e}")
            await asyncio.sleep(ROTATION_CHECK_INTERVAL)
