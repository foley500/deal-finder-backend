import statistics
import os
import redis
import json
import requests
import re
import time
from datetime import datetime, timezone
from app.services.ebay_rate_limiter import throttle_ebay
from app.services.ebay_browse_service import (
    get_ebay_access_token,
    get_item_detail,
    _trip_circuit,
    _is_circuit_open,
    _reset_circuit_trip_count,
    BROWSE_CIRCUIT_KEY,
)

SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
REDIS_URL = os.getenv("CELERY_BROKER_URL")
redis_client = redis.from_url(REDIS_URL)

CACHE_TTL = 90000          # 25 hours — outlasts 24hr prewarm cycle so all makes stay warm
MAX_DETAIL_EXPANSIONS = 60  # Scan-time expansion cap (live valuations on cache miss)
MAX_PREWARM_EXPANSIONS = 25 # Prewarm cap — year-only, missing mileage does NOT trigger expansion
MIN_SAMPLE_SIZE = 5
MAX_ENRICHED_TARGET = 80    # Stop once 80 items have full year data


MILEAGE_BLOCK_SIZE = 5000
MILEAGE_BLOCK_RATE = 0.010
MAX_LINEAR_BLOCKS = 8
EXTREME_MILEAGE_THRESHOLD = 120000

MAX_ACCEPTABLE_IQR_RATIO = 0.55
WIDE_SPREAD_DISCOUNT = 0.97

# eBay sold BIN prices systematically overstate true private clean values.
# The degree of overstatement depends on price tier:
#   Budget cars  (<£5k raw): small-trader BIN dominance; raw ≈ 2.3× true private
#   Mid-range    (£5k-12k):  mix of private and trader; raw ≈ 1.9× true private
#   Upper-mid    (£12k-25k): less contamination;        raw ≈ 1.55× true private
#   Premium      (>£25k):    genuine private pool larger; raw ≈ 1.35× true private
# Tiered corrections calibrated against Regit/CAP private clean data:
#   Corsa 2013 77k: raw £4,520 × 0.43 = £1,944 vs Regit £1,927  ✓
#   Kia Sportage 2016 104k: raw £9,260 × 0.53 = £4,908 vs Regit £4,909 ✓
# Applied in run_filter_layer — not a single constant.

# UK motor trade valuation multipliers relative to eBay private sold median.
# Private is sourced from actual completed eBay private sales (soldItems:true +
# sellerAccountTypes:{INDIVIDUAL}), which sit slightly below AutoTrader private
# but are real market-clearing prices.
#   Retail ≈ 35% above eBay private sold  (dealer forecourt prep/warranty/margin)
#   Trade  ≈ mileage-adjusted via get_trade_multiplier() — 0.72–0.88× private
RETAIL_MULTIPLIER = 1.35
TRADE_MULTIPLIER  = 0.78  # Fallback for active-listing path only


def get_trade_multiplier(mileage: int, make: str = "") -> float:
    """
    Returns a mileage and make-adjusted trade/auction multiplier.
    Calibrated relative to auction-derived private clean values.
    Lower mileage cars command a premium at auction; high mileage are discounted.
    """
    if mileage and mileage < 30000:
        base = 0.88
    elif mileage and mileage < 60000:
        base = 0.84
    elif mileage and mileage < 100000:
        base = 0.80
    elif mileage and mileage < 120000:
        base = 0.76
    elif mileage and mileage >= 120000:
        base = 0.72
    else:
        base = 0.80  # no mileage data

    # Prestige makes hold trade value slightly better
    prestige = {"bmw", "mercedes", "mercedes-benz", "audi", "porsche", "land rover", "lexus"}
    if make and make.lower().strip() in prestige:
        base = min(base + 0.02, 0.90)

    return round(base, 4)

# Active listing asking prices sit above what cars actually sell for.
# Discount to realistic sale price before deriving the three values.
ACTIVE_SALE_DISCOUNT = 0.85

# Mileage bands for dynamic layer_1 tolerance scaling.
# Layer 1 is always ±15k miles — tight first pass to get like-for-like comparables.
# Layer 2+ progressively widens as fallback when comparable pools are thin.
# High mileage cars have thin pools — only layer 2+ widens to compensate.
# Format: (mileage_threshold, layer_1_tolerance, layer_2_tolerance)
MILEAGE_TOLERANCE_BANDS = [
    (60000,  15000, 25000),   # <60k miles  — tight pool, plenty of comparables
    (100000, 15000, 28000),   # 60k-100k    — l1 stays ±15k, l2 widens
    (140000, 15000, 35000),   # 100k-140k   — l1 stays ±15k, l2 widens more
    (float("inf"), 15000, 45000),  # 140k+  — l1 stays ±15k, l2 very wide
]


# ---------------------------------------------------
# HELPERS
# ---------------------------------------------------

def get_mileage_tolerances(target_mileage: int) -> tuple:
    """Returns (layer_1_tolerance, layer_2_tolerance) based on target mileage."""
    for threshold, l1, l2 in MILEAGE_TOLERANCE_BANDS:
        if target_mileage < threshold:
            return l1, l2
    return 15000, 45000  # Should never be reached — float("inf") band covers everything


# Make normalisation applied before building cache keys.
# Must match the aliases applied in deal_engine.py so prewarm and live
# valuations always generate identical cache keys for the same vehicle.
_MAKE_CACHE_ALIASES = {
    "Mercedes-Benz": "Mercedes",   # deal_engine aliases DVSA "MERCEDES-BENZ" → "Mercedes"
    "Vw":            "Volkswagen", # DVSA "VW" (rare) → eBay-standard "Volkswagen"
    "Mg":            "MG",         # deal_engine aliases back to "MG"; normalise cache key
    "Bmw":           "BMW",        # deal_engine aliases back to "BMW"; normalise cache key
}

def bucket_engine_size(engine_litre):
    """
    Normalises engine sizes into buckets.
    """
    if engine_litre is None:
        return None

    if engine_litre < 1.3:
        return 1.0
    elif engine_litre < 1.8:
        return 1.6
    elif engine_litre < 2.3:
        return 2.0
    elif engine_litre < 3.5:
        return 3.0
    else:
        return 4.0


def extract_year_from_title(title: str):
    # Direct 4-digit year — most reliable, try first
    match = re.search(r"\b(19\d{2}|20\d{2})\b", title)
    if match:
        return int(match.group(1))

    # UK plate year codes: "63 plate", "15 reg", "71 plate", "65-reg" etc.
    # UK registration format (post Sept 2001):
    #   Codes 02–49 → March registration: year = 2000 + code  (e.g. 15 → 2015)
    #   Codes 51–99 → September registration: year = 2000 + (code − 50)  (e.g. 63 → 2013)
    # This covers the vast majority of used cars on eBay (2002–present).
    plate_match = re.search(r"\b(\d{2})\s*[-]?\s*(?:plate|reg(?:istration)?)\b", title.lower())
    if plate_match:
        code = int(plate_match.group(1))
        if 2 <= code <= 49:
            year = 2000 + code
        elif 51 <= code <= 99:
            year = 2000 + (code - 50)
        else:
            year = None
        if year and 2001 <= year <= 2030:
            return year

    return None


def extract_mileage_from_text(text: str):
    if not text:
        return None

    text = text.lower().replace(",", "")

    match = re.search(r"(\d{2,3})\s?k\s*(miles|mile|mi)?", text)
    if match:
        return int(match.group(1)) * 1000

    match = re.search(r"\b(\d{5,6})\b", text)
    if match:
        val = int(match.group(1))
        if 1000 < val < 300000:
            return val

    return None


def normalise_base_model(make: str, base_model: str, full_model: str = "") -> str:
    """
    Converts DVSA/eBay make+model into an eBay-searchable base model term.

    full_model: the complete model string before splitting (e.g. "C Class", "Range Rover Sport").
    Without it, DVSA multi-word models like "C CLASS" only pass "C" as base_model,
    making Mercedes/Land Rover queries far too vague.

    DVSA API returns make/model/fuelType/engineSize as separate JSON fields — they
    are NOT embedded in the model string. Model can contain variant/trim (e.g.
    "GOLF GTI", "TRANSIT CUSTOM", "320D M SPORT") but NOT engine size or fuel type.
    """
    make_lower = make.lower()
    model_lower = base_model.lower().strip()
    full_lower = (full_model or base_model).lower().strip()

    if make_lower == "bmw":
        # "118", "120", "318" etc → extract leading digit → "1 Series", "3 Series"
        if model_lower.isdigit():
            if len(model_lower) >= 3:
                series_num = model_lower[0]
                return f"{series_num} Series"
            return f"{base_model} Series"
        # Handle variants like "118d", "320i", "320D M Sport" etc
        m = re.match(r'^([1-9])\d{2}', model_lower)
        if m:
            return f"{m.group(1)} Series"
        # X models: "X1", "X3", "X5" — uppercase is correct for eBay
        if re.match(r'^x[1-9]$', model_lower):
            return base_model.upper()
        # MINI models that ended up under BMW make
        if model_lower in ["hatch", "cooper", "one", "john cooper works", "jcw"]:
            return "Mini"

    if make_lower in ["mercedes", "mercedes-benz"]:
        # Handle DVSA format: "C CLASS", "A CLASS", "E CLASS", "GLA CLASS" etc
        # full_lower catches these where base_model alone is just "C", "A" etc
        if "class" in full_lower:
            class_match = re.match(r'^([a-z]{1,4})\s*[-\s]?class', full_lower)
            if class_match:
                letter = class_match.group(1).upper()
                return f"{letter}-Class"
        # Handle concatenated legacy format: "Aclass", "Cclass"
        if "class" in model_lower and "-" not in model_lower:
            return model_lower.replace("class", "-class").title()
        # ML-Class: "Ml350", "Ml320" etc
        if re.match(r'^ml\d', model_lower):
            return "ML-Class"
        # GLA/GLB/GLC/GLE/GLS shorthand — uppercase them
        if re.match(r'^gl[abces]$', model_lower):
            return base_model.upper()
        # "mercedes" as model (bad title fallback) — return as-is
        if model_lower == "mercedes":
            return base_model

    if make_lower == "land rover":
        # DVSA returns "RANGE ROVER", "RANGE ROVER SPORT", "RANGE ROVER EVOQUE" etc
        # Taking only the first word gives "Range" — useless for eBay search
        if full_lower.startswith("range rover"):
            return "Range Rover"
        if full_lower.startswith("discovery sport"):
            return "Discovery Sport"
        # Defender, Freelander, Discovery — first word is correct
        return base_model

    if make_lower == "ford":
        # Ford Transit family — all are distinct vehicles with separate eBay pools.
        # "TRANSIT CUSTOM" → "Transit" loses thousands of relevant comparables.
        if full_lower.startswith("transit custom"):
            return "Transit Custom"
        if full_lower.startswith("transit connect"):
            return "Transit Connect"
        if full_lower.startswith("transit courier"):
            return "Transit Courier"
        if full_lower.startswith("transit tourneo"):
            return "Tourneo Custom"
        if full_lower.startswith("mustang mach"):
            return "Mustang Mach-E"

    if make_lower == "toyota":
        # Yaris Cross and Proace family are distinct from base Yaris/Proace
        if full_lower.startswith("yaris cross"):
            return "Yaris Cross"
        if full_lower.startswith("proace city"):
            return "Proace City"
        if full_lower.startswith("proace verso"):
            return "Proace Verso"
        if full_lower.startswith("proace"):
            return "Proace"
        # C-HR — hyphen makes first word "C-Hr", restore correct form
        if full_lower.startswith("c-hr") or full_lower.startswith("chr"):
            return "C-HR"
        # RAV4 — titles split it as "RAV 4" making first word "Rav". Normalise to "Rav4"
        # so it matches the prewarm key generated from ("Toyota", "RAV4", ...)
        if model_lower in ("rav", "rav4") or full_lower.startswith("rav 4") or full_lower.startswith("rav4"):
            return "Rav4"

    if make_lower == "hyundai":
        # Ioniq 5 and 6 are separate models, not variants of "Ioniq"
        if full_lower.startswith("ioniq 5"):
            return "Ioniq 5"
        if full_lower.startswith("ioniq 6"):
            return "Ioniq 6"
        if full_lower.startswith("ioniq 9"):
            return "Ioniq 9"

    if make_lower in ["mg", "mg motor"]:
        # MG model names are typically already single strings (MG ZS, MG HS etc)
        # but DVSA may return "ZS", "HS", "MG5" etc. Ensure clean uppercase.
        if model_lower in ["zs", "hs", "mg5", "mg4", "mg3", "zst", "zs ev"]:
            return base_model.upper()

    if make_lower == "volkswagen":
        # DVSA sometimes prefixes VW commercial models differently
        if full_lower.startswith("caddy maxi"):
            return "Caddy Maxi"
        if full_lower.startswith("grand california"):
            return "Grand California"

    return base_model


def calculate_mileage_adjustment(base_price: float, listing_mileage: int, target_mileage: int) -> float:
    """
    Adjusts a comparable listing's price to what it would be worth at the target mileage.

    Only the linear mileage differential is applied here.
    The extreme mileage penalty (for when the TARGET car itself is extreme) is applied
    once at the final result level — applying it per-comparable would double-count it
    when the target and comparable are both in the extreme mileage range.
    """
    mileage_diff = listing_mileage - target_mileage
    abs_diff = abs(mileage_diff)

    blocks = min(abs_diff / MILEAGE_BLOCK_SIZE, MAX_LINEAR_BLOCKS)
    linear_adjustment = base_price * MILEAGE_BLOCK_RATE * blocks

    if mileage_diff > 0:
        return base_price - linear_adjustment
    else:
        return base_price + linear_adjustment


def recency_weight(sold_date) -> int:
    """
    Returns a weight multiplier based on how recently the comparable sold.
    Recent sales reflect the current market; older sales may be stale.

    ≤30 days  → 3 (recent, most relevant)
    31-60 days → 2 (still current)
    >60 days   → 1 (neutral — don't discard, just don't overweight)

    Markets can shift fast (diesel collapse, EV premiums eroding) so
    a 5-month-old sale should carry less weight than last week's.
    """
    if sold_date is None:
        return 1
    try:
        now = datetime.now(timezone.utc)
        if sold_date.tzinfo is None:
            sold_date = sold_date.replace(tzinfo=timezone.utc)
        days_ago = (now - sold_date).days
        if days_ago <= 30:
            return 3
        elif days_ago <= 60:
            return 2
        return 1
    except Exception:
        return 1


def mileage_proximity_weight(listing_mileage: int, target_mileage: int, tolerance: int) -> int:
    """
    Returns a repeat count (weight) for a comparable based on how close
    its mileage is to the target. Uses a continuous linear scale rather than
    fixed steps — avoids the cliff-edge where a car 1 mile outside the 25%
    band drops from weight 3 to weight 2.

    Scale: 1 (at tolerance boundary) → 3 (exact match)
    Cars with no mileage data get weight 1 (neutral, not excluded).
    """
    if listing_mileage is None:
        return 1

    abs_diff = abs(listing_mileage - target_mileage)
    if tolerance <= 0:
        return 1

    # Linear interpolation: 3 at diff=0, 1 at diff=tolerance
    ratio = abs_diff / tolerance
    weight = 3.0 - (2.0 * ratio)
    return max(1, round(weight))


def check_spread(prices: list, label: str) -> float:
    if len(prices) < 4:
        return 1.0

    sorted_p = sorted(prices)
    mid = len(sorted_p) // 2
    q1 = statistics.median(sorted_p[:mid])
    q3 = statistics.median(sorted_p[mid:])
    iqr = q3 - q1
    median = statistics.median(sorted_p)

    if median == 0:
        return 1.0

    iqr_ratio = iqr / median
    print(f"   📐 IQR spread [{label}]: Q1=£{round(q1)}, Q3=£{round(q3)}, IQR=£{round(iqr)} ({round(iqr_ratio*100, 1)}% of median)")

    if iqr_ratio > MAX_ACCEPTABLE_IQR_RATIO:
        print(f"   ⚠️ Wide spread ({round(iqr_ratio*100,1)}% > {int(MAX_ACCEPTABLE_IQR_RATIO*100)}%) — applying {WIDE_SPREAD_DISCOUNT} conservatism discount")
        return WIDE_SPREAD_DISCOUNT

    return 1.0


# ---------------------------------------------------
# EBAY SOLD SEARCH
# ---------------------------------------------------

def get_sold_listings(query: str, limit: int = 100, budget_fn=None):
    """
    Fetches sold listings only — active listings are NOT included.
    Asking prices are not comparable sale prices and skew valuations high.

    Two passes:
      1. Private sellers (INDIVIDUAL) — used at face value, weighted ×2 in filter layer
      2. All sellers — deduped, fills out the pool where private data is thin

    budget_fn: optional callable(n_calls: int) -> bool
      Called before each eBay request. Returns False = stop immediately.
      Max 2 eBay calls per invocation (1 private + 1 all-seller).
    """
    token = get_ebay_access_token()
    if not token:
        return []

    # If circuit is open at the start of a new model's search, wait for it to expire
    # rather than skipping all pagination pages instantly (which keeps the circuit open
    # indefinitely during the prewarm loop — all remaining models skip in milliseconds,
    # never giving the 90s TTL a chance to expire).
    if _is_circuit_open():
        wait = redis_client.ttl(BROWSE_CIRCUIT_KEY)
        wait = wait if wait > 0 else 90  # fallback if TTL not readable
        print(f"⚡ Circuit open — waiting {wait}s for reset before fetching '{query}'")
        time.sleep(wait + 2)
        if _is_circuit_open():
            print(f"⚡ Circuit still open after wait — skipping '{query}'")
            return []

    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_GB",
    }

    all_items = []
    combined_seen_ids = set()

    def run_sold_search(seller_filter: str, label: str, use_category: bool = True):

        collected = []
        seen_ids = set()

        # fetch up to 5 pages of sold results
        for offset in [0, 50, 100, 150, 200]:

            # Circuit check FIRST — if open, stop immediately.
            # Without this, each request gets 429, resets the 90s TTL,
            # and the circuit never actually expires during an active run.
            if _is_circuit_open():
                print(f"⚡ [{label}] Circuit open — aborting pagination")
                break

            if budget_fn and not budget_fn(1):
                print(f"🛑 Budget exhausted — stopping [{label}] pagination")
                break

            params = {
                "q": query,
                "limit": 50,
                "offset": offset,
                "filter": f"soldItems:true{seller_filter}",
                "sort": "endingSoonest",
            }

            if use_category:
                params["category_ids"] = "9801"

            throttle_ebay()

            if _is_circuit_open():
                print(f"⚡ [{label}] Circuit open (post-throttle) — aborting pagination")
                break

            response = requests.get(SEARCH_URL, headers=headers, params=params)

            if response.status_code == 429:
                _trip_circuit()
                print(f"❌ [{label}] Rate limited — circuit tripped, stopping sold search")
                break

            if response.status_code != 200:
                print(f"❌ [{label}] search error: {response.status_code}")
                break

            _reset_circuit_trip_count()

            items = response.json().get("itemSummaries", [])

            if not items:
                break

            for item in items:

                item_id = item.get("itemId") or item.get("title", "")[:60]

                if item_id in seen_ids:
                    continue

                seen_ids.add(item_id)
                collected.append(item)

            # stop early if we already have enough comparables
            if len(collected) >= 200:
                break

        print(f"✅ [{label}] '{query[:35]}' → {len(collected)} items")

        return collected

    def _parse_sold_date(item):
        """Extract sold date from eBay item summary. Returns datetime or None."""
        for field in ("itemEndDate", "endDate", "soldDate"):
            raw = item.get(field)
            if raw:
                try:
                    return datetime.fromisoformat(raw.replace("Z", "+00:00"))
                except Exception:
                    pass
        return None

    def _tag_item(item, seller_pool, sold_date):
        """Tag a sold item with metadata needed for auction-aware weighting."""
        item["_source_type"] = "sold"
        item["_seller_pool"] = seller_pool
        item["_sold_date"] = sold_date
        # Auction completions are competitive bids = true market-clearing price.
        # BIN (FIXED_PRICE) sales are asking prices paid without negotiation —
        # systematically inflated by trader asking price premiums.
        item["_is_auction"] = "AUCTION" in item.get("buyingOptions", [])

    # Pass 1: private sold listings — most accurate price signal
    for item in run_sold_search(",sellerAccountTypes:{INDIVIDUAL}", "sold_private"):
        item_id = item.get("itemId") or item.get("title", "")[:60]
        if item_id:
            _tag_item(item, "private", _parse_sold_date(item))
            item["_resolved_id"] = item_id
            combined_seen_ids.add(item_id)
            all_items.append(item)
    print(f"📦 Private sold: {sum(1 for i in all_items if i['_seller_pool'] == 'private')} "
          f"({sum(1 for i in all_items if i.get('_is_auction'))} auction)")

    # Pass 2: all-seller sold listings — deduped, adds volume
    for item in run_sold_search("", "sold_all"):
        item_id = item.get("itemId") or item.get("title", "")[:60]
        if item_id and item_id not in combined_seen_ids:
            _tag_item(item, "all", _parse_sold_date(item))
            item["_resolved_id"] = item_id
            combined_seen_ids.add(item_id)
            all_items.append(item)
    print(f"📦 Total blended sold: {len(all_items)} "
          f"({sum(1 for i in all_items if i.get('_is_auction'))} auction total)")

    # Fallback: if zero results (very rare model), retry without category filter
    if not all_items:
        print(f"⚠️ Zero results with category filter — retrying without category_ids")
        for item in run_sold_search("", "sold_all_nocat", use_category=False):
            item_id = item.get("itemId") or item.get("title", "")[:60]
            if item_id and item_id not in combined_seen_ids:
                _tag_item(item, "all", None)
                item["_resolved_id"] = item_id
                combined_seen_ids.add(item_id)
                all_items.append(item)
        print(f"📦 After no-category fallback: {len(all_items)}")

    return all_items

def get_active_listings(query: str, limit: int = 40, budget_fn=None):
    """
    Fetches ACTIVE listings to determine the market floor price.
    """

    token = get_ebay_access_token()
    if not token:
        return []

    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_GB",
    }

    params = {
        "q": query,
        "limit": limit,
        "filter": "conditions:{USED}",
        "sort": "price",
        "category_ids": "9801"
    }

    if _is_circuit_open():
        print("⚡ Active listing fallback — circuit open, skipping")
        return []

    if budget_fn and not budget_fn(1):
        return []

    throttle_ebay()
    response = requests.get(SEARCH_URL, headers=headers, params=params)

    if response.status_code == 429:
        _trip_circuit()
        print("⚡ Active listing fallback — rate limited, circuit tripped")
        return []

    if response.status_code != 200:
        return []

    return response.json().get("itemSummaries", [])


# ---------------------------------------------------
# CORE FILTER ENGINE
# ---------------------------------------------------

def run_filter_layer(
    summaries,
    target_year,
    target_mileage,
    year_tolerance,
    mileage_tolerance,
    adjust_mileage=True,
    layer_name="",
    private_only=False,
    base_model=None,
    engine_litre=None
):
    sold_prices   = []   # Private BIN → private market value
    auction_prices = []  # Auction completions → trade/wholesale value

    rejected_no_year = 0
    rejected_year = 0
    rejected_mileage = 0
    rejected_no_price = 0
    rejected_dealer = 0
    accepted_sold = 0
    mileage_diffs = []
    adjustments = []
    sample_comps = []

    for summary in summaries:
        # If private_only mode, skip dealer-sourced listings entirely
        seller_pool = summary.get("_seller_pool", "all")

        if private_only and seller_pool != "private":
            rejected_dealer += 1
            continue

        title = summary.get("title", "").lower()

        if any(x in title for x in [
            "breaking",
            "spares",
            "parts",
            "engine",
            "gearbox",
            "wheel",
            "door",
            "bumper",
        ]):
            continue

        if any(x in title for x in [
            "finance available",
            "warranty included",
            "dealer warranty",
            "part exchange welcome",
        ]):
            continue

        # strict base model match — with badge fallbacks for BMW/Mercedes
        if base_model:
            model_pattern = rf"\b{re.escape(base_model.lower())}\b"
            title_match = re.search(model_pattern, title)

            if not title_match:
                # BMW "3 Series" → also accept badge variants "330d", "320i", "328i" etc.
                series_m = re.match(r'^(\d) series$', base_model.lower())
                if series_m:
                    n = series_m.group(1)
                    title_match = re.search(rf"\b{n}\d{{2}}[a-z]?\b", title)

            if not title_match:
                # Mercedes "C-Class" → also accept "c220d", "c250", "c180" etc.
                class_m = re.match(r'^([a-z]{1,3})-class$', base_model.lower())
                if class_m:
                    letter = re.escape(class_m.group(1))
                    title_match = re.search(rf"\b{letter}\d{{2,3}}[a-z]?\b", title)

            if not title_match:
                continue

        if engine_litre:
            engine_match = re.search(r"\b(\d\.\d)\b", title)

            if engine_match:
                listing_engine = float(engine_match.group(1))
            else:
                badge_match = re.search(r"\b(\d{2,3})[di]\b", title)
                if badge_match:
                    digits = badge_match.group(1)

                    # BMW style badge parsing
                    if len(digits) == 3:
                        listing_engine = float(digits[1] + "." + digits[2])
                    else:
                        listing_engine = None
                else:
                    listing_engine = None

            if listing_engine and abs(listing_engine - engine_litre) > 0.25:
                continue
                
        listing_year = summary.get("_year")
        listing_mileage = summary.get("_mileage")
        source_type = summary.get("_source_type", "sold")

        total_accepted = accepted_sold

        if listing_year is None:
            rejected_no_year += 1
            continue

        year_diff = abs(listing_year - target_year)
        if year_diff > year_tolerance:
            rejected_year += 1
            continue

        mileage_diff = None
        if listing_mileage is not None:
            mileage_diff = listing_mileage - target_mileage
            abs_mileage_diff = abs(mileage_diff)

            if abs_mileage_diff > mileage_tolerance:
                rejected_mileage += 1
                continue

        price_obj = summary.get("price")
        if not price_obj:
            rejected_no_price += 1
            continue

        base_price = float(price_obj["value"])

        # Reject junk listings — parts, scams, placeholder prices.
        # Floor is £300, not £700: budget cars (Saab 9-3, Ka, Punto) have
        # legitimate comparables in the £300-£699 range that must not be
        # excluded. IQR trimming handles extreme low-end outliers.
        if base_price < 300:
            continue

        adjusted_price = base_price

        if mileage_diff is not None and adjust_mileage and listing_mileage is not None:
            mileage_diffs.append(mileage_diff)
            adjusted_price = calculate_mileage_adjustment(base_price, listing_mileage, target_mileage)
            adjustments.append(abs(adjusted_price - base_price))
        elif mileage_diff is not None:
            mileage_diffs.append(mileage_diff)

        # Weight by mileage proximity — closer comparables repeated more in pool
        weight = mileage_proximity_weight(listing_mileage, target_mileage, mileage_tolerance)

        if source_type == "sold":
            # Three pools — each measures a different market:
            #
            # PRIVATE BIN (INDIVIDUAL seller, not auction):
            #   Private person selling to private buyer at an agreed asking price.
            #   This is what the user gets if they can't sell at retail — the floor.
            #   Goes into sold_prices → price_private.
            #
            # AUCTION (any seller):
            #   Competitive bidding sets the wholesale clearing price. This is what
            #   dealers pay at auction — the trade/wholesale value, below private.
            #   Goes into auction_prices → price_trade.
            #
            # BUSINESS BIN (dealer retail):
            #   Dealer forecourt pricing. Feeds into price_retail via RETAIL_MULTIPLIER.
            #   Excluded from both sold_prices and auction_prices.
            seller_pool = summary.get("_seller_pool", "all")
            is_auction  = summary.get("_is_auction", False)
            r_weight    = recency_weight(summary.get("_sold_date"))

            if is_auction:
                total_weight = weight * r_weight
                auction_prices.extend([adjusted_price] * total_weight)
                accepted_sold += 1
            elif seller_pool == "private":
                # Private BIN — primary signal for private market value
                total_weight = weight * r_weight
                sold_prices.extend([adjusted_price] * total_weight)
                accepted_sold += 1
            elif not private_only:
                # Business BIN — only included in blended fallback (private_only=False).
                # Excluded from private_only=True pass so private data dominates.
                # Lower weight (÷3) and tiered correction handles the retail contamination.
                total_weight = max(1, weight // 3) * r_weight
                sold_prices.extend([adjusted_price] * total_weight)
                accepted_sold += 1
            # Capture a sample of comparables for deal detail display
            if len(sample_comps) < 12:
                _sold_dt = summary.get("_sold_date")
                _date_str = _sold_dt.strftime("%d %b %Y") if _sold_dt else None
                sample_comps.append({
                    "title": summary.get("title", "")[:70],
                    "price": int(round(base_price, 0)),
                    "adjusted_price": int(round(adjusted_price, 0)),
                    "year": listing_year,
                    "mileage": listing_mileage,
                    "date": _date_str,
                    "url": summary.get("itemWebUrl", "") or "",
                    "seller_pool": summary.get("_seller_pool", ""),
                })
            accepted_sold += 1

    print(f"📊 FILTER DEBUG [{layer_name}{'|private_only' if private_only else ''}]:")
    print(f"   Sold accepted: {accepted_sold} ({len(sold_prices)} weighted entries)")
    if private_only:
        print(f"   Rejected (dealer pool): {rejected_dealer}")
    print(f"   Rejected (no year): {rejected_no_year}")
    print(f"   Rejected (year tolerance ±{year_tolerance}yr): {rejected_year}")
    print(f"   Rejected (mileage tolerance ±{mileage_tolerance}mi): {rejected_mileage}")
    print(f"   Rejected (no price): {rejected_no_price}")

    if mileage_diffs:
        print(f"   Avg mileage diff: {round(statistics.mean(mileage_diffs), 0)}")
        print(f"   Max mileage diff: {round(max(abs(x) for x in mileage_diffs), 0)}")
        if adjustments:
            print(f"   Avg price adjustment: £{round(statistics.mean(adjustments), 2)}")
    else:
        print("   No mileage data available")

    if len(sold_prices) >= MIN_SAMPLE_SIZE:
        final_prices = sold_prices
        source_label = "sold_only"
        print(f"   ✅ Using sold prices ({len(sold_prices)} weighted entries)")
    else:
        print(f"❌ Failed — {len(sold_prices)} weighted entries (min required: {MIN_SAMPLE_SIZE})")
        return None

    final_prices = sorted(final_prices)

    # IQR-based outlier trimming (Tukey fences: Q1 − 1.5×IQR, Q3 + 1.5×IQR).
    # More adaptive than a flat 10% cut — small samples (5-10 comps) are barely
    # touched; genuine outliers in larger pools are cleanly removed.
    if len(final_prices) >= 4:
        mid = len(final_prices) // 2
        q1 = statistics.median(final_prices[:mid])
        q3 = statistics.median(final_prices[mid:])
        iqr = q3 - q1
        if iqr > 0:
            lower_fence = q1 - 1.5 * iqr
            upper_fence = q3 + 1.5 * iqr
            trimmed = [p for p in final_prices if lower_fence <= p <= upper_fence]
            if len(trimmed) >= 3:
                removed = len(final_prices) - len(trimmed)
                if removed > 0:
                    print(f"   ✂️ IQR trim: removed {removed} outliers (fences £{round(lower_fence)}–£{round(upper_fence)})")
                final_prices = trimmed

    spread_discount = check_spread(final_prices, layer_name)

    sample_count = len(final_prices)

    # Confidence is based on distinct sold items, not weighted entry count.
    # Weighting inflates the pool (3× private, 3× recency) — using it for
    # confidence would report "high" from as few as 4 actual sold cars.
    no_mileage = not mileage_diffs
    if "layer_5" in layer_name:
        confidence = "low"  # Year-only fallback — mileage adjustment carrying whole comparison
    elif accepted_sold >= 10:
        confidence = "high" if not no_mileage else "medium"
    elif accepted_sold >= 5:
        confidence = "medium"
    else:
        confidence = "low"

    # Wide spread with no mileage data means we're averaging across different
    # mileage bands — the value is directionally useful but not precise.
    if no_mileage and spread_discount < 1.0 and confidence == "high":
        confidence = "medium"

    sold_median = statistics.median(final_prices)
    raw_private = sold_median * spread_discount

    # Private value: soldItems:true + sellerAccountTypes:{INDIVIDUAL} means these are
    # actual completed private transactions — the median is the private market price.
    # No correction factor applied. If Regit validation shows consistent over/under,
    # a single flat multiplier can be introduced here.
    price_private = round(raw_private, 2)

    # Retail value: private × RETAIL_MULTIPLIER (dealer forecourt ≈ 30% above private clean)
    price_retail = round(price_private * RETAIL_MULTIPLIER, 2)

    # Trade value: mileage-tiered multiplier against private.
    # eBay auction-format sold prices are consumer bids, not BCA/Manheim trade
    # clearing prices, so we don't use auction comps here.
    # get_trade_multiplier: 0.88 (<30k mi) → 0.72 (120k+ mi), +0.02 for prestige makes.
    if auction_prices:
        print(f"   📦 {len(auction_prices)} eBay auction comps found (not used for trade — consumer bids ≠ trade prices)")
    price_trade = round(price_private * get_trade_multiplier(target_mileage), 2)

    print(f"   💰 Private: £{price_private} | Retail: £{price_retail} | Trade: £{price_trade} ({confidence} confidence, pool: {source_label})")

    return {
        "market_price":  price_private,   # backward-compat alias
        "price_private": price_private,
        "price_retail":  price_retail,
        "price_trade":   price_trade,
        "sample_size":   sample_count,
        "confidence":    confidence,
        "source_label":  source_label,
        "sample_comps":  sample_comps,
    }


# --------------------------------------------------
# PUBLIC ENTRY
# --------------------------------------------------

def get_market_price_from_sold(
    make, model, year, mileage,
    engine_size=None, listing_title=None, listing_aspects=None,
    fuel_type=None, body_style=None,
    cache_only=False, budget_fn=None,
):
    """
    cache_only=True  → return cached result or None, never burn eBay API calls.
    budget_fn        → optional callable(n: int) -> bool, called before each eBay
                       request inside this function. Returns False = stop immediately.
                       Pass tasks._check_budget to route all valuation calls through
                       the shared daily budget guard.
    """
    if not make or not model or not year:
        return None

    if not mileage:
        mileage = 100000

    make = str(make).strip().title()
    # Normalise make name so prewarm and live valuations share the same cache key.
    # deal_engine.py aliases "MERCEDES-BENZ" → "Mercedes" before calling here,
    # but prewarm calls this directly with "Mercedes-Benz" from PREWARM_TARGETS.
    # Without this, every Mercedes car is a cache miss — prewarm is completely wasted.
    make = _MAKE_CACHE_ALIASES.get(make, make)
    model = str(model).strip().title()
    model_words = model.split()
    base_model = model_words[0]
    # Pass the full model so multi-word DVSA models ("C Class", "Range Rover Sport")
    # are resolved correctly instead of only using the first word
    base_model = normalise_base_model(make, base_model, full_model=model)
    trim = " ".join(model_words[1:]) if len(model_words) > 1 else None

    engine_litre = None
    if engine_size:
        try:
            cleaned = re.sub(r"[^\d.]", "", str(engine_size))
            size = float(cleaned)
            engine_litre = round(size / 1000, 1) if size > 10 else round(size, 1)
        except:
            pass

    # Compute fuel suffix early so it can be included in the cache key.
    # Diesel/petrol pools differ by 15-20% — sharing a key contaminates valuations.
    # Hybrid gets its own suffix: large pool, sits between petrol and EV pricing.
    # Cache keys without fuel (prewarm) are the generic fallback.
    fuel_suffix = ""
    if fuel_type:
        ft = fuel_type.lower()
        if "diesel" in ft:
            fuel_suffix = " diesel"
        elif "hybrid" in ft:
            fuel_suffix = " hybrid"
        elif "petrol" in ft:
            fuel_suffix = " petrol"
        elif "electric" in ft:
            fuel_suffix = " electric"

    # Round to nearest 20k bucket rather than always down.
    # A 77k car rounded DOWN to 60k gets comps centered on 60k (45k-75k range) which
    # systematically excludes the car itself. Rounding to nearest uses the 80k bucket
    # (65k-95k) which correctly includes the target mileage.
    mileage_bucket = max(20000, int((mileage + 10000) / 20000) * 20000)
    engine_bucket = bucket_engine_size(engine_litre)
    fuel_key = fuel_suffix.strip()
    # Normalise make to lowercase so prewarm keys always match sniper/live lookup keys
    # regardless of whether the source is DVSA (uppercase e.g. "BMW") or prewarm targets (e.g. "Bmw").
    make_key = make.lower()
    cache_key = f"sold_cache:{make_key}:{base_model}:{engine_bucket}:{year}:{mileage_bucket}:{fuel_key}" if fuel_key else f"sold_cache:{make_key}:{base_model}:{engine_bucket}:{year}:{mileage_bucket}"
    # Prewarm stores keys without fuel type and also without engine bucket (None).
    # Build fallback keys for progressive lookup: fuel+engine → no-fuel → no-engine.
    no_fuel_key    = f"sold_cache:{make_key}:{base_model}:{engine_bucket}:{year}:{mileage_bucket}"
    no_engine_key  = f"sold_cache:{make_key}:{base_model}:None:{year}:{mileage_bucket}"
    print(f"   🔑 Cache key: {cache_key}")
    cached = redis_client.get(cache_key)
    # Fallback 1: no-fuel key (prewarm stores generic keys without fuel type)
    if not cached and fuel_key and cache_key != no_fuel_key:
        cached = redis_client.get(no_fuel_key)
        if cached:
            print(f"   ↩️  Fuel-specific miss — fell back to no-fuel key: {no_fuel_key}")
    # Fallback 2: no-engine-no-fuel key (prewarm engine=None bucket always populated)
    if not cached and engine_bucket is not None and no_fuel_key != no_engine_key:
        cached = redis_client.get(no_engine_key)
        if cached:
            print(f"   ↩️  Engine-specific miss — fell back to engine=None key: {no_engine_key}")
    if cached:
        data = json.loads(cached)
        # Recalculate trade price with mileage-adjusted multiplier on cache hit
        if data.get("price_private"):
            trade_mult = get_trade_multiplier(mileage, make)
            data["price_trade"] = round(data["price_private"] * trade_mult, 2)
            data["trade_multiplier"] = trade_mult
        print(f"   ✅ Cache HIT — private £{data.get('price_private', data.get('market_price'))} | retail £{data.get('price_retail', '?')} | trade £{data.get('price_trade', '?')} (trade mult: {data.get('trade_multiplier', 0.78)})")
        return data

    # During scan tasks, never fall through to live eBay calls.
    # Cache miss = no valuation. Prewarm fills the cache.
    if cache_only:
        print(f"   ❌ Cache miss — skipping live valuation (cache_only=True, prewarm will fill)")
        return None
    title_lower = listing_title.lower() if listing_title else ""

    if not engine_litre and listing_title:
        litre_match = re.search(r"\b(\d\.\d)\b", title_lower)
        if litre_match:
            try:
                engine_litre = float(litre_match.group(1))
            except:
                pass

        if not engine_litre:
            badge_match = re.search(r"\b(\d{2,3})[di]\b", title_lower)
            if badge_match:
                digits = badge_match.group(1)
                try:
                    if len(digits) == 3:
                        engine_litre = float(digits[1] + "." + digits[2])
                    elif len(digits) == 2:
                        engine_litre = float(digits[0] + "." + digits[1])
                except:
                    pass
      
    if listing_aspects:
        if not engine_litre:
            aspect_engine = listing_aspects.get("Engine Size")
            if aspect_engine:
                try:
                    cleaned = re.sub(r"[^\d.]", "", str(aspect_engine))
                    size = float(cleaned)
                    engine_litre = round(size / 1000, 1) if size > 10 else round(size, 1)
                except:
                    pass

    # Include year in live valuation queries: we know the exact target year so eBay
    # will surface more year-matching sold listings, giving the filter layers more
    # correct comparables to work with. The prewarm intentionally omits year because
    # it searches once per model and filters across all year buckets from one result set.
    # Strip hyphens from make/model — eBay treats hyphen as exclusion operator,
    # so "Ford S-Max" searches for "Ford S" NOT "Max" and returns 0 results.
    query = f"{make.replace('-', ' ')} {base_model.replace('-', ' ')} {year}{fuel_suffix}"

    # Dynamically scale mileage tolerance based on target mileage.
    # High mileage cars have thin comparable pools — widen tolerance to compensate.
    # Low mileage cars stay tight — they have plenty of comparables already.
    l1_tolerance, l2_tolerance = get_mileage_tolerances(mileage)

    print(f"🔎 LIVE VALUATION: make={make} base_model={base_model} engine={engine_litre}L year={year} mileage={mileage}mi")
    print(f"   Query: '{query}' | Mileage tolerances: L1=±{l1_tolerance}, L2=±{l2_tolerance}")
    print(f"   cache_key={cache_key}")

    all_summaries = get_sold_listings(query, budget_fn=budget_fn)

    print(f"📦 Total unique summaries collected: {len(all_summaries)}")

    if not all_summaries:
        return None

    enriched_summaries = _pre_expand_details(all_summaries, budget_fn=budget_fn, prewarm_mode=False)

    for tolerance_config in [
        # layer_1: ±1 year — same model year only, prevents newer/more-valuable comps
        # from contaminating valuations (e.g. 2010 Range Rover inflating a 2008 valuation).
        {"year_tolerance": 1, "mileage_tolerance": l1_tolerance,         "source": "layer_1_strict",          "adjust_mileage": True},
        {"year_tolerance": 2, "mileage_tolerance": l2_tolerance,         "source": "layer_2_relaxed_mileage", "adjust_mileage": True},
        {"year_tolerance": 3, "mileage_tolerance": l2_tolerance + 5000,  "source": "layer_3_relaxed_year",    "adjust_mileage": True},
        {"year_tolerance": 4, "mileage_tolerance": l2_tolerance + 15000, "source": "layer_4_wide",            "adjust_mileage": True},
        # layer_5 (year_only, unlimited mileage) removed from live valuations —
        # it matches cars from completely different model years and inflates prices.
        # Prewarm still uses layers 1-4 only, so cache coverage is unaffected.
    ]:
        # Try private-sold-only first — no dealer retail contamination
        result = run_filter_layer(
            enriched_summaries,
            target_year=year,
            target_mileage=mileage,
            base_model=base_model,
            engine_litre=engine_litre,
            year_tolerance=tolerance_config["year_tolerance"],
            mileage_tolerance=tolerance_config["mileage_tolerance"],
            adjust_mileage=tolerance_config["adjust_mileage"],
            layer_name=tolerance_config["source"],
            private_only=True,
        )

        # Fall back to blended (private + dealer) only if private pool is too thin
        if not result:
            result = run_filter_layer(
                enriched_summaries,
                target_year=year,
                target_mileage=mileage,
                base_model=base_model,
                engine_litre=engine_litre,
                year_tolerance=tolerance_config["year_tolerance"],
                mileage_tolerance=tolerance_config["mileage_tolerance"],
                adjust_mileage=tolerance_config["adjust_mileage"],
                layer_name=f"{tolerance_config['source']}+blended",
            )

        if result:
            layer = tolerance_config["source"]
            print(f"   ✅ Layer '{layer}' succeeded — private £{result['price_private']} | retail £{result['price_retail']} | trade £{result['price_trade']} | confidence={result['confidence']}")
            # Apply direct extreme mileage penalty to final market prices.
            # Corrects for when the target car itself has extreme mileage
            # and comparables couldn't fully account for it.
            if mileage > EXTREME_MILEAGE_THRESHOLD:
                excess = mileage - EXTREME_MILEAGE_THRESHOLD
                extra_blocks = min(excess / 10000, 15)
                extreme_penalty_pct = min(0.025 * extra_blocks, 0.50)
                factor = 1 - extreme_penalty_pct
                original = result["price_private"]
                result["price_private"] = round(original * factor, 2)
                result["price_retail"]  = round(result["price_retail"] * factor, 2)
                result["price_trade"]   = round(result["price_trade"] * factor, 2)
                result["market_price"]  = result["price_private"]
                print(f"   🔻 Final extreme mileage penalty: {mileage}mi → −{round(extreme_penalty_pct*100,1)}% → private £{result['price_private']} (was £{original})")

            result["source"] = tolerance_config["source"]
            redis_client.set(cache_key, json.dumps(result), ex=CACHE_TTL)
            return result

    # All sold layers failed — try active listings as a last resort
    print("⚠️ Sold comparables insufficient across all layers — attempting active listing fallback")
    active_result = _active_listing_fallback(
        query=query,
        year=year,
        mileage=mileage,
        base_model=base_model,
        engine_litre=engine_litre,
        mileage_tolerance=l2_tolerance + 15000,
        budget_fn=budget_fn,
    )

    if active_result:
        if mileage > EXTREME_MILEAGE_THRESHOLD:
            excess = mileage - EXTREME_MILEAGE_THRESHOLD
            extra_blocks = min(excess / 10000, 15)
            extreme_penalty_pct = min(0.025 * extra_blocks, 0.50)
            factor = 1 - extreme_penalty_pct
            active_result["price_private"] = round(active_result["price_private"] * factor, 2)
            active_result["price_retail"]  = round(active_result["price_retail"] * factor, 2)
            active_result["price_trade"]   = round(active_result["price_trade"] * factor, 2)
            active_result["market_price"]  = active_result["price_private"]
            print(f"   🔻 Active fallback extreme mileage penalty: −{round(extreme_penalty_pct*100,1)}% → private £{active_result['price_private']}")

        active_result["source"] = "active_fallback"
        redis_client.set(cache_key, json.dumps(active_result), ex=CACHE_TTL)
        return active_result

    redis_client.set(cache_key, json.dumps({"market_price": None}), ex=3600)
    return None


def _active_listing_fallback(
    query, year, mileage, base_model, engine_litre, mileage_tolerance, budget_fn=None
):
    """
    Fallback valuation from active (non-sold) listings when sold comparables are
    insufficient across all filter layers.

    Returns a result dict with confidence="low" and source="active_fallback",
    or None if too few matching listings.

    Price path: asking_price → ×MARKET_REALISM_FACTOR × ACTIVE_LISTING_DISCOUNT
    (accounts for eBay retail premium AND the gap between asking and likely sale price)
    """
    print("📋 Attempting active listing fallback...")
    active_summaries = get_active_listings(query, limit=40, budget_fn=budget_fn)

    if not active_summaries:
        print("📋 Active fallback: no listings returned")
        return None

    prices = []
    year_tol = 3

    for item in active_summaries:
        title = item.get("title", "").lower()

        if any(x in title for x in ["breaking", "spares", "parts", "engine", "gearbox"]):
            continue

        if base_model and not re.search(rf"\b{re.escape(base_model.lower())}\b", title):
            continue

        if engine_litre:
            engine_match = re.search(r"\b(\d\.\d)\b", title)
            if engine_match and abs(float(engine_match.group(1)) - engine_litre) > 0.5:
                continue

        listing_year = extract_year_from_title(item.get("title", ""))
        if listing_year and abs(listing_year - year) > year_tol:
            continue

        listing_mileage = extract_mileage_from_text(item.get("title", ""))
        if listing_mileage and abs(listing_mileage - mileage) > mileage_tolerance:
            continue

        price_obj = item.get("price")
        if not price_obj:
            continue

        price = float(price_obj.get("value", 0))
        if price < 300:
            continue

        prices.append(price)

    print(f"📋 Active fallback: {len(prices)} qualifying listings (year ±{year_tol}, mileage ±{mileage_tolerance})")

    # Active fallback minimum is 3, not MIN_SAMPLE_SIZE (5).
    # For rare/old makes (Saab, Ssangyong, Chrysler) eBay may only return
    # 3-4 matching active listings — 3 well-matched asking prices is still
    # enough for a low-confidence directional estimate rather than no value.
    if len(prices) < 3:
        print(f"📋 Active fallback: insufficient sample ({len(prices)} < 3)")
        return None

    prices = sorted(prices)
    cut = int(len(prices) * 0.1)
    if cut > 0:
        prices = prices[cut:-cut]

    spread_discount = check_spread(prices, "active_fallback")
    median_asking = statistics.median(prices)

    # asking price → realistic sold price → three CAP-equivalent values
    price_private = round(median_asking * spread_discount * ACTIVE_SALE_DISCOUNT, 2)
    price_retail  = round(price_private * RETAIL_MULTIPLIER, 2)
    price_trade   = round(price_private * TRADE_MULTIPLIER, 2)

    print(f"   📋 Active fallback: asking median £{round(median_asking)} → private £{price_private} | retail £{price_retail} | trade £{price_trade}")

    return {
        "market_price":  price_private,
        "price_private": price_private,
        "price_retail":  price_retail,
        "price_trade":   price_trade,
        "sample_size":   len(prices),
        "confidence":    "low",
        "source_label":  "active_fallback",
    }


def _pre_expand_details(summaries: list, budget_fn=None, prewarm_mode: bool = False) -> list:
    """
    Enriches sold listing summaries with year and mileage data.

    KEY RULE: Only expand a detail call if YEAR is missing from the title.
      - Missing mileage alone does NOT trigger a detail call.
      - The filter layer handles mileage-less listings gracefully (weight 1).
      - Year missing = listing gets rejected entirely = worth 1 API call to fix.
      - While we have the detail open, we grab mileage too at zero extra cost.

    prewarm_mode=True:  cap = MAX_PREWARM_EXPANSIONS (15 per model)
    prewarm_mode=False: cap = MAX_DETAIL_EXPANSIONS  (60, live scan cache miss)

    Models returning fewer than 8 results skip expansion entirely —
    not enough data to be useful regardless of enrichment.
    """
    expansions = 0
    cap = MAX_PREWARM_EXPANSIONS if prewarm_mode else MAX_DETAIL_EXPANSIONS
    enriched = []

    # Skip expansion entirely for thin result sets — not worth the API calls
    if len(summaries) < 8:
        for summary in summaries:
            title = summary.get("title", "")
            summary["_year"] = extract_year_from_title(title)
            summary["_mileage"] = extract_mileage_from_text(title)
            enriched.append(summary)
        print(f"🔍 Pre-expansion skipped: only {len(summaries)} results (min 8 required)")
        return enriched

    for summary in summaries:
        title = summary.get("title", "")
        item_id = summary.get("itemId")

        listing_year = extract_year_from_title(title)
        listing_mileage = extract_mileage_from_text(title)

        # Only expand if YEAR is missing — mileage alone doesn't justify a detail call
        already_enriched = sum(1 for s in enriched if s.get("_year") is not None)

        if listing_year is None and expansions < cap and already_enriched < MAX_ENRICHED_TARGET:
            if budget_fn and not budget_fn(1):
                print("🛑 Budget exhausted — stopping detail expansions")
                summary["_year"] = listing_year
                summary["_mileage"] = listing_mileage
                enriched.append(summary)
                continue

            detail = get_item_detail(item_id)
            expansions += 1

            if detail:
                for aspect in detail.get("localizedAspects", []):
                    name = aspect.get("name", "").lower()
                    raw_value = aspect.get("value", "")
                    val = str(raw_value).strip() if isinstance(raw_value, str) else str(raw_value[0]).strip()

                    if not val:
                        continue

                    if listing_year is None and any(k in name for k in ["year", "reg"]):
                        match = re.search(r"(19\d{2}|20\d{2})", val)
                        if match:
                            listing_year = int(match.group(1))

                    # Grab mileage too while we have the detail open — free since we already called
                    if listing_mileage is None and any(k in name for k in ["mileage", "miles", "odometer"]):
                        try:
                            listing_mileage = int(val.replace(",", "").replace(" ", "").split(".")[0])
                        except:
                            pass

        summary["_year"] = listing_year
        summary["_mileage"] = listing_mileage
        enriched.append(summary)

    has_year = sum(1 for s in enriched if s.get("_year") is not None)
    has_mileage = sum(1 for s in enriched if s.get("_mileage") is not None)
    mode_label = "prewarm" if prewarm_mode else "live"
    print(f"🔍 Pre-expansion complete: {expansions} detail calls used ({mode_label}, cap={cap}) — {has_year}/{len(enriched)} have year, {has_mileage}/{len(enriched)} have mileage")
    return enriched