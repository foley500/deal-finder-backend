import statistics
import os
import redis
import json
import requests
import re
import time
from app.services.ebay_rate_limiter import throttle_ebay
from app.services.ebay_browse_service import (
    get_ebay_access_token,
    get_item_detail
)

SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
REDIS_URL = os.getenv("CELERY_BROKER_URL")
redis_client = redis.from_url(REDIS_URL)

CACHE_TTL = 21600          # 6 hours — prices don't change hourly
MAX_DETAIL_EXPANSIONS = 60  # Scan-time expansion cap (live valuations on cache miss)
MAX_PREWARM_EXPANSIONS = 15 # Prewarm cap — year-only, missing mileage does NOT trigger expansion
MIN_SAMPLE_SIZE = 5
MAX_ENRICHED_TARGET = 80    # Stop once 80 items have full year data


MILEAGE_BLOCK_SIZE = 5000
MILEAGE_BLOCK_RATE = 0.015
MAX_LINEAR_BLOCKS = 8
EXTREME_MILEAGE_THRESHOLD = 120000

MAX_ACCEPTABLE_IQR_RATIO = 0.55
WIDE_SPREAD_DISCOUNT = 0.97

# Mileage bands for dynamic layer_1 tolerance scaling.
# Low mileage cars have plenty of comparables — keep tight.
# High mileage cars have thin comparable pools — widen to compensate.
# Format: (mileage_threshold, layer_1_tolerance, layer_2_tolerance)
MILEAGE_TOLERANCE_BANDS = [
    (60000,  15000, 25000),   # <60k miles  — tight, plenty of comparables
    (100000, 18000, 28000),   # 60k-100k    — slightly wider
    (140000, 25000, 35000),   # 100k-140k   — wider, pool is thinner
    (float("inf"), 32000, 45000),  # 140k+  — wide, very thin pool at this mileage
]


# ---------------------------------------------------
# HELPERS
# ---------------------------------------------------

def get_mileage_tolerances(target_mileage: int) -> tuple:
    """Returns (layer_1_tolerance, layer_2_tolerance) based on target mileage."""
    for threshold, l1, l2 in MILEAGE_TOLERANCE_BANDS:
        if target_mileage < threshold:
            return l1, l2
    return 32000, 45000


def extract_year_from_title(title: str):
    match = re.search(r"\b(19\d{2}|20\d{2})\b", title)
    return int(match.group(1)) if match else None


def extract_mileage_from_text(text: str):
    if not text:
        return None

    text = text.lower().replace(",", "")

    match = re.search(r"(\d{4,6})\s*(miles|mile|mi)\b", text)
    if match:
        val = int(match.group(1))
        if 1000 < val < 300000:
            return val

    match = re.search(r"(\d{2,3})\s?k\b", text)
    if match:
        val = int(match.group(1)) * 1000
        if 1000 < val < 300000:
            return val

    return None


def normalise_base_model(make: str, base_model: str) -> str:
    make_lower = make.lower()
    model_lower = base_model.lower().strip()

    if make_lower == "bmw":
        # "118", "120", "318" etc → extract leading digit → "1 Series", "3 Series"
        if model_lower.isdigit():
            if len(model_lower) >= 3:
                series_num = model_lower[0]
                return f"{series_num} Series"
            return f"{base_model} Series"
        # Handle variants like "118d", "320i" etc
        m = re.match(r'^([1-9])\d{2}', model_lower)
        if m:
            return f"{m.group(1)} Series"
        # MINI models that ended up under BMW make
        if model_lower in ["hatch", "cooper", "one", "john cooper works", "jcw"]:
            return "Mini"

    if make_lower in ["mercedes", "mercedes-benz"]:
        # "Aclass"/"A Class" → "A-Class", "Cclass" → "C-Class" etc
        if "class" in model_lower and "-" not in model_lower:
            return model_lower.replace("class", "-class").title()
        # "Gla", "Glb", "Glc", "Gle", "Cls", "Clk", "Slk", "Amg" etc
        # These are fine as-is — eBay recognises them
        # Fix ML: "Ml350", "Ml320" etc → "ML-Class"
        if re.match(r'^ml\d', model_lower):
            return "ML-Class"
        # Fix GLA/GLB/GLC/GLE/GLS: already correct, just ensure title case
        if re.match(r'^gl[abces]$', model_lower):
            return base_model.upper()
        # "mercedes" as model (bad title fallback) — return as-is, valuation will fail gracefully
        if model_lower == "mercedes":
            return base_model

    return base_model


def calculate_mileage_adjustment(base_price: float, listing_mileage: int, target_mileage: int) -> float:
    mileage_diff = listing_mileage - target_mileage
    abs_diff = abs(mileage_diff)

    blocks = min(abs_diff / MILEAGE_BLOCK_SIZE, MAX_LINEAR_BLOCKS)
    linear_adjustment = base_price * MILEAGE_BLOCK_RATE * blocks

    extreme_penalty = 0.0
    if listing_mileage > EXTREME_MILEAGE_THRESHOLD:
        excess = listing_mileage - EXTREME_MILEAGE_THRESHOLD
        extra_blocks = min(excess / 10000, 15)
        extreme_penalty = base_price * 0.02 * extra_blocks
        print(f"   ⚠️ Extreme mileage penalty: {listing_mileage}mi → −£{round(extreme_penalty, 2)}")

    total_adjustment = linear_adjustment + extreme_penalty

    if mileage_diff > 0:
        return base_price - total_adjustment
    else:
        return base_price + linear_adjustment


def mileage_proximity_weight(listing_mileage: int, target_mileage: int, tolerance: int) -> int:
    """
    Returns a repeat count (weight) for a comparable based on how close
    its mileage is to the target. Closer = higher weight in the median pool.

    Weight 3 = very close  (within 25% of tolerance)
    Weight 2 = close       (within 60% of tolerance)
    Weight 1 = acceptable  (within tolerance)

    Cars with no mileage data get weight 1 (neutral).
    """
    if listing_mileage is None:
        return 1

    abs_diff = abs(listing_mileage - target_mileage)

    if abs_diff <= tolerance * 0.25:
        return 3
    elif abs_diff <= tolerance * 0.60:
        return 2
    else:
        return 1


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

    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_GB",
    }

    all_items = []
    combined_seen_ids = set()

    def run_sold_search(seller_filter: str, label: str, use_category: bool = True):
        if budget_fn and not budget_fn(1):
            print(f"🛑 Budget exhausted — skipping [{label}]")
            return []

        params = {
            "q": query,
            "limit": limit,
            "filter": f"soldItems:true,conditions:{{USED}}{seller_filter}",
            "sort": "newlyListed",
        }
        if use_category:
            params["category_ids"] = "9801"

        throttle_ebay()
        response = None
        for attempt in range(3):
            response = requests.get(SEARCH_URL, headers=headers, params=params)
            if response.status_code == 429:
                wait = 15 * (2 ** attempt)
                print(f"⏳ [{label}] Rate limited — waiting {wait}s (attempt {attempt+1}/3)")
                time.sleep(wait)
            else:
                break

        if response.status_code == 429:
            print(f"❌ [{label}] Rate limited after 3 retries — skipping")
            return []
        if response.status_code != 200:
            print(f"❌ [{label}] search error: {response.status_code}")
            return []

        items = response.json().get("itemSummaries", [])
        print(f"✅ [{label}] '{query[:35]}' → {len(items)} items")
        return items

    # Pass 1: private sold listings — most accurate price signal
    for item in run_sold_search(",sellerAccountTypes:{INDIVIDUAL}", "sold_private"):
        item_id = item.get("itemId") or item.get("title", "")[:60]
        if item_id:
            item["_source_type"] = "sold"
            item["_seller_pool"] = "private"
            item["_resolved_id"] = item_id
            combined_seen_ids.add(item_id)
            all_items.append(item)
    print(f"📦 Private sold: {sum(1 for i in all_items if i['_seller_pool'] == 'private')}")

    # Pass 2: all-seller sold listings — deduped, adds volume
    for item in run_sold_search("", "sold_all"):
        item_id = item.get("itemId") or item.get("title", "")[:60]
        if item_id and item_id not in combined_seen_ids:
            item["_source_type"] = "sold"
            item["_seller_pool"] = "all"
            item["_resolved_id"] = item_id
            combined_seen_ids.add(item_id)
            all_items.append(item)
    print(f"📦 Total blended sold: {len(all_items)}")

    # Fallback: if zero results (very rare model), retry without category filter
    if not all_items:
        print(f"⚠️ Zero results with category filter — retrying without category_ids")
        for item in run_sold_search("", "sold_all_nocat", use_category=False):
            item_id = item.get("itemId") or item.get("title", "")[:60]
            if item_id and item_id not in combined_seen_ids:
                item["_source_type"] = "sold"
                item["_seller_pool"] = "all"
                item["_resolved_id"] = item_id
                combined_seen_ids.add(item_id)
                all_items.append(item)
        print(f"📦 After no-category fallback: {len(all_items)}")

    return all_items


# ---------------------------------------------------
# CORE FILTER ENGINE
# ---------------------------------------------------

def run_filter_layer(summaries, target_year, target_mileage, year_tolerance, mileage_tolerance, adjust_mileage=True, layer_name="", private_only=False):
    sold_prices = []

    rejected_no_year = 0
    rejected_year = 0
    rejected_mileage = 0
    rejected_no_price = 0
    rejected_dealer = 0
    accepted_sold = 0
    mileage_diffs = []
    adjustments = []

    for summary in summaries:
        # If private_only mode, skip dealer-sourced listings entirely
        if private_only and summary.get("_seller_pool") == "all":
            rejected_dealer += 1
            continue

        listing_year = summary.get("_year")
        listing_mileage = summary.get("_mileage")
        source_type = summary.get("_source_type", "sold")

        total_accepted = accepted_sold

        if listing_year is None:
            if total_accepted >= MIN_SAMPLE_SIZE:
                rejected_no_year += 1
                continue
            else:
                listing_year = target_year

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

        # Reject junk listings — parts, scams, placeholder prices
        if base_price < 200:
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
            # Private sold results are the most accurate comparables.
            # Dealer-sold prices reflect retail, not trade/private values — downweight heavily.
            seller_pool = summary.get("_seller_pool", "all")
            pool_weight = 3 if seller_pool == "private" else 1
            sold_prices.extend([adjusted_price] * (weight * pool_weight))
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

    # Trim outliers (10% each end)
    cut = int(len(final_prices) * 0.1)
    if cut > 0:
        final_prices = final_prices[cut:-cut]

    spread_discount = check_spread(final_prices, layer_name)

    sample_count = len(final_prices)

    if "layer_5" in layer_name:
        confidence = "low"  # Year-only fallback — mileage adjustment is carrying the whole comparison
    elif sample_count >= 10:
        confidence = "high"
    elif sample_count >= 5:
        confidence = "medium"
    else:
        confidence = "low"

    market_price = round(statistics.median(final_prices) * spread_discount, 2)
    print(f"   💰 Market price: £{market_price} ({confidence} confidence, pool: {source_label})")

    return {
        "market_price": market_price,
        "sample_size": sample_count,
        "confidence": confidence,
        "source_label": source_label,
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
    model = str(model).strip().title()
    model_words = model.split()
    base_model = model_words[0]
    base_model = normalise_base_model(make, base_model)
    trim = " ".join(model_words[1:]) if len(model_words) > 1 else None

    engine_litre = None
    if engine_size:
        try:
            cleaned = re.sub(r"[^\d.]", "", str(engine_size))
            size = float(cleaned)
            engine_litre = round(size / 1000, 1) if size > 10 else round(size, 1)
        except:
            pass

    mileage_bucket = round(mileage / 20000) * 20000
    cache_key = f"sold_cache:{make}:{base_model}:{year}:{mileage_bucket}"
    print(f"   🔑 Cache key: {cache_key}")
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    # During scan tasks, never fall through to live eBay calls.
    # Cache miss = no valuation. Prewarm fills the cache.
    if cache_only:
        print(f"   ⚡ Cache miss (cache_only) — skipping: {make} {base_model} {year} {mileage_bucket}mi")
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
            cc_match = re.search(r"\b(\d{3,4})\s?cc\b", title_lower)
            if cc_match:
                try:
                    engine_litre = round(int(cc_match.group(1)) / 1000, 1)
                except:
                    pass

        if not engine_litre:
            badge_match = re.search(r"\b(\d{2,3})([di])\b", title_lower)
            if badge_match:
                try:
                    digits = badge_match.group(1)
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

    # Build a targeted query using fuel type and body style when available.
    # This narrows the comparable pool to more relevant vehicles — e.g. a
    # "Ford Focus petrol 3dr" won't be valued against 5dr TDCi estates.
    # Falls through to broad query naturally if the tighter pool is too thin.
    query_parts = [make, base_model]

    # Normalise fuel type to eBay-friendly terms
    fuel_type_normalised = None
    if fuel_type:
        ft = str(fuel_type).lower().strip()
        if ft in ("petrol", "p"):
            fuel_type_normalised = "petrol"
        elif ft in ("diesel", "d"):
            fuel_type_normalised = "diesel"
        # Hybrid/electric deliberately excluded — too few comparables to narrow safely

    if fuel_type_normalised:
        query_parts.append(fuel_type_normalised)

    # Extract body style from title — "3dr"/"5dr"/"estate"/"convertible" etc.
    body_style_term = None
    if listing_title:
        title_lower = listing_title.lower()
        if "estate" in title_lower:
            body_style_term = "estate"
        elif "convertible" in title_lower or "cabriolet" in title_lower or "cabrio" in title_lower:
            body_style_term = "convertible"
        elif "coupe" in title_lower or "coupé" in title_lower:
            body_style_term = "coupe"
        elif "van" in title_lower:
            body_style_term = "van"
        elif re.search(r"\b3\s*dr\b|\b3-door\b", title_lower):
            body_style_term = "3dr"
        elif re.search(r"\b5\s*dr\b|\b5-door\b", title_lower):
            body_style_term = "5dr"

    if body_style_term:
        query_parts.append(body_style_term)

    query = " ".join(query_parts)

    # Dynamically scale mileage tolerance based on target mileage.
    # High mileage cars have thin comparable pools — widen tolerance to compensate.
    # Low mileage cars stay tight — they have plenty of comparables already.
    l1_tolerance, l2_tolerance = get_mileage_tolerances(mileage)

    print(f"🔎 Searching: make={make} base_model={base_model} engine={engine_litre} year={year} mileage={mileage}")
    print(f"   Query: '{query}' | Mileage tolerances: L1=±{l1_tolerance}, L2=±{l2_tolerance}")

    all_summaries = get_sold_listings(query, budget_fn=budget_fn)

    print(f"📦 Total unique summaries collected: {len(all_summaries)}")

    if not all_summaries:
        return None

    enriched_summaries = _pre_expand_details(all_summaries, budget_fn=budget_fn, prewarm_mode=False)

    for tolerance_config in [
        {"year_tolerance": 2, "mileage_tolerance": l1_tolerance,         "source": "layer_1_strict",          "adjust_mileage": True},
        {"year_tolerance": 2, "mileage_tolerance": l2_tolerance,         "source": "layer_2_relaxed_mileage", "adjust_mileage": True},
        {"year_tolerance": 3, "mileage_tolerance": l2_tolerance + 5000,  "source": "layer_3_relaxed_year",    "adjust_mileage": True},
        {"year_tolerance": 4, "mileage_tolerance": l2_tolerance + 15000, "source": "layer_4_wide",            "adjust_mileage": True},
        {"year_tolerance": 5, "mileage_tolerance": 999999,               "source": "layer_5_year_only",       "adjust_mileage": True},
    ]:
        # Try private-sold-only first — no dealer retail contamination
        result = run_filter_layer(
            enriched_summaries,
            target_year=year,
            target_mileage=mileage,
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
                year_tolerance=tolerance_config["year_tolerance"],
                mileage_tolerance=tolerance_config["mileage_tolerance"],
                adjust_mileage=tolerance_config["adjust_mileage"],
                layer_name=f"{tolerance_config['source']}+blended",
            )

        if result:
            # Apply direct extreme mileage penalty to final market price.
            # Corrects for when the target car itself has extreme mileage
            # and comparables couldn't fully account for it.
            if mileage > EXTREME_MILEAGE_THRESHOLD:
                excess = mileage - EXTREME_MILEAGE_THRESHOLD
                extra_blocks = min(excess / 10000, 15)
                extreme_penalty_pct = min(0.025 * extra_blocks, 0.50)
                original = result["market_price"]
                result["market_price"] = round(original * (1 - extreme_penalty_pct), 2)
                print(f"   🔻 Final extreme mileage penalty: {mileage}mi → −{round(extreme_penalty_pct*100,1)}% → £{result['market_price']} (was £{original})")

            result["source"] = tolerance_config["source"]
            redis_client.set(cache_key, json.dumps(result), ex=CACHE_TTL)
            return result

    return None


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