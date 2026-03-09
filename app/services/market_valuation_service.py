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
MAX_DETAIL_EXPANSIONS = 20
MIN_SAMPLE_SIZE = 5
MAX_ENRICHED_TARGET = 30

ACTIVE_LISTING_DISCOUNT = 0.85

MILEAGE_BLOCK_SIZE = 5000
MILEAGE_BLOCK_RATE = 0.015
MAX_LINEAR_BLOCKS = 8
EXTREME_MILEAGE_THRESHOLD = 120000

MAX_ACCEPTABLE_IQR_RATIO = 0.40
WIDE_SPREAD_DISCOUNT = 0.95

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
    model_lower = base_model.lower()

    if make_lower == "bmw" and model_lower.isdigit():
        return f"{base_model} Series"

    if make_lower in ["mercedes", "mercedes-benz"]:
        if "class" in model_lower and "-" not in model_lower:
            return model_lower.replace("class", "-class").title()

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
# EBAY SOLD SEARCH — private-first, fallback to all
# ---------------------------------------------------

PRIVATE_FIRST_THRESHOLD = 20

def get_sold_listings(query: str, limit: int = 100):
    token = get_ebay_access_token()
    if not token:
        return []

    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_GB",
    }

    all_items = []
    combined_seen_ids = set()  # tracks IDs across both passes to avoid duplicates when combining

    def run_searches(seller_filter: str, label_suffix: str):
        searches = [
            {
                "filter": f"soldItems:true,conditions:{{USED}}{seller_filter}",
                "sort": "newlyListed",
                "label": f"sold{label_suffix}",
                "limit": limit,
                "source_type": "sold",
            },
            {
                "filter": f"buyingOptions:{{FIXED_PRICE}},conditions:{{USED}}{seller_filter}",
                "sort": "price",
                "label": f"active_cheap{label_suffix}",
                "limit": 50,
                "source_type": "active",
            },
            {
                "filter": f"buyingOptions:{{FIXED_PRICE}},conditions:{{USED}}{seller_filter}",
                "sort": "newlyListed",
                "label": f"active_new{label_suffix}",
                "limit": 50,
                "source_type": "active",
            },
        ]

        results = []
        local_seen = set()  # dedup within this pass only
        for search in searches:
            params = {
                "q": query,
                "limit": search["limit"],
                "category_ids": "9801",
                "filter": search["filter"],
            }
            if "sort" in search:
                params["sort"] = search["sort"]

            throttle_ebay()
            response = requests.get(SEARCH_URL, headers=headers, params=params)

            if response.status_code == 429:
                time.sleep(5)
                continue

            if response.status_code != 200:
                print(f"❌ [{search['label']}] search error: {response.status_code}")
                continue

            items = response.json().get("itemSummaries", [])
            print(f"✅ [{search['label']}] '{query[:35]}' → {len(items)} items")

            for item in items:
                item_id = item.get("itemId") or item.get("epid") or item.get("title", "")[:60]
                if item_id and item_id not in local_seen:
                    local_seen.add(item_id)
                    item["_source_type"] = search["source_type"]
                    item["_resolved_id"] = item_id
                    results.append(item)

        return results

    private_results = run_searches(",sellers:{PRIVATE}", "_private")
    for item in private_results:
        combined_seen_ids.add(item.get("_resolved_id", ""))
    all_items.extend(private_results)

    print(f"📦 Private-only results: {len(private_results)}")

    if len(private_results) < PRIVATE_FIRST_THRESHOLD:
        print(f"⚠️ Private results thin ({len(private_results)}) — falling back to all sellers")
        all_sellers_results = run_searches("", "_all")
        for item in all_sellers_results:
            item_id = item.get("_resolved_id", "")
            if item_id and item_id not in combined_seen_ids:
                combined_seen_ids.add(item_id)
                all_items.append(item)
        print(f"📦 After all-seller fallback: {len(all_items)} total")

    return all_items


# ---------------------------------------------------
# CORE FILTER ENGINE
# ---------------------------------------------------

def run_filter_layer(summaries, target_year, target_mileage, year_tolerance, mileage_tolerance, adjust_mileage=True, layer_name=""):
    sold_prices = []
    active_prices = []

    rejected_no_year = 0
    rejected_year = 0
    rejected_mileage = 0
    rejected_no_price = 0
    accepted_sold = 0
    accepted_active = 0
    mileage_diffs = []
    adjustments = []

    for summary in summaries:
        listing_year = summary.get("_year")
        listing_mileage = summary.get("_mileage")
        source_type = summary.get("_source_type", "active")

        total_accepted = accepted_sold + accepted_active

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

        if source_type == "active":
            base_price = base_price * ACTIVE_LISTING_DISCOUNT

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
            sold_prices.extend([adjusted_price] * weight)
            accepted_sold += 1
        else:
            active_prices.extend([adjusted_price] * weight)
            accepted_active += 1

    print(f"📊 FILTER DEBUG [{layer_name}]:")
    print(f"   Sold accepted: {accepted_sold} ({len(sold_prices)} weighted), Active accepted: {accepted_active} ({len(active_prices)} weighted)")
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
        print(f"   ✅ Using sold-only prices ({len(sold_prices)} weighted samples)")

    elif len(sold_prices) > 0 and (len(sold_prices) + len(active_prices)) >= MIN_SAMPLE_SIZE:
        final_prices = (sold_prices * 2) + active_prices
        source_label = "sold_weighted_blend"
        print(f"   ⚠️ Blending: {len(sold_prices)} sold (×2 weight) + {len(active_prices)} active")

    elif len(active_prices) >= MIN_SAMPLE_SIZE and len(sold_prices) == 0:
        final_prices = [p * 0.95 for p in active_prices]
        source_label = "active_only_cautious"
        print(f"   ⚠️ Active-only ({len(active_prices)} weighted samples) — extra 5% caution discount applied")

    else:
        print(f"❌ Failed — sold: {len(sold_prices)}, active: {len(active_prices)} (min required: {MIN_SAMPLE_SIZE})")
        return None

    final_prices = sorted(final_prices)

    # Trim outliers (10% each end)
    cut = int(len(final_prices) * 0.1)
    if cut > 0:
        final_prices = final_prices[cut:-cut]

    spread_discount = check_spread(final_prices, layer_name)

    sample_count = len(final_prices)

    if sample_count >= 10:
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
    cache_only=False,
):
    """
    cache_only=True: return cached result or None, never burn eBay API calls.
    Used during scan tasks. Only the prewarm job calls with cache_only=False.
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

    query = f"{make} {base_model}"

    # Dynamically scale mileage tolerance based on target mileage.
    # High mileage cars have thin comparable pools — widen tolerance to compensate.
    # Low mileage cars stay tight — they have plenty of comparables already.
    l1_tolerance, l2_tolerance = get_mileage_tolerances(mileage)

    print(f"🔎 Searching: make={make} base_model={base_model} engine={engine_litre} year={year} mileage={mileage}")
    print(f"   Query: '{query}' | Mileage tolerances: L1=±{l1_tolerance}, L2=±{l2_tolerance}")

    all_summaries = get_sold_listings(query)

    print(f"📦 Total unique summaries collected: {len(all_summaries)}")

    if not all_summaries:
        return None

    enriched_summaries = _pre_expand_details(all_summaries)

    for tolerance_config in [
        {"year_tolerance": 2, "mileage_tolerance": l1_tolerance, "source": "layer_1_strict",          "adjust_mileage": True},
        {"year_tolerance": 2, "mileage_tolerance": l2_tolerance, "source": "layer_2_relaxed_mileage", "adjust_mileage": True},
        {"year_tolerance": 3, "mileage_tolerance": l2_tolerance + 5000,  "source": "layer_3_relaxed_year",    "adjust_mileage": True},
        {"year_tolerance": 4, "mileage_tolerance": l2_tolerance + 15000, "source": "layer_4_wide",            "adjust_mileage": True},
    ]:
        result = run_filter_layer(
            enriched_summaries,
            target_year=year,
            target_mileage=mileage,
            year_tolerance=tolerance_config["year_tolerance"],
            mileage_tolerance=tolerance_config["mileage_tolerance"],
            adjust_mileage=tolerance_config["adjust_mileage"],
            layer_name=tolerance_config["source"],
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


def _pre_expand_details(summaries: list) -> list:
    expansions = 0
    enriched = []

    for summary in summaries:
        title = summary.get("title", "")
        item_id = summary.get("itemId")

        listing_year = extract_year_from_title(title)
        listing_mileage = extract_mileage_from_text(title)

        already_enriched = sum(1 for s in enriched if s.get("_year") is not None)

        if (listing_year is None or listing_mileage is None) and expansions < MAX_DETAIL_EXPANSIONS and already_enriched < MAX_ENRICHED_TARGET:
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

                    if listing_mileage is None and any(k in name for k in ["mileage", "miles", "odometer"]):
                        try:
                            listing_mileage = int(val.replace(",", "").replace(" ", "").split(".")[0])
                        except:
                            pass

        summary["_year"] = listing_year
        summary["_mileage"] = listing_mileage
        enriched.append(summary)

    print(f"🔍 Pre-expansion complete: {expansions} detail calls used")
    return enriched