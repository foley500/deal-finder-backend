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

CACHE_TTL = 1800
MAX_DETAIL_EXPANSIONS = 10
MIN_SAMPLE_SIZE = 5
MAX_ENRICHED_TARGET = 15

# Active listings are asking prices, not sold prices.
# Dealers typically achieve 82-88% of asking on eBay private sales.
# We discount active listings to estimate what they'll actually sell for.
ACTIVE_LISTING_DISCOUNT = 0.85  # assume 15% haircut on active BIN prices

# Mileage adjustment: 1.5% per 5k miles, capped at 8 blocks (40k miles = 12%)
# Beyond that we apply an exponential penalty for extreme mileage
MILEAGE_BLOCK_SIZE = 5000
MILEAGE_BLOCK_RATE = 0.015
MAX_LINEAR_BLOCKS = 8
EXTREME_MILEAGE_THRESHOLD = 120000  # above this, apply extra penalty per 10k miles


# ---------------------------------------------------
# HELPERS
# ---------------------------------------------------

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
    """
    Returns an adjusted price accounting for mileage difference.

    - Linear adjustment up to 40k miles difference (1.5% per 5k block, max 8 blocks = 12%)
    - Extra penalty if the listing itself has extreme mileage (>120k)
    - Both directions: high mileage listing = lower adjusted price, low mileage = higher
    """
    mileage_diff = listing_mileage - target_mileage
    abs_diff = abs(mileage_diff)

    # Linear blocks capped at MAX_LINEAR_BLOCKS
    blocks = min(abs_diff / MILEAGE_BLOCK_SIZE, MAX_LINEAR_BLOCKS)
    linear_adjustment = base_price * MILEAGE_BLOCK_RATE * blocks

    # Extra penalty if the listing itself has extreme mileage
    extreme_penalty = 0.0
    if listing_mileage > EXTREME_MILEAGE_THRESHOLD:
        excess = listing_mileage - EXTREME_MILEAGE_THRESHOLD
        # 2% extra per 10k miles above threshold, capped at 30%
        extra_blocks = min(excess / 10000, 15)
        extreme_penalty = base_price * 0.02 * extra_blocks
        print(f"   ⚠️ Extreme mileage penalty: {listing_mileage}mi → −£{round(extreme_penalty, 2)}")

    total_adjustment = linear_adjustment + extreme_penalty

    if mileage_diff > 0:
        # listing has MORE miles than target → price down
        return base_price - total_adjustment
    else:
        # listing has FEWER miles than target → price up (linear only, no extreme bonus)
        return base_price + linear_adjustment


# ---------------------------------------------------
# EBAY SOLD SEARCH — single broad query, 3 API calls
# ---------------------------------------------------

PRIVATE_FIRST_THRESHOLD = 20  # if private search returns fewer than this, fall back to all sellers

def get_sold_listings(query: str, limit: int = 100):
    token = get_ebay_access_token()
    if not token:
        return []

    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_GB",
    }

    all_items = []
    seen_ids = set()

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
                item_id = item.get("itemId")
                if item_id and item_id not in seen_ids:
                    seen_ids.add(item_id)
                    item["_source_type"] = search["source_type"]
                    results.append(item)

        return results

    # Pass 1 — private sellers only
    private_results = run_searches(",sellers:{PRIVATE}", "_private")
    all_items.extend(private_results)

    print(f"📦 Private-only results: {len(private_results)}")

    # Pass 2 — fall back to all sellers if private is thin
    if len(private_results) < PRIVATE_FIRST_THRESHOLD:
        print(f"⚠️ Private results thin ({len(private_results)}) — falling back to all sellers")
        all_sellers_results = run_searches("", "_all")
        for item in all_sellers_results:
            item_id = item.get("itemId")
            if item_id and item_id not in seen_ids:
                seen_ids.add(item_id)
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
        abs_mileage_diff = None
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

        # Active listings are asking prices — discount to estimate actual sale price
        if source_type == "active":
            base_price = base_price * ACTIVE_LISTING_DISCOUNT

        adjusted_price = base_price

        if mileage_diff is not None and adjust_mileage and listing_mileage is not None:
            mileage_diffs.append(mileage_diff)
            adjusted_price = calculate_mileage_adjustment(base_price, listing_mileage, target_mileage)
            adjustments.append(abs(adjusted_price - base_price))
        elif mileage_diff is not None:
            mileage_diffs.append(mileage_diff)

        if source_type == "sold":
            sold_prices.append(adjusted_price)
            accepted_sold += 1
        else:
            active_prices.append(adjusted_price)
            accepted_active += 1

    print(f"📊 FILTER DEBUG [{layer_name}]:")
    print(f"   Sold accepted: {accepted_sold}, Active accepted: {accepted_active}")
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

    # Build final price pool:
    # Priority 1: sold listings alone — ground truth
    # Priority 2: sold (weighted ×2) + discounted active blend if sold is thin
    # Priority 3: active-only with extra caution discount (last resort)

    if len(sold_prices) >= MIN_SAMPLE_SIZE:
        final_prices = sold_prices
        source_label = "sold_only"
        print(f"   ✅ Using sold-only prices ({len(sold_prices)} samples)")

    elif len(sold_prices) > 0 and (len(sold_prices) + len(active_prices)) >= MIN_SAMPLE_SIZE:
        # Weight sold 2x by repeating them
        final_prices = (sold_prices * 2) + active_prices
        source_label = "sold_weighted_blend"
        print(f"   ⚠️ Blending: {len(sold_prices)} sold (×2 weight) + {len(active_prices)} active")

    elif len(active_prices) >= MIN_SAMPLE_SIZE and len(sold_prices) == 0:
        # No sold data — use active with extra 5% caution discount on top of the 15%
        final_prices = [p * 0.95 for p in active_prices]
        source_label = "active_only_cautious"
        print(f"   ⚠️ Active-only ({len(active_prices)} samples) — extra 5% caution discount applied")

    else:
        print(f"❌ Failed — sold: {len(sold_prices)}, active: {len(active_prices)} (min required: {MIN_SAMPLE_SIZE})")
        return None

    final_prices = sorted(final_prices)

    # Trim outliers (10% each end)
    cut = int(len(final_prices) * 0.1)
    if cut > 0:
        final_prices = final_prices[cut:-cut]

    sample_count = len(final_prices)

    if sample_count >= 10:
        confidence = "high"
    elif sample_count >= 5:
        confidence = "medium"
    else:
        confidence = "low"

    market_price = round(statistics.median(final_prices), 2)
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
):

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

    mileage_bucket = round(mileage / 10000) * 10000
    cache_key = f"sold_cache:{make}:{base_model}:{year}:{mileage_bucket}"
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

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

    # Single broad query — 3 API calls max per unique make/model
    # Cache TTL 30min means repeat listings of same model cost zero calls
    query = f"{make} {base_model}"

    print(f"🔎 Searching: make={make} base_model={base_model} engine={engine_litre} year={year} mileage={mileage}")
    print(f"   Query: '{query}'")

    all_summaries = get_sold_listings(query)

    print(f"📦 Total unique summaries collected: {len(all_summaries)}")

    if not all_summaries:
        return None

    enriched_summaries = _pre_expand_details(all_summaries)

    for tolerance_config in [
        {"year_tolerance": 2, "mileage_tolerance": 15000, "source": "layer_1_strict",          "adjust_mileage": False},
        {"year_tolerance": 2, "mileage_tolerance": 25000, "source": "layer_2_relaxed_mileage", "adjust_mileage": True},
        {"year_tolerance": 3, "mileage_tolerance": 30000, "source": "layer_3_relaxed_year",    "adjust_mileage": True},
        {"year_tolerance": 4, "mileage_tolerance": 40000, "source": "layer_4_wide",            "adjust_mileage": True},
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