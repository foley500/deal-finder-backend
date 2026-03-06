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
MAX_DETAIL_EXPANSIONS = 25
MIN_SAMPLE_SIZE = 5


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
    """
    Expands common DVSA shorthand model names into what sellers
    typically write on eBay.
    """

    make_lower = make.lower()
    model_lower = base_model.lower()

    # -----------------------------
    # BMW (3 → 3 Series)
    # -----------------------------
    if make_lower == "bmw" and model_lower.isdigit():
        return f"{base_model} Series"

    # -----------------------------
    # Mercedes (C Class → C-Class)
    # -----------------------------
    if make_lower in ["mercedes", "mercedes-benz"]:
        if "class" in model_lower and "-" not in model_lower:
            return model_lower.replace("class", "-class").title()

    # -----------------------------
    # Generic cleanup
    # -----------------------------
    return base_model


# ---------------------------------------------------
# EBAY SOLD SEARCH
# ---------------------------------------------------

def get_sold_listings(query: str, limit: int = 100):

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
        "category_ids": "9801",
        "filter": "soldItems:true,conditions:{USED}"
    }

    throttle_ebay()
    response = requests.get(SEARCH_URL, headers=headers, params=params)

    if response.status_code == 429:
        time.sleep(5)
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
):

    prices = []
    expansions = 0

    rejected_no_year = 0
    rejected_year = 0
    rejected_mileage = 0
    rejected_no_price = 0
    accepted = 0
    mileage_diffs = []
    adjustments = []

    for summary in summaries:

        if expansions >= MAX_DETAIL_EXPANSIONS:
            print("⚠️ Hit MAX_DETAIL_EXPANSIONS limit")
            break

        title = summary.get("title", "")
        item_id = summary.get("itemId")

        if not item_id:
            continue

        detail = get_item_detail(item_id)
        expansions += 1

        if not detail:
            continue

        listing_year = None
        listing_mileage = None

        for aspect in detail.get("localizedAspects", []):
            name = aspect.get("name", "").lower()
            value = aspect.get("value", [])
            if not value:
                continue

            val = str(value[0]).strip()

            if name in ["year", "model_year", "registration_year"]:
                match = re.search(r"(19\d{2}|20\d{2})", val)
                if match:
                    listing_year = int(match.group(1))

            if name in ["mileage", "miles"]:
                try:
                    listing_mileage = int(val.replace(",", ""))
                except:
                    pass

        if listing_year is None:
            listing_year = extract_year_from_title(title)

        if listing_mileage is None:
            listing_mileage = extract_mileage_from_text(title)

        if listing_mileage is None:
            description = detail.get("description", "")
            listing_mileage = extract_mileage_from_text(description)

        if listing_year is None:
            rejected_no_year += 1
            continue

        if abs(listing_year - target_year) > year_tolerance:
            rejected_year += 1
            continue

        price_obj = summary.get("price")
        if not price_obj:
            rejected_no_price += 1
            continue

        base_price = float(price_obj["value"])
        adjusted_price = base_price

        if listing_mileage is not None:

            mileage_diff = listing_mileage - target_mileage
            mileage_diffs.append(mileage_diff)

            # ----------------------------------
            # Mileage depreciation adjustment
            # 1.5% per 5,000 miles
            # ----------------------------------

            blocks = abs(mileage_diff) / 5000
            depreciation_rate = 0.015  # 1.5% per 5k miles

            mileage_adjustment = base_price * depreciation_rate * blocks
            adjustments.append(mileage_adjustment)

            if mileage_diff > 0:
                # Listing has MORE miles → worth LESS
                adjusted_price = base_price - mileage_adjustment
            else:
                # Listing has FEWER miles → worth MORE
                adjusted_price = base_price + mileage_adjustment

        prices.append(adjusted_price)
        accepted += 1

    print("📊 FILTER DEBUG:")
    print("   Expansions used:", expansions)
    print("   Accepted:", accepted)
    print("   Rejected (no year):", rejected_no_year)
    print("   Rejected (year tolerance):", rejected_year)
    print("   Rejected (no price):", rejected_no_price)

    if mileage_diffs:
        print("   Avg mileage diff:", round(statistics.mean(mileage_diffs), 0))
        print("   Max mileage diff:", round(max(abs(x) for x in mileage_diffs), 0))
        print("   Avg price adjustment:", round(statistics.mean(adjustments), 2))
    else:
        print("   No mileage data available")

    if len(prices) < MIN_SAMPLE_SIZE:
        print("❌ Failed — only", len(prices), "samples (min required:", MIN_SAMPLE_SIZE, ")")
        return None

    prices = sorted(prices)

    cut = int(len(prices) * 0.1)
    if cut > 0:
        prices = prices[cut:-cut]

    return {
        "market_price": round(statistics.median(prices), 2),
        "sample_size": len(prices),
        "expansions_used": expansions,
    }


# --------------------------------------------------
# PUBLIC ENTRY
# --------------------------------------------------

def get_market_price_from_sold(
    make,
    model,
    year,
    mileage,
    engine_size=None,
    listing_title=None,
    listing_aspects=None,
):

    if not make or not model or not year:
        return None

    if not mileage:
        mileage = 100000

    # ---------------------------------
    # NORMALISE DVSA DATA
    # ---------------------------------

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

    cache_key = f"sold_cache:{make}:{base_model}:{year}:{mileage}"
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    search_queries = []
    year_range = range(year - 2, year + 3)

     # ---------------------------------
    # FALLBACK TRIM + ENGINE FROM LISTING
    # ---------------------------------

    title_lower = listing_title.lower() if listing_title else ""

    # If DVSA trim missing, attempt from listing title
    if not trim and listing_title:
        title_words = listing_title.title().split()
        if base_model in title_words:
            idx = title_words.index(base_model)
            possible_trim = title_words[idx+1:idx+3]
            if possible_trim:
                trim = " ".join(possible_trim)

    # Extract engine from title if DVSA missing
    if not engine_litre and listing_title:

        title_lower = listing_title.lower()

    # Pattern 1 — 2.0 / 1.6 / 3.0
        litre_match = re.search(r"\b(\d\.\d)\b", title_lower)
        if litre_match:
            try:
                engine_litre = float(litre_match.group(1))
            except:
                pass

    # Pattern 2 — 1998cc / 1998 cc
        if not engine_litre:
            cc_match = re.search(r"\b(\d{3,4})\s?cc\b", title_lower)
            if cc_match:
                try:
                    cc = int(cc_match.group(1))
                    engine_litre = round(cc / 1000, 1)
                except:
                    pass

    # Pattern 3 — 320d / 118i / 20d (BMW-style badges)
        if not engine_litre:
            badge_match = re.search(r"\b(\d{2,3})([di])\b", title_lower)
            if badge_match:
                try:
                    digits = badge_match.group(1)
                    if len(digits) == 3:
                        engine_litre = float(digits[0] + "." + digits[1])
                    elif len(digits) == 2:
                        engine_litre = float(digits[0] + "." + digits[1])
                except:
                    pass

    # Extract from structured aspects if still missing
    if listing_aspects:

        if not trim:
            trim = listing_aspects.get("Derivative") or listing_aspects.get("Model")

        if not engine_litre:
            aspect_engine = listing_aspects.get("Engine Size")
            if aspect_engine:
                try:
                    cleaned = re.sub(r"[^\d.]", "", str(aspect_engine))
                    size = float(cleaned)
                    engine_litre = round(size / 1000, 1) if size > 10 else round(size, 1)
                except:
                    pass

    # ---------------------------------
    # LAYER 1 — Base Model Spread
    # ---------------------------------
    for y in year_range:
        search_queries.append(f"{make} {base_model} {y}")

    # ---------------------------------
    # LAYER 2 — Trim Spread
    # ---------------------------------
    if trim:
        for y in year_range:
            search_queries.append(f"{make} {base_model} {trim} {y}")

    # ---------------------------------
    # LAYER 3 — Engine Spread
    # ---------------------------------
    if engine_litre:
        for y in year_range:
            search_queries.append(f"{make} {base_model} {engine_litre} {y}")

    all_summaries = []
    seen_ids = set()

    for query in search_queries:
        results = get_sold_listings(query)
        for item in results:
            item_id = item.get("itemId")
            if item_id and item_id not in seen_ids:
                seen_ids.add(item_id)
                all_summaries.append(item)

    if not all_summaries:
        return None


    # ---------------------------------
    # FILTER LAYERS
    # ---------------------------------

    # Strict: ±2 years / ±15k
    result = run_filter_layer(
        all_summaries,
        target_year=year,
        target_mileage=mileage,
        year_tolerance=2,
        mileage_tolerance=15000,
    )

    if result:
        result["source"] = "layer_1_strict"
        redis_client.set(cache_key, json.dumps(result), ex=CACHE_TTL)
        return result

    # Relax mileage
    result = run_filter_layer(
        all_summaries,
        target_year=year,
        target_mileage=mileage,
        year_tolerance=2,
        mileage_tolerance=25000,
    )

    if result:
        result["source"] = "layer_2_relaxed_mileage"
        redis_client.set(cache_key, json.dumps(result), ex=CACHE_TTL)
        return result

    # Relax year
    result = run_filter_layer(
        all_summaries,
        target_year=year,
        target_mileage=mileage,
        year_tolerance=3,
        mileage_tolerance=30000,
    )

    if result:
        result["source"] = "layer_3_relaxed_year"
        redis_client.set(cache_key, json.dumps(result), ex=CACHE_TTL)
        return result

    return None