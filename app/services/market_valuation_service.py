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

    # Match 37000 miles / 37000 mi
    match = re.search(r"(\d{4,6})\s*(miles|mile|mi)\b", text)
    if match:
        val = int(match.group(1))
        if 1000 < val < 300000:
            return val

    # Match 37k / 100k
    match = re.search(r"(\d{2,3})\s?k\b", text)
    if match:
        val = int(match.group(1)) * 1000
        if 1000 < val < 300000:
            return val

    return None


def normalise_engine(engine_size):
    if not engine_size:
        return None
    try:
        cleaned = re.sub(r"[^\d.]", "", str(engine_size))
        size = float(cleaned)

        litre = size / 1000 if size > 10 else size
        return f"{litre:.1f}"
    except:
        return None


def split_model_components(model_string: str):
    if not model_string:
        return None, None

    words = model_string.upper().split()
    base_model = words[0]
    trim = " ".join(words[1:]) if len(words) > 1 else None

    return base_model, trim


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
        print("Sold search rate limited - sleeping 5s")
        time.sleep(5)
        return []

    if response.status_code != 200:
        time.sleep(1)
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

    for summary in summaries:

        title = summary.get("title", "")
        item_id = summary.get("itemId")

        if not item_id:
            continue

        # -----------------------------
        # 1️⃣ FAST TITLE FILTER FIRST
        # -----------------------------
        listing_year = extract_year_from_title(title)
        listing_mileage = extract_mileage_from_text(title)

        if listing_year is None:
            continue

        if abs(listing_year - target_year) > year_tolerance:
            continue

        if listing_mileage is not None:
            if abs(listing_mileage - target_mileage) > mileage_tolerance:
                continue

        # -----------------------------
        # 2️⃣ ONLY NOW EXPAND DETAIL
        # -----------------------------
        if expansions >= MAX_DETAIL_EXPANSIONS:
            break

        detail = get_item_detail(item_id)
        expansions += 1

        if not detail:
            continue

        # If mileage missing from title, try description
        if listing_mileage is None:
            description = detail.get("description", "")
            listing_mileage = extract_mileage_from_text(description)

            if listing_mileage is not None:
                if abs(listing_mileage - target_mileage) > mileage_tolerance:
                    continue

        price_obj = summary.get("price")
        if not price_obj:
            continue

        prices.append(float(price_obj["value"]))

    if len(prices) < MIN_SAMPLE_SIZE:
        return None

    prices = sorted(prices)

    cut = int(len(prices) * 0.1)
    if cut > 0:
        prices = prices[cut:-cut]

    return {
        "market_price": round(statistics.median(prices), 2),
        "sample_size": len(prices),
    }

# --------------------------------------------------
# PUBLIC ENTRY
# --------------------------------------------------

def get_market_price_from_sold(make, model, year, mileage, engine_size=None):

    if not make or not model:
        return None

    if not year:
        return None

# If mileage missing, assume neutral midpoint
    if not mileage:
        mileage = 100000

    base_model, trim = split_model_components(model)
    engine_litre = normalise_engine(engine_size) if engine_size else None

    cache_key = f"sold_cache:{make}:{model}:{year}:{mileage}"
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    # ==========================================
    # STAGED + YEAR-SPREAD SEARCH
    # ==========================================

    search_queries = []
    year_range = range(year - 2, year + 3)  # 2017–2021

    # Stage 1 — Make Model Year Spread
    for y in year_range:
        search_queries.append(f"{make} {base_model} {y}")

    # Stage 2 — Add Trim
    if trim:
        search_queries.append(f"{make} {base_model} {year} {trim}")

    # Stage 3 — Add Engine
    if trim and engine_litre:
        search_queries.append(f"{make} {base_model} {year} {trim} {engine_litre}")

    all_summaries = []
    seen_ids = set()

    for query in search_queries:

        print("SEARCHING:", query)

        results = get_sold_listings(query)

        for item in results:
            item_id = item.get("itemId")
            if item_id and item_id not in seen_ids:
                seen_ids.add(item_id)
                all_summaries.append(item)

    if not all_summaries:
        return None

    # ==========================================
    # LAYER 1 — STRICT
    # ±2 years / ±15k miles
    # ==========================================

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

    # ==========================================
    # LAYER 2 — Relax Mileage
    # ±2 years / ±25k miles
    # ==========================================

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

    # ==========================================
    # LAYER 3 — Relax Year
    # ±3 years / ±25k miles
    # ==========================================

    result = run_filter_layer(
        all_summaries,
        target_year=year,
        target_mileage=mileage,
        year_tolerance=3,
        mileage_tolerance=25000,
    )

    if result:
        result["source"] = "layer_3_relaxed_year"
        redis_client.set(cache_key, json.dumps(result), ex=CACHE_TTL)
        return result

    return None

