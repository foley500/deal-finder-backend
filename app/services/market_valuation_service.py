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
MAX_DETAIL_EXPANSIONS = 50
MIN_SAMPLE_SIZE = 3


# ---------------------------------------------------
# HELPERS
# ---------------------------------------------------

def extract_year_from_title(title: str):
    match = re.search(r"\b(19\d{2}|20\d{2})\b", title)
    return int(match.group(1)) if match else None


def extract_mileage_from_title(title: str):
    match = re.search(r"(\d{2,3},?\d{3})\s?miles?", title.lower())
    return int(match.group(1).replace(",", "")) if match else None


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

        if expansions >= MAX_DETAIL_EXPANSIONS:
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

        # -----------------------------
        # 1️⃣ Try aspects
        # -----------------------------
        for aspect in detail.get("localizedAspects", []):
            name = aspect.get("name", "").lower()
            value = aspect.get("value", [])
            if not value:
                continue

            val = str(value[0]).lower()

            if "year" in name:
                try:
                    listing_year = int(val)
                except:
                    pass

            if "mileage" in name:
                try:
                    listing_mileage = int(val.replace(",", ""))
                except:
                    pass

        # -----------------------------
        # 2️⃣ Fallback: title
        # -----------------------------
        if listing_year is None:
            listing_year = extract_year_from_title(title)

        if listing_mileage is None:
            listing_mileage = extract_mileage_from_title(title)

        # -----------------------------
        # 3️⃣ Fallback: description
        # -----------------------------
        if listing_mileage is None:
            description = detail.get("description", "")
            listing_mileage = extract_mileage_from_title(description)

        # -----------------------------
        # HARD REQUIREMENTS
        # -----------------------------
        if listing_year is None:
            continue

        if listing_mileage is None:
            continue

        if abs(listing_year - target_year) > year_tolerance:
            continue

        if abs(listing_mileage - target_mileage) > mileage_tolerance:
            continue

        price_obj = summary.get("price")
        if not price_obj:
            continue

        prices.append(float(price_obj["value"]))

    if len(prices) < MIN_SAMPLE_SIZE:
        return None

    prices = sorted(prices)

    # Remove extreme 10% outliers
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

    if not make or not model or not year or not mileage:
        return None

    base_model, _ = split_model_components(model)

    cache_key = f"sold_cache:{make}:{model}:{year}:{mileage}"
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    # -----------------------------
    # SEARCH
    # -----------------------------
    query = f"{make} {base_model}"
    summaries = get_sold_listings(query)

    if not summaries:
        return None

    # -----------------------------
    # LAYER 1
    # ±2 years
    # ±15k miles
    # -----------------------------
    result = run_filter_layer(
        summaries,
        target_year=year,
        target_mileage=mileage,
        year_tolerance=2,
        mileage_tolerance=15000,
    )

    if result:
        result["source"] = "layer_1_strict"
        redis_client.set(cache_key, json.dumps(result), ex=CACHE_TTL)
        return result

    # -----------------------------
    # LAYER 2
    # ±2 years
    # ±25k miles
    # -----------------------------
    result = run_filter_layer(
        summaries,
        target_year=year,
        target_mileage=mileage,
        year_tolerance=2,
        mileage_tolerance=25000,
    )

    if result:
        result["source"] = "layer_2_relaxed_mileage"
        redis_client.set(cache_key, json.dumps(result), ex=CACHE_TTL)
        return result

    # -----------------------------
    # LAYER 3
    # ±3 years
    # ±25k miles
    # -----------------------------
    result = run_filter_layer(
        summaries,
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
