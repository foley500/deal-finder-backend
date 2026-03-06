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

    for summary in summaries:

        if expansions >= MAX_DETAIL_EXPANSIONS:
            break

        title = summary.get("title", "")
        item_id = summary.get("itemId")

        if not item_id:
            continue

        # Soft year screen (cheap filter)
        title_year = extract_year_from_title(title)
        if title_year is not None and abs(title_year - target_year) > 4:
            continue

        detail = get_item_detail(item_id)
        expansions += 1

        if not detail:
            continue

        listing_year = None
        listing_mileage = None

        # Structured aspects first
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

        # Fallbacks
        if listing_year is None:
            listing_year = extract_year_from_title(title)

        if listing_mileage is None:
            listing_mileage = extract_mileage_from_text(title)

        if listing_mileage is None:
            description = detail.get("description", "")
            listing_mileage = extract_mileage_from_text(description)

        # Strict validation
        if listing_year is None:
            continue

        if abs(listing_year - target_year) > year_tolerance:
            continue

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
        "expansions_used": expansions,
    }


# --------------------------------------------------
# PUBLIC ENTRY
# --------------------------------------------------

def get_market_price_from_sold(make, model, year, mileage, engine_size=None):

    if not make or not model or not year:
        return None

    if not mileage:
        mileage = 100000

    cache_key = f"sold_cache:{make}:{model}:{year}:{mileage}"
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    search_queries = []
    year_range = range(year - 2, year + 3)

    # Use FULL model string (fixes BMW 5 issue)
    for y in year_range:
        search_queries.append(f"{make} {model} {y}")

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

    # Layer 1
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

    # Layer 2
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

    # Layer 3
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