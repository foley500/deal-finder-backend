import statistics
import os
import redis
import json
import requests
from app.services.ebay_browse_service import (
    get_ebay_access_token,
    get_item_detail
)

SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

REDIS_URL = os.getenv("CELERY_BROKER_URL")
redis_client = redis.from_url(REDIS_URL)
CACHE_TTL = 1800  # 30 minutes

MAX_DETAIL_EXPANSIONS = 15
MIN_SAMPLE_SIZE = 3


# ---------------------------------------------------
# eBay Sold Search (Summary)
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

    response = requests.get(SEARCH_URL, headers=headers, params=params)

    if response.status_code != 200:
        print("❌ SOLD search error:", response.text)
        return []

    return response.json().get("itemSummaries", [])


# ---------------------------------------------------
# Extract Structured Year + Mileage
# ---------------------------------------------------

def extract_structured_data(item_detail):
    aspects = item_detail.get("localizedAspects", [])

    year = None
    mileage = None

    for aspect in aspects:
        name = aspect.get("name", "").lower()
        values = aspect.get("value", [])

        if not values:
            continue

        value = values[0]

        if "year" in name:
            try:
                year = int(value)
            except:
                pass

        if "mileage" in name:
            try:
                mileage = int(value.replace(",", ""))
            except:
                pass

    return year, mileage


# ---------------------------------------------------
# Core Filtering Logic
# ---------------------------------------------------

def filter_sold_data(sold_summaries, target_year, target_mileage):

    prices = []
    mileage_samples = []

    expansions = 0

    for summary in sold_summaries:

        if expansions >= MAX_DETAIL_EXPANSIONS:
            break

        item_id = summary.get("itemId")
        if not item_id:
            continue

        detail = get_item_detail(item_id)
        expansions += 1

        if not detail:
            continue

        listing_year, listing_mileage = extract_structured_data(detail)

        if not listing_year or not listing_mileage:
            continue

        # YEAR FILTER ±2
        if target_year and abs(listing_year - target_year) > 2:
            continue

        # MILEAGE FILTER ±15k
        if target_mileage and abs(listing_mileage - target_mileage) > 15000:
            continue

        price_obj = summary.get("price")
        if not price_obj:
            continue

        price = float(price_obj["value"])

        prices.append(price)
        mileage_samples.append(listing_mileage)

    if len(prices) < MIN_SAMPLE_SIZE:
        return None

    median_price = statistics.median(prices)

    sample_avg_mileage = int(statistics.mean(mileage_samples))

    # Light mileage normalisation
    mileage_diff = target_mileage - sample_avg_mileage
    adjustment = mileage_diff * 0.04  # ~£40 per 1k miles
    adjusted_price = round(median_price - adjustment, 2)

    return {
        "market_price": adjusted_price,
        "sample_size": len(prices),
        "source": "ebay_structured_sold_model"
    }


# ---------------------------------------------------
# PUBLIC FUNCTION
# ---------------------------------------------------

def get_market_price_from_sold(make, model, year, mileage):

    if not make or not model:
        return None

    query = f"{make} {model}"

    cache_key = f"sold_cache:{query}:{year}:{mileage}"

    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    sold_summaries = get_sold_listings(query)

    if not sold_summaries:
        return None

    result = filter_sold_data(
        sold_summaries,
        target_year=year,
        target_mileage=mileage
    )

    if result:
        redis_client.set(cache_key, json.dumps(result), ex=CACHE_TTL)

    return result