import statistics
import os
import redis
import json
import requests
import re

from app.services.ebay_browse_service import (
    get_ebay_access_token,
    get_item_detail
)

SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

REDIS_URL = os.getenv("CELERY_BROKER_URL")
redis_client = redis.from_url(REDIS_URL)

CACHE_TTL = 1800
MAX_DETAIL_EXPANSIONS = 40
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
        size = float(engine_size)
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

    response = requests.get(SEARCH_URL, headers=headers, params=params)

    if response.status_code != 200:
        return []

    return response.json().get("itemSummaries", [])


# ---------------------------------------------------
# CORE FILTER ENGINE
# ---------------------------------------------------

def filter_sold_data(summaries, target_year, target_mileage):

    tolerance_stages = [
        (2, 15000),
        (3, 20000),
        (None, None)
    ]

    for YEAR_TOL, MILE_TOL in tolerance_stages:

        prices = []
        mileage_samples = []
        expansions = 0

        for summary in summaries:

            if expansions >= MAX_DETAIL_EXPANSIONS:
                break

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

                val = value[0]

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

            if not listing_year or not listing_mileage:
                title = summary.get("title", "")
                listing_year = listing_year or extract_year_from_title(title)
                listing_mileage = listing_mileage or extract_mileage_from_title(title)

            if not listing_year or not listing_mileage:
                continue

            if YEAR_TOL is not None and target_year:
                if abs(listing_year - target_year) > YEAR_TOL:
                    continue

            if MILE_TOL is not None and target_mileage:
                if abs(listing_mileage - target_mileage) > MILE_TOL:
                    continue

            price_obj = summary.get("price")
            if not price_obj:
                continue

            price = float(price_obj["value"])

            prices.append(price)
            mileage_samples.append(listing_mileage)

        if len(prices) >= MIN_SAMPLE_SIZE:

            median_price = statistics.median(prices)
            sample_avg_mileage = int(statistics.mean(mileage_samples))
            mileage_diff = target_mileage - sample_avg_mileage
            adjustment = mileage_diff * 0.04
            adjusted_price = round(median_price - adjustment, 2)

            return {
                "market_price": adjusted_price,
                "sample_size": len(prices),
                "source": "ebay_progressive_combined_model"
            }

    return None

# ---------------------------------------------------
# PUBLIC ENTRY
# ---------------------------------------------------

def get_market_price_from_sold(make, model, year, mileage, engine_size=None):

    if not make or not model or not year or not mileage:
        return None

    base_model, trim = split_model_components(model)
    engine_litre = normalise_engine(engine_size)

    search_layers = []

    search_layers.append(f"{year} {make} {base_model}")

    if trim:
        search_layers.append(f"{year} {make} {base_model} {trim}")

    if engine_litre:
        search_layers.append(f"{year} {make} {base_model} {engine_litre}")

    if trim and engine_litre:
        search_layers.append(f"{year} {make} {base_model} {trim} {engine_litre}")

    cache_key = f"sold_cache:{make}:{model}:{year}:{mileage}"
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    all_summaries = []
    seen_ids = set()

    for query in search_layers:
        results = get_sold_listings(query)
        for item in results:
            item_id = item.get("itemId")
            if item_id and item_id not in seen_ids:
                seen_ids.add(item_id)
                all_summaries.append(item)

    if not all_summaries:
        return None

    result = filter_sold_data(
        all_summaries,
        target_year=year,
        target_mileage=mileage
    )

    if result:
        redis_client.set(cache_key, json.dumps(result), ex=CACHE_TTL)

    return result