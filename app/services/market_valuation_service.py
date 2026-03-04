import statistics
import re
import os
import redis
import requests
from app.services.ebay_browse_service import get_ebay_access_token

SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

REDIS_URL = os.getenv("CELERY_BROKER_URL")
redis_client = redis.from_url(REDIS_URL)
CACHE_TTL = 1800  # 30 minutes


# ---------------------------------------------------
# eBay Sold Search
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
# Extractors
# ---------------------------------------------------

def extract_year_from_title(title: str):
    match = re.search(r"\b(20\d{2}|19\d{2})\b", title)
    return int(match.group(1)) if match else None


def extract_mileage_from_title(title: str):
    match = re.search(r"(\d{2,3},?\d{3})\s?miles?", title.lower())
    return int(match.group(1).replace(",", "")) if match else None


# ---------------------------------------------------
# Mileage Adjustment
# ---------------------------------------------------

def adjust_for_mileage(base_price, target_mileage, sample_avg):
    if not target_mileage or not sample_avg:
        return base_price

    diff = target_mileage - sample_avg
    adjustment = diff * 0.04  # £40 per 1k miles approx
    return round(base_price - adjustment, 2)


# ---------------------------------------------------
# Progressive Filtering Engine
# ---------------------------------------------------

def progressive_filter(sold_listings, year, mileage):

    # Each stage:
    # (year_tolerance, mileage_tolerance, min_samples, require_year, require_mileage)
    tolerance_stages = [
        (2, 15000, 3, True, True),   # strict
        (3, 20000, 3, True, True),   # wider
        (None, 15000, 3, False, True),  # ignore year but require mileage
    ]

    for YEAR_TOLERANCE, MILEAGE_TOLERANCE, MIN_SAMPLES, REQUIRE_YEAR, REQUIRE_MILEAGE in tolerance_stages:

        filtered_prices = []
        mileage_samples = []

        for listing in sold_listings:

            price_obj = listing.get("price")
            if not price_obj:
                continue

            price = float(price_obj["value"])
            title = listing.get("title", "")

            listing_year = extract_year_from_title(title)
            listing_mileage = extract_mileage_from_title(title)

            # --------------------
            # REQUIREMENTS
            # --------------------
            if REQUIRE_YEAR and not listing_year:
                continue

            if REQUIRE_MILEAGE and not listing_mileage:
                continue

            # --------------------
            # YEAR FILTER
            # --------------------
            if YEAR_TOLERANCE is not None and year and listing_year:
                if abs(listing_year - year) > YEAR_TOLERANCE:
                    continue

            # --------------------
            # MILEAGE FILTER
            # --------------------
            if MILEAGE_TOLERANCE is not None and mileage and listing_mileage:
                if abs(listing_mileage - mileage) > MILEAGE_TOLERANCE:
                    continue

            filtered_prices.append(price)

            if listing_mileage:
                mileage_samples.append(listing_mileage)

        if len(filtered_prices) >= MIN_SAMPLES:

            median_price = statistics.median(filtered_prices)

            sample_avg_mileage = (
                int(statistics.mean(mileage_samples))
                if mileage_samples
                else None
            )

            adjusted_price = adjust_for_mileage(
                median_price,
                mileage,
                sample_avg_mileage
            )

            return {
                "market_price": round(adjusted_price, 2),
                "sample_size": len(filtered_prices),
                "source": "ebay_sold_progressive_model"
            }

    return None


# ---------------------------------------------------
# PUBLIC FUNCTION (REQUIRED BY DEAL ENGINE)
# ---------------------------------------------------

def get_market_price_from_sold(make, model, year, mileage):

    if not make or not model:
        return None

    # STRICTLY search only make + model
    query = f"{make} {model}"

    cache_key = f"sold_cache:{query}:{year}:{mileage}"

    cached = redis_client.get(cache_key)
    if cached:
        return eval(cached)

    sold_listings = get_sold_listings(query)

    if not sold_listings:
        return None

    result = progressive_filter(sold_listings, year, mileage)

    if result:
        redis_client.set(cache_key, str(result), ex=CACHE_TTL)

    return result