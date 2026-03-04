import statistics
import re, os, redis
from app.services.ebay_browse_service import get_ebay_access_token
import requests

SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

REDIS_URL = os.getenv("CELERY_BROKER_URL")
redis_client = redis.from_url(REDIS_URL)
CACHE_TTL = 1800  # 30 minutes


def build_search_query(make: str, model: str, year: int | None):
    parts = []
    if make:
        parts.append(make)
    if model:
        parts.append(model)
    if year:
        parts.append(str(year))
    return " ".join(parts)


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


def filter_reasonable_prices(prices: list[float]):
    if not prices:
        return []

    median = statistics.median(prices)
    lower = median * 0.7
    upper = median * 1.3

    return [p for p in prices if lower <= p <= upper]


def extract_mileage_from_title(title: str):
    match = re.search(r"(\d{2,3},?\d{3})\s?miles?", title.lower())
    if match:
        return int(match.group(1).replace(",", ""))
    return None


def adjust_for_mileage(base_price, target_mileage, sample_avg):
    if not target_mileage or not sample_avg:
        return base_price

    diff = target_mileage - sample_avg
    adjustment = diff * 0.04  # more conservative
    return round(base_price - adjustment, 2)


import statistics
import re, os, redis
from app.services.ebay_browse_service import get_ebay_access_token
import requests

SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

REDIS_URL = os.getenv("CELERY_BROKER_URL")
redis_client = redis.from_url(REDIS_URL)
CACHE_TTL = 1800  # 30 minutes


def build_search_query(make: str, model: str, year: int | None):
    parts = []
    if make:
        parts.append(make)
    if model:
        parts.append(model)
    if year:
        parts.append(str(year))
    return " ".join(parts)


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


def filter_reasonable_prices(prices: list[float]):
    if not prices:
        return []

    median = statistics.median(prices)
    lower = median * 0.7
    upper = median * 1.3

    return [p for p in prices if lower <= p <= upper]


def extract_mileage_from_title(title: str):
    match = re.search(r"(\d{2,3},?\d{3})\s?miles?", title.lower())
    if match:
        return int(match.group(1).replace(",", ""))
    return None


def adjust_for_mileage(base_price, target_mileage, sample_avg):
    if not target_mileage or not sample_avg:
        return base_price

    diff = target_mileage - sample_avg
    adjustment = diff * 0.04  # more conservative
    return round(base_price - adjustment, 2)


def get_market_price_from_sold(make, model, year, mileage):

    if not make or not model:
        return None

    # Tolerance settings
    YEAR_TOLERANCE = 2
    MILEAGE_TOLERANCE = 15000

    # Broader search query (no exact year restriction)
    query = f"{make} {model}"

    cache_key = f"sold_cache:{query}:{year}:{mileage}"
    cached = redis_client.get(cache_key)

    if cached:
        return eval(cached)

    sold_listings = get_sold_listings(query)

    if not sold_listings:
        return None

    filtered_prices = []
    mileage_samples = []

    for listing in sold_listings:

        price_obj = listing.get("price")
        if not price_obj:
            continue

        price = float(price_obj["value"])
        title = listing.get("title", "")

        # Extract year from title
        year_match = re.search(r"\b(20\d{2}|19\d{2})\b", title)
        listing_year = int(year_match.group(1)) if year_match else None

        # Extract mileage
        listing_mileage = extract_mileage_from_title(title)

        # Apply year tolerance
        if year and listing_year:
            if abs(listing_year - year) > YEAR_TOLERANCE:
                continue

        # Apply mileage tolerance
        if mileage and listing_mileage:
            if abs(listing_mileage - mileage) > MILEAGE_TOLERANCE:
                continue
            mileage_samples.append(listing_mileage)

        filtered_prices.append(price)

    if len(filtered_prices) < 3:
        return None

    median_price = statistics.median(filtered_prices)

    # Soft mileage adjustment
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

    result = {
        "market_price": round(adjusted_price, 2),
        "sample_size": len(filtered_prices),
        "source": "ebay_sold_cluster_model"
    }

    redis_client.set(cache_key, str(result), ex=CACHE_TTL)

    return result