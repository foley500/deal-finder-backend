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
MAX_DETAIL_EXPANSIONS = 70
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

    response = requests.get(SEARCH_URL, headers=headers, params=params)

    if response.status_code != 200:
        return []

    return response.json().get("itemSummaries", [])


# ---------------------------------------------------
# CORE FILTER ENGINE
# ---------------------------------------------------

def strict_filter(
    summaries,
    target_year,
    target_mileage,
    target_engine_litre=None,
    target_trim=None,
):
    prices = []
    expansions = 0

    for summary in summaries:

        if expansions >= MAX_DETAIL_EXPANSIONS:
            break

        title = summary.get("title", "").lower()
        item_id = summary.get("itemId")

        if not item_id:
            continue

        detail = get_item_detail(item_id)
        expansions += 1

        if not detail:
            continue

        listing_year = None
        listing_mileage = None
        engine_match = True
        trim_match = True

        # Extract aspects
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

            if target_engine_litre and (
                "engine" in name or "cc" in name or "capacity" in name
            ):
                normalised = normalise_engine(val)
                if normalised != target_engine_litre:
                    engine_match = False

        # Trim match (title fallback)
        if target_trim:
            if target_trim.lower() not in title:
                trim_match = False

        # HARD RULES
        if listing_year is None:
            continue

        if abs(listing_year - target_year) > 2:
            continue

        if listing_mileage is None:
            continue

        if abs(listing_mileage - target_mileage) > 15000:
            continue

        if not engine_match:
            continue

        if not trim_match:
            continue

        price_obj = summary.get("price")
        if not price_obj:
            continue

        prices.append(float(price_obj["value"]))

    if len(prices) < 5:
        return None

    prices = sorted(prices)
    cut = int(len(prices) * 0.1)
    if cut > 0:
        prices = prices[cut:-cut]

    return {
        "market_price": round(statistics.median(prices), 2),
        "sample_size": len(prices),
        "source": "ebay_strict_spec"
    }

# --------------------------------------------------
# PUBLIC ENTRY
# --------------------------------------------------

def get_market_price_from_sold(make, model, year, mileage, engine_size=None):

    if not make or not model or not year or not mileage:
        return None

    base_model, trim = split_model_components(model)
    engine_litre = normalise_engine(engine_size) if engine_size else None

    search_layers = []

    # Broad engine market
    search_layers.append(f"{make} {base_model}")

    if trim:
        search_layers.append(f"{make} {base_model} {trim}")

    cache_key = f"sold_cache:{make}:{model}:{year}:{mileage}:{engine_litre}"
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

    layers = [
    {"engine": True, "trim": True},
    {"engine": True, "trim": False},
    {"engine": False, "trim": True},
    {"engine": False, "trim": False},
]

    for layer in layers:

        result = strict_filter(
            all_summaries,
            target_year=year,
            target_mileage=mileage,
            target_engine_litre=engine_litre if layer["engine"] else None,
            target_trim=trim if layer["trim"] else None,
        )

        if result:
            redis_client.set(cache_key, json.dumps(result), ex=CACHE_TTL)
            return result

    return None

    if result:
        redis_client.set(cache_key, json.dumps(result), ex=CACHE_TTL)

    return result