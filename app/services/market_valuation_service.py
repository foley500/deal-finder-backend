import statistics
import re
from app.services.ebay_browse_service import get_ebay_access_token
import requests

SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
MIN_SAMPLE_SIZE = 5


def build_search_query(make: str, model: str, year: int | None):
    parts = []
    if make:
        parts.append(make)
    if model:
        parts.append(model)
    if year:
        parts.append(str(year))
    return " ".join(parts)


def get_sold_listings(query: str, limit: int = 50):
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

    query = build_search_query(make, model, year)
    sold_listings = get_sold_listings(query)

    if not sold_listings:
        return None

    prices = [
        float(l["price"]["value"])
        for l in sold_listings
        if l.get("price")
    ]

    if len(prices) < MIN_SAMPLE_SIZE:
        return None

    prices = filter_reasonable_prices(prices)
    if not prices:
        return None

    median_price = statistics.median(prices)

    mileages = []
    for l in sold_listings:
        m = extract_mileage_from_title(l.get("title", ""))
        if m:
            mileages.append(m)

    sample_avg = int(statistics.mean(mileages)) if mileages else None

    adjusted = adjust_for_mileage(median_price, mileage, sample_avg)

    return {
        "market_price": round(adjusted, 2),
        "sample_size": len(prices),
        "source": "ebay_sold_market_model"
    }