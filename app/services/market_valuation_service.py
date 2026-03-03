import statistics
from app.services.ebay_browse_service import search_ebay_browse

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


def filter_reasonable_prices(prices: list[float]):
    if not prices:
        return []

    median = statistics.median(prices)

    # Remove extreme outliers (+/- 40%)
    lower = median * 0.6
    upper = median * 1.4

    return [p for p in prices if lower <= p <= upper]


def adjust_for_mileage(base_price: float, target_mileage: int | None, sample_mileage_avg: int | None):
    if not target_mileage or not sample_mileage_avg:
        return base_price

    mileage_diff = target_mileage - sample_mileage_avg

    # £0.05 per mile adjustment (conservative)
    adjustment = mileage_diff * 0.05

    return round(base_price - adjustment, 2)


def get_market_price_from_sold(make: str, model: str, year: int | None, mileage: int | None):
    """
    Pull SOLD eBay listings and calculate a market price.
    """

    query = build_search_query(make, model, year)

    sold_listings = search_ebay_browse(
        keywords=query,
        limit=50,
        min_price=500,
        max_price=100000,
        sort="newlyListed"
    )

    if not sold_listings:
        return None

    prices = [l["price"] for l in sold_listings if l.get("price")]

    if len(prices) < MIN_SAMPLE_SIZE:
        return None

    prices = filter_reasonable_prices(prices)

    if not prices:
        return None

    median_price = statistics.median(prices)

    sample_mileage_values = []
    for listing in sold_listings:
        title = listing.get("title", "").lower()
        import re
        match = re.search(r"(\d{2,3},?\d{3})\s?miles?", title)
        if match:
            sample_mileage_values.append(int(match.group(1).replace(",", "")))

    sample_mileage_avg = int(statistics.mean(sample_mileage_values)) if sample_mileage_values else None

    adjusted_price = adjust_for_mileage(
        median_price,
        mileage,
        sample_mileage_avg
    )

    return {
        "market_price": round(adjusted_price, 2),
        "sample_size": len(prices),
        "source": "ebay_sold_market_model"
    }