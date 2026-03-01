import os
import requests

AUTOTRADER_API_KEY = os.getenv("AUTOTRADER_API_KEY")
AUTOTRADER_URL = os.getenv("AUTOTRADER_URL")


def search_autotrader(
    keywords,
    min_price=None,
    max_price=None,
    min_year=None,
    max_year=None,
    max_mileage=None,
):

    if not AUTOTRADER_API_KEY:
        print("⚠ AutoTrader API key not configured")
        return []

    headers = {
        "Authorization": f"Bearer {AUTOTRADER_API_KEY}",
        "Content-Type": "application/json"
    }

    params = {
        "searchTerm": keywords,
        "minPrice": min_price,
        "maxPrice": max_price,
        "minYear": min_year,
        "maxYear": max_year,
        "maxMileage": max_mileage,
    }

    response = requests.get(AUTOTRADER_URL, headers=headers, params=params)

    if response.status_code != 200:
        print("AutoTrader error:", response.text)
        return []

    data = response.json()

    listings = []

    for item in data.get("results", []):
        listings.append({
            "id": item.get("id"),
            "title": item.get("title"),
            "price": item.get("price"),
            "description": item.get("description"),
            "view_url": item.get("url"),
            "image_url": item.get("image"),
            "mileage": item.get("mileage"),
            "year": item.get("year"),
            "body_type": item.get("bodyType"),
            "source": "autotrader"
        })

    return listings