import os
import requests
import base64
import time
import redis
from app.services.ebay_rate_limiter import throttle_ebay

EBAY_CLIENT_ID = os.getenv("EBAY_CLIENT_ID")
EBAY_CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")

TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
ITEM_URL = "https://api.ebay.com/buy/browse/v1/item/"

REDIS_URL = os.getenv("CELERY_BROKER_URL")
redis_client = redis.from_url(REDIS_URL)

EBAY_TOKEN_KEY = "ebay:access_token"


def get_ebay_access_token():
    cached = redis_client.get(EBAY_TOKEN_KEY)
    if cached:
        return cached.decode()

    credentials = f"{EBAY_CLIENT_ID}:{EBAY_CLIENT_SECRET}"
    encoded = base64.b64encode(credentials.encode()).decode()

    headers = {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope"
    }

    response = requests.post(TOKEN_URL, headers=headers, data=data)
    if response.status_code != 200:
        print("❌ eBay OAuth error:", response.text)
        return None

    token_data = response.json()
    token = token_data.get("access_token")
    expires_in = int(token_data.get("expires_in", 7200)) - 60

    redis_client.set(EBAY_TOKEN_KEY, token, ex=expires_in)
    return token


def search_ebay_browse(
    keywords="cars",
    limit=20,
    min_price=500,
    max_price=50000,
    sort="newlyListed",
    offset=0
):
    token = get_ebay_access_token()
    if not token:
        return []

    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_GB",
    }

    params = {
        "q": keywords,
        "limit": limit,
        "offset": offset,
        "category_ids": "9801",
        "sort": sort,
        "filter": f"price:[{min_price}..{max_price}],buyingOptions:{{FIXED_PRICE}},conditions:{{USED}}"
    }

    for attempt in range(2):
        throttle_ebay()
        response = requests.get(SEARCH_URL, headers=headers, params=params)

        if response.status_code == 429:
            print("⚠️ Browse rate limited — sleeping 5s")
            time.sleep(5)
            continue

        if response.status_code != 200:
            print("❌ Browse API error:", response.text)
            time.sleep(1)
            return []

        break
    else:
        return []

    summaries = response.json().get("itemSummaries", [])

    listings = []

    # Only filter listings that are clearly parts/spares, not whole cars.
    # IMPORTANT: do NOT include "door", "mirror", "wheel", "parts", "repair" here —
    # these appear in legitimate full-car listings ("5 door hatchback", "alloy wheels
    # included", etc.) and would silently discard a large portion of real inventory.
    banned_words = [
        "breaking",
        "spares only",
        "parts only",
        "gearbox only",
        "bumper only",
        "for parts",
        "for spares",
        "not running",
    ]

    for summary in summaries:
        title = summary.get("title", "").lower()

        if any(word in title for word in banned_words):
            continue

        listings.append({
            "id": summary.get("itemId"),
            "title": summary.get("title"),
            "price": float(summary.get("price", {}).get("value", 0)),
            "view_url": summary.get("itemWebUrl"),
            "image_url": summary.get("image", {}).get("imageUrl"),
            "location": summary.get("itemLocation", {}).get("postalCode"),
            "listing_date": summary.get("itemCreationDate"),
            "source": "ebay_browse",
            "summary_only": True
        })

    print(f"✅ eBay returned {len(listings)} vehicle summaries")
    return listings

def search_sniper_windows(make, model):
    """
    Runs multiple price-window searches to catch mispriced listings.
    Dramatically increases deal detection.
    """

    windows = [
        (500, 3000),
        (3000, 7000),
        (7000, 15000),
        (15000, 40000),
    ]

    all_results = []
    seen_ids = set()

    for min_price, max_price in windows:

        listings = search_ebay_browse(
            keywords=f"{make} {model}",
            limit=50,
            min_price=min_price,
            max_price=max_price,
            sort="newlyListed"
        )

        for listing in listings:

            item_id = listing["id"]

            if item_id in seen_ids:
                continue

            seen_ids.add(item_id)
            all_results.append(listing)

    print(f"🎯 Sniper windows returned {len(all_results)} unique listings")

    return all_results

def get_model_variants(make, model):
    """
    Generates common typo / shorthand search variants.
    """

    variants = [(make, model)]

    make_lower = make.lower()

    replacements = {
        "mercedes": ["merc"],
        "volkswagen": ["vw", "volkswagon"],
        "bmw": ["bm"],
        "land rover": ["landrover"],
    }

    if make_lower in replacements:

        for variant in replacements[make_lower]:
            variants.append((variant, model))

    return variants

def sniper_search(make, model):
    """
    Runs full sniper search strategy:
    - price windows
    - typo variants
    """

    all_results = []
    seen_ids = set()

    variants = [(make, model)] if not model else get_model_variants(make, model)

    for variant_make, variant_model in variants:

        listings = search_sniper_windows(variant_make, variant_model)

        for listing in listings:

            item_id = listing["id"]

            if item_id in seen_ids:
                continue

            seen_ids.add(item_id)
            all_results.append(listing)

    print(f"🚀 Sniper collected {len(all_results)} listings")

    return all_results


def get_item_detail(item_id):
    token = get_ebay_access_token()
    if not token:
        return None

    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_GB",
    }

    throttle_ebay()
    response = requests.get(
        f"{ITEM_URL}{item_id}?fieldgroups=PRODUCT",
        headers=headers
    )

    if response.status_code == 429:
        print("Rate limited - sleeping 5s")
        time.sleep(5)
        return None

    if response.status_code != 200:
        print("❌ Item detail error:", response.text)
        time.sleep(1)
        return None

    return response.json()