import os
import requests
import base64
import time
from app.services.ebay_rate_limiter import throttle_ebay

EBAY_CLIENT_ID = os.getenv("EBAY_CLIENT_ID")
EBAY_CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")

TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
ITEM_URL = "https://api.ebay.com/buy/browse/v1/item/"

_cached_token = None
_token_expiry = 0


def get_ebay_access_token():
    global _cached_token, _token_expiry

    if _cached_token and time.time() < _token_expiry:
        return _cached_token

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

    _cached_token = token_data.get("access_token")
    _token_expiry = time.time() + token_data.get("expires_in", 7200) - 60

    return _cached_token


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

    for attempt in range(2):  # retry once if rate limited

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

    banned_words = [
        "breaking", "spares", "repair", "parts",
        "gearbox", "bumper", "door", "mirror",
        "alloy", "wheel", "tyre", "tire"
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
        return []

    if response.status_code != 200:
        print("❌ Item detail error:", response.text)
        time.sleep(1)
        return []


    return response.json()