import os
import requests
import base64
import time

EBAY_CLIENT_ID = os.getenv("EBAY_CLIENT_ID")
EBAY_CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")

TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
ITEM_URL = "https://api.ebay.com/buy/browse/v1/item/"

_cached_token = None
_token_expiry = 0


# ==============================
# TOKEN
# ==============================

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


# ==============================
# SEARCH (LIGHTWEIGHT)
# ==============================

def search_ebay_browse(
    keywords="used car",
    limit=50,
    min_price=1000,
    max_price=50000,
    sort="newlyListed"
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
        "category_ids": "9801",
        "sort": sort,
        "filter": f"price:[{min_price}..{max_price}],buyingOptions:{{FIXED_PRICE}}"
    }

    response = requests.get(SEARCH_URL, headers=headers, params=params)

    if response.status_code != 200:
        print("❌ Browse API error:", response.text)
        return []

    summaries = response.json().get("itemSummaries", [])

    listings = []

    for item in summaries:
        listings.append({
            "id": item.get("itemId"),
            "title": item.get("title"),
            "price": float(item.get("price", {}).get("value", 0)),
            "view_url": item.get("itemWebUrl"),
            "image_url": item.get("image", {}).get("imageUrl"),
            "location": item.get("itemLocation", {}).get("postalCode"),
            "listing_date": item.get("itemCreationDate"),
            "source": "ebay",
            "summary_only": True  # 🔥 important
        })

    print(f"✅ eBay summary returned {len(listings)} items")
    return listings


# ==============================
# FETCH FULL DETAILS (ON DEMAND)
# ==============================

def fetch_ebay_item_details(item_id):

    token = get_ebay_access_token()
    if not token:
        return None

    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_GB",
    }

    response = requests.get(f"{ITEM_URL}{item_id}", headers=headers)

    if response.status_code != 200:
        print("❌ Item detail error:", response.text)
        return None

    item = response.json()

    aspect_dict = {}
    for aspect in item.get("localizedAspects", []):
        name = aspect.get("name")
        values = aspect.get("value")
        if name and values:
            aspect_dict[name] = values[0] if isinstance(values, list) else values

    return {
        "description": item.get("description"),
        "aspects": aspect_dict,
        "all_images": [
            img.get("imageUrl")
            for img in item.get("additionalImages", [])
        ]
    }