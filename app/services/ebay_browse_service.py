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


# ==========================================
# OAUTH TOKEN (WITH CORRECT BUY SCOPE)
# ==========================================

def get_ebay_access_token():
    global _cached_token, _token_expiry

    # Use cached token if still valid
    if _cached_token and time.time() < _token_expiry:
        return _cached_token

    credentials = f"{EBAY_CLIENT_ID}:{EBAY_CLIENT_SECRET}"
    encoded = base64.b64encode(credentials.encode()).decode()

    headers = {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    # ✅ Proper Browse API scope
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope/buy.browse"
    }

    response = requests.post(TOKEN_URL, headers=headers, data=data)

    print("OAUTH STATUS:", response.status_code)

    if response.status_code != 200:
        print("❌ eBay OAuth error:", response.text)
        return None

    token_data = response.json()

    _cached_token = token_data.get("access_token")
    _token_expiry = time.time() + token_data.get("expires_in", 7200) - 60

    return _cached_token


# ==========================================
# SEARCH — SUMMARY ONLY (LIGHTWEIGHT)
# ==========================================

def search_ebay_browse(
    keywords="cars",
    limit=20,
    min_price=0,
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

    # 🔍 DEBUG THROTTLING INFO
    print("SEARCH STATUS:", response.status_code)
    print("SEARCH HEADERS:", dict(response.headers))

    if response.status_code != 200:
        print("❌ Browse API error:", response.text)
        return []

    summaries = response.json().get("itemSummaries", [])

    listings = []

    for summary in summaries:
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

    print(f"✅ eBay returned {len(listings)} summaries")

    return listings


# ==========================================
# DETAIL FETCH (EXPENSIVE)
# ==========================================

def get_item_detail(item_id):

    token = get_ebay_access_token()
    if not token:
        return None

    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_GB",
    }

    response = requests.get(f"{ITEM_URL}{item_id}", headers=headers)

    print("DETAIL STATUS:", response.status_code)
    print("DETAIL HEADERS:", dict(response.headers))

    if response.status_code != 200:
        print("❌ Item detail error:", response.text)
        return None

    return response.json()