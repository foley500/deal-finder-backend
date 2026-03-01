import os
import requests
import base64
import time

# ==========================================
# 🔐 Environment Credentials
# ==========================================
EBAY_CLIENT_ID = os.getenv("EBAY_CLIENT_ID")
EBAY_CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")

TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

_cached_token = None
_token_expiry = 0


# ==========================================
# 🔐 OAuth Token Handling
# ==========================================
def get_ebay_access_token():
    global _cached_token, _token_expiry

    if _cached_token and time.time() < _token_expiry:
        return _cached_token

    if not EBAY_CLIENT_ID or not EBAY_CLIENT_SECRET:
        print("❌ eBay credentials missing")
        return None

    credentials = f"{EBAY_CLIENT_ID}:{EBAY_CLIENT_SECRET}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()

    headers = {
        "Authorization": f"Basic {encoded_credentials}",
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


# ==========================================
# 🖼 Force Full Resolution Image
# ==========================================
def upgrade_image_resolution(url):
    if not url:
        return None

    # eBay image URLs follow predictable pattern
    # Replace size (s-l225, s-l500 etc) with s-l1600
    if "s-l" in url:
        base = url.split("s-l")[0]
        return base + "s-l1600.jpg"

    return url


# ==========================================
# 🚗 Browse Search (Production)
# ==========================================
def search_ebay_browse(
    keywords="used car",
    limit=50,
    min_price=1000,
    max_price=50000,
):

    token = get_ebay_access_token()
    if not token:
        return []

    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_GB",
        "Content-Type": "application/json"
    }

    params = {
        "q": keywords,
        "limit": limit,
        "category_ids": "9801",
        "filter": f"price:[{min_price}..{max_price}],buyingOptions:{{FIXED_PRICE}}"
    }

    response = requests.get(SEARCH_URL, headers=headers, params=params)

    if response.status_code != 200:
        print("❌ Browse API error:", response.text)
        return []

    data = response.json()
    items = data.get("itemSummaries", [])

    listings = []

    for item in items:
        price_obj = item.get("price", {})
        image_obj = item.get("image", {})

        raw_image = image_obj.get("imageUrl")
        high_res_image = upgrade_image_resolution(raw_image)

        listings.append({
            "id": item.get("itemId"),
            "title": item.get("title"),
            "price": float(price_obj.get("value", 0)) if price_obj else 0,
            "view_url": item.get("itemWebUrl"),
            "image_url": high_res_image,
            "source": "ebay",
            "aspects": item.get("localizedAspects", {})
        })

    print(f"✅ eBay returned {len(listings)} listings")

    return listings