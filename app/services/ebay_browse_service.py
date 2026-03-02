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


def upgrade_image_resolution(url):
    if not url:
        return None
    if "s-l" in url:
        return url.split("s-l")[0] + "s-l1600.jpg"
    return url


def search_ebay_browse(keywords="used car", limit=5, min_price=1000, max_price=50000):

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
        "sort": "newlyListed",  # 🔥 CRITICAL
        "filter": f"price:[{min_price}..{max_price}],buyingOptions:{{FIXED_PRICE}}"
    }

    response = requests.get(SEARCH_URL, headers=headers, params=params)

    if response.status_code != 200:
        print("❌ Browse API error:", response.text)
        return []

    summaries = response.json().get("itemSummaries", [])
    listings = []

    for summary in summaries:

        item_id = summary.get("itemId")

        detail = requests.get(f"{ITEM_URL}{item_id}", headers=headers)

        if detail.status_code == 429:
            print("⚠️ eBay rate limited")
            time.sleep(1)
            continue

        if detail.status_code != 200:
            print("❌ Item detail error:", detail.text)
            continue

        item = detail.json()

        # BUILD ASPECT DICT
        aspect_dict = {}
        for aspect in item.get("localizedAspects", []):
            name = aspect.get("name")
            values = aspect.get("value")

            if name and values:
                if isinstance(values, list):
                    aspect_dict[name] = values[0]
                else:
                    aspect_dict[name] = values

        # IMAGE GALLERY
        all_images = []

        if item.get("image"):
            all_images.append(
                upgrade_image_resolution(item["image"].get("imageUrl"))
            )

        for img in item.get("additionalImages", []):
            all_images.append(
                upgrade_image_resolution(img.get("imageUrl"))
            )

        listings.append({
            "id": item_id,
            "title": item.get("title"),
            "description": item.get("description"),
            "price": float(item.get("price", {}).get("value", 0)),
            "view_url": item.get("itemWebUrl"),
            "image_url": all_images[0] if all_images else None,
            "all_images": all_images,
            "seller": item.get("seller", {}).get("username"),
            "location": item.get("itemLocation", {}).get("postalCode"),
            "listing_date": (
                item.get("itemCreationDate")
                or summary.get("itemCreationDate")
            ),
            "aspects": aspect_dict,
            "source": "ebay",
        })

    print(f"✅ eBay returned {len(listings)} listings with full details")
    return listings