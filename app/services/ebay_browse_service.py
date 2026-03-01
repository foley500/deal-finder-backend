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
ITEM_URL = "https://api.ebay.com/buy/browse/v1/item/"

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
# 🖼 Upgrade Image Resolution
# ==========================================
def upgrade_image_resolution(url):
    if not url:
        return None

    if "s-l" in url:
        base = url.split("s-l")[0]
        return base + "s-l1600.jpg"

    return url


# ==========================================
# 🖼 Get Full Gallery Images
# ==========================================
def get_ebay_item_images(item_id: str, token: str):

    url = f"{ITEM_URL}{item_id}"

    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_GB",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print("❌ Failed to fetch item details:", response.text)
        return []

    data = response.json()

    images = []

    # Primary image
    if data.get("image"):
        images.append(
            upgrade_image_resolution(
                data["image"].get("imageUrl")
            )
        )

    # Additional images
    for img in data.get("additionalImages", []):
        images.append(
            upgrade_image_resolution(
                img.get("imageUrl")
            )
        )

    return images


# ==========================================
# 🚗 Browse Search (Production)
# ==========================================
def search_ebay_browse(
    keywords="used car",
    limit=5,
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
    summaries = data.get("itemSummaries", [])

    listings = []

    for summary in summaries:

        item_id = summary.get("itemId")

        # 🔥 FULL ITEM DETAIL CALL
        detail_response = requests.get(
            f"{ITEM_URL}{item_id}",
            headers=headers
        )

        if detail_response.status_code != 200:
            print("❌ Item detail fetch failed:", detail_response.text)
            continue

        item = detail_response.json()

        # ----------------------------------
        # BUILD ASPECT DICT
        # ----------------------------------
        aspect_dict = {}

        for aspect in item.get("localizedAspects", []):
            name = aspect.get("name")
            value = aspect.get("value")
            if name and value:
                aspect_dict[name] = value

        # ----------------------------------
        # IMAGE GALLERY
        # ----------------------------------
        all_images = []

        if item.get("image"):
            all_images.append(
                upgrade_image_resolution(
                    item["image"].get("imageUrl")
                )
            )

        for img in item.get("additionalImages", []):
            all_images.append(
                upgrade_image_resolution(
                    img.get("imageUrl")
                )
            )

        # ----------------------------------
        # BUILD LISTING
        # ----------------------------------

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
            "aspects": aspect_dict,
            "source": "ebay",
        })

    print(f"✅ eBay returned {len(listings)} listings with full details")

    return listings