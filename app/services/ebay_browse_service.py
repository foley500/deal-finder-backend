import os
import requests
import base64
import time
import redis
import random
from app.services.ebay_rate_limiter import throttle_ebay

EBAY_CLIENT_ID = os.getenv("EBAY_CLIENT_ID")
EBAY_CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")

TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
ITEM_URL = "https://api.ebay.com/buy/browse/v1/item/"

REDIS_URL = os.getenv("CELERY_BROKER_URL")
redis_client = redis.from_url(REDIS_URL)

EBAY_TOKEN_KEY = "ebay:access_token"

# Circuit breaker — tripped on any 429 to prevent hammering the API.
# All browse calls check this first and fast-fail if the circuit is open.
BROWSE_CIRCUIT_KEY = "ebay_browse_circuit_open"
BROWSE_CIRCUIT_TTL = 90  # seconds to cool down


def _is_circuit_open() -> bool:
    return bool(redis_client.exists(BROWSE_CIRCUIT_KEY))


def _trip_circuit():
    redis_client.set(BROWSE_CIRCUIT_KEY, "1", ex=BROWSE_CIRCUIT_TTL)
    print(f"⚡ Browse circuit tripped — pausing all browse calls for {BROWSE_CIRCUIT_TTL}s")


EBAY_TOKEN_LOCK_KEY = "ebay:token_fetch_lock"

def get_ebay_access_token():
    cached = redis_client.get(EBAY_TOKEN_KEY)
    if cached:
        return cached.decode()

    # Atomic lock — only one worker fetches a new token when the cache is cold.
    # Without this, concurrent workers all request new tokens simultaneously,
    # burning auth calls and risking transient credential blocks.
    lock_acquired = redis_client.set(EBAY_TOKEN_LOCK_KEY, "1", nx=True, ex=15)
    if not lock_acquired:
        # Another worker is fetching — wait briefly then read from cache
        import time
        time.sleep(2)
        cached = redis_client.get(EBAY_TOKEN_KEY)
        return cached.decode() if cached else None

    try:
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
    finally:
        redis_client.delete(EBAY_TOKEN_LOCK_KEY)


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
        "sort": sort,
        "category_ids": "9801",
        "filter": f"price:[{min_price}..{max_price}],buyingOptions:{{FIXED_PRICE}},conditions:{{USED}},itemLocationCountry:GB",
        "fieldgroups": "SELLER_DETAILS",
    }


    if _is_circuit_open():
        print("⚡ Browse circuit open — skipping request")
        return []

    throttle_ebay()
    response = requests.get(SEARCH_URL, headers=headers, params=params)

    if response.status_code == 429:
        _trip_circuit()
        print("⚠️ Browse rate limited — circuit tripped")
        return []

    if response.status_code != 200:
        print("❌ Browse API error:", response.text)
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

        seller_info = summary.get("seller", {})
        seller_type = seller_info.get("sellerAccountType") or (
            "INDIVIDUAL" if seller_info.get("feedbackScore", 9999) < 200 else "BUSINESS"
        )

        listings.append({
            "id": summary.get("itemId"),
            "title": summary.get("title"),
            "price": float(summary.get("price", {}).get("value", 0)),
            "view_url": summary.get("itemWebUrl"),
            "image_url": summary.get("image", {}).get("imageUrl"),
            "location": summary.get("itemLocation", {}).get("postalCode"),
            "listing_date": summary.get("itemCreationDate"),
            "seller_type": seller_type,
            "source": "ebay_browse",
            "summary_only": True
        })

    print(f"✅ eBay returned {len(listings)} vehicle summaries")
    return listings

def search_sniper_windows(make, model):
    """
    Runs multiple price-window searches to catch mispriced listings.
    Also scans multiple result pages and reverse keyword order.
    """

    windows = [
        (500, 1500),
        (1500, 4000),
        (4000, 8000),
        (8000, 20000),
    ]

    # Build deduplicated search terms — reversed order only adds value when
    # make and model are distinct. When model is empty, both terms are identical.
    seen_terms = set()
    search_terms = []
    for candidate in [
        f"{make} {model}".strip(),
        f"{model} {make}".strip() if model.strip() else None,
        model.strip() if model and (any(c.isdigit() for c in model) or len(model) >= 4) else None,
    ]:
        if candidate and candidate not in seen_terms:
            seen_terms.add(candidate)
            search_terms.append(candidate)

    # Sniper: freshest listings only — one sort, no pagination offset.
    # Budget = len(search_terms) × len(windows) calls.
    # Typical: 1 term × 4 windows = 4 calls vs old 32 calls.
    all_results = []
    seen_ids = set()

    for term in search_terms:

        for min_price, max_price in windows:

            if _is_circuit_open():
                print("⚡ Browse circuit open — stopping sniper windows")
                break

            listings = search_ebay_browse(
                keywords=term,
                limit=50,
                min_price=min_price,
                max_price=max_price,
                sort="newlyListed",
                offset=0
            )

            if not listings:
                continue

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
    if _is_circuit_open():
        print("⚡ Browse circuit open — skipping item detail")
        return None

    token = get_ebay_access_token()
    if not token:
        return None

    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_GB",
    }

    throttle_ebay()
    response = requests.get(
        f"{ITEM_URL}{item_id}?fieldgroups=PRODUCT,SELLER_DETAILS",
        headers=headers
    )

    if response.status_code == 429:
        _trip_circuit()
        print("Rate limited - circuit tripped")
        return None

    if response.status_code != 200:
        print("❌ Item detail error:", response.text)
        return None

    data = response.json()

    # Hoist seller account type to top level for easy access in deal_engine
    seller = data.get("seller", {})
    if seller and "sellerAccountType" not in data:
        data["sellerAccountType"] = seller.get("sellerAccountType")

    return data