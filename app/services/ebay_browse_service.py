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
# Exponential backoff: each consecutive circuit opening doubles the cooldown (capped at 30 min).
# nx=True on the circuit key prevents concurrent 429s from resetting an active cooldown.
BROWSE_CIRCUIT_KEY = "ebay_browse_circuit_open"
BROWSE_CIRCUIT_TRIP_COUNT_KEY = "ebay_browse_circuit_trip_count"
BROWSE_CIRCUIT_BASE_TTL = 300   # 5 minutes base cooldown
BROWSE_CIRCUIT_MAX_TTL  = 1800  # 30 minutes maximum cooldown


def _is_circuit_open() -> bool:
    return bool(redis_client.exists(BROWSE_CIRCUIT_KEY))


def _trip_circuit():
    # Increment trip counter atomically to determine backoff TTL.
    pipe = redis_client.pipeline()
    pipe.incr(BROWSE_CIRCUIT_TRIP_COUNT_KEY)
    pipe.expire(BROWSE_CIRCUIT_TRIP_COUNT_KEY, 3600)  # counter resets after 1 hour of calm
    trip_count, _ = pipe.execute()

    ttl = min(BROWSE_CIRCUIT_BASE_TTL * (2 ** (trip_count - 1)), BROWSE_CIRCUIT_MAX_TTL)
    # nx=True: don't reset an active cooldown if a concurrent task also hits 429.
    # The first task to trip the circuit wins and sets the TTL; others are no-ops.
    redis_client.set(BROWSE_CIRCUIT_KEY, "1", ex=int(ttl), nx=True)
    print(f"⚡ Browse circuit tripped (trip #{trip_count}) — pausing all browse calls for {ttl}s")


def _reset_circuit_trip_count():
    """Call this on a successful eBay response to reset the backoff counter."""
    redis_client.delete(BROWSE_CIRCUIT_TRIP_COUNT_KEY)


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
    offset=0,
    start_time_filter=None,  # ISO 8601 UTC string — only return listings created after this time
    buyer_postcode=None,     # Dealer's postcode — restricts results to within radius_miles
    radius_miles=None,       # Search radius in miles (eBay UK uses miles)
):
    token = get_ebay_access_token()
    if not token:
        return []

    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_GB",
    }

    filter_str = f"price:[{min_price}..{max_price}],buyingOptions:{{FIXED_PRICE}},itemLocationCountry:GB"
    if start_time_filter:
        filter_str += f",itemStartDate:[{start_time_filter}..]"
    if buyer_postcode and radius_miles:
        filter_str += f",maxDistance:{int(radius_miles)}"

    params = {
        "q": keywords,
        "limit": limit,
        "offset": offset,
        "sort": sort,
        "category_ids": "9801",
        "filter": filter_str,
        "fieldgroups": "SELLER_DETAILS",
    }
    if buyer_postcode:
        params["buyerPostalCode"] = buyer_postcode


    if _is_circuit_open():
        print("⚡ Browse circuit open — skipping request")
        return []

    throttle_ebay()

    # Re-check after acquiring the throttle slot — another concurrent worker may have
    # tripped the circuit while we were waiting in the throttle queue.
    if _is_circuit_open():
        print("⚡ Browse circuit open (post-throttle check) — skipping request")
        return []

    response = requests.get(SEARCH_URL, headers=headers, params=params)

    if response.status_code == 429:
        _trip_circuit()
        print("⚠️ Browse rate limited — circuit tripped")
        return []

    if response.status_code != 200:
        print("❌ Browse API error:", response.text)
        return []

    _reset_circuit_trip_count()
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

def search_sniper_recent(query, since, buyer_postcode=None, radius_miles=None):
    """
    Single-call sniper search for recently-listed vehicles.

    Used when `since` is set to a short recent window (≤ 2 hours).
    With a tight time filter, no make will have 200+ new listings locally —
    so the 5-window price-band approach is pure waste (5 API calls vs 1).

    Cost: 1 call per query (vs 5 with search_sniper_windows).
    Budget: 66 queries × 1 call × 24 runs/day = 1,584 sniper calls/day ✅

    Still paginates up to 3 pages (600 results) as a safety net for busy
    makes during peak hours — far more than any 60-min local window needs.
    """
    PAGE_LIMIT = 200
    MAX_PAGES = 3  # 600 results max — never hit in a 60-min local window

    all_results = []
    seen_ids = set()

    for page in range(MAX_PAGES):
        if _is_circuit_open():
            print("⚡ Browse circuit open — stopping sniper recent search")
            return all_results

        listings = search_ebay_browse(
            keywords=query,
            limit=PAGE_LIMIT,
            sort="newlyListed",
            offset=page * PAGE_LIMIT,
            start_time_filter=since,
            buyer_postcode=buyer_postcode,
            radius_miles=radius_miles,
        )

        if not listings:
            break

        for listing in listings:
            item_id = listing["id"]
            if item_id not in seen_ids:
                seen_ids.add(item_id)
                all_results.append(listing)

        if len(listings) < PAGE_LIMIT:
            break

    return all_results


def search_sniper_windows(make, model, since=None, buyer_postcode=None, radius_miles=None):
    """
    Runs multiple price-window searches to catch mispriced listings.
    Paginates each window (up to 5 pages × 200 = 1,000 listings/window) so that
    busy makes (Ford, Vauxhall, etc.) never hit a listing cap within the time window.

    since:          ISO 8601 UTC string — only return listings created after this time.
                    Covers the full rotation window (~14 hrs) so no listings are missed
                    between cycles.
    buyer_postcode: Dealer's postcode — restricts results to within radius_miles.
    radius_miles:   Search radius from buyer_postcode (eBay UK uses miles).
    """
    PAGE_LIMIT = 200       # eBay's maximum per page
    MAX_PAGES  = 5         # Up to 1,000 listings per price window

    windows = [
        (500, 1500),
        (1500, 4000),
        (4000, 8000),
        (8000, 20000),
        (20000, 35000),  # Catches BMW 3/5 Series, Merc C/E-Class, Land Rover — all hit £20k+
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

    all_results = []
    seen_ids = set()

    for term in search_terms:
        for min_price, max_price in windows:
            for page in range(MAX_PAGES):
                if _is_circuit_open():
                    print("⚡ Browse circuit open — stopping sniper windows")
                    return all_results

                listings = search_ebay_browse(
                    keywords=term,
                    limit=PAGE_LIMIT,
                    min_price=min_price,
                    max_price=max_price,
                    sort="newlyListed",
                    offset=page * PAGE_LIMIT,
                    start_time_filter=since,
                    buyer_postcode=buyer_postcode,
                    radius_miles=radius_miles,
                )

                if not listings:
                    break

                for listing in listings:
                    item_id = listing["id"]
                    if item_id in seen_ids:
                        continue
                    seen_ids.add(item_id)
                    all_results.append(listing)

                # If we got fewer than a full page, there are no more results
                if len(listings) < PAGE_LIMIT:
                    break

    print(f"🎯 Sniper windows returned {len(all_results)} unique listings")
    return all_results

def get_model_variants(make, model):
    """
    Generates common typo / shorthand search variants.
    Broader coverage = more mispriced listings found by sniper.
    Typo listings get fewer views → lower competition → cheaper prices.
    """

    variants = [(make, model)]

    make_lower = make.lower()

    # Common UK eBay make abbreviations and typos.
    # Each entry adds 1 extra search term × 4 price windows = 4 more API calls.
    replacements = {
        "mercedes-benz": ["mercedes", "merc", "mersedes"],
        "mercedes":      ["merc", "mersedes"],
        "volkswagen":    ["vw", "volkswagon", "volksvagen"],
        "bmw":           ["bm"],
        "land rover":    ["landrover", "land-rover"],
        "vauxhall":      ["vauxhal", "vaxhaul"],
        "audi":          ["adi"],
        "toyota":        ["toyata", "toyot"],
        "ford":          ["foord"],
        "nissan":        ["nissan"],   # keep original only — rare typos
        "hyundai":       ["hundai", "hyundia"],
        "kia":           [],           # short name — typos don't help
        "renault":       ["renult", "renualt"],
        "peugeot":       ["peugot", "puegot"],
        "citroen":       ["citron", "citreon"],
        "skoda":         ["schkoda"],
        "seat":          [],           # too short
        "mini":          [],           # too generic
        "porsche":       ["porche", "porshe"],
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

    if _is_circuit_open():
        print("⚡ Browse circuit open (post-throttle check) — skipping item detail")
        return None

    response = requests.get(
        f"{ITEM_URL}{item_id}?fieldgroups=PRODUCT,ADDITIONAL_SELLER_DETAILS",
        headers=headers
    )

    if response.status_code == 429:
        _trip_circuit()
        print("Rate limited - circuit tripped")
        return None

    if response.status_code != 200:
        print("❌ Item detail error:", response.text)
        return None

    _reset_circuit_trip_count()
    data = response.json()

    # Hoist seller account type to top level for easy access in deal_engine
    seller = data.get("seller", {})
    if seller and "sellerAccountType" not in data:
        data["sellerAccountType"] = seller.get("sellerAccountType")

    return data