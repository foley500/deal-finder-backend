import statistics
import os
import redis
import json
import requests
import re
import time
from app.services.ebay_rate_limiter import throttle_ebay
from app.services.ebay_browse_service import (
    get_ebay_access_token,
    get_item_detail
)

SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
FINDING_API_URL = "https://svcs.ebay.com/services/search/FindingService/v1"
REDIS_URL = os.getenv("CELERY_BROKER_URL")
redis_client = redis.from_url(REDIS_URL)

CACHE_TTL = 1800
MAX_DETAIL_EXPANSIONS = 25
MIN_SAMPLE_SIZE = 5


# ---------------------------------------------------
# HELPERS
# ---------------------------------------------------

def extract_year_from_title(title: str):
    match = re.search(r"\b(19\d{2}|20\d{2})\b", title)
    return int(match.group(1)) if match else None


def extract_mileage_from_text(text: str):
    if not text:
        return None

    text = text.lower().replace(",", "")

    match = re.search(r"(\d{4,6})\s*(miles|mile|mi)\b", text)
    if match:
        val = int(match.group(1))
        if 1000 < val < 300000:
            return val

    match = re.search(r"(\d{2,3})\s?k\b", text)
    if match:
        val = int(match.group(1)) * 1000
        if 1000 < val < 300000:
            return val

    return None


def normalise_base_model(make: str, base_model: str) -> str:
    make_lower = make.lower()
    model_lower = base_model.lower()

    if make_lower == "bmw" and model_lower.isdigit():
        return f"{base_model} Series"

    if make_lower in ["mercedes", "mercedes-benz"]:
        if "class" in model_lower and "-" not in model_lower:
            return model_lower.replace("class", "-class").title()

    return base_model


# ---------------------------------------------------
# EBAY SOLD SEARCH
# ---------------------------------------------------

def get_sold_listings(query: str, limit: int = 100):
    app_id = os.getenv("EBAY_CLIENT_ID")
    if not app_id:
        return []

    params = {
        "OPERATION-NAME": "findCompletedItems",
        "SERVICE-VERSION": "1.0.0",
        "SECURITY-APPNAME": app_id,
        "RESPONSE-DATA-FORMAT": "JSON",
        "REST-PAYLOAD": "",
        "keywords": query,
        "categoryId": "9801",
        "itemFilter(0).name": "SoldItemsOnly",
        "itemFilter(0).value": "true",
        "itemFilter(1).name": "Condition",
        "itemFilter(1).value": "Used",
        "itemFilter(2).name": "ListingType",
        "itemFilter(2).value": "AuctionWithBIN",
        "itemFilter(3).name": "ListingType",
        "itemFilter(3).value[0]": "FixedPrice",
        "paginationInput.entriesPerPage": min(limit, 100),
        "sortOrder": "EndTimeSoonest",
    }

    throttle_ebay()
    response = requests.get(FINDING_API_URL, params=params)

    if response.status_code != 200:
        return []

    try:
        data = response.json()
        search_result = (
            data
            .get("findCompletedItemsResponse", [{}])[0]
            .get("searchResult", [{}])[0]
        )
        items = search_result.get("item", [])
    except Exception:
        return []

    summaries = []
    for item in items:
        try:
            price_val = float(
                item.get("sellingStatus", [{}])[0]
                    .get("currentPrice", [{}])[0]
                    .get("__value__", 0)
            )
            title = item.get("title", [""])[0]
            item_id = item.get("itemId", [""])[0]

            summaries.append({
                "itemId": item_id,
                "title": title,
                "price": {"value": str(price_val)},
            })
        except Exception:
            continue

    return summaries


# ---------------------------------------------------
# CORE FILTER ENGINE
# ---------------------------------------------------

def run_filter_layer(summaries, target_year, target_mileage, year_tolerance, mileage_tolerance, adjust_mileage=True):
    prices = []
    rejected_no_year = 0
    rejected_year = 0
    rejected_mileage = 0
    rejected_no_price = 0
    accepted = 0
    mileage_diffs = []
    adjustments = []

    for summary in summaries:
        listing_year = summary.get("_year")
        listing_mileage = summary.get("_mileage")

        if listing_year is None:
            if accepted >= MIN_SAMPLE_SIZE:
                rejected_no_year += 1
                continue
            else:
                listing_year = target_year

        year_diff = abs(listing_year - target_year)

        if year_diff > year_tolerance:
            rejected_year += 1
            continue

        mileage_diff = None
        abs_mileage_diff = None
        if listing_mileage is not None:
            mileage_diff = listing_mileage - target_mileage
            abs_mileage_diff = abs(mileage_diff)

            if abs_mileage_diff > mileage_tolerance:
                rejected_mileage += 1
                continue

        price_obj = summary.get("price")
        if not price_obj:
            rejected_no_price += 1
            continue

        base_price = float(price_obj["value"])
        adjusted_price = base_price

        if mileage_diff is not None and adjust_mileage:
            mileage_diffs.append(mileage_diff)
            blocks = min(abs_mileage_diff / 5000, 8)
            mileage_adjustment = base_price * 0.015 * blocks
            adjustments.append(mileage_adjustment)
            adjusted_price = base_price - mileage_adjustment if mileage_diff > 0 else base_price + mileage_adjustment
        elif mileage_diff is not None:
            mileage_diffs.append(mileage_diff)

        prices.append(adjusted_price)
        accepted += 1

    print("📊 FILTER DEBUG:")
    print("   Accepted:", accepted)
    print("   Rejected (no year):", rejected_no_year)
    print("   Rejected (year tolerance):", rejected_year)
    print("   Rejected (mileage tolerance):", rejected_mileage)
    print("   Rejected (no price):", rejected_no_price)

    if mileage_diffs:
        print("   Avg mileage diff:", round(statistics.mean(mileage_diffs), 0))
        print("   Max mileage diff:", round(max(abs(x) for x in mileage_diffs), 0))
        if adjustments:
            print("   Avg price adjustment:", round(statistics.mean(adjustments), 2))
        else:
            print("   No price adjustment applied (layer 1)")
    else:
        print("   No mileage data available")

    if len(prices) < MIN_SAMPLE_SIZE:
        print("❌ Failed — only", len(prices), "samples (min required:", MIN_SAMPLE_SIZE, ")")
        return None

    prices = sorted(prices)

    cut = int(len(prices) * 0.1)
    if cut > 0:
        prices = prices[cut:-cut]

    sample_count = len(prices)

    if sample_count >= 10:
        confidence = "high"
    elif sample_count >= 5:
        confidence = "medium"
    else:
        confidence = "low"

    return {
        "market_price": round(statistics.median(prices), 2),
        "sample_size": sample_count,
        "confidence": confidence,
    }


# --------------------------------------------------
# PUBLIC ENTRY
# --------------------------------------------------

def get_market_price_from_sold(
    make, model, year, mileage,
    engine_size=None, listing_title=None, listing_aspects=None,
):

    if not make or not model or not year:
        return None

    if not mileage:
        mileage = 100000

    make = str(make).strip().title()
    model = str(model).strip().title()
    model_words = model.split()
    base_model = model_words[0]
    base_model = normalise_base_model(make, base_model)
    trim = " ".join(model_words[1:]) if len(model_words) > 1 else None

    engine_litre = None
    if engine_size:
        try:
            cleaned = re.sub(r"[^\d.]", "", str(engine_size))
            size = float(cleaned)
            engine_litre = round(size / 1000, 1) if size > 10 else round(size, 1)
        except:
            pass

    mileage_bucket = round(mileage / 10000) * 10000
    cache_key = f"sold_cache:{make}:{base_model}:{year}:{mileage_bucket}"
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    year_range = range(year - 2, year + 3)

    title_lower = listing_title.lower() if listing_title else ""

    if not engine_litre and listing_title:
        litre_match = re.search(r"\b(\d\.\d)\b", title_lower)
        if litre_match:
            try:
                engine_litre = float(litre_match.group(1))
            except:
                pass

        if not engine_litre:
            cc_match = re.search(r"\b(\d{3,4})\s?cc\b", title_lower)
            if cc_match:
                try:
                    engine_litre = round(int(cc_match.group(1)) / 1000, 1)
                except:
                    pass

        if not engine_litre:
            badge_match = re.search(r"\b(\d{2,3})([di])\b", title_lower)
            if badge_match:
                try:
                    digits = badge_match.group(1)
                    engine_litre = float(digits[0] + "." + digits[1])
                except:
                    pass

    if listing_aspects:
        if not engine_litre:
            aspect_engine = listing_aspects.get("Engine Size")
            if aspect_engine:
                try:
                    cleaned = re.sub(r"[^\d.]", "", str(aspect_engine))
                    size = float(cleaned)
                    engine_litre = round(size / 1000, 1) if size > 10 else round(size, 1)
                except:
                    pass

    search_queries = []

    for y in year_range:
        search_queries.append(f"{make} {base_model} {y}")

    if engine_litre:
        for y in year_range:
            search_queries.append(f"{make} {base_model} {engine_litre} {y}")

    print(f"🔎 Searching: make={make} base_model={base_model} engine={engine_litre} years={list(year_range)}")
    print(f"📋 Queries: {search_queries}")

    all_summaries = []
    seen_ids = set()

    for query in search_queries:
        results = get_sold_listings(query)
        for item in results:
            item_id = item.get("itemId")
            if item_id and item_id not in seen_ids:
                seen_ids.add(item_id)
                all_summaries.append(item)

    print(f"📦 Total unique summaries collected: {len(all_summaries)}")

    if not all_summaries:
        return None

    enriched_summaries = _pre_expand_details(all_summaries)

    for tolerance_config in [
        {"year_tolerance": 2, "mileage_tolerance": 15000, "source": "layer_1_strict", "adjust_mileage": False},
        {"year_tolerance": 2, "mileage_tolerance": 25000, "source": "layer_2_relaxed_mileage", "adjust_mileage": True},
        {"year_tolerance": 3, "mileage_tolerance": 30000, "source": "layer_3_relaxed_year", "adjust_mileage": True},
    ]:
        result = run_filter_layer(
            enriched_summaries,
            target_year=year,
            target_mileage=mileage,
            year_tolerance=tolerance_config["year_tolerance"],
            mileage_tolerance=tolerance_config["mileage_tolerance"],
            adjust_mileage=tolerance_config["adjust_mileage"],
        )
        if result:
            result["source"] = tolerance_config["source"]
            redis_client.set(cache_key, json.dumps(result), ex=CACHE_TTL)
            return result

    return None


def _pre_expand_details(summaries: list) -> list:
    expansions = 0
    enriched = []

    for summary in summaries:
        title = summary.get("title", "")
        item_id = summary.get("itemId")

        listing_year = extract_year_from_title(title)
        listing_mileage = extract_mileage_from_text(title)

        if (listing_year is None or listing_mileage is None) and expansions < MAX_DETAIL_EXPANSIONS:
            detail = get_item_detail(item_id)
            expansions += 1

            if detail:
                for aspect in detail.get("localizedAspects", []):
                    name = aspect.get("name", "").lower()
                    raw_value = aspect.get("value", "")
                    val = str(raw_value).strip() if isinstance(raw_value, str) else str(raw_value[0]).strip()

                    if not val:
                        continue

                    if listing_year is None and any(k in name for k in ["year", "reg"]):
                        match = re.search(r"(19\d{2}|20\d{2})", val)
                        if match:
                            listing_year = int(match.group(1))

                    if listing_mileage is None and any(k in name for k in ["mileage", "miles", "odometer"]):
                        try:
                            listing_mileage = int(val.replace(",", "").replace(" ", "").split(".")[0])
                        except:
                            pass

        summary["_year"] = listing_year
        summary["_mileage"] = listing_mileage
        enriched.append(summary)

    print(f"🔍 Pre-expansion complete: {expansions} detail calls used")
    return enriched