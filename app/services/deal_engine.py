from app.margin import calculate_true_profit
from app.risk import description_risk
from app.scoring import calculate_score
from app.registration import extract_registration
from app.models import Deal, DealerSettings
from app.database import SessionLocal
from app.services.ocr_service import extract_plate_from_images, generate_fuzzy_variants, is_valid_uk_plate, score_plate_candidate
from app.services.mot_service import get_mot_data
from app.services.ebay_browse_service import get_item_detail, search_ebay_browse
from app.services.market_valuation_service import get_market_price_from_sold

from math import radians, sin, cos, sqrt, atan2
import datetime
import unicodedata
import os
import redis
import json
import requests
import re

REDIS_URL = os.getenv("CELERY_BROKER_URL")
_redis = redis.from_url(REDIS_URL)
POSTCODE_CACHE_TTL = 60 * 60 * 24 * 30  # 30 days


TARGET_POSTCODE = "S43 4TW"
MAX_DISTANCE_MILES = 50

# Maximum plausible miles per year — used to sanity check MOT mileage
MAX_MILES_PER_YEAR = 20000

# Known UK makes for title-based fallback extraction
KNOWN_MAKES = [
    "Ford", "Vauxhall", "Volkswagen", "Audi", "BMW", "Mercedes-Benz", "Mercedes",
    "Toyota", "Nissan", "Honda", "Hyundai", "Kia", "Seat", "Skoda", "Peugeot",
    "Renault", "Citroen", "Fiat", "Mini", "Mazda", "Volvo", "Land Rover",
    "Jaguar", "Subaru", "Mitsubishi", "Suzuki", "Dacia", "Alfa Romeo", "Jeep",
    "Tesla", "Lexus", "Porsche", "Isuzu", "DS", "MG", "Cupra", "Genesis",
]


# ---------------------------------
# HELPERS
# ---------------------------------

def extract_mileage_from_text(text: str) -> int:
    match = re.search(r"(\d{2,3},?\d{3})\s?miles?", text.lower())
    if match:
        return int(match.group(1).replace(",", ""))
    return 0


def is_valid_vehicle(title: str, price: float) -> bool:
    title = title.lower()

    # Phrase-based filters only — single words like "door", "mirror", "wheel"
    # appear in legitimate full-car listings ("5 door hatchback", "alloy wheels
    # included") and would silently discard large portions of real inventory.
    banned_words = [
        "breaking",
        "spares only",
        "parts only",
        "gearbox only",
        "bumper only",
        "for parts",
        "for spares",
        "not running",
        "no engine",
    ]

    if any(word in title for word in banned_words):
        return False

    if price < 500:
        return False

    return True


def extract_year_from_text(text: str):
    match = re.search(r"\b(20\d{2}|19\d{2})\b", text)
    if match:
        return int(match.group(1))
    return None


def extract_structured_value(aspects: dict, possible_keys: list[str]):
    for key in possible_keys:
        if key in aspects:
            value = aspects.get(key)
            if value:
                return value
    return None


def safe_int(value):
    try:
        return int(str(value).replace(",", "").strip())
    except Exception:
        return None


def assign_confidence(score: float) -> str:
    """
    Maps score -> deal confidence tier.
    Score tops out ~28-30 for a perfect deal (profit >3k, clean MOT, low mileage).
    Good deal (profit 1-2k, clean): ~10-15. Bands calibrated to match.
    """
    if score >= 20:
        return "high"
    elif score >= 10:
        return "medium"
    return "low"


def get_lat_long(postcode: str):
    if not postcode:
        return None, None
    clean = re.sub(r"\s+", "", postcode.upper())
    cache_key = f"postcode:{clean}"
    try:
        cached = _redis.get(cache_key)
        if cached:
            data = json.loads(cached)
            return data.get("lat"), data.get("lon")
    except Exception:
        pass
    try:
        response = requests.get(
            f"https://api.postcodes.io/postcodes/{clean}",
            timeout=5,
        )
        if response.status_code != 200:
            return None, None
        result = response.json().get("result", {})
        lat = result.get("latitude")
        lon = result.get("longitude")
        if lat and lon:
            try:
                _redis.set(cache_key, json.dumps({"lat": lat, "lon": lon}), ex=POSTCODE_CACHE_TTL)
            except Exception:
                pass
        return lat, lon
    except Exception:
        return None, None


def calculate_distance(lat1, lon1, lat2, lon2):
    R = 3958.8
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)

    a = sin(dlat / 2) * 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) * 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c


TARGET_LAT, TARGET_LON = get_lat_long(TARGET_POSTCODE)


# ---------------------------------
# FALLBACK VALUATION
# ---------------------------------

def smart_temp_valuation(price, year, mileage):
    """
    Conservative fallback valuation when sold data is unavailable.
    We assume equal market value to asking price instead of forcing
    an artificial 15% loss.
    """
    if not price:
        return 0
    return float(price)


# ---------------------------------
# MILEAGE SANITY CHECK
# ---------------------------------

def is_mileage_plausible(mileage: int, year: int) -> bool:
    """
    Reject MOT mileage readings that are physically impossible for the vehicle's age.
    e.g. a 2024 car cannot have 100,000 miles — that would require 100k miles in <1 year.
    """
    if not mileage or not year:
        return True  # can't validate without both — allow it

    current_year = datetime.datetime.now().year
    vehicle_age_years = max(current_year - year, 1)
    max_plausible = vehicle_age_years * MAX_MILES_PER_YEAR

    if mileage > max_plausible:
        print(f"   ⚠️ MOT mileage {mileage} implausible for {year} vehicle (max ~{max_plausible}) — rejecting")
        return False

    return True


# ---------------------------------
# TITLE-BASED MAKE/MODEL EXTRACTION
# ---------------------------------

def extract_make_model_from_title(title: str):
    """
    Last-resort fallback: parse make and model from listing title.
    eBay titles typically start with: Make Model Year ...
    e.g. "Ford Focus 2015 1.6 TDCi Titanium"
    """
    if not title:
        return None, None

    title_clean = title.strip()

    for make in KNOWN_MAKES:
        pattern = re.compile(re.escape(make), re.IGNORECASE)
        match = pattern.search(title_clean)
        if match:
            # Extract everything after the make
            after_make = title_clean[match.end():].strip()
            # First word after make is likely the model
            model_match = re.match(r"([A-Za-z0-9\-]+)", after_make)
            if model_match:
                model = model_match.group(1)
                # Skip if model looks like a year
                if re.match(r"^(19|20)\d{2}$", model):
                    return make, None
                print(f"   🔍 Title fallback: make={make}, model={model}")
                return make, model

    return None, None


# ---------------------------------
# MAIN ENGINE
# ---------------------------------

def check_market_depth(make: str, model: str, year: int, asking_price: float, budget_fn=None) -> int:
    """
    Counts competing active eBay listings at ≤ asking_price × 1.15 for
    the same make/model/year. Uses one API call, cached for 30 minutes.

    Returns number of competing listings (int). Returns -1 on failure.

    A deal with 2 competitors is rare. A deal with 30 competitors is just
    market price — it only looks cheap because it's the market floor.
    """
    if not make or not model or not year:
        return -1

    cache_key = f"market_depth:{make.lower()}:{model.lower()}:{year}:{int(asking_price)}"
    try:
        cached = _redis.get(cache_key)
        if cached:
            return int(cached)
    except Exception:
        pass

    if budget_fn and not budget_fn(1):
        return -1

    try:
        ceiling = round(asking_price * 1.15)
        results = search_ebay_browse(
            keywords=f"{make} {model} {year}",
            limit=50,
            min_price=500,
            max_price=ceiling,
            sort="price",
            offset=0,
        )
        depth = len(results)
        print(f"   🌊 Market depth: {depth} competing listings for {make} {model} {year} ≤ £{ceiling}")
        try:
            _redis.set(cache_key, depth, ex=1800)  # 30 min cache
        except Exception:
            pass
        return depth
    except Exception as e:
        print(f"   ⚠️ Market depth check failed: {e}")
        return -1


def upgrade_image_resolution(url: str):
    if not url:
        return url

    return (
        url.replace("s-l500", "s-l1600")
           .replace("s-l640", "s-l1600")
           .replace("s-l800", "s-l1600")
           .replace("s-l960", "s-l1600")
    )


def process_listing(raw_item: dict, dealer_id: int, source="ebay", filters=None, budget_fn=None):

    db = SessionLocal()

    try:

        external_id = raw_item.get("id") or raw_item.get("view_url")
        if not external_id:
            return None

        existing = db.query(Deal).filter(
            Deal.external_id == external_id,
            Deal.source == source
        ).first()

        if existing:
            return None

        settings = db.query(DealerSettings).filter(
            DealerSettings.dealer_id == dealer_id
        ).first()

        if not settings:
            return None

        title = raw_item.get("title", "") or ""
        # Normalise unicode — eBay sometimes returns smart quotes, em-dashes, etc.
        # Without this, year/make regexes fail silently on non-ASCII titles.
        title = unicodedata.normalize("NFKD", title)
        price = float(raw_item.get("price", 0) or 0)

        if not price:
            return None

        if not is_valid_vehicle(title, price):
            return None

        # ---------------------------------
        # Expand summary listing
        # ---------------------------------
        if raw_item.get("summary_only") and not raw_item.get("skip_detail"):

            detail = get_item_detail(raw_item.get("id"))
            if not detail:
                return None

            raw_item["description"] = detail.get("description", "")

            aspect_dict = {}
            for aspect in detail.get("localizedAspects", []):
                name = aspect.get("name")
                value = aspect.get("value")
                if name and value:
                    aspect_dict[name] = value[0] if isinstance(value, list) else value

            raw_item["aspects"] = aspect_dict
            detail_seller = detail.get("seller", {})
            raw_item["seller"] = detail_seller.get("username")
            # Prefer authoritative seller type from item detail over heuristic from summary
            detail_account_type = detail.get("sellerAccountType") or detail_seller.get("sellerAccountType")
            if detail_account_type:
                seller_type = detail_account_type
                raw_item["seller_type"] = seller_type

            image_urls = []

            if detail.get("image") and detail["image"].get("imageUrl"):
                image_urls.append(
                    upgrade_image_resolution(detail["image"]["imageUrl"])
                )

            for img in detail.get("additionalImages", []):
                if img.get("imageUrl"):
                    image_urls.append(
                        upgrade_image_resolution(img["imageUrl"])
                    )

            # Fallback to summary thumbnail if detail returned no images
            if not image_urls and raw_item.get("image_url"):
                image_urls.append(upgrade_image_resolution(raw_item["image_url"]))

            seen = set()
            cleaned = []
            for url in image_urls:
                if url not in seen:
                    cleaned.append(url)
                    seen.add(url)

            raw_item["image_urls"] = cleaned

        # ---------------------------------
        # Extract fields
        # ---------------------------------

        description = raw_item.get("description", "") or ""
        aspects = raw_item.get("aspects", {}) or {}
        listing_url = raw_item.get("view_url")
        seller = raw_item.get("seller")
        seller_type = raw_item.get("seller_type")  # INDIVIDUAL / BUSINESS from summary
        location = raw_item.get("location")
        image_urls = raw_item.get("image_urls", [])
        primary_image = image_urls[0] if image_urls else None
        listing_date_raw = raw_item.get("listing_date")
        price_drop_amount = raw_item.get("price_drop_amount")
        price_drop_pct = raw_item.get("price_drop_pct")

        # Days on market — how long since this listing was first spotted
        days_on_market = None
        if listing_date_raw:
            try:
                listed_at = datetime.datetime.fromisoformat(listing_date_raw.replace("Z", "+00:00"))
                if listed_at.tzinfo is None:
                    listed_at = listed_at.replace(tzinfo=datetime.timezone.utc)
                days_on_market = (datetime.datetime.now(datetime.timezone.utc) - listed_at).days
            except Exception:
                pass

        # ---------------------------------
        # Initial extraction from listing
        # ---------------------------------

        structured_year = extract_structured_value(
            aspects, ["Year", "Model Year", "Registration Year"]
        )

        structured_mileage = extract_structured_value(
            aspects, ["Mileage", "Miles"]
        )

        listing_year = safe_int(structured_year) or extract_year_from_text(title)
        listing_mileage = safe_int(structured_mileage) or extract_mileage_from_text(title)

        listing_make = aspects.get("Make")
        listing_model = aspects.get("Model")

        # ---------------------------------
        # Registration extraction
        # ---------------------------------

        reg = extract_registration(title)

        if not reg:
            reg = extract_registration(description)

        if not reg and raw_item.get("image_urls"):
            reg = extract_plate_from_images(raw_item["image_urls"])

        if not reg:
            print("⚠️ No reg found — continuing without DVSA data")

        # ---------------------------------
        # DVSA Lookup
        # ---------------------------------

        mot_penalty = 0
        mot_summary = {}
        mot_full_data = []
        vehicle_data = {}

        if reg:
            try:
                mot_response = get_mot_data(reg, asking_price=price)

                # If exact plate fails DVSA, try top-5 fuzzy variants scored by confidence.
                # Limit is critical — each DVSA call is a live API hit.
                if mot_response and not mot_response.get("vehicle_data"):
                    raw_variants = generate_fuzzy_variants(reg)
                    # Score and sort: prefer valid UK formats, then by edit distance proxy
                    scored = sorted(
                        [(v, score_plate_candidate(v, 0.8, 0.8)) for v in raw_variants if is_valid_uk_plate(v)],
                        key=lambda x: x[1],
                        reverse=True,
                    )
                    top_variants = [v for v, _ in scored[:5]]
                    for variant in top_variants:
                        print(f"   🔁 Trying fuzzy DVSA variant: {variant}")
                        fuzzy_response = get_mot_data(variant, asking_price=price)
                        if fuzzy_response and fuzzy_response.get("vehicle_data"):
                            print(f"   ✅ Fuzzy DVSA match: {variant}")
                            mot_response = fuzzy_response
                            break

                if mot_response:
                    mot_summary = mot_response.get("mot_summary", {})
                    mot_full_data = mot_response.get("mot_full_data", [])
                    vehicle_data = mot_response.get("vehicle_data", {})
                    mot_penalty = mot_summary.get("mot_penalty", 0)

            except Exception as e:
                print("MOT processing error:", e)

        # DO NOT HARD FAIL IF DVSA FAILS
        if not vehicle_data:
            print("⚠️ DVSA lookup failed — continuing with listing data")

        # ---------------------------------
        # Final Field Resolution (DVSA First)
        # ---------------------------------

        year = listing_year
        if vehicle_data.get("first_used_date"):
            try:
                year = int(vehicle_data["first_used_date"][:4])
            except Exception:
                pass

        if not year:
            year = extract_year_from_text(description)

        # ---------------------------------
        # Mileage resolution with sanity check
        # ---------------------------------

        mileage = listing_mileage

        if mot_full_data:
            try:
                latest_mot = sorted(
                    mot_full_data,
                    key=lambda x: x.get("completedDate", ""),
                    reverse=True
                )[0]
                mot_mileage = safe_int(latest_mot.get("odometerValue"))
                if mot_mileage:
                    if is_mileage_plausible(mot_mileage, year):
                        mileage = mot_mileage
                    else:
                        print(f"   ⚠️ Keeping listing mileage {listing_mileage} over implausible MOT mileage {mot_mileage}")
            except Exception:
                pass

        if not mileage:
            mileage = extract_mileage_from_text(description)

        if not mileage:
            mileage = 100000

        # ---------------------------------
        # Make/model resolution — DVSA → aspects → title fallback
        # ---------------------------------

        make = vehicle_data.get("make") or listing_make
        model = vehicle_data.get("model") or listing_model

        if not make or not model:
            title_make, title_model = extract_make_model_from_title(title)
            if not make and title_make:
                make = title_make
                print(f"   🔍 Make from title fallback: {make}")
            if not model and title_model:
                model = title_model
                print(f"   🔍 Model from title fallback: {model}")

        print(f"   🚗 Resolved: make={make}, model={model}, year={year}, mileage={mileage}")

        valuation_result = None

        if make and model:
            valuation_result = get_market_price_from_sold(
                make=make,
                model=model,
                year=year,
                mileage=mileage,
                engine_size=vehicle_data.get("engine_size"),
                listing_title=title,
                listing_aspects=aspects,
                fuel_type=vehicle_data.get("fuel_type") or aspects.get("Fuel Type"),
                cache_only=False,
                budget_fn=budget_fn,  # Routes all valuation eBay calls through daily budget guard
            )
        else:
            print(f"   ❌ Cannot value — make={make}, model={model} — skipping")

        if valuation_result:
            market_value = valuation_result.get("price_private") or valuation_result["market_price"]
            price_retail = valuation_result.get("price_retail")
            price_trade  = valuation_result.get("price_trade")
        else:
            print("⚠️ No cached valuation found — skipping listing (prewarm will fill cache)")
            return None

        valuation_data = {
            "market_price":  market_value,
            "price_private": market_value,
            "price_retail":  price_retail,
            "price_trade":   price_trade,
            "source":        valuation_result["source"] if valuation_result else "fallback_model",
            "source_label":  valuation_result.get("source_label") if valuation_result else None,
            "sample_size":   valuation_result.get("sample_size") if valuation_result else None,
            "confidence":    valuation_result.get("confidence") if valuation_result else None,
        }

        description_penalty = description_risk(description, price)
        risk_penalty = description_penalty + mot_penalty

        profit_result = calculate_true_profit(
            market_value,
            price,
            risk_penalty=risk_penalty
        )

        gross_profit = profit_result["gross_profit"]
        net_profit = profit_result["net_profit"]
        est_costs = profit_result["costs"]

        print(f"   📊 Values — Trade: £{price_trade} | Private: £{market_value} | Retail: £{price_retail}")
        print(f"   💷 Gross profit: £{gross_profit} | Est costs: £{profit_result['total_deductions']} | Net profit: £{net_profit}")

        score = calculate_score(
            profit=gross_profit,
            risk_penalty=risk_penalty,
            mileage=mileage,
            seller_type=seller_type,
            price_drop_pct=price_drop_pct,
            days_on_market=days_on_market,
        )
        confidence = assign_confidence(score)

        # Filter on GROSS profit — costs are shown separately, not used as a gate
        if settings.min_profit is not None and gross_profit < settings.min_profit:
            print("❌ Filtered by gross profit:", gross_profit)
            return None

        if settings.min_score is not None and score < settings.min_score:
            print("❌ Filtered by score:", score)
            return None

        # Market depth check — done AFTER profit/score gates to keep API cost minimal.
        # Only confirmed deals trigger this single extra call. Cached 30 minutes.
        market_depth = -1
        if make and model and year:
            market_depth = check_market_depth(
                make=make,
                model=model,
                year=year,
                asking_price=price,
                budget_fn=budget_fn,
            )
            # Re-score with market depth signal
            score = calculate_score(
                profit=gross_profit,
                risk_penalty=risk_penalty,
                mileage=mileage,
                seller_type=seller_type,
                price_drop_pct=price_drop_pct,
                days_on_market=days_on_market,
                market_depth=market_depth,
            )
            confidence = assign_confidence(score)

        deal = Deal(
            dealer_id=dealer_id,
            external_id=external_id,
            title=title,
            reg=reg,
            mileage=mileage,
            listing_price=price,
            market_value=market_value,
            profit=gross_profit,
            net_profit=net_profit,
            risk_penalty=risk_penalty,
            score=score,
            source=source,
            status=confidence,
            report={
                "financials": {
                    "listing_price": price,
                    "market_value": market_value,
                    "price_private": market_value,
                    "price_retail":  price_retail,
                    "price_trade":   price_trade,
                    "gross_profit": gross_profit,
                    "net_profit": net_profit,
                    "est_transport": est_costs["transport"],
                    "est_prep": est_costs["prep"],
                    "est_warranty": est_costs["warranty"],
                    "est_total_costs": est_costs["total"],
                    "risk_penalty": risk_penalty,
                },
                "market_model": valuation_data,
                "risk_breakdown": {
                    "description_penalty": description_penalty,
                    "mot_penalty": mot_penalty,
                    "total_risk_penalty": risk_penalty,
                },
                "scoring": {
                    "score": score,
                    "confidence_level": confidence,
                },
                "deal_signals": {
                    "seller_type": seller_type,
                    "price_drop_amount": price_drop_amount,
                    "price_drop_pct": price_drop_pct,
                    "days_on_market": days_on_market,
                    "market_depth": market_depth,
                    "listing_date": listing_date_raw,
                },
                "mot_summary": mot_summary,
                "mot_full_data": mot_full_data,
                "vehicle_data": vehicle_data,
                "listing_details": {
                    "transmission": aspects.get("Transmission"),
                    "fuel_type": aspects.get("Fuel Type"),
                    "exterior_color": aspects.get("Exterior Colour"),
                    "interior_color": aspects.get("Interior Colour"),
                },
                "seller": seller,
                "location": location,
                "listing_url": listing_url,
                "primary_image": primary_image,
            }
        )

        db.add(deal)
        db.commit()
        db.refresh(deal)

        return deal

    finally:
        db.close()