from app.margin import calculate_true_profit
from app.risk import description_risk
from app.scoring import calculate_score
from app.registration import extract_registration
from app.models import Deal, DealerSettings
from app.database import SessionLocal
from app.services.ocr_service import extract_plate_from_images
from app.services.mot_service import get_mot_data
from app.services.ebay_browse_service import get_item_detail
from app.services.market_valuation_service import get_market_price_from_sold

from math import radians, sin, cos, sqrt, atan2
import datetime
import requests
import re


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

    banned_words = [
        "breaking", "spares", "repair", "parts", "engine",
        "gearbox", "bumper", "door", "mirror", "alloy",
        "wheel", "tyre", "tire"
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
    if score >= 60:
        return "very_high"
    elif score >= 40:
        return "high"
    elif score >= 20:
        return "medium"
    return "low"


def get_lat_long(postcode: str):
    try:
        response = requests.get(f"https://api.postcodes.io/postcodes/{postcode}")
        if response.status_code != 200:
            return None, None
        data = response.json().get("result", {})
        return data.get("latitude"), data.get("longitude")
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

def upgrade_image_resolution(url: str):
    if not url:
        return url

    return (
        url.replace("s-l500", "s-l1600")
           .replace("s-l640", "s-l1600")
           .replace("s-l800", "s-l1600")
           .replace("s-l960", "s-l1600")
    )


def process_listing(raw_item: dict, dealer_id: int, source="ebay", filters=None):

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
            raw_item["seller"] = detail.get("seller", {}).get("username")

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
        location = raw_item.get("location")
        image_urls = raw_item.get("image_urls", [])
        primary_image = image_urls[0] if image_urls else None

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

                if mot_response:
                    mot_summary = mot_response.get("mot_summary", {})
                    mot_full_data = mot_response.get("mot_full_data", [])
                    vehicle_data = mot_response.get("vehicle_data", {})
                    mot_penalty = mot_summary.get("mot_penalty", 0)

            except Exception as e:
                print("MOT processing error:", e)

        # 🔥 DO NOT HARD FAIL IF DVSA FAILS
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
                cache_only=False,
            )
        else:
            print(f"   ❌ Cannot value — make={make}, model={model} — skipping")

        if valuation_result:
            market_value = valuation_result["market_price"]
        else:
            print("⚠️ No sold data found — skipping listing")
            return None

        valuation_data = {
            "market_price": market_value,
            "source": valuation_result["source"] if valuation_result else "fallback_model",
            "source_label": valuation_result.get("source_label") if valuation_result else None,
            "sample_size": valuation_result.get("sample_size") if valuation_result else None,
            "confidence": valuation_result.get("confidence") if valuation_result else None,
        }

        description_penalty = description_risk(description, price)
        risk_penalty = description_penalty + mot_penalty

        profit = calculate_true_profit(
            market_value,
            price,
            risk_penalty=risk_penalty
        )

        score = calculate_score(profit, risk_penalty, mileage)
        confidence = assign_confidence(score)

        if settings.min_profit is not None and profit < settings.min_profit:
            print("❌ Filtered by profit:", profit)
            return None

        if settings.min_score is not None and score < settings.min_score:
            print("❌ Filtered by score:", score)
            return None

        deal = Deal(
            dealer_id=dealer_id,
            external_id=external_id,
            title=title,
            reg=reg,
            mileage=mileage,
            listing_price=price,
            market_value=market_value,
            profit=profit,
            risk_penalty=risk_penalty,
            score=score,
            source=source,
            status=confidence,
            report={
                "financials": {
                    "listing_price": price,
                    "market_value": market_value,
                    "profit": profit,
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