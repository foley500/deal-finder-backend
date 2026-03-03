from app.margin import calculate_true_profit
from app.risk import description_risk
from app.scoring import calculate_score
from app.registration import extract_registration
from app.services.valuation_service import get_market_value
from app.models import Deal, DealerSettings
from app.database import SessionLocal
from app.services.ocr_service import extract_plate_from_image_url
from app.services.mot_service import get_mot_data
from app.valuation import get_market_value_from_reg
from app.services.ebay_browse_service import get_item_detail

from math import radians, sin, cos, sqrt, atan2
from datetime import datetime
import requests
import re


TARGET_POSTCODE = "S43 4TW"
MAX_DISTANCE_MILES = 50


# ---------------------------------
# HELPERS
# ---------------------------------

def extract_mileage_from_text(text: str) -> int:
    match = re.search(r"(\d{2,3},?\d{3})\s?miles?", text.lower())
    if match:
        return int(match.group(1).replace(",", ""))
    return 0


def extract_year_from_text(text: str) -> int | None:
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
# TEMP VALUATION (Conservative)
# ---------------------------------

def smart_temp_valuation(price, year, mileage):
    """
    Conservative fallback model until CAP API is live.
    Prevents fake inflated profits.
    """

    if not price:
        return {
            "clean": 0,
            "retail": 0,
            "trade": 0,
            "part_ex": 0,
            "source": "temporary_model"
        }

    estimated_trade = price * 0.85
    estimated_part_ex = price * 0.90
    estimated_retail = price * 1.05

    return {
        "clean": round(estimated_trade, 2),
        "retail": round(estimated_retail, 2),
        "trade": round(estimated_trade, 2),
        "part_ex": round(estimated_part_ex, 2),
        "source": "temporary_model"
    }


# ---------------------------------
# MAIN ENGINE
# ---------------------------------

def process_listing(raw_item: dict, dealer_id: int, source="ebay", filters=None):

    db = SessionLocal()

    try:
        # ---------------------------------
        # Prevent duplicates
        # ---------------------------------
        external_id = raw_item.get("id") or raw_item.get("view_url")
        if not external_id:
            return None

        existing = db.query(Deal).filter(
            Deal.external_id == external_id,
            Deal.source == source
        ).first()

        if existing:
            return None

        # ---------------------------------
        # Load dealer settings
        # ---------------------------------
        settings = db.query(DealerSettings).filter(
            DealerSettings.dealer_id == dealer_id
        ).first()

        if not settings:
            return None

        # ---------------------------------
        # Basic listing data
        # ---------------------------------
        title = raw_item.get("title", "") or ""
        price = float(raw_item.get("price", 0) or 0)

        if not price:
            return None

        max_price = filters.get("max_price") if filters else None
        if max_price and price > max_price:
            return None

        # ---------------------------------
        # Expand summary listing
        # ---------------------------------
        if raw_item.get("summary_only"):
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

            images = []
            if detail.get("image"):
                images.append(detail["image"].get("imageUrl"))

            raw_item["image_url"] = images[0] if images else None

        # ---------------------------------
        # Extract fields
        # ---------------------------------
        description = raw_item.get("description", "") or ""
        aspects = raw_item.get("aspects", {}) or {}
        listing_url = raw_item.get("view_url")
        image_url = raw_item.get("image_url")
        seller = raw_item.get("seller")
        location = raw_item.get("location")

        structured_year = extract_structured_value(
            aspects, ["Year", "Model Year", "Registration Year"]
        )

        structured_mileage = extract_structured_value(
            aspects, ["Mileage", "Miles"]
        )

        year = safe_int(structured_year) or extract_year_from_text(title)
        mileage = safe_int(structured_mileage) or extract_mileage_from_text(title)

        # ---------------------------------
        # Apply filters
        # ---------------------------------
        if year:
            if settings.min_year and year < settings.min_year:
                return None
            if settings.max_year and year > settings.max_year:
                return None

        if mileage:
            if settings.max_mileage and mileage > settings.max_mileage:
                return None

        # ---------------------------------
        # Distance filter
        # ---------------------------------
        if location and TARGET_LAT:
            listing_lat, listing_lon = get_lat_long(location)
            if listing_lat:
                distance = calculate_distance(
                    TARGET_LAT, TARGET_LON,
                    listing_lat, listing_lon
                )
                if distance > MAX_DISTANCE_MILES:
                    return None

        # ---------------------------------
        # Registration detection (single OCR)
        # ---------------------------------
        reg = extract_registration(title)

        if not reg and image_url:
            reg = extract_plate_from_image_url(image_url)

        # ---------------------------------
        # VALUATION PRIORITY
        # CAP → REG MODEL → TEMP
        # ---------------------------------
        valuation_data = None

        if reg:
            valuation_data = get_market_value(reg)

        if not valuation_data or not valuation_data.get("clean"):

            if reg:
                reg_value = get_market_value_from_reg(reg, mileage or 0)

                if reg_value:
                    valuation_data = {
                        "clean": reg_value,
                        "retail": reg_value * 1.15,
                        "trade": reg_value,
                        "source": "reg_model"
                    }

        if not valuation_data:
            valuation_data = smart_temp_valuation(price, year, mileage)

        market_value = valuation_data.get("trade", 0)

        # ---------------------------------
        # MOT
        # ---------------------------------
        mot_penalty = 0
        mot_summary = {}
        mot_full_data = []
        vehicle_data = {}

        if reg:
            try:
                mot_response = get_mot_data(reg)

                if mot_response:
                    mot_summary = mot_response.get("mot_summary", {})
                    mot_full_data = mot_response.get("mot_full_data", [])
                    vehicle_data = mot_response.get("vehicle_data", {})
                    mot_penalty = mot_summary.get("mot_penalty", 0)

            except Exception as e:
                print("MOT processing error:", e)

        # ---------------------------------
        # Risk + Profit
        # ---------------------------------
        description_penalty = description_risk(description, price)
        risk_penalty = description_penalty + mot_penalty

        profit = calculate_true_profit(
            market_value,
            price,
            risk_penalty=risk_penalty
        )

        score = calculate_score(profit, risk_penalty, mileage)
        confidence = assign_confidence(score)

        # ---------------------------------
        # Final filters
        # ---------------------------------
        if settings.min_profit is not None and profit < settings.min_profit:
            return None

        if settings.min_score is not None and score < settings.min_score:
            return None

        # ---------------------------------
        # Save Deal
        # ---------------------------------
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
                "valuation": valuation_data,
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
            }
        )

        db.add(deal)
        db.commit()
        db.refresh(deal)

        return deal

    finally:
        db.close()