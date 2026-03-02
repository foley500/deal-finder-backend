from app.margin import calculate_true_profit
from app.risk import description_risk
from app.scoring import calculate_score
from app.registration import extract_registration
from app.services.valuation_service import get_market_value
from app.models import Deal
from app.database import SessionLocal
from app.services.ocr_service import extract_plate_from_image_url
from app.services.mot_service import get_mot_data

from math import radians, sin, cos, sqrt, atan2
from datetime import datetime, timezone

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
    except:
        return None, None


def calculate_distance(lat1, lon1, lat2, lon2):
    R = 3958.8
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)

    a = sin(dlat / 2)*2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)*2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c


# ---------------------------------
# MAIN ENGINE
# ---------------------------------

def process_listing(raw_item: dict, dealer_id: int, source="ebay", filters=None):

    db = SessionLocal()

    try:
        external_id = raw_item.get("id") or raw_item.get("view_url")

        # ✅ Prevent duplicates
        existing = db.query(Deal).filter(
            Deal.external_id == external_id,
            Deal.source == source
        ).first()

        if existing:
            return None

        title = raw_item.get("title", "") or ""
        description = raw_item.get("description", "") or ""
        aspects = raw_item.get("aspects", {}) or {}

        listing_url = raw_item.get("view_url")
        image_url = raw_item.get("image_url")
        all_images = raw_item.get("all_images", []) or []
        seller = raw_item.get("seller")
        location = raw_item.get("location")
        listing_date = raw_item.get("listing_date")

        price = float(raw_item.get("price", 0) or 0)

        # ---------------------------------
        # STRUCTURED EXTRACTION
        # ---------------------------------

        structured_year = extract_structured_value(
            aspects, ["Year", "Model Year", "Registration Year"]
        )

        structured_mileage = extract_structured_value(
            aspects, ["Mileage", "Miles"]
        )

        year = safe_int(structured_year) or extract_year_from_text(title)
        mileage = safe_int(structured_mileage) or extract_mileage_from_text(title)

        # ---------------------------------
        # HARD FILTERS
        # ---------------------------------

        if not year or year < 2014:
            return None

        if not mileage or mileage > 100000:
            return None

        if not price or price > 4000:
            return None

        # ---------------------------------
        # DISTANCE FILTER
        # ---------------------------------

        if location:
            target_lat, target_lon = get_lat_long(TARGET_POSTCODE)
            listing_lat, listing_lon = get_lat_long(location)

            if target_lat and listing_lat:
                distance = calculate_distance(
                    target_lat, target_lon,
                    listing_lat, listing_lon
                )

                if distance > MAX_DISTANCE_MILES:
                    return None

        # ---------------------------------
        # REGISTRATION DETECTION
        # ---------------------------------

        reg = extract_registration(title)

        if not reg:
            images_to_scan = []

            if image_url:
                images_to_scan.append(image_url)

            for img in all_images:
                if img not in images_to_scan:
                    images_to_scan.append(img)
                if len(images_to_scan) >= 5:
                    break

            for img in images_to_scan:
                reg = extract_plate_from_image_url(img)
                if reg:
                    break

        # ---------------------------------
        # VALUATION
        # ---------------------------------

        valuation_data = get_market_value(reg) or {}
        market_value = valuation_data.get("clean", 0)

        # ---------------------------------
        # MOT
        # ---------------------------------

        mot_penalty = 0
        mot_tests = []

        if reg:
            try:
                mot_raw = get_mot_data(reg)
                if mot_raw and isinstance(mot_raw, list):
                    mot_tests = mot_raw[0].get("motTests", [])

                    for test in mot_tests:
                        if test.get("testResult") == "FAIL":
                            mot_penalty += 500

                        if test.get("rfrAndComments"):
                            mot_penalty += 200
            except:
                pass

        # ---------------------------------
        # RISK + SCORING
        # ---------------------------------

        description_penalty = description_risk(description)
        risk_penalty = description_penalty + mot_penalty

        profit = calculate_true_profit(
            market_value,
            price,
            risk_penalty=risk_penalty
        )

        score = calculate_score(profit, risk_penalty, mileage)

        # ---------------------------------
        # FRESHNESS BONUS
        # ---------------------------------

        freshness_bonus = 0

        if listing_date:
            try:
                created = datetime.fromisoformat(
                    listing_date.replace("Z", "+00:00")
                )
                hours_old = (
                    datetime.now(timezone.utc) - created
                ).total_seconds() / 3600

                if hours_old <= 2:
                    freshness_bonus = 15
                elif hours_old <= 6:
                    freshness_bonus = 10
                elif hours_old <= 12:
                    freshness_bonus = 5
            except:
                pass

        score += freshness_bonus

        confidence = assign_confidence(score)

        # ---------------------------------
        # SAVE DEAL
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
                "year": year,
                "listing_url": listing_url,
                "seller": seller,
                "location": location,
                "listing_date": listing_date,
                "freshness_bonus": freshness_bonus,
                "listing_price": price,
                "market_value": market_value,
                "profit": profit,
                "risk_penalty": risk_penalty,
            }
        )

        db.add(deal)
        db.commit()
        db.refresh(deal)

        return deal

    finally:
        db.close()