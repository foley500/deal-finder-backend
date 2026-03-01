from app.margin import calculate_true_profit
from app.risk import description_risk
from app.scoring import calculate_score
from app.registration import extract_registration
from app.services.valuation_service import get_market_value
from app.models import Deal
from app.database import SessionLocal
from app.services.ocr_service import extract_plate_from_image_url
from app.services.mot_service import get_mot_data

import re


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
    except:
        return None


def assign_confidence(score: float) -> str:
    if score >= 60:
        return "very_high"
    elif score >= 40:
        return "high"
    elif score >= 20:
        return "medium"
    return "low"


# ---------------------------------
# MAIN ENGINE
# ---------------------------------

def process_listing(
    raw_item: dict,
    dealer_id: int,
    source: str = "ebay",
    filters: dict | None = None,
):

    db = SessionLocal()

    try:
        filters = filters or {}

        min_year = filters.get("min_year")
        max_year = filters.get("max_year")
        max_mileage = filters.get("max_mileage")
        required_keywords = filters.get("required_keywords", [])
        excluded_keywords = filters.get("excluded_keywords", [])
        min_profit = filters.get("min_profit")
        min_score = filters.get("min_score")
        allowed_body_types = filters.get("allowed_body_types", [])

        external_id = raw_item.get("id") or raw_item.get("view_url")

        existing = db.query(Deal).filter(
            Deal.external_id == external_id,
            Deal.source == source
        ).first()

        if existing:
            return existing

        title = raw_item.get("title", "")
        description = raw_item.get("description", "")
        aspects = raw_item.get("aspects", {}) or {}

        listing_url = raw_item.get("view_url")
        image_url = raw_item.get("image_url")
        seller = raw_item.get("seller")
        location = raw_item.get("location")

        # ---------------------------------
        # PRICE EXTRACTION (TITLE BASED)
        # ---------------------------------

        price = 0

        # Format: "Car name / £5,000 / Location"
        if "/" in title:
            parts = [p.strip() for p in title.split("/")]

            if len(parts) >= 2:
                price_part = parts[1]

                price_clean = (
                    price_part
                    .replace("£", "")
                    .replace(",", "")
                    .strip()
                )

                if price_clean.isdigit():
                    price = float(price_clean)

        # Fallback if title parsing fails
        if not price:
            price = float(raw_item.get("price", 0) or 0)

        # ---------------------------------
        # FACEBOOK DIRECT FIELDS
        # ---------------------------------

        fb_mileage = raw_item.get("mileage")
        fb_transmission = raw_item.get("transmission")
        fb_fuel = raw_item.get("fuelType")
        fb_exterior = raw_item.get("exteriorColor")
        fb_interior = raw_item.get("interiorColor")

        title_lower = title.lower()

        # ---------------------------------
        # STRUCTURED EXTRACTION
        # ---------------------------------

        structured_year = extract_structured_value(
            aspects,
            ["Year", "Model Year", "Registration Year"]
        )

        structured_mileage = extract_structured_value(
            aspects,
            ["Mileage", "Miles"]
        )

        structured_body = extract_structured_value(
            aspects,
            ["Body Type", "BodyStyle"]
        )

        structured_transmission = extract_structured_value(
            aspects,
            ["Transmission"]
        )

        structured_fuel = extract_structured_value(
            aspects,
            ["Fuel Type", "Fuel"]
        )

        structured_exterior = extract_structured_value(
            aspects,
            ["Exterior Colour", "Colour"]
        )

        # ---------------------------------
        # PRIORITY MERGING
        # ---------------------------------

        year = safe_int(structured_year) or extract_year_from_text(title)

        mileage = (
            safe_int(fb_mileage)
            or safe_int(structured_mileage)
            or extract_mileage_from_text(title)
        )

        transmission = fb_transmission or structured_transmission
        fuel_type = fb_fuel or structured_fuel
        exterior_color = fb_exterior or structured_exterior
        interior_color = fb_interior

        # ---------------------------------
        # EARLY FILTERING
        # ---------------------------------

        if min_year and year and year < min_year:
            return None

        if max_year and year and year > max_year:
            return None

        if max_mileage and mileage and mileage > max_mileage:
            return None

        for word in required_keywords:
            if word.lower() not in title_lower:
                return None

        for word in excluded_keywords:
            if word.lower() in title_lower:
                return None

        if allowed_body_types:
            body_value = (structured_body or "").lower()
            if structured_body:
                if not any(bt.lower() in body_value for bt in allowed_body_types):
                    return None
            else:
                if not any(bt.lower() in title_lower for bt in allowed_body_types):
                    return None

        # ---------------------------------
        # REGISTRATION PRIORITY
        # ---------------------------------

        reg = raw_item.get("registration")

        if not reg:
            reg = extract_registration(title)

        if not reg and image_url:
            reg = extract_plate_from_image_url(image_url)

        # ---------------------------------
        # VALUATION
        # ---------------------------------

        valuation_data = get_market_value(reg) or {}
        market_value = valuation_data.get("clean", 0)

        # ---------------------------------
        # MOT ANALYSIS
        # ---------------------------------

        mot_raw = None
        mot_tests = []
        mot_penalty = 0
        fail_count = 0
        advisory_count = 0

        if reg:
            try:
                mot_raw = get_mot_data(reg)
            except Exception:
                mot_raw = None

        if mot_raw and isinstance(mot_raw, list):
            mot_tests = mot_raw[0].get("motTests", [])

            for test in mot_tests:
                if test.get("testResult") == "FAIL":
                    fail_count += 1
                    mot_penalty += 500

                comments = test.get("rfrAndComments", [])
                advisory_count += len(comments)

                if comments:
                    mot_penalty += 200

        # ---------------------------------
        # RISK MODEL
        # ---------------------------------

        description_penalty = description_risk(description)
        risk_penalty = description_penalty + mot_penalty

        # ---------------------------------
        # PROFIT & SCORE
        # ---------------------------------

        profit = calculate_true_profit(
            market_value,
            price,
            risk_penalty=risk_penalty
        )

        score = calculate_score(
            profit,
            risk_penalty,
            mileage
        )

        if min_profit and profit < min_profit:
            return None

        if min_score and score < min_score:
            return None

        confidence = assign_confidence(score)

        # ---------------------------------
        # STRUCTURED REPORT
        # ---------------------------------

        report_data = {
            "year": year,
            "body_type": structured_body,
            "listing_url": listing_url,
            "image_url": image_url,
            "seller": seller,
            "location": location,

            "listing_details": {
                "transmission": transmission,
                "fuel_type": fuel_type,
                "exterior_color": exterior_color,
                "interior_color": interior_color,
            },

            "cap_data": {
                "clean": valuation_data.get("clean"),
                "retail": valuation_data.get("retail"),
                "trade": valuation_data.get("trade"),
                "source": valuation_data.get("source"),
            },

            "mot_summary": {
                "fail_count": fail_count,
                "advisory_count": advisory_count,
                "mot_penalty": mot_penalty,
            },

            "mot_full_data": mot_tests,

            "risk_breakdown": {
                "description_penalty": description_penalty,
                "mot_penalty": mot_penalty,
                "total_risk_penalty": risk_penalty,
            },

            "financials": {
                "listing_price": price,
                "market_value": market_value,
                "profit": profit,
            },

            "scoring": {
                "score": score,
                "confidence_level": confidence,
            }
        }

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
            report=report_data
        )

        db.add(deal)
        db.commit()
        db.refresh(deal)

        return deal

    finally:
        db.close()