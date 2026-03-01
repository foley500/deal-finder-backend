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
        all_images = raw_item.get("all_images", [])
        seller = raw_item.get("seller")
        location = raw_item.get("location")

        price = float(raw_item.get("price", 0) or 0)

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

        year = safe_int(structured_year) or extract_year_from_text(title)
        mileage = safe_int(structured_mileage) or extract_mileage_from_text(title)

        title_lower = title.lower()

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

        if allowed_body_types and structured_body:
            if not any(bt.lower() in structured_body.lower() for bt in allowed_body_types):
                return None

        # ---------------------------------
        # REGISTRATION DETECTION
        # ---------------------------------

        reg = raw_item.get("registration")

        # 1️⃣ Try title first (free)
        if not reg:
            reg = extract_registration(title)

        # 2️⃣ Only OCR if still no reg
        if not reg:

            images_to_scan = []

            if image_url:
                images_to_scan.append(image_url)

            # only 1 extra image
            for img in all_images[:1]:
                if img and img != image_url:
                    images_to_scan.append(img)

            for img in images_to_scan:
                reg = extract_plate_from_image_url(img)
                if reg:
                    print(f"✅ Plate detected: {reg}")
                    break

        if not reg:
            print("ℹ️ No plate detected for listing.")

        # ---------------------------------
        # VALUATION
        # ---------------------------------

        valuation_data = get_market_value(reg) or {}
        market_value = valuation_data.get("clean", 0)

        # ---------------------------------
        # MOT
        # ---------------------------------

        mot_tests = []
        mot_penalty = 0
        fail_count = 0
        advisory_count = 0

        if reg:
            try:
                mot_raw = get_mot_data(reg)
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
            except Exception:
                pass

        # ---------------------------------
        # RISK
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

        confidence = assign_confidence(score)

        # ---------------------------------
        # REPORT
        # ---------------------------------

        report_data = {
            "year": year,
            "body_type": structured_body,
            "listing_url": listing_url,
            "image_url": image_url,
            "seller": seller,
            "location": location,
            "cap_data": valuation_data,
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
        # SAVE
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