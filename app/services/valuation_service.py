import os
from datetime import datetime, timedelta

from app.database import SessionLocal
from app.models import Valuation
from app.services.cap_service import get_cap_valuation


CACHE_DAYS = 7


def mock_valuation(registration: str) -> dict:
    """
    Safe fallback valuation.
    Returns neutral values (no artificial profit inflation).
    """
    return {
        "clean": 0,
        "retail": 0,
        "trade": 0,
        "source": "fallback_none"
    }


def cap_credentials_available() -> bool:
    return all([
        os.getenv("CAP_USERNAME"),
        os.getenv("CAP_PASSWORD"),
        os.getenv("CAP_URL")
    ])


def get_market_value(registration: str) -> dict:

    if not registration:
        return mock_valuation(registration)

    db = SessionLocal()

    try:
        existing = db.query(Valuation).filter(
            Valuation.registration == registration
        ).first()

        if existing:
            if existing.created_at > datetime.utcnow() - timedelta(days=CACHE_DAYS):
                return {
                    "clean": existing.market_value,
                    "retail": existing.market_value * 1.15,
                    "trade": existing.market_value,
                    "source": existing.source
                }

        if not cap_credentials_available():
            return mock_valuation(registration)

        cap_data = get_cap_valuation(registration)

        if cap_data and cap_data.get("clean"):

            clean_value = cap_data.get("clean")

            if existing:
                existing.market_value = clean_value
                existing.created_at = datetime.utcnow()
                existing.source = "CAP"
            else:
                new_val = Valuation(
                    registration=registration,
                    market_value=clean_value,
                    source="CAP"
                )
                db.add(new_val)

            db.commit()

            cap_data["source"] = "CAP"
            return cap_data

        return mock_valuation(registration)

    except Exception as e:
        print("Valuation service error:", e)
        return mock_valuation(registration)

    finally:
        db.close()