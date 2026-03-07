import os
import requests
import time
import re
import redis
import json
from datetime import datetime

DVSA_CLIENT_ID = os.getenv("DVSA_CLIENT_ID")
DVSA_CLIENT_SECRET = os.getenv("DVSA_CLIENT_SECRET")
DVSA_API_KEY = os.getenv("DVSA_API_KEY")
DVSA_TOKEN_URL = os.getenv("DVSA_TOKEN_URL")
DVSA_SCOPE_URL = os.getenv("DVSA_SCOPE_URL")

MOT_TRADE_URL = "https://history.mot.api.gov.uk/v1/trade/vehicles/registration"

REDIS_URL = os.getenv("CELERY_BROKER_URL")
redis_client = redis.from_url(REDIS_URL)

DVSA_TOKEN_KEY = "dvsa:access_token"
DVSA_CACHE_TTL = 86400  # 24 hours


def get_dvsa_token():
    cached = redis_client.get(DVSA_TOKEN_KEY)
    if cached:
        return cached.decode()

    if not DVSA_CLIENT_ID or not DVSA_CLIENT_SECRET:
        print("❌ DVSA credentials missing")
        return None

    payload = {
        "client_id": DVSA_CLIENT_ID,
        "client_secret": DVSA_CLIENT_SECRET,
        "scope": DVSA_SCOPE_URL,
        "grant_type": "client_credentials"
    }

    try:
        response = requests.post(
            DVSA_TOKEN_URL,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10
        )

        print("🔐 DVSA TOKEN STATUS:", response.status_code)

        if response.status_code != 200:
            print("❌ DVSA token error:", response.text)
            return None

        token_data = response.json()
        token = token_data.get("access_token")
        expires_in = int(token_data.get("expires_in", 3600)) - 60

        redis_client.set(DVSA_TOKEN_KEY, token, ex=expires_in)
        return token

    except Exception as e:
        print("❌ DVSA token exception:", e)
        return None


def get_mot_data(registration: str, asking_price: float = None):

    if not registration:
        return build_empty_response()

    clean_reg = re.sub(r"[^A-Z0-9]", "", registration.upper())

    # Check Redis cache first — MOT data doesn't change hourly
    cache_key = f"dvsa:{clean_reg}"
    cached = redis_client.get(cache_key)
    if cached:
        print(f"✅ DVSA cache hit: {clean_reg}")
        return json.loads(cached)

    token = get_dvsa_token()
    if not token:
        return build_empty_response()

    url = f"{MOT_TRADE_URL}/{clean_reg}"

    headers = {
        "Authorization": f"Bearer {token}",
        "x-api-key": DVSA_API_KEY,
        "Accept": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)

        print("🚗 DVSA MOT Status:", response.status_code)

        if response.status_code == 200:
            result = parse_mot_trade_response(response.json(), asking_price=asking_price)
            redis_client.set(cache_key, json.dumps(result), ex=DVSA_CACHE_TTL)
            return result
        else:
            print("❌ DVSA MOT error body:", response.text)

    except Exception as e:
        print("DVSA MOT exception:", e)

    return build_empty_response()


def parse_mot_trade_response(data, asking_price=None):

    if not data or not isinstance(data, dict):
        return build_empty_response()

    mot_tests = data.get("motTests", [])

    first_used_date = data.get("firstUsedDate")
    vehicle_year = None

    if first_used_date:
        try:
            vehicle_year = int(first_used_date.split("-")[0])
        except:
            pass

    current_year = datetime.utcnow().year
    vehicle_age = current_year - vehicle_year if vehicle_year else 10

    recent_window_years = 3
    medium_window_years = 6

    recent_fails = 0
    recent_advisories = 0
    medium_fails = 0
    medium_advisories = 0

    for test in mot_tests:

        test_date = test.get("completedDate")
        if not test_date:
            continue

        try:
            test_year = int(test_date.split("-")[0])
        except:
            continue

        years_ago = current_year - test_year
        is_failed = test.get("testResult") == "FAILED"

        advisory_count_this_test = sum(
            1 for defect in test.get("defects", [])
            if defect.get("type") == "ADVISORY"
        )

        if years_ago <= recent_window_years:
            if is_failed:
                recent_fails += 1
            recent_advisories += advisory_count_this_test

        elif years_ago <= medium_window_years:
            if is_failed:
                medium_fails += 1
            medium_advisories += advisory_count_this_test

    fail_penalty = (recent_fails * 200) + (medium_fails * 100)
    advisory_penalty = (recent_advisories * 15) + (medium_advisories * 5)
    advisory_penalty = min(advisory_penalty, 700)
    raw_penalty = fail_penalty + advisory_penalty

    if vehicle_age <= 5:
        age_factor = 1.0
    elif vehicle_age <= 10:
        age_factor = 0.85
    elif vehicle_age <= 15:
        age_factor = 0.7
    else:
        age_factor = 0.55

    adjusted_penalty = raw_penalty * age_factor

    # Cap penalty relative to vehicle value
    # Never penalise more than 30% of asking price
    if asking_price and asking_price > 0:
        value_cap = asking_price * 0.30
    else:
        value_cap = 2500

    final_penalty = min(adjusted_penalty, value_cap)

    return {
        "mot_summary": {
            "fail_count": recent_fails + medium_fails,
            "advisory_count": recent_advisories + medium_advisories,
            "mot_penalty": round(final_penalty, 2),
        },
        "mot_full_data": mot_tests,
        "vehicle_data": {
            "make": data.get("make"),
            "model": data.get("model"),
            "first_used_date": data.get("firstUsedDate"),
            "fuel_type": data.get("fuelType"),
            "engine_size": data.get("engineSize"),
            "colour": data.get("primaryColour")
        }
    }


def build_empty_response():
    return {
        "mot_summary": {
            "fail_count": 0,
            "advisory_count": 0,
            "mot_penalty": 0,
        },
        "mot_full_data": [],
        "vehicle_data": {}
    }