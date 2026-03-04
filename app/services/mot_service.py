import os
import requests
import time
import re

DVSA_CLIENT_ID = os.getenv("DVSA_CLIENT_ID")
DVSA_CLIENT_SECRET = os.getenv("DVSA_CLIENT_SECRET")
DVSA_API_KEY = os.getenv("DVSA_API_KEY")
DVSA_TOKEN_URL = os.getenv("DVSA_TOKEN_URL")
DVSA_SCOPE_URL = os.getenv("DVSA_SCOPE_URL")

# ✅ Correct endpoint for registration lookup
MOT_TRADE_URL = "https://history.mot.api.gov.uk/v1/trade/vehicles/registration"

_cached_token = None
_token_expiry = 0


# ==========================================
# GET OAUTH TOKEN
# ==========================================

def get_dvsa_token():
    global _cached_token, _token_expiry

    if _cached_token and time.time() < _token_expiry:
        return _cached_token

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

        _cached_token = token_data.get("access_token")
        _token_expiry = time.time() + token_data.get("expires_in", 3600) - 60

        return _cached_token

    except Exception as e:
        print("❌ DVSA token exception:", e)
        return None


# ==========================================
# MAIN ENTRY
# ==========================================

def get_mot_data(registration: str):

    if not registration:
        return build_empty_response()

    # 🔥 Clean registration properly
    clean_reg = re.sub(r"[^A-Z0-9]", "", registration.upper())

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
        response = requests.get(
            url,
            headers=headers,
            timeout=10
        )

        print("🚗 DVSA MOT Status:", response.status_code)

        if response.status_code == 200:
            return parse_mot_trade_response(response.json())
        else:
            print("❌ DVSA MOT error body:", response.text)

    except Exception as e:
        print("DVSA MOT exception:", e)

    return build_empty_response()


# ==========================================
# PARSE RESPONSE (FIXED)
# ==========================================

from datetime import datetime


def parse_mot_trade_response(data):

    if not data or not isinstance(data, dict):
        return build_empty_response()

    mot_tests = data.get("motTests", [])

    # --------------------------------------
    # VEHICLE AGE CALCULATION
    # --------------------------------------

    first_used_date = data.get("firstUsedDate")
    vehicle_year = None

    if first_used_date:
        try:
            vehicle_year = int(first_used_date.split("-")[0])
        except:
            pass

    current_year = datetime.utcnow().year
    vehicle_age = current_year - vehicle_year if vehicle_year else 10

    # --------------------------------------
    # TIME-WEIGHTED MOT ANALYSIS
    # --------------------------------------

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

        # Count fails
        is_failed = test.get("testResult") == "FAILED"

        advisory_count_this_test = sum(
            1 for defect in test.get("defects", [])
            if defect.get("type") == "ADVISORY"
        )

        # RECENT (0–3 years) – full weight
        if years_ago <= recent_window_years:
            if is_failed:
                recent_fails += 1
            recent_advisories += advisory_count_this_test

        # MEDIUM (3–6 years) – half weight
        elif years_ago <= medium_window_years:
            if is_failed:
                medium_fails += 1
            medium_advisories += advisory_count_this_test

        # Older than 6 years → ignored completely

    # --------------------------------------
    # PROFESSIONAL RISK WEIGHTING
    # --------------------------------------

    # Fails matter more than advisories
    fail_penalty = (recent_fails * 200) + (medium_fails * 100)

    # Advisories diminish quickly
    advisory_penalty = (recent_advisories * 15) + (medium_advisories * 5)

    # Cap advisory stacking
    advisory_penalty = min(advisory_penalty, 700)

    raw_penalty = fail_penalty + advisory_penalty

    # --------------------------------------
    # AGE SCALING
    # --------------------------------------

    # Older cars expected to have wear
    # 0–5 yrs → full weight
    # 6–10 yrs → 85%
    # 11–15 yrs → 70%
    # 16+ yrs → 55%

    if vehicle_age <= 5:
        age_factor = 1.0
    elif vehicle_age <= 10:
        age_factor = 0.85
    elif vehicle_age <= 15:
        age_factor = 0.7
    else:
        age_factor = 0.55

    adjusted_penalty = raw_penalty * age_factor

    # --------------------------------------
    # SAFETY CAP
    # --------------------------------------

    # Absolute cap so MOT never destroys valuation
    final_penalty = min(adjusted_penalty, 2500)

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


# ==========================================
# SAFE EMPTY STRUCTURE
# ==========================================

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