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

def parse_mot_trade_response(data):

    # 🔥 This endpoint returns a dict, NOT a list
    if not data or not isinstance(data, dict):
        return build_empty_response()

    mot_tests = data.get("motTests", [])

    fail_count = 0
    advisory_count = 0

    for test in mot_tests:
        if test.get("testResult") == "FAILED":
            fail_count += 1

        # NEW API uses "defects" not rfrAndComments
        for defect in test.get("defects", []):
            if defect.get("type") == "ADVISORY":
                advisory_count += 1

    return {
        "mot_summary": {
            "fail_count": fail_count,
            "advisory_count": advisory_count,
            "mot_penalty": fail_count * 150 + advisory_count * 25,
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