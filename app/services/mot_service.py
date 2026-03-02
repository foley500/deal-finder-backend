import os
import requests


# ===============================
# API KEYS
# ===============================
MOT_TRADE_API_KEY = os.getenv("DVSA_API_KEY")  # full MOT history (future)
DVLA_API_KEY = os.getenv("DVLA_API_KEY")       # open data fallback


# ===============================
# ENDPOINTS
# ===============================
MOT_TRADE_URL = "https://history.mot.api.gov.uk/v1/trade/vehicles/mot-tests"
DVLA_URL = "https://driver-vehicle-licensing.api.gov.uk/vehicle-enquiry/v1/vehicles"


# ===============================
# MAIN ENTRY POINT
# ===============================
def get_mot_data(registration: str):

    if not registration:
        return build_empty_response()

    registration = registration.upper().strip()

    # ---------------------------------
    # 1️⃣ Try FULL MOT History API
    # ---------------------------------
    if MOT_TRADE_API_KEY:
        print("🔎 Attempting MOT Trade API...")

        headers = {
            "x-api-key": MOT_TRADE_API_KEY,
            "Accept": "application/json"
        }

        params = {
            "registration": registration
        }

        try:
            response = requests.get(
                MOT_TRADE_URL,
                headers=headers,
                params=params,
                timeout=10
            )

            print("MOT Trade Status:", response.status_code)

            if response.status_code == 200:
                return parse_mot_trade_response(response.json())

        except Exception as e:
            print("MOT Trade API error:", e)

    # ---------------------------------
    # 2️⃣ Fallback to DVLA Open Data
    # ---------------------------------
    if DVLA_API_KEY:
        print("🔎 Using DVLA Open Data fallback...")

        headers = {
            "x-api-key": DVLA_API_KEY,
            "Content-Type": "application/json"
        }

        payload = {
            "registrationNumber": registration
        }

        try:
            response = requests.post(
                DVLA_URL,
                headers=headers,
                json=payload,
                timeout=10
            )

            print("DVLA Status:", response.status_code)

            if response.status_code == 200:
                return parse_dvla_response(response.json())

        except Exception as e:
            print("DVLA fallback error:", e)

    return build_empty_response()


# ===============================
# PARSE FULL MOT TRADE DATA
# ===============================
def parse_mot_trade_response(data):

    if not data:
        return build_empty_response()

    vehicle = data[0]
    mot_tests = vehicle.get("motTests", [])

    fail_count = 0
    advisory_count = 0

    for test in mot_tests:
        if test.get("testResult") == "FAILED":
            fail_count += 1

        for item in test.get("rfrAndComments", []):
            if item.get("type") == "ADVISORY":
                advisory_count += 1

    return {
        "mot_summary": {
            "fail_count": fail_count,
            "advisory_count": advisory_count,
            "mot_penalty": fail_count * 150 + advisory_count * 25,
        },
        "mot_full_data": mot_tests,
        "vehicle_data": {}
    }


# ===============================
# PARSE DVLA OPEN DATA
# ===============================
def parse_dvla_response(data):

    return {
        "mot_summary": {
            "fail_count": 0,
            "advisory_count": 0,
            "mot_penalty": 0,
            "mot_status": data.get("motStatus"),
            "mot_expiry_date": data.get("motExpiryDate"),
        },
        "mot_full_data": [],
        "vehicle_data": {
            "make": data.get("make"),
            "model": data.get("model"),
            "colour": data.get("colour"),
            "fuel_type": data.get("fuelType"),
            "engine_capacity": data.get("engineCapacity"),
            "year_of_manufacture": data.get("yearOfManufacture"),
            "tax_status": data.get("taxStatus"),
            "tax_due_date": data.get("taxDueDate"),
            "first_registration": data.get("monthOfFirstRegistration"),
        }
    }


# ===============================
# SAFE EMPTY STRUCTURE
# ===============================
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