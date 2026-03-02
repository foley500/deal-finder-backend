import os
import requests


# =====================================================
# API KEYS
# =====================================================

DVSA_API_KEY = os.getenv("DVSA_API_KEY")      # Full MOT history
DVLA_API_KEY = os.getenv("DVLA_API_KEY")      # Basic vehicle + MOT status


DVSA_URL = "https://history.mot.api.gov.uk/v1/trade/vehicles/mot-tests"
DVLA_URL = "https://driver-vehicle-licensing.api.gov.uk/vehicle-enquiry/v1/vehicles"


# =====================================================
# MAIN ENTRY POINT
# =====================================================

def get_mot_data(registration: str):

    if not registration:
        return None

    registration = registration.strip().upper()

    # -------------------------------------------------
    # OPTION 1: Try DVSA (Full History)
    # -------------------------------------------------
    if DVSA_API_KEY:
        print("🔎 Attempting DVSA full MOT history...")
        dvsa_data = fetch_dvsa_data(registration)
        if dvsa_data:
            return dvsa_data

    # -------------------------------------------------
    # OPTION 2: Fallback to DVLA Open Data
    # -------------------------------------------------
    if DVLA_API_KEY:
        print("🔎 Using DVLA open data fallback...")
        return fetch_dvla_data(registration)

    print("❌ No MOT API keys configured")
    return None


# =====================================================
# DVSA FULL HISTORY
# =====================================================

def fetch_dvsa_data(registration: str):

    headers = {
        "x-api-key": DVSA_API_KEY,
        "Accept": "application/json"
    }

    params = {
        "registration": registration
    }

    try:
        response = requests.get(
            DVSA_URL,
            headers=headers,
            params=params,
            timeout=10
        )

        print("DVSA Status:", response.status_code)

        if response.status_code != 200:
            print("DVSA response error:", response.text)
            return None

        data = response.json()

        if not data:
            return None

        vehicle = data[0]
        return vehicle.get("motTests", [])

    except Exception as e:
        print("DVSA MOT service error:", e)
        return None


# =====================================================
# DVLA BASIC DATA (Fallback Mode)
# =====================================================

def fetch_dvla_data(registration: str):

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

        if response.status_code != 200:
            print("DVLA response error:", response.text)
            return None

        data = response.json()

        # Build synthetic MOT history structure
        synthetic_mot = [{
            "testDate": data.get("motExpiryDate"),
            "expiryDate": data.get("motExpiryDate"),
            "testResult": data.get("motStatus"),
            "odometerValue": None,
            "rfrAndComments": []
        }]

        return synthetic_mot

    except Exception as e:
        print("DVLA MOT fallback error:", e)
        return None