import os
import requests


DVSA_API_KEY = os.getenv("DVSA_API_KEY")

DVSA_URL = "https://history.mot.api.gov.uk/v1/trade/vehicles/mot-tests"


def get_mot_data(registration: str):

    if not registration:
        return None

    if not DVSA_API_KEY:
        print("DVSA API key missing")
        return None

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

        return response.json()

    except Exception as e:
        print("MOT service error:", e)
        return None