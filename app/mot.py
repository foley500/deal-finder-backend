# app/mot.py

import requests
import os

DVSA_API_KEY = os.getenv("DVSA_API_KEY")

def get_mot_status(reg: str):

    if not reg or not DVSA_API_KEY:
        return {"mot_valid": None}

    url = f"https://driver-vehicle-licensing.api.gov.uk/vehicle-enquiry/v1/vehicles"

    headers = {
        "x-api-key": DVSA_API_KEY,
        "Content-Type": "application/json"
    }

    payload = {
        "registrationNumber": reg
    }

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code != 200:
        return {"mot_valid": None}

    data = response.json()

    return {
        "mot_status": data.get("motStatus"),
        "tax_status": data.get("taxStatus")
    }
