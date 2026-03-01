import requests
import os
import time
import re

PLATE_API_KEY = os.getenv("PLATE_API_KEY")
PLATE_API_URL = "https://api.platerecognizer.com/v1/plate-reader/"


def extract_plate_from_image_url(image_url: str):

    try:
        image_response = requests.get(image_url, timeout=10)
        if image_response.status_code != 200:
            return None

        response = requests.post(
            PLATE_API_URL,
            headers={
                "Authorization": f"Token {PLATE_API_KEY}"
            },
            data={
                "regions": "gb",
                "recognize_vehicle": 1
            },
            files={
                "upload": ("image.jpg", image_response.content)
            },
            timeout=20
        )

        if response.status_code == 429:
            time.sleep(1)
            return None

        if response.status_code != 200:
            return None

        data = response.json()

        if "results" in data and len(data["results"]) > 0:

            plate = data["results"][0]["plate"].upper()

            # Validate UK format
            if re.match(r"^[A-Z]{2}[0-9]{2}[A-Z]{3}$", plate):
                return plate

        return None

    except Exception:
        return None