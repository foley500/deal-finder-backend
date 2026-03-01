import requests
import os
import time

PLATE_API_KEY = os.getenv("PLATE_API_KEY")

PLATE_API_URL = "https://api.platerecognizer.com/v1/plate-reader/"


def extract_plate_from_image_url(image_url: str):

    try:

        # Download image first (safer than streaming raw twice)
        image_response = requests.get(image_url, timeout=10)
        if image_response.status_code != 200:
            print("❌ Failed to download image")
            return None

        response = requests.post(
            PLATE_API_URL,
            headers={
                "Authorization": f"Token {PLATE_API_KEY}"
            },
            data={
                "regions": "gb",              # FIXED
                "recognize_vehicle": 1        # improves context accuracy
            },
            files={
                "upload": ("image.jpg", image_response.content)
            },
            timeout=20
        )

        # Handle rate limiting
        if response.status_code == 429:
            print("⚠️ Rate limited. Sleeping 1 second...")
            time.sleep(1)
            return None

        if response.status_code != 200:
            print("❌ Plate API error:", response.text)
            return None

        data = response.json()

        # Success case
        if "results" in data and len(data["results"]) > 0:

            plate = data["results"][0]["plate"].upper()

            print("✅ Plate detected:", plate)
            return plate

        print("ℹ️ No plate detected.")
        return None

    except Exception as e:
        print("❌ Plate API exception:", e)
        return None