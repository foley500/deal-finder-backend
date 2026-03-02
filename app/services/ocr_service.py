import requests
import os
import time
import re

PLATE_API_KEY = os.getenv("PLATE_API_KEY")
PLATE_API_URL = "https://api.platerecognizer.com/v1/plate-reader/"


def extract_plate_from_image_url(image_url: str):

    if not PLATE_API_KEY:
        print("❌ PLATE_API_KEY is missing!")
        return None

    try:
        print("⬇️ Downloading image...")
        image_response = requests.get(image_url, timeout=10)

        if image_response.status_code != 200:
            print("❌ Failed to download image:", image_response.status_code)
            return None

        print("📡 Sending to Plate Recognizer API...")

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

        print("🔎 API Status Code:", response.status_code)

        if response.status_code == 429:
            print("⚠️ Rate limited")
            time.sleep(1)
            return None

        if response.status_code != 200:
            print("❌ API Error Response:", response.text)
            return None

        data = response.json()
        print("📄 API Response:", data)

        if "results" in data and len(data["results"]) > 0:

            plate = data["results"][0]["plate"].upper()
            print("🔤 Raw plate from API:", plate)

            # Normalize spaces
            plate = plate.replace(" ", "")

            if re.match(r"^[A-Z]{2}[0-9]{2}[A-Z]{3}$", plate):
                print("✅ Valid UK plate detected:", plate)
                return plate
            else:
                print("⚠️ Plate format invalid:", plate)

        else:
            print("⚠️ No results returned from API")

        return None

    except Exception as e:
        print("💥 OCR Exception:", str(e))
        return None