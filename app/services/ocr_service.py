import requests
import re
import io
import numpy as np
import cv2
from PIL import Image
import easyocr

# Load once globally (important for Celery performance)
reader = easyocr.Reader(['en'], gpu=False)


def normalise_uk_plate(raw_plate: str) -> str:
    plate = raw_plate.upper()
    plate = re.sub(r"[^A-Z0-9]", "", plate)
    return plate


def is_valid_uk_plate(plate: str) -> bool:
    return bool(re.match(r"^[A-Z]{2}[0-9]{2}[A-Z]{3}$", plate))


def extract_plate_from_image_url(image_url: str):

    try:
        print("⬇️ Downloading image...")
        response = requests.get(image_url, timeout=10)

        if response.status_code != 200:
            print("❌ Image download failed")
            return None

        image = Image.open(io.BytesIO(response.content)).convert("RGB")
        img_np = np.array(image)

        print("🔎 Running EasyOCR...")
        results = reader.readtext(img_np)

        for (_, text, confidence) in results:

            cleaned = normalise_uk_plate(text)

            print("🔍 OCR Raw:", text, "| Cleaned:", cleaned)

            if is_valid_uk_plate(cleaned):
                print("✅ Valid UK Plate:", cleaned)
                return cleaned

        print("❌ No valid UK plate detected")
        return None

    except Exception as e:
        print("💥 OCR Exception:", str(e))
        return None