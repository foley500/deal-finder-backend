import io
import re
import requests
import easyocr
from PIL import Image

reader = easyocr.Reader(["en"])

UK_REGEX = r"[A-Z]{2}[0-9]{2}[A-Z]{3}"

def normalise_uk_plate(text: str) -> str:
    text = text.upper().replace(" ", "")
    corrections = {"O": "0", "I": "1"}
    return "".join(corrections.get(c, c) for c in text)


def extract_plate_from_image_url(image_url: str):

    try:
        response = requests.get(image_url, timeout=10)
        image = Image.open(io.BytesIO(response.content)).convert("RGB")

        results = reader.readtext(image, detail=0)

        combined = "".join(results).replace(" ", "").upper()

        match = re.search(UK_REGEX, combined)

        if match:
            plate = normalise_uk_plate(match.group(0))
            print("✅ OCR Detected:", plate)
            return plate

        print("❌ OCR no valid plate")
        return None

    except Exception as e:
        print("OCR error:", e)
        return None