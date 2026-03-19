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

        # Try each OCR token individually first — plates often appear as a single
        # token and this avoids false matches from adjacent text being concatenated.
        for token in results:
            cleaned = token.upper().replace(" ", "").replace("-", "")
            match = re.search(UK_REGEX, cleaned)
            if match:
                plate = normalise_uk_plate(match.group(0))
                print("✅ OCR Detected (single token):", plate)
                return plate

        # Fallback: join all tokens and search — catches plates split across OCR results
        combined = "".join(results).replace(" ", "").upper().replace("-", "")
        match = re.search(UK_REGEX, combined)
        if match:
            plate = normalise_uk_plate(match.group(0))
            print("✅ OCR Detected (combined):", plate)
            return plate

        print("❌ OCR no valid plate")
        return None

    except Exception as e:
        print("OCR error:", e)
        return None