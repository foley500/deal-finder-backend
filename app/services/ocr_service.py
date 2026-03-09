import io
import re
import cv2
import numpy as np
import requests
import base64
from PIL import Image
from ultralytics import YOLO
import easyocr
import gc 

gc.collect()


# ===============================
# LOAD MODELS ONCE
# ===============================
model = YOLO("app/services/license_plate_detector.pt")
reader = easyocr.Reader(["en"], gpu=False)


# ===============================
# CONFIG (TUNED FOR FULL RES EBAY IMAGES)
# ===============================
MIN_YOLO_CONFIDENCE = 0.35
MIN_OCR_CONFIDENCE = 0.21
MAX_IMAGES_PER_LISTING = 5
BOX_PADDING_RATIO = 0.15


BANNED_PLATE_STRINGS = {
    "DEALER", "DEALERS", "REVIEWS", "REVIEW", "FORECOURT",
    "APPROVED", "FINANCE", "WARRANTY", "AUTOTRADER", "MOTORS", 
    "CARGURUS", "VEHICLE", "QUALITY", "CONTACT", "WEBSITE",
    "FACEBOOK", "INSTAGRAM", "CERTIFIED", "RESERVE", "AUCTION",
}

def is_banned_plate(plate: str) -> bool:
    if plate in BANNED_PLATE_STRINGS:
        return True
    if plate.isalpha() and len(plate) >= 6:
        return True
    return False

# ===============================
# UK PLATE HELPERS
# ===============================
def normalise_uk_plate(raw_plate: str) -> str:
    plate = raw_plate.upper().replace(" ", "")
    plate = re.sub(r"[^A-Z0-9]", "", plate)
    return plate


def correct_common_ocr_errors(plate: str) -> str:
    if len(plate) == 7:
        plate = list(plate)
        for i in [2, 3]:
            plate[i] = plate[i].replace("O","0").replace("I","1").replace("Z","2")
        for i in [0, 1, 4, 5, 6]:
            plate[i] = plate[i].replace("0","O").replace("1","I").replace("2","Z")
        return "".join(plate)

    elif len(plate) >= 5:
        plate = list(plate)
        corrected = []
        for i, c in enumerate(plate):
            prev_digit = plate[i-1].isdigit() if i > 0 else False
            next_digit = plate[i+1].isdigit() if i < len(plate)-1 else False
            if c == "O" and (prev_digit or next_digit):
                corrected.append("0")
            elif c == "0" and not (prev_digit or next_digit):
                corrected.append("O")
            else:
                corrected.append(c)
        return "".join(corrected)

    return plate

def is_valid_uk_plate(plate: str) -> bool:
    patterns = [
        r"^[A-Z]{2}[0-9]{2}[A-Z]{3}$",
        r"^[A-Z][0-9]{1,3}[A-Z]{3}$",
        r"^[A-Z]{3}[0-9]{1,3}[A-Z]$",
        r"^[0-9]{1,4}[A-Z]{1,3}$",
        r"^[A-Z]{1,3}[0-9]{1,4}$",
    ]
    return any(re.match(p, plate) for p in patterns)


# ===============================
# IMAGE HELPERS
# ===============================
def expand_box(x1, y1, x2, y2, img_shape):
    h, w = img_shape[:2]
    pad_x = int((x2 - x1) * BOX_PADDING_RATIO)
    pad_y = int((y2 - y1) * BOX_PADDING_RATIO)
    x1 = max(0, x1 - pad_x)
    y1 = max(0, y1 - pad_y)
    x2 = min(w, x2 + pad_x)
    y2 = min(h, y2 + pad_y)
    return x1, y1, x2, y2


def preprocess_variants(plate_crop):
    variants = []
    gray = cv2.cvtColor(plate_crop, cv2.COLOR_BGR2GRAY)
    variants.append(gray)
    adaptive = cv2.adaptiveThreshold(
        gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2
    )
    variants.append(adaptive)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    contrast = clahe.apply(gray)
    variants.append(contrast)
    upscaled = cv2.resize(gray, None, fx=1.5, fy=1.5, interpolation=cv2.INTER_CUBIC)
    variants.append(upscaled)
    return variants


# ===============================
# CORE OCR — works on a PIL Image
# ===============================
def _run_ocr_on_image(image: Image.Image) -> str | None:
    """
    Runs YOLO + EasyOCR on a PIL Image and returns the first valid UK plate found.
    Shared by both URL-based and base64-based entry points.
    """
    img_np = None
    results = None

    try:
        image.thumbnail((1280, 1280))
        img_np = np.array(image)
        results = model(img_np, imgsz=640)

        if not results or len(results[0].boxes) == 0:
            return None

        boxes = sorted(
            results[0].boxes,
            key=lambda b: float(b.conf[0]),
            reverse=True
        )

        for box in boxes:
            conf = float(box.conf[0])
            if conf < MIN_YOLO_CONFIDENCE:
                continue

            x1, y1, x2, y2 = map(int, box.xyxy[0])
            x1, y1, x2, y2 = expand_box(x1, y1, x2, y2, img_np.shape)

            plate_crop = img_np[y1:y2, x1:x2]
            if plate_crop.size == 0:
                continue

            h, w = plate_crop.shape[:2]
            aspect_ratio = w / float(h)
            if aspect_ratio < 1.5 or aspect_ratio > 7.5:
                continue

            variants = preprocess_variants(plate_crop)

            for variant in variants:
                ocr_results = reader.readtext(variant)

                for (_, text, ocr_conf) in ocr_results:
                    if ocr_conf < MIN_OCR_CONFIDENCE:
                        continue

                    plate = normalise_uk_plate(text)
                    if len(plate) < 6 or len(plate) > 8:
                        continue

                    plate = correct_common_ocr_errors(plate)
                    if len(plate) < 5 or len(plate) > 8:
                        continue

                    if is_banned_plate(plate):
                        print(f"🚫 Banned plate string rejected: {plate}")
                        continue

                    print("🔍 OCR detected:", plate, "conf:", ocr_conf)

                    if is_valid_uk_plate(plate):
                        print("✅ VALID UK PLATE:", plate)
                        return plate

    except Exception as e:
        print("OCR exception:", e)

    finally:
        if img_np is not None:
            del img_np
        if results is not None:
            del results
        gc.collect()

    return None


# ===============================
# ENTRY POINT 1 — list of URLs (eBay engine)
# ===============================
def extract_plate_from_images(image_urls: list[str]):
    print("🔎 Starting OCR for listing with", len(image_urls), "images")

    if not image_urls:
        return None

    for image_url in image_urls[:MAX_IMAGES_PER_LISTING]:
        image = None
        try:
            response = requests.get(image_url, timeout=10)
            if response.status_code != 200:
                continue

            image = Image.open(io.BytesIO(response.content)).convert("RGB")
            result = _run_ocr_on_image(image)
            if result:
                return result

        except Exception as e:
            print("OCR URL exception:", e)
        finally:
            if image is not None:
                del image
            gc.collect()

    print("❌ No valid plate found across listing images")
    return None


# ===============================
# ENTRY POINT 2 — base64 image (Facebook extension)
# ===============================
def extract_plate_from_base64(image_base64: str) -> str | None:
    """
    Accepts a base64 data URI (e.g. "data:image/jpeg;base64,/9j/...")
    and runs the same YOLO + EasyOCR pipeline.
    """
    image = None
    try:
        if "," in image_base64:
            _, encoded = image_base64.split(",", 1)
        else:
            encoded = image_base64

        image_bytes = base64.b64decode(encoded)
        image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        return _run_ocr_on_image(image)

    except Exception as e:
        print("OCR base64 exception:", e)
        return None
    finally:
        if image is not None:
            del image
        gc.collect()


# ===============================
# TEXT FALLBACK
# ===============================
def extract_plate_from_text(text: str):
    if not text:
        return None
    text = text.upper()
    matches = re.findall(r"[A-Z]{2}[0-9]{2}[A-Z]{3}", text)
    if matches:
        return matches[0]
    return None