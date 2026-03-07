import io
import re
import cv2
import numpy as np
import requests
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
MIN_YOLO_CONFIDENCE = 0.35      # lowered from 0.45
MIN_OCR_CONFIDENCE = 0.21       # lowered from 0.35
MAX_IMAGES_PER_LISTING = 5
BOX_PADDING_RATIO = 0.15        # slightly increased padding


# ===============================
# UK PLATE HELPERS
# ===============================
def normalise_uk_plate(raw_plate: str) -> str:
    plate = raw_plate.upper().replace(" ", "")
    plate = re.sub(r"[^A-Z0-9]", "", plate)
    return plate


def correct_common_ocr_errors(plate: str) -> str:
    if len(plate) == 7:
        # Current format AA00AAA
        plate = list(plate)
        for i in [2, 3]:  # number positions
            plate[i] = plate[i].replace("O","0").replace("I","1").replace("Z","2")
        for i in [0, 1, 4, 5, 6]:  # letter positions
            plate[i] = plate[i].replace("0","O").replace("1","I").replace("2","Z")
        return "".join(plate)

    elif len(plate) >= 5:
        # Older formats — just fix obvious O/0 and I/1 swaps
        plate = list(plate)
        corrected = []
        for i, c in enumerate(plate):
            # If surrounded by letters, likely a letter
            # If surrounded by digits, likely a digit — simple heuristic
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
        r"^[A-Z]{2}[0-9]{2}[A-Z]{3}$",   # Current: AB12CDE (2001+)
        r"^[A-Z][0-9]{1,3}[A-Z]{3}$",     # Prefix: A123BCD (1983-2001)
        r"^[A-Z]{3}[0-9]{1,3}[A-Z]$",     # Suffix: ABC123D (1963-1983)
        r"^[0-9]{1,4}[A-Z]{1,3}$",        # Dateless short
        r"^[A-Z]{1,3}[0-9]{1,4}$",        # Dateless long
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

    # Raw
    variants.append(gray)

    # Adaptive threshold
    adaptive = cv2.adaptiveThreshold(
        gray,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        11,
        2
    )
    variants.append(adaptive)

    # Contrast boost (CLAHE)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    contrast = clahe.apply(gray)
    variants.append(contrast)

    # Upscaled
    upscaled = cv2.resize(gray, None, fx=1.5, fy=1.5, interpolation=cv2.INTER_CUBIC)
    variants.append(upscaled)

    return variants


# ===============================
# MAIN ENTRY
# ===============================

def extract_plate_from_images(image_urls: list[str]):
    print("🔎 Starting OCR for listing with", len(image_urls), "images")

    if not image_urls:
        return None

    for image_url in image_urls[:MAX_IMAGES_PER_LISTING]:

        img_np = None
        image = None
        results = None

        try:
            response = requests.get(image_url, timeout=10)
            if response.status_code != 200:
                continue

            image = Image.open(io.BytesIO(response.content)).convert("RGB")

            image.thumbnail((1280, 1280))

            img_np = np.array(image)

            results = model(img_np, imgsz=640)

            if not results or len(results[0].boxes) == 0:
                continue

            # 🔥 SORT boxes by highest confidence first
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

                # 🔥 Aspect ratio filter (UK plates are wide)
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

                        print("🔍 OCR detected:", plate, "conf:", ocr_conf)

                        if is_valid_uk_plate(plate):
                            print("✅ VALID UK PLATE:", plate)
                            return plate

        except Exception as e:
            print("OCR exception:", e)

        finally:
            if img_np is not None:
                del img_np
            if image is not None:
                del image
            if results is not None:
                del results
            gc.collect()

    print("❌ No valid plate found across listing images")
    return None

def extract_plate_from_text(text: str):
    if not text:
        return None

    text = text.upper()
    matches = re.findall(r"[A-Z]{2}[0-9]{2}[A-Z]{3}", text)

    if matches:
        return matches[0]

    return None