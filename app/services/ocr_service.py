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
    """
    Fix common UK OCR mistakes based on position.
    UK format: AA00AAA
    """
    if len(plate) != 7:
        return plate

    plate = list(plate)

    # Numbers positions
    for i in [2, 3]:
        if plate[i] == "O":
            plate[i] = "0"
        if plate[i] == "I":
            plate[i] = "1"
        if plate[i] == "Z":
            plate[i] = "2"

    # Letter positions
    for i in [0, 1, 4, 5, 6]:
        if plate[i] == "0":
            plate[i] = "O"
        if plate[i] == "1":
            plate[i] = "I"
        if plate[i] == "2":
            plate[i] = "Z"

    return "".join(plate)


def is_valid_uk_plate(plate: str) -> bool:
    return bool(re.match(r"^[A-Z]{2}[0-9]{2}[A-Z]{3}$", plate))


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

                        if len(plate) == 8:
                            plate = plate[:7]

                        if len(plate) != 7:
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
            continue

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