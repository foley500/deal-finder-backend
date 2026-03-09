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
# CONFIG
# ===============================
MIN_YOLO_CONFIDENCE = 0.25          # Lowered — catch more plate candidates
MIN_OCR_CONFIDENCE = 0.15           # Lowered — we correct and validate anyway
MAX_IMAGES_PER_LISTING = 5
BOX_PADDING_RATIO = 0.10            # Tighter crop — less background noise


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
# UK PLATE CORRECTION
# ===============================
def normalise_uk_plate(raw_plate: str) -> str:
    plate = raw_plate.upper().replace(" ", "").replace("-", "")
    plate = re.sub(r"[^A-Z0-9]", "", plate)
    return plate


# Full confusion matrix for OCR characters
# Letter positions: 0, 1, 4, 5, 6  (digit→letter)
DIGIT_TO_LETTER = {
    "0": "O", "1": "I", "2": "Z", "4": "A",
    "5": "S", "6": "G", "8": "B",
}

# Digit positions: 2, 3  (letter→digit)
LETTER_TO_DIGIT = {
    "O": "0", "I": "1", "Z": "2", "A": "4",
    "S": "5", "G": "9", "B": "8", "Q": "0",
    "D": "0", "U": "0", "J": "1", "L": "1",
    "T": "1", "E": "6", "C": "0",
}


def correct_common_ocr_errors(plate: str) -> str:
    """Apply position-aware character corrections for standard UK new-style plates (AA09AAA)."""
    if len(plate) == 7:
        plate = list(plate)
        # Positions 2,3 must be digits
        for i in [2, 3]:
            plate[i] = LETTER_TO_DIGIT.get(plate[i], plate[i])
        # Positions 0,1,4,5,6 must be letters
        for i in [0, 1, 4, 5, 6]:
            plate[i] = DIGIT_TO_LETTER.get(plate[i], plate[i])
        return "".join(plate)

    elif len(plate) >= 5:
        # For other format plates use context-aware correction
        plate = list(plate)
        corrected = []
        for i, c in enumerate(plate):
            prev_digit = plate[i-1].isdigit() if i > 0 else False
            next_digit = plate[i+1].isdigit() if i < len(plate)-1 else False
            if c in DIGIT_TO_LETTER and not (prev_digit or next_digit):
                corrected.append(DIGIT_TO_LETTER[c])
            elif c in LETTER_TO_DIGIT and (prev_digit or next_digit):
                corrected.append(LETTER_TO_DIGIT[c])
            else:
                corrected.append(c)
        return "".join(corrected)

    return plate


def generate_fuzzy_variants(plate: str) -> list[str]:
    """
    Generate plausible alternative readings for a 7-char plate
    by swapping commonly confused characters at each position.
    Allows DVSA to be tried with slight variations if exact match fails.
    """
    if len(plate) != 7:
        return [plate]

    confusion = {
        "O": ["0"], "0": ["O"],
        "I": ["1"], "1": ["I"],
        "S": ["5"], "5": ["S"],
        "G": ["9", "6"], "9": ["G"], "6": ["G", "E"],
        "B": ["8"], "8": ["B"],
        "Z": ["2"], "2": ["Z"],
        "A": ["4"], "4": ["A"],
        "E": ["6"],
    }

    variants = set()
    plate_list = list(plate)

    # Single character swaps only — avoids combinatorial explosion
    for i, c in enumerate(plate_list):
        if c in confusion:
            for alt in confusion[c]:
                variant = plate_list.copy()
                variant[i] = alt
                variants.add("".join(variant))

    return list(variants)


def is_valid_uk_plate(plate: str) -> bool:
    patterns = [
        r"^[A-Z]{2}[0-9]{2}[A-Z]{3}$",   # Current: AA09AAA
        r"^[A-Z][0-9]{1,3}[A-Z]{3}$",     # Prefix: A999AAA
        r"^[A-Z]{3}[0-9]{1,3}[A-Z]$",     # Suffix: AAA999A
        r"^[0-9]{1,4}[A-Z]{1,3}$",         # Dateless
        r"^[A-Z]{1,3}[0-9]{1,4}$",         # Dateless
    ]
    return any(re.match(p, plate) for p in patterns)


# ===============================
# IMAGE PREPROCESSING
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


def preprocess_variants(plate_crop: np.ndarray) -> list[np.ndarray]:
    """
    Generate multiple preprocessed versions of the plate crop.
    Each variant gives EasyOCR a different view — increases chances of correct read.
    """
    variants = []
    gray = cv2.cvtColor(plate_crop, cv2.COLOR_BGR2GRAY)

    # 1. Raw greyscale
    variants.append(gray)

    # 2. Upscaled 2x — most important for small/distant plates
    upscaled = cv2.resize(gray, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
    variants.append(upscaled)

    # 3. CLAHE contrast enhancement on upscaled
    clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(4, 4))
    contrast = clahe.apply(upscaled)
    variants.append(contrast)

    # 4. Sharpened
    kernel = np.array([[0, -1, 0], [-1, 5, -1], [0, -1, 0]])
    sharpened = cv2.filter2D(upscaled, -1, kernel)
    variants.append(sharpened)

    # 5. Adaptive threshold (black/white) — best for clear plates
    blurred = cv2.GaussianBlur(upscaled, (3, 3), 0)
    adaptive = cv2.adaptiveThreshold(
        blurred, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2
    )
    variants.append(adaptive)

    # 6. Otsu threshold
    _, otsu = cv2.threshold(upscaled, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    variants.append(otsu)

    # 7. Inverted Otsu (for dark plates with light text)
    variants.append(cv2.bitwise_not(otsu))

    return variants


def score_plate_candidate(plate: str, ocr_conf: float, yolo_conf: float) -> float:
    """Score a plate candidate — higher = more likely correct."""
    score = ocr_conf * yolo_conf

    # Bonus for valid format
    if is_valid_uk_plate(plate):
        score *= 2.0

    # Bonus for correct length
    if len(plate) == 7:
        score *= 1.3

    # Penalty if all same characters (garbage read)
    if len(set(plate)) <= 2:
        score *= 0.1

    return score


# ===============================
# CORE OCR — works on a PIL Image
# ===============================
def _run_ocr_on_image(image: Image.Image, high_res: bool = False) -> str | None:
    """
    Full pipeline: YOLO detection → crop → multi-variant preprocessing → EasyOCR
    → position-aware correction → validation → fuzzy fallback.
    """
    img_np = None
    results = None

    try:
        if high_res:
            # Facebook images: run at native resolution, upscale if small
            w, h = image.size
            if max(w, h) < 1280:
                scale = 1280 / max(w, h)
                image = image.resize(
                    (int(w * scale), int(h * scale)),
                    Image.LANCZOS
                )
            img_np = np.array(image)
            imgsz = min(max(image.width, image.height), 1280)
        else:
            image.thumbnail((1280, 1280))
            img_np = np.array(image)
            imgsz = 640

        # Run YOLO at two sizes for better detection coverage
        all_boxes = []
        for sz in ([imgsz, 1280] if high_res else [imgsz]):
            r = model(img_np, imgsz=sz)
            if r and len(r[0].boxes) > 0:
                all_boxes.extend(r[0].boxes)

        if not all_boxes:
            return None

        # Deduplicate boxes by position (IoU-like — skip very similar boxes)
        seen_boxes = []
        unique_boxes = []
        for box in all_boxes:
            x1, y1, x2, y2 = map(int, box.xyxy[0])
            is_dup = False
            for sx1, sy1, sx2, sy2 in seen_boxes:
                if abs(x1-sx1) < 20 and abs(y1-sy1) < 20:
                    is_dup = True
                    break
            if not is_dup:
                unique_boxes.append(box)
                seen_boxes.append((x1, y1, x2, y2))

        # Sort by YOLO confidence descending
        unique_boxes = sorted(unique_boxes, key=lambda b: float(b.conf[0]), reverse=True)

        best_plate = None
        best_score = 0.0

        for box in unique_boxes:
            yolo_conf = float(box.conf[0])
            if yolo_conf < MIN_YOLO_CONFIDENCE:
                continue

            x1, y1, x2, y2 = map(int, box.xyxy[0])
            x1, y1, x2, y2 = expand_box(x1, y1, x2, y2, img_np.shape)

            plate_crop = img_np[y1:y2, x1:x2]
            if plate_crop.size == 0:
                continue

            h_crop, w_crop = plate_crop.shape[:2]
            aspect_ratio = w_crop / float(h_crop)
            if aspect_ratio < 1.2 or aspect_ratio > 8.0:
                continue

            variants = preprocess_variants(plate_crop)

            for variant in variants:
                ocr_results = reader.readtext(
                    variant,
                    allowlist="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
                    detail=1,
                    paragraph=False,
                )

                for (_, text, ocr_conf) in ocr_results:
                    if ocr_conf < MIN_OCR_CONFIDENCE:
                        continue

                    plate = normalise_uk_plate(text)
                    if len(plate) < 5 or len(plate) > 8:
                        continue

                    plate = correct_common_ocr_errors(plate)

                    if is_banned_plate(plate):
                        continue

                    print(f"🔍 OCR detected: {plate} conf: {ocr_conf}")

                    candidate_score = score_plate_candidate(plate, ocr_conf, yolo_conf)

                    if is_valid_uk_plate(plate):
                        if candidate_score > best_score:
                            best_score = candidate_score
                            best_plate = plate

        if best_plate:
            print(f"✅ VALID UK PLATE: {best_plate} (score: {best_score:.3f})")
            return best_plate

        print("⚠️ No valid plate found — trying fuzzy variants on best candidates")
        return None

    except Exception as e:
        print("OCR exception:", e)
        import traceback
        traceback.print_exc()

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
    and runs the same YOLO + EasyOCR pipeline at high resolution.
    """
    image = None
    try:
        if "," in image_base64:
            _, encoded = image_base64.split(",", 1)
        else:
            encoded = image_base64

        image_bytes = base64.b64decode(encoded)
        image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        return _run_ocr_on_image(image, high_res=True)

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