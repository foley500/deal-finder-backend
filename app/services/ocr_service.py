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
MIN_YOLO_CONFIDENCE = 0.45  
MIN_OCR_CONFIDENCE = 0.35
MAX_IMAGES_PER_LISTING = 2
BOX_PADDING_RATIO = 0.10
BOX_PADDING_RIGHT_EXTRA = 0.08     # Extra right-side padding for angled plates


BANNED_PLATE_STRINGS = {
    "DEALER", "DEALERS", "REVIEWS", "REVIEW", "FORECOURT",
    "APPROVED", "FINANCE", "WARRANTY", "AUTOTRADER", "MOTORS",
    "CARGURUS", "VEHICLE", "QUALITY", "CONTACT", "WEBSITE",
    "FACEBOOK", "INSTAGRAM", "CERTIFIED", "RESERVE", "AUCTION",
}

def is_banned_plate(plate: str) -> bool:
    # Substring match — catches "CARDEALER", "MYAUTO_DEALER", watermark-style strings
    if any(banned in plate for banned in BANNED_PLATE_STRINGS):
        return True
    # All-alpha plates 6+ chars are almost certainly not real registrations
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


DIGIT_TO_LETTER = {
    "0": "O", "1": "I", "2": "Z", "4": "A",
    "5": "S", "6": "G", "8": "B",
}

LETTER_TO_DIGIT = {
    # High-confidence swaps only — visually unambiguous
    "O": "0", "I": "1", "Z": "2", "A": "4",
    "S": "5", "G": "9", "B": "8", "Q": "0",
    # Removed: D, C, U, J, L, T, E — too ambiguous, causes trailing letter corruption
    # e.g. DWF → 0WF, LXU → 1XU on angled plates
}


def correct_common_ocr_errors(plate: str) -> str:
    if len(plate) == 7:
        plate = list(plate)
        for i in [2, 3]:
            plate[i] = LETTER_TO_DIGIT.get(plate[i], plate[i])
        for i in [0, 1, 4, 5, 6]:
            plate[i] = DIGIT_TO_LETTER.get(plate[i], plate[i])
        return "".join(plate)

    elif len(plate) >= 5:
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

    for i, c in enumerate(plate_list):
        if c in confusion:
            for alt in confusion[c]:
                variant = plate_list.copy()
                variant[i] = alt
                variants.add("".join(variant))

    return list(variants)


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
# IMAGE PREPROCESSING
# ===============================
def expand_box(x1, y1, x2, y2, img_shape):
    h, w = img_shape[:2]
    box_w = x2 - x1
    box_h = y2 - y1

    aspect = box_w / float(box_h)

    pad_x = int(box_w * 0.20)
    pad_y = int(box_h * 0.20)

    # angled plates tend to have lower aspect ratio
    if aspect < 3:
        pad_right = int(box_w * 0.35)
    else:
        pad_right = int(box_w * 0.20)

    x1 = max(0, x1 - pad_x)
    y1 = max(0, y1 - pad_y)
    x2 = min(w, x2 + pad_right)
    y2 = min(h, y2 + pad_y)

    return x1, y1, x2, y2


def deskew_plate(crop: np.ndarray) -> np.ndarray | None:
    """
    Perspective correction for angled plate crops.
    Finds largest 4-corner contour and warps it to a flat rectangle.
    Returns corrected BGR image or None if no suitable contour found.
    """
    try:
        gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
        blurred = cv2.GaussianBlur(gray, (5, 5), 0)
        edged = cv2.Canny(blurred, 30, 150)

        contours, _ = cv2.findContours(edged, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        if not contours:
            return None

        contours = sorted(contours, key=cv2.contourArea, reverse=True)
        crop_area = crop.shape[0] * crop.shape[1]
        plate_contour = None
        for c in contours[:5]:
            # Skip contours too small to be the plate boundary (< 10% of crop area)
            if cv2.contourArea(c) < crop_area * 0.10:
                continue
            peri = cv2.arcLength(c, True)
            approx = cv2.approxPolyDP(c, 0.02 * peri, True)
            if len(approx) == 4:
                plate_contour = approx
                break

        if plate_contour is None:
            return None

        pts = plate_contour.reshape(4, 2).astype(np.float32)
        rect = np.zeros((4, 2), dtype=np.float32)
        s = pts.sum(axis=1)
        rect[0] = pts[np.argmin(s)]    # top-left
        rect[2] = pts[np.argmax(s)]    # bottom-right
        diff = np.diff(pts, axis=1)
        rect[1] = pts[np.argmin(diff)] # top-right
        rect[3] = pts[np.argmax(diff)] # bottom-left

        h, w = crop.shape[:2]
        target_w = max(w, 400)
        target_h = int(target_w / 4.5)
        dst = np.array([
            [0, 0],
            [target_w - 1, 0],
            [target_w - 1, target_h - 1],
            [0, target_h - 1]
        ], dtype=np.float32)

        M = cv2.getPerspectiveTransform(rect, dst)
        warped = cv2.warpPerspective(crop, M, (target_w, target_h))
        return warped

    except Exception:
        return None


def preprocess_variants(plate_crop: np.ndarray) -> list[np.ndarray]:
    """
    Multiple preprocessed versions for EasyOCR.
    Variants 1-7: standard pipeline (works well for front-facing plates).
    Variants 8-11: deskewed versions for angled plates.
    """
    variants = []
    gray = cv2.cvtColor(plate_crop, cv2.COLOR_BGR2GRAY)
    clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(4, 4))

    # 1. Raw greyscale
    variants.append(gray)

    # 2. Upscaled 2x
    upscaled = cv2.resize(gray, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
    variants.append(upscaled)

    # 3. CLAHE on upscaled
    variants.append(clahe.apply(upscaled))

    # 4. Sharpened
    kernel = np.array([[0, -1, 0], [-1, 5, -1], [0, -1, 0]])
    variants.append(cv2.filter2D(upscaled, -1, kernel))

    # 5. Adaptive threshold
    blurred = cv2.GaussianBlur(upscaled, (3, 3), 0)
    variants.append(cv2.adaptiveThreshold(
        blurred, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2
    ))

    # 6. Otsu threshold
    _, otsu = cv2.threshold(upscaled, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    variants.append(otsu)

    # 7. Inverted Otsu
    variants.append(cv2.bitwise_not(otsu))

    # 8-11. Deskewed variants for angled plates
    deskewed = deskew_plate(plate_crop)
    if deskewed is not None:
        deskew_gray = cv2.cvtColor(deskewed, cv2.COLOR_BGR2GRAY)
        deskew_up = cv2.resize(deskew_gray, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
        variants.append(deskew_gray)
        variants.append(deskew_up)
        variants.append(clahe.apply(deskew_up))
        blurred_d = cv2.GaussianBlur(deskew_up, (3, 3), 0)
        variants.append(cv2.adaptiveThreshold(
            blurred_d, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2
        ))

    for base_img in [plate_crop, deskewed] if deskewed is not None else [plate_crop]:

        h, w = base_img.shape[:2]

        for angle in [-10, -5, 5, 10]:

            M = cv2.getRotationMatrix2D((w / 2, h / 2), angle, 1)

            rotated = cv2.warpAffine(base_img, M, (w, h))

            variants.append(cv2.cvtColor(rotated, cv2.COLOR_BGR2GRAY))

    return variants


def score_plate_candidate(plate: str, ocr_conf: float, yolo_conf: float) -> float:
    score = ocr_conf * yolo_conf
    if is_valid_uk_plate(plate):
        score *= 2.0
    if len(plate) == 7:
        score *= 1.3
    if len(set(plate)) <= 2:
        score *= 0.1
    return score


# ===============================
# CORE OCR — works on a PIL Image
# ===============================
def _run_ocr_on_image(image: Image.Image, high_res: bool = False) -> str | None:
    img_np = None
    results = None

    try:
        if high_res:
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

        all_boxes = []
        for sz in ([imgsz, 1280] if high_res else [imgsz]):
            r = model(img_np, imgsz=sz)
            if r and len(r[0].boxes) > 0:
                all_boxes.extend(r[0].boxes)

        if not all_boxes:
            return None

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

        unique_boxes = sorted(unique_boxes, key=lambda b: float(b.conf[0]), reverse=True)

        best_plate = None
        best_score = 0.0
        best_invalid_plate = None   # Best candidate that didn't pass format validation
        best_invalid_score = 0.0

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

            # Angled/mounted plates can have very low or very high aspect ratios.
            # Before discarding, try a perspective correction — if deskew brings
            # the ratio into a plausible range, use the corrected crop going forward.
            if aspect_ratio < 0.9 or aspect_ratio > 10.0:
                deskewed_early = deskew_plate(plate_crop)
                if deskewed_early is not None:
                    h_d, w_d = deskewed_early.shape[:2]
                    fixed_ratio = w_d / float(h_d)
                    if 0.9 <= fixed_ratio <= 10.0:
                        plate_crop = deskewed_early  # use corrected geometry
                    else:
                        continue  # too distorted even after deskew
                else:
                    continue

            variants = preprocess_variants(plate_crop)

            for variant in variants:

                variant = cv2.copyMakeBorder(
                    variant,
                    10, 10, 10, 10,
                    cv2.BORDER_CONSTANT,
                    value=255
                )

                ocr_results = reader.readtext(
                    variant,
                    allowlist="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
                    detail=1,
                    paragraph=False,
                    width_ths=0.7,
                    decoder="beamsearch"
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
                    else:
                        if candidate_score > best_invalid_score:
                            best_invalid_score = candidate_score
                            best_invalid_plate = plate

        if best_plate:
            print(f"✅ VALID UK PLATE: {best_plate} (score: {best_score:.3f})")
            return best_plate

        # Fuzzy fallback — try character swap variants on best near-miss
        if best_invalid_plate:
            print(f"⚠️ No valid plate — trying fuzzy variants on: {best_invalid_plate}")
            for variant_plate in generate_fuzzy_variants(best_invalid_plate):
                if is_valid_uk_plate(variant_plate):
                    print(f"✅ FUZZY MATCH: {variant_plate} (from {best_invalid_plate})")
                    return variant_plate

        print("⚠️ No valid plate found across all variants")
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
    # Current format (post Sep 2001): AB12CDE
    matches = re.findall(r"\b[A-Z]{2}[0-9]{2}[A-Z]{3}\b", text)
    if matches:
        return matches[0]
    # Pre-2001 suffix: A123BCD
    matches = re.findall(r"\b[A-Z][0-9]{1,3}[A-Z]{3}\b", text)
    if matches:
        return matches[0]
    # Pre-2001 prefix: ABC123D
    matches = re.findall(r"\b[A-Z]{3}[0-9]{1,3}[A-Z]\b", text)
    if matches:
        return matches[0]
    return None