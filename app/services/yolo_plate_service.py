import io
import re
import cv2
import numpy as np
from PIL import Image
from ultralytics import YOLO


# Load once at startup
model = YOLO("yolov8n.pt")  # Replace with your trained plate model if you have one


def normalise_uk_plate(raw_plate: str) -> str:
    plate = raw_plate.upper()
    corrections = {"0": "O", "1": "I", "5": "S", "8": "B"}
    return "".join(corrections.get(c, c) for c in plate)


def is_valid_uk_plate(plate: str) -> bool:
    return bool(re.match(r"^[A-Z]{2}[0-9]{2}[A-Z]{3}$", plate))


def detect_plate_with_yolo(image_bytes: bytes):

    try:
        image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        img_np = np.array(image)

        results = model(img_np)

        if not results or len(results[0].boxes) == 0:
            print("❌ YOLO: No detections")
            return None

        for box in results[0].boxes:
            x1, y1, x2, y2 = map(int, box.xyxy[0])

            plate_crop = img_np[y1:y2, x1:x2]

            # Convert to grayscale
            gray = cv2.cvtColor(plate_crop, cv2.COLOR_BGR2GRAY)

            # Simple threshold
            _, thresh = cv2.threshold(gray, 150, 255, cv2.THRESH_BINARY)

            # OCR via pytesseract
            import pytesseract
            raw_text = pytesseract.image_to_string(
                thresh,
                config="--psm 7 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
            ).strip()

            raw_text = raw_text.replace(" ", "").replace("\n", "")
            normalised = normalise_uk_plate(raw_text)

            print("🔍 YOLO OCR Raw:", raw_text, "| Normalised:", normalised)

            if is_valid_uk_plate(normalised):
                print("✅ YOLO VALID PLATE:", normalised)
                return normalised

        print("❌ YOLO: No valid UK plate found")
        return None

    except Exception as e:
        print("❌ YOLO exception:", e)
        return None