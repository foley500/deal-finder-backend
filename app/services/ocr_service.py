import io
import re
import cv2
import numpy as np
import requests
from PIL import Image
from ultralytics import YOLO
import easyocr


# Load once
model = YOLO("app/services/license_plate_detector.pt")
reader = easyocr.Reader(["en"], gpu=False)


def normalise_uk_plate(raw_plate: str) -> str:
    plate = raw_plate.upper().replace(" ", "")
    plate = re.sub(r"[^A-Z0-9]", "", plate)
    return plate


def is_valid_uk_plate(plate: str) -> bool:
    return bool(re.match(r"^[A-Z]{2}[0-9]{2}[A-Z]{3}$", plate))


def extract_plate_from_image_url(image_url: str):

    try:
        response = requests.get(image_url, timeout=10)
        if response.status_code != 200:
            return None

        image = Image.open(io.BytesIO(response.content)).convert("RGB")
        img_np = np.array(image)

        # Detect plate region
        results = model(img_np)

        if not results or len(results[0].boxes) == 0:
            print("❌ No plate detected")
            return None

        for box in results[0].boxes:
            x1, y1, x2, y2 = map(int, box.xyxy[0])
            plate_crop = img_np[y1:y2, x1:x2]

            # Improve OCR accuracy
            gray = cv2.cvtColor(plate_crop, cv2.COLOR_BGR2GRAY)
            _, thresh = cv2.threshold(gray, 150, 255, cv2.THRESH_BINARY)

            results = reader.readtext(thresh)

            for (_, text, _) in results:
                plate = normalise_uk_plate(text)
                print("🔍 OCR detected:", plate)

                if is_valid_uk_plate(plate):
                    print("✅ VALID UK PLATE:", plate)
                    return plate

        return None

    except Exception as e:
        print("OCR exception:", e)
        return None