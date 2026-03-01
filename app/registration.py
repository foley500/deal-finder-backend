# app/registration.py

import re

UK_REG_PATTERN = r"\b([A-Z]{2}[0-9]{2}[A-Z]{3})\b|\b([A-Z]{2}[0-9]{2}\s[A-Z]{3})\b"

def extract_registration(title: str) -> str:
    if not title:
        return ""

    title = title.upper()

    match = re.search(UK_REG_PATTERN, title)

    if not match:
        return ""

    reg = match.group().replace(" ", "")
    return reg