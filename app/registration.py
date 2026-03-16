# app/registration.py

import re

# Current format (post Sep 2001): AB12CDE or AB12 CDE
_CURRENT_PLATE = r"\b([A-Z]{2}[0-9]{2}\s?[A-Z]{3})\b"

# Pre-2001 suffix format: A123BCD
_SUFFIX_PLATE = r"\b([A-Z][0-9]{1,3}[A-Z]{3})\b"

# Pre-2001 prefix format: ABC123D or ABC 123 D
_PREFIX_PLATE = r"\b([A-Z]{3}[0-9]{1,3}[A-Z])\b"

UK_REG_PATTERN = f"{_CURRENT_PLATE}|{_SUFFIX_PLATE}|{_PREFIX_PLATE}"


def extract_registration(title: str) -> str:
    if not title:
        return ""

    title = title.upper()

    match = re.search(UK_REG_PATTERN, title)

    if not match:
        return ""

    reg = match.group().replace(" ", "")
    return reg