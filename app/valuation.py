# app/valuation.py

def get_market_value_from_reg(reg: str, mileage: int = 0) -> float:
    """
    Mock CAP-style valuation.
    Deterministic (no randomness).
    """

    if not reg:
        return 0

    reg = reg.upper().strip()

    # Extract year digits (UK style: AB16CDE -> 16)
    year_digits = "".join([c for c in reg if c.isdigit()])

    if year_digits:
        year = int(year_digits[:2])

        # Convert to proper year
        if year <= 30:
            year_full = 2000 + year
        else:
            year_full = 1900 + year

        age = 2024 - year_full
    else:
        age = 10  # default fallback

    # Base starting value
    base_value = 14000

    # Age depreciation (900 per year)
    value = base_value - (age * 900)

    # Mileage adjustment
    if mileage > 120000:
        value -= 1500
    elif mileage > 80000:
        value -= 800
    elif mileage > 60000:
        value -= 400

    # Floor value
    if value < 1000:
        value = 1000

    return value