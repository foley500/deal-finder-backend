WRITE_OFF_KEYWORDS = [
    "cat s",
    "cat n",
    "category s",
    "category n",
    "write off",
    "insurance payout",
    "damaged",
    "spares or repair"
]

MINOR_RISK_WORDS = {
    "misfire": 400,
    "engine light": 300,
    "needs service": 200
}

MAX_RISK_PERCENTAGE = 0.4  # 40% of listing price hard cap


def description_risk(description: str, listing_price: float = 0):

    if not description:
        return 0

    description = description.lower()
    penalty = 0

    # Write-off detection (only apply once)
    for word in WRITE_OFF_KEYWORDS:
        if word in description:
            penalty += 1000
            break  # stop stacking write-off terms

    # Minor risks
    for word, value in MINOR_RISK_WORDS.items():
        if word in description:
            penalty += value

    # Hard cap based on listing price
    if listing_price:
        max_allowed = listing_price * MAX_RISK_PERCENTAGE
        penalty = min(penalty, max_allowed)

    return round(penalty, 2)