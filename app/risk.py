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

def description_risk(description: str):

    if not description:
        return 0

    description = description.lower()
    penalty = 0

    for word in WRITE_OFF_KEYWORDS:
        if word in description:
            penalty += 800

    for word, value in MINOR_RISK_WORDS.items():
        if word in description:
            penalty += value

    return penalty