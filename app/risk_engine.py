def risk_score(description: str) -> int:
    description = description.lower()

    risk_words = ["write off", "cat s", "cat n", "needs repair", "spares", "fault"]

    penalty = 0

    for word in risk_words:
        if word in description:
            penalty += 500

    return penalty
