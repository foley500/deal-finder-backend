def calculate_score(profit, risk_penalty, mileage):

    score = 0

    # Profit weighting
    if profit > 2000:
        score += 30
    elif profit > 1000:
        score += 20
    elif profit > 500:
        score += 10

    # Risk penalty
    score -= risk_penalty / 100

    # Mileage penalty
    if mileage > 120000:
        score -= 15
    elif mileage > 80000:
        score -= 5

    return score