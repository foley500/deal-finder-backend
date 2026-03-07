def calculate_true_profit(
    market_value,
    asking_price,
    transport=150,
    prep=400,
    warranty=300,
    risk_penalty=0
):
    # Scale prep and warranty to vehicle value
    # Cheap cars need less prep budget and simpler warranties
    if asking_price < 2000:
        prep = 200
        warranty = 150
        transport = 100
    elif asking_price < 4000:
        prep = 300
        warranty = 200
        transport = 150
    elif asking_price < 8000:
        prep = 400
        warranty = 250
        transport = 150
    else:
        prep = 500
        warranty = 300
        transport = 200

    total_costs = asking_price + transport + prep + warranty + risk_penalty
    return market_value - total_costs

def calculate_score(profit, risk_penalty, mileage):
    score = 0

    # Profit weighting
    if profit > 2000:
        score += 30
    elif profit > 1000:
        score += 20
    elif profit > 500:
        score += 10
    elif profit > 0:
        score += 5  # at least positive — worth knowing about

    # Risk penalty — soften the divisor slightly
    score -= risk_penalty / 150

    # Mileage penalty
    if mileage > 120000:
        score -= 15
    elif mileage > 80000:
        score -= 5

    return round(score, 2)
