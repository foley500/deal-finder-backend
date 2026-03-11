def calculate_costs(asking_price):
    """Returns estimated costs breakdown based on vehicle price."""
    if asking_price < 2000:
        transport = 100
        prep = 200
        warranty = 150
    elif asking_price < 4000:
        transport = 150
        prep = 300
        warranty = 200
    elif asking_price < 8000:
        transport = 150
        prep = 400
        warranty = 250
    else:
        transport = 200
        prep = 500
        warranty = 300

    return {
        "transport": transport,
        "prep": prep,
        "warranty": warranty,
        "total": transport + prep + warranty,
    }


def calculate_true_profit(
    market_value,
    asking_price,
    transport=150,
    prep=400,
    warranty=300,
    risk_penalty=0
):
    costs = calculate_costs(asking_price)
    gross_profit = round(market_value - asking_price, 2)
    total_costs = costs["total"] + risk_penalty
    net_profit = round(gross_profit - total_costs, 2)

    return {
        "gross_profit": gross_profit,
        "net_profit": net_profit,
        "costs": costs,
        "risk_penalty": risk_penalty,
        "total_deductions": round(total_costs, 2),
    }


def calculate_score(profit, risk_penalty, mileage):
    # profit here is gross profit
    score = 0

    if profit > 3000:
        score += 30
    elif profit > 2000:
        score += 25
    elif profit > 1000:
        score += 15
    elif profit > 500:
        score += 8
    elif profit > 0:
        score += 3

    # Risk penalty
    score -= risk_penalty / 150

    # Mileage penalty
    if mileage > 120000:
        score -= 15
    elif mileage > 80000:
        score -= 5

    return round(score, 2)