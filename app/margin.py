# Premium makes: higher servicing, parts, and reconditioning costs
PREMIUM_MAKES = {
    "bmw", "mercedes", "mercedes-benz", "audi", "jaguar",
    "land rover", "porsche", "lexus", "tesla", "bentley", "maserati",
}

# Budget makes: lower parts cost, simpler servicing
BUDGET_MAKES = {
    "dacia", "skoda", "seat", "suzuki", "fiat", "citroen", "peugeot",
}


def get_make_prep_multiplier(make: str) -> float:
    """
    Returns a prep cost multiplier based on make.
    Premium German/prestige cars cost significantly more to prep:
    higher parts prices, specialist labour, and deeper pre-sale inspection.
    """
    if not make:
        return 1.0
    m = make.lower().strip()
    if m in PREMIUM_MAKES:
        return 1.4   # 40% more prep for prestige makes
    if m in BUDGET_MAKES:
        return 0.85  # 15% less for budget makes
    return 1.0


def calculate_costs(asking_price, make: str = ""):
    """
    Returns estimated costs breakdown based on vehicle price and make.
    Make-aware: prestige cars cost more to prep and carry higher warranty risk.
    """
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

    prep_multiplier = get_make_prep_multiplier(make)
    prep = round(prep * prep_multiplier)

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
    risk_penalty=0,
    make: str = "",
):
    costs = calculate_costs(asking_price, make=make)
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
