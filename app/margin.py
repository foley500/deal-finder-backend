def calculate_true_profit(
    market_value,
    asking_price,
    transport=150,
    prep=400,
    warranty=300,
    risk_penalty=0
):
    total_costs = asking_price + transport + prep + warranty + risk_penalty
    return market_value - total_costs