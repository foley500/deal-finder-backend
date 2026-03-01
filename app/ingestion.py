from app.margin import calculate_true_profit
from app.risk import description_risk

def process_listing(title, price, description, cap_private):
    risk_penalty = description_risk(description)

    profit = calculate_true_profit(
        cap_private,
        price,
        risk_penalty=risk_penalty
    )

    return profit