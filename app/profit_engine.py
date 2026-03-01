def calculate_profit(listing_price, market_value):

    auction_fee = listing_price * 0.08
    recon_estimate = 400
    transport = 150

    total_cost = listing_price + auction_fee + recon_estimate + transport

    profit = market_value - total_cost

    return round(profit, 2)
