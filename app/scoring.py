def calculate_score(
    profit,
    risk_penalty,
    mileage,
    seller_type=None,
    price_drop_pct=None,
    days_on_market=None,
    market_depth=-1,
):
    """
    Dealer-grade deal scoring. Returns a float score.

    Score bands (see assign_confidence in deal_engine.py):
      ≥20  → high confidence
      ≥10  → medium confidence
      <10  → low confidence

    Typical ceiling for a perfect deal: ~50 (£2k+ profit, private seller,
    recent price drop, fresh listing, scarce inventory, low mileage, clean MOT).
    """
    score = 0

    # ------------------------------------------------------------------
    # PROFIT TIER — primary deal quality signal
    # Gross profit tiers sized for UK used car trade reality.
    # ------------------------------------------------------------------
    if profit >= 3000:
        score += 40
    elif profit >= 2000:
        score += 30
    elif profit >= 1500:
        score += 22
    elif profit >= 1000:
        score += 15
    elif profit >= 500:
        score += 8
    elif profit >= 250:
        score += 3

    # ------------------------------------------------------------------
    # RISK PENALTY — every £1 of risk penalty costs 1/80 of a score point.
    # A £800 risk penalty (e.g. Cat N history) removes 10 points.
    # ------------------------------------------------------------------
    score -= risk_penalty / 80

    # ------------------------------------------------------------------
    # MILEAGE PENALTY
    # ------------------------------------------------------------------
    if mileage and mileage > 120000:
        score -= 15
    elif mileage and mileage > 80000:
        score -= 5

    # ------------------------------------------------------------------
    # SELLER TYPE — individual (private) selling cheap is a stronger
    # deal signal than a dealer listing at the same price.
    # Dealers price at retail; a private seller £800 below private clean
    # is motivated. A dealer at the same price is just trying market rate.
    # ------------------------------------------------------------------
    if seller_type == "INDIVIDUAL":
        score += 5
    elif seller_type == "BUSINESS":
        score -= 2  # Slight discount — dealer at private-deal price = needs investigation

    # ------------------------------------------------------------------
    # PRICE DROP SIGNAL — value sweep killer feature.
    # A listing that was £2,500 and is now £1,900 has a motivated seller.
    # The drop size indicates urgency. This is NOT captured by current profit
    # since profit is calculated against market value, not original asking price.
    # ------------------------------------------------------------------
    if price_drop_pct is not None and price_drop_pct > 0:
        if price_drop_pct >= 20:
            score += 12  # Aggressive cut — seller needs to move it
        elif price_drop_pct >= 10:
            score += 8
        elif price_drop_pct >= 5:
            score += 5
        else:
            score += 3  # Even a small drop signals flexibility

    # ------------------------------------------------------------------
    # DAYS ON MARKET — fresh is better for sniper; stale needs a reason.
    # Very fresh (<1 day) = sniper opportunity. Old without sale = red flag.
    # Note: sniper age gate (90 min) largely handles this for sniper runs.
    # This signal is most useful for value sweep.
    # ------------------------------------------------------------------
    if days_on_market is not None:
        if days_on_market <= 1:
            score += 5   # Just listed — first-mover advantage
        elif days_on_market <= 7:
            score += 2   # Still fresh
        elif days_on_market >= 45:
            score -= 3   # Sat for 6+ weeks — ask why

    # ------------------------------------------------------------------
    # MARKET DEPTH — how many competing listings exist at ≤ asking + 15%.
    # Few competitors = genuinely scarce pricing. Many = just market floor.
    # -1 = check wasn't run (budget gate or failure) — no adjustment.
    # ------------------------------------------------------------------
    if market_depth >= 0:
        if market_depth <= 2:
            score += 8   # Rare — few alternatives for buyers
        elif market_depth <= 5:
            score += 4   # Reasonably scarce
        elif market_depth <= 10:
            score += 1   # Some competition
        elif market_depth >= 25:
            score -= 5   # It's just market price, not underpriced

    return round(score, 1)
