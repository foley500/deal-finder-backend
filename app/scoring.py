def calculate_score(
    profit,
    risk_penalty,
    mileage,
    seller_type=None,
    price_drop_pct=None,
    days_on_market=None,
    market_depth=-1,
    motivated_seller=False,
    fsh=False,
    mot_months_remaining=None,
    ulez_diesel_risk=False,
):
    """
    Dealer-grade deal scoring. Returns a float score.

    Score bands (see assign_confidence in deal_engine.py):
      ≥20  → high confidence
      ≥10  → medium confidence
      <10  → low confidence

    Typical ceiling for a perfect deal: ~65 (£2k+ profit, private seller,
    motivated seller, FSH, fresh listing, scarce inventory, low mileage, clean MOT).
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
    # ------------------------------------------------------------------
    if seller_type == "INDIVIDUAL":
        score += 5
    elif seller_type == "BUSINESS":
        score -= 2

    # ------------------------------------------------------------------
    # MOTIVATED SELLER — strongest buying opportunity signal.
    # Phrases like "quick sale", "moving abroad", "reluctant sale" indicate
    # a seller willing to accept below-market offers.
    # ------------------------------------------------------------------
    if motivated_seller:
        score += 10

    # ------------------------------------------------------------------
    # FULL SERVICE HISTORY — easier to retail, commands a small premium.
    # Reduces reconditioning risk and buyer objections at the forecourt.
    # ------------------------------------------------------------------
    if fsh:
        score += 5

    # ------------------------------------------------------------------
    # PRICE DROP SIGNAL — value sweep killer feature.
    # ------------------------------------------------------------------
    if price_drop_pct is not None and price_drop_pct > 0:
        if price_drop_pct >= 20:
            score += 12
        elif price_drop_pct >= 10:
            score += 8
        elif price_drop_pct >= 5:
            score += 5
        else:
            score += 3

    # ------------------------------------------------------------------
    # DAYS ON MARKET — fresh is better for sniper; stale needs a reason.
    # ------------------------------------------------------------------
    if days_on_market is not None:
        if days_on_market <= 1:
            score += 5
        elif days_on_market <= 7:
            score += 2
        elif days_on_market >= 45:
            score -= 3

    # ------------------------------------------------------------------
    # MOT MONTHS REMAINING — affects dealer's immediate cost and retail ease.
    # >10 months = no action needed before resale. <3 months = immediate cost.
    # ------------------------------------------------------------------
    if mot_months_remaining is not None:
        if mot_months_remaining >= 10:
            score += 3
        elif mot_months_remaining < 3:
            score -= 5

    # ------------------------------------------------------------------
    # ULEZ / DIESEL RISK — pre-2015 Euro 5 diesels face structural
    # resale headwinds in the UK. Harder to retail in London and growing
    # Clean Air Zones. Penalty reflects reduced buyer pool.
    # ------------------------------------------------------------------
    if ulez_diesel_risk:
        score -= 8

    # ------------------------------------------------------------------
    # MARKET DEPTH — how many competing listings exist at ≤ asking + 15%.
    # Few competitors = genuinely scarce pricing. Many = just market floor.
    # -1 = check wasn't run (budget gate or failure) — no adjustment.
    # ------------------------------------------------------------------
    if market_depth >= 0:
        if market_depth <= 2:
            score += 8
        elif market_depth <= 5:
            score += 4
        elif market_depth <= 10:
            score += 1
        elif market_depth >= 25:
            score -= 5

    return round(score, 1)
