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
    one_owner=False,
    valuation_confidence=None,
    is_auction=False,
    regional_signal=None,
    buy_below_trade=None,
    recent_service=False,
):
    """
    Dealer-grade deal scoring. Returns a float score.

    Score bands (see assign_confidence in deal_engine.py):
      ≥20  → high confidence
      ≥10  → medium confidence
      <10  → low confidence

    Typical ceiling for a perfect deal: ~85 (£3k+ profit, private seller,
    motivated seller, FSH, one owner, fresh listing, scarce inventory, low mileage,
    clean MOT, buy below trade, recent service, discount region).

    valuation_confidence: "high" | "medium" | "low" | None
      Scales effective profit used in tier selection only — displayed values are unchanged.
      High=1.0, Medium=0.90, Low=0.75. Unknown/None defaults to 1.0.
    buy_below_trade: positive float = asking price is this many £ below trade/auction value.
      Means you could flip to a trader immediately for profit. Very strong signal.
    recent_service: True if description mentions recent maintenance (new tyres, timing belt, etc.)
    """
    score = 0

    # ------------------------------------------------------------------
    # CONFIDENCE-ADJUSTED EFFECTIVE PROFIT
    # When valuation data is thin (few sold comps), the market value
    # estimate is less reliable. Discount effective profit for scoring
    # without changing the displayed figures.
    # ------------------------------------------------------------------
    _confidence_multiplier = {"high": 1.0, "medium": 0.90, "low": 0.75}.get(
        (valuation_confidence or "").lower(), 1.0
    )
    effective_profit = profit * _confidence_multiplier

    # ------------------------------------------------------------------
    # PROFIT TIER — primary deal quality signal
    # Gross profit tiers sized for UK used car trade reality.
    # Uses confidence-adjusted effective profit, not raw profit.
    # ------------------------------------------------------------------
    if effective_profit >= 3000:
        score += 40
    elif effective_profit >= 2000:
        score += 30
    elif effective_profit >= 1500:
        score += 22
    elif effective_profit >= 1000:
        score += 15
    elif effective_profit >= 500:
        score += 8
    elif effective_profit >= 250:
        score += 3

    # ------------------------------------------------------------------
    # BUY BELOW TRADE — asking price is below what a dealer would pay
    # at auction. This means you can flip to trade immediately for profit
    # before even touching the car. Extremely strong signal.
    # ------------------------------------------------------------------
    if buy_below_trade is not None and buy_below_trade > 0:
        if buy_below_trade >= 1000:
            score += 20   # Below trade by £1k+ — exceptional, instant trade flip
        elif buy_below_trade >= 500:
            score += 14   # Below trade by £500+ — strong
        else:
            score += 8    # Below trade — still significant

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
    # SELLER TYPE — private sellers are a stronger deal signal since
    # they're typically less market-savvy than trade sellers. Business
    # sellers are neutral (0) — vans are almost always business-listed
    # so penalising BUSINESS would suppress nearly all van deals.
    # ------------------------------------------------------------------
    if seller_type == "INDIVIDUAL":
        score += 5

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
    # ONE OWNER — single keeper history reduces risk and boosts retail
    # appeal. Buyers pay a premium; provenance is simpler to verify.
    # ------------------------------------------------------------------
    if one_owner:
        score += 5

    # ------------------------------------------------------------------
    # RECENT SERVICE / MAINTENANCE — seller has invested in the car.
    # New tyres, timing belt, brakes or recent service reduce prep cost
    # and buyer objections at point of retail.
    # ------------------------------------------------------------------
    if recent_service:
        score += 4

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

    # ------------------------------------------------------------------
    # AUCTION LISTING — uncertain final price; timing risk; can't negotiate.
    # ------------------------------------------------------------------
    if is_auction:
        score -= 3

    # ------------------------------------------------------------------
    # REGIONAL SIGNAL — discount regions (Scotland, Wales, NI, North England)
    # offer genuine arbitrage: buy cheap locally, retail into a wider market.
    # Premium regions (London, Home Counties) may reflect inflated asking prices.
    # ------------------------------------------------------------------
    if regional_signal == "discount_region":
        score += 3
    elif regional_signal == "premium_region":
        score -= 2

    return round(score, 1)


def calculate_score_breakdown(
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
    one_owner=False,
    valuation_confidence=None,
    is_auction=False,
    regional_signal=None,
    buy_below_trade=None,
    recent_service=False,
):
    """
    Returns (score, breakdown_dict) where breakdown_dict maps
    label -> point contribution for display in the deal detail UI.
    """
    breakdown = {}

    _confidence_multiplier = {"high": 1.0, "medium": 0.90, "low": 0.75}.get(
        (valuation_confidence or "").lower(), 1.0
    )
    effective_profit = profit * _confidence_multiplier

    if effective_profit >= 3000:
        pts = 40; label = "Profit ≥£3,000"
    elif effective_profit >= 2000:
        pts = 30; label = "Profit ≥£2,000"
    elif effective_profit >= 1500:
        pts = 22; label = "Profit ≥£1,500"
    elif effective_profit >= 1000:
        pts = 15; label = "Profit ≥£1,000"
    elif effective_profit >= 500:
        pts = 8;  label = "Profit ≥£500"
    elif effective_profit >= 250:
        pts = 3;  label = "Profit ≥£250"
    else:
        pts = 0;  label = "Profit <£250"
    breakdown[label] = pts

    if buy_below_trade is not None and buy_below_trade > 0:
        if buy_below_trade >= 1000:
            breakdown["Buy Below Trade (£1k+)"] = 20
        elif buy_below_trade >= 500:
            breakdown["Buy Below Trade (£500+)"] = 14
        else:
            breakdown["Buy Below Trade"] = 8

    risk_pts = -round(risk_penalty / 80, 1)
    if risk_pts:
        breakdown["Risk Penalty"] = risk_pts

    if mileage and mileage > 120000:
        breakdown["Very High Mileage (120k+)"] = -15
    elif mileage and mileage > 80000:
        breakdown["High Mileage (80k+)"] = -5

    if seller_type == "INDIVIDUAL":
        breakdown["Private Seller"] = 5

    if motivated_seller:
        breakdown["Motivated Seller"] = 10

    if fsh:
        breakdown["Full Service History"] = 5

    if one_owner:
        breakdown["One Previous Owner"] = 5

    if recent_service:
        breakdown["Recent Maintenance"] = 4

    if price_drop_pct is not None and price_drop_pct > 0:
        if price_drop_pct >= 20:    breakdown["Price Drop ≥20%"] = 12
        elif price_drop_pct >= 10:  breakdown["Price Drop ≥10%"] = 8
        elif price_drop_pct >= 5:   breakdown["Price Drop ≥5%"] = 5
        else:                       breakdown["Price Drop"] = 3

    if days_on_market is not None:
        if days_on_market <= 1:         breakdown["Fresh Listing (≤1d)"] = 5
        elif days_on_market <= 7:       breakdown["Recent Listing (≤7d)"] = 2
        elif days_on_market >= 45:      breakdown["Stale Listing (45d+)"] = -3

    if mot_months_remaining is not None:
        if mot_months_remaining >= 10:  breakdown["Long MOT Remaining"] = 3
        elif mot_months_remaining < 3:  breakdown["Short MOT (<3 months)"] = -5

    if ulez_diesel_risk:
        breakdown["ULEZ Diesel Risk"] = -8

    if market_depth >= 0:
        if market_depth <= 2:       breakdown["Very Low Competition (≤2)"] = 8
        elif market_depth <= 5:     breakdown["Low Competition (≤5)"] = 4
        elif market_depth <= 10:    breakdown["Moderate Competition (≤10)"] = 1
        elif market_depth >= 25:    breakdown["High Competition (25+)"] = -5

    if is_auction:
        breakdown["Auction Listing (uncertain price)"] = -3

    if regional_signal == "discount_region":
        breakdown["Discount Region (arbitrage opportunity)"] = 3
    elif regional_signal == "premium_region":
        breakdown["Premium Region (price may be inflated)"] = -2

    score = round(sum(breakdown.values()), 1)
    return score, breakdown
