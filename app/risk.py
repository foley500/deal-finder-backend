# ============================================================
# RISK ENGINE — description-based penalty scoring
#
# Two tiers:
#   CRITICAL  — deal-killers (write-offs, salvage, stolen, flooded).
#               Apply once only, large penalty. Dealers should avoid these entirely.
#   HIGH      — serious mechanical or legal risk. Per-keyword penalty.
#   MEDIUM    — condition issues that add reconditioning cost.
#   LOW       — minor items worth noting.
#
# All penalties are capped at MAX_RISK_PERCENTAGE of asking price.
# ============================================================

# Write-off / total loss categories — DVLA categories A/B/S/N
CRITICAL_KEYWORDS = {
    "cat a":            2000,
    "cat b":            2000,
    "category a":       2000,
    "category b":       2000,
    "cat s":            1500,
    "cat n":            1200,
    "category s":       1500,
    "category n":       1200,
    "write off":        1500,
    "write-off":        1500,
    "written off":      1500,
    "insurance write":  1500,
    "insurance payout": 1200,
    "total loss":       1200,
    "salvage title":    1500,
    "salvage vehicle":  1200,
}

HIGH_RISK_KEYWORDS = {
    # Structural / flood / fire
    "flood damage":     1500,
    "flood damaged":    1500,
    "water damage":     1200,
    "fire damage":      1200,
    "fire damaged":     1200,
    "structural damage":1000,

    # Theft / legal status
    "stolen recovery":  1000,
    "recovered stolen":  900,
    "vin changed":       800,
    "chassis altered":   800,

    # Documentation issues — serious for dealers
    "no v5":             600,
    "no log book":       600,
    "no documents":      500,
    "no paperwork":      500,
    "log book lost":     500,
    "v5 applied for":    400,

    # Key issues
    "no keys":           500,
    "key unknown":       400,
    "no key":            400,
    "1 key":             200,
    "one key":           200,

    # Odometer / mileage integrity
    "odometer broken":   600,
    "speedo broken":     500,
    "mileage unknown":   400,
    "miles unknown":     400,
    "clocked":           800,
    "mileage not guaranteed": 400,

    # Auction / trade flags (not necessarily bad but adds risk premium)
    "auction":           300,
    "trade sale":        300,
    "trade only":        300,
    "sold as seen":      400,
    "no warranty":       200,
}

MEDIUM_RISK_KEYWORDS = {
    # Engine / drivetrain
    "misfire":           400,
    "engine light":      300,
    "check engine":      300,
    "engine noise":      350,
    "gearbox fault":     400,
    "gearbox noise":     350,
    "clutch slip":       300,
    "clutch judder":     250,
    "timing chain":      400,
    "timing belt":       300,
    "head gasket":       600,
    "overheating":       400,
    "oil leak":          250,
    "coolant leak":      250,
    "transmission fault":400,
    "dpf fault":         350,
    "turbo fault":       400,
    "turbo noise":       300,

    # Service / maintenance
    "needs service":     200,
    "service overdue":   250,
    "service light":     200,
    "no service history":200,
    "no history":        150,
    "unknown history":   150,

    # Cosmetic / structural
    "airbag fault":      400,
    "abs fault":         300,
    "brake fault":       350,
    "damaged":           300,
    "accident damage":   400,
    "spares or repair":  500,
    "spares or repairs": 500,
    "not running":       600,
    "non runner":        600,
    "seized":            500,
}

LOW_RISK_KEYWORDS = {
    "rust":              150,
    "corrosion":         150,
    "bodywork needed":   150,
    "paint needed":      100,
    "dent":              100,
    "scratch":            75,
    "scuff":              75,
    "advisory":          100,
    "advisories":        100,
    "worn tyres":        150,
    "needs tyres":       150,
    "battery fault":     150,
}

MAX_RISK_PERCENTAGE = 0.40  # Hard cap: never more than 40% of asking price


# ============================================================
# POSITIVE SIGNAL DETECTION
# ============================================================

MOTIVATED_SELLER_PHRASES = [
    "quick sale",
    "need quick sale",
    "need gone",
    "needs to go",
    "need to sell",
    "must sell",
    "must go",
    "needs gone",
    "reduced to sell",
    "priced to sell",
    "moving abroad",
    "moving overseas",
    "relocating",
    "emigrating",
    "divorce",
    "separation",
    "reluctant sale",
    "sadly selling",
    "forced to sell",
    "health reasons",
    "unfortunately selling",
    "any offers",
    "all offers",
    "offers considered",
    "open to offers",
    "reasonable offers",
    "sensible offers",
    "no offers refused",
    "below market",
    "genuine bargain",
    "too cheap",
    "selling below",
    "need money",
    "financial reasons",
    "can't afford",
    "cannot afford",
    "accepting offers",
    "buyer to collect",  # urgency signal — seller wants shot of it
    "cash needed",
    "cash buyer",        # often signals price flexibility
    "no time wasters",   # seller is motivated but frustrated
    "serious buyers",
    "ono",               # Or Nearest Offer — UK negotiation signal
    " o.n.o",
    "or near offer",
    "or nearest offer",
]

FSH_PHRASES = [
    "full service history",
    "full dealer service history",
    "full main dealer service",
    "full stamped service",
    "complete service history",
    " fsh",
    "fsh ",
    "(fsh)",
    "stamped service book",
    "all stamps present",
    "all services present",
    "dealer serviced throughout",
    "main dealer serviced",
]


def motivated_seller_signal(title: str, description: str) -> bool:
    """
    Returns True if title or description contains motivated seller language.
    These phrases strongly indicate a seller willing to accept below-market offers.
    """
    combined = (title + " " + description).lower()
    return any(phrase in combined for phrase in MOTIVATED_SELLER_PHRASES)


def fsh_signal(title: str, description: str) -> bool:
    """
    Returns True if title or description indicates full service history.
    FSH cars are easier to retail and command a small premium over no-history cars.
    """
    combined = (title + " " + description).lower()
    return any(phrase in combined for phrase in FSH_PHRASES)


ONE_OWNER_PHRASES = [
    "one owner",
    "1 owner",
    "1 previous owner",
    "one previous owner",
    "single owner",
    "only owner",
    "sole owner",
    "first owner",
    "1 former keeper",
    "one former keeper",
    "only one keeper",
    "1 keeper",
    "one keeper",
]


def one_owner_signal(title: str, description: str) -> bool:
    """
    Returns True if title or description indicates single previous owner.
    One-owner cars are easier to retail — buyers pay a small premium and
    the vehicle history is simpler to verify. Reduces reconditioning risk.
    """
    combined = (title + " " + description).lower()
    return any(phrase in combined for phrase in ONE_OWNER_PHRASES)


RECENT_SERVICE_PHRASES = [
    # Tyres
    "new tyres",
    "brand new tyres",
    "fresh tyres",
    "new front tyres",
    "new rear tyres",
    "4 new tyres",
    "four new tyres",
    # Timing belt / chain
    "new timing belt",
    "timing belt done",
    "timing belt changed",
    "timing belt replaced",
    "timing chain done",
    "timing chain replaced",
    "cam belt done",
    "cam belt changed",
    "cam belt replaced",
    # Brakes
    "new brakes",
    "new brake pads",
    "new discs",
    "new disc and pads",
    "brakes done",
    "brakes replaced",
    # Service
    "recently serviced",
    "just been serviced",
    "fresh service",
    "service just done",
    "full service done",
    "service completed",
    "new service",
    # MOT
    "fresh mot",
    "new mot",
    "just mot",
    "mot just done",
    "12 months mot",
    "12 month mot",
    "full year mot",
    # Battery
    "new battery",
    "battery replaced",
    "brand new battery",
    # Other
    "new clutch",
    "clutch replaced",
    "new exhaust",
    "new water pump",
]


def recent_service_signal(title: str, description: str) -> bool:
    """
    Returns True if title or description mentions recent maintenance.
    Sellers who recently invested in the car are less likely to have hidden issues
    and the cost reduces the buyer's immediate reconditioning exposure.
    """
    combined = (title + " " + description).lower()
    return any(phrase in combined for phrase in RECENT_SERVICE_PHRASES)


def is_ulez_diesel_risk(fuel_type: str, year: int) -> bool:
    """
    Returns True if the vehicle is a pre-2015 diesel facing ongoing ULEZ resale risk.
    Euro 5 diesel vehicles (registered before September 2015) are non-compliant with
    the London ULEZ and growing number of UK Clean Air Zones.
    This structurally reduces their resale values, particularly in urban markets.
    """
    if not fuel_type or not year:
        return False
    return "diesel" in fuel_type.lower() and year < 2015


def description_risk(description: str, listing_price: float = 0) -> float:
    if not description:
        return 0

    text = description.lower()
    penalty = 0.0

    # Critical keywords — apply once only (don't stack multiple write-off terms)
    for phrase, amount in CRITICAL_KEYWORDS.items():
        if phrase in text:
            penalty += amount
            break  # one critical penalty only

    # High, medium, low — cumulative but no double-counting per phrase
    for keywords in (HIGH_RISK_KEYWORDS, MEDIUM_RISK_KEYWORDS, LOW_RISK_KEYWORDS):
        for phrase, amount in keywords.items():
            if phrase in text:
                penalty += amount

    # Hard cap relative to asking price
    if listing_price and listing_price > 0:
        max_allowed = listing_price * MAX_RISK_PERCENTAGE
        penalty = min(penalty, max_allowed)

    return round(penalty, 2)
