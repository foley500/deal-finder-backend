import os
import redis
import time
from app.celery_app import celery
from app.database import SessionLocal
from app.models import Dealer, DealerSettings, ScanRun
from app.services.deal_engine import process_listing
from app.services.listing_sources.factory import get_listing_source
from app.services.pdf_service import generate_deal_pdf
from app.services.telegram_service import send_telegram_document


# ==========================================
# SOURCES
# ==========================================
SOURCES = ["ebay_browse"]

REDIS_URL = os.getenv("CELERY_BROKER_URL")
redis_client = redis.from_url(REDIS_URL)

SNIPER_LIMIT = 20
VALUE_SWEEP_LIMIT = 150

# ==========================================
# COMMON UK CARS FOR CACHE PREWARM
# Format: (make, model, years, mileage_buckets)
# Covers ~90% of what appears on eBay UK
# ==========================================
PREWARM_TARGETS = [
    ("Ford",       "Fiesta",    [2014, 2015, 2016, 2017, 2018, 2019], [20000, 40000, 60000, 80000, 100000]),
    ("Ford",       "Focus",     [2014, 2015, 2016, 2017, 2018, 2019], [20000, 40000, 60000, 80000, 100000]),
    ("Ford",       "Ka",        [2012, 2013, 2014, 2015, 2016],       [30000, 50000, 70000, 90000]),
    ("Ford",       "Kuga",      [2015, 2016, 2017, 2018, 2019],       [30000, 50000, 70000, 90000]),
    ("Ford",       "Mondeo",    [2014, 2015, 2016, 2017, 2018],       [40000, 60000, 80000, 100000]),
    ("Vauxhall",   "Corsa",     [2014, 2015, 2016, 2017, 2018, 2019], [20000, 40000, 60000, 80000, 100000]),
    ("Vauxhall",   "Astra",     [2014, 2015, 2016, 2017, 2018],       [30000, 50000, 70000, 90000]),
    ("Vauxhall",   "Insignia",  [2014, 2015, 2016, 2017, 2018],       [40000, 60000, 80000, 100000]),
    ("Vauxhall",   "Mokka",     [2014, 2015, 2016, 2017, 2018],       [30000, 50000, 70000, 90000]),
    ("Volkswagen", "Golf",      [2014, 2015, 2016, 2017, 2018, 2019], [20000, 40000, 60000, 80000, 100000]),
    ("Volkswagen", "Polo",      [2014, 2015, 2016, 2017, 2018],       [20000, 40000, 60000, 80000]),
    ("Volkswagen", "Passat",    [2014, 2015, 2016, 2017, 2018],       [40000, 60000, 80000, 100000]),
    ("Volkswagen", "Tiguan",    [2015, 2016, 2017, 2018, 2019],       [30000, 50000, 70000, 90000]),
    ("Audi",       "A1",        [2013, 2014, 2015, 2016, 2017, 2018], [20000, 40000, 60000, 80000]),
    ("Audi",       "A3",        [2014, 2015, 2016, 2017, 2018, 2019], [20000, 40000, 60000, 80000, 100000]),
    ("Audi",       "A4",        [2014, 2015, 2016, 2017, 2018],       [30000, 50000, 70000, 90000]),
    ("Audi",       "Q3",        [2014, 2015, 2016, 2017, 2018],       [30000, 50000, 70000, 90000]),
    ("BMW",        "1 Series",  [2014, 2015, 2016, 2017, 2018],       [20000, 40000, 60000, 80000]),
    ("BMW",        "3 Series",  [2013, 2014, 2015, 2016, 2017, 2018], [30000, 50000, 70000, 90000]),
    ("BMW",        "5 Series",  [2013, 2014, 2015, 2016, 2017],       [40000, 60000, 80000, 100000]),
    ("Mercedes-Benz", "A-Class",[2014, 2015, 2016, 2017, 2018],       [20000, 40000, 60000, 80000]),
    ("Mercedes-Benz", "C-Class",[2014, 2015, 2016, 2017, 2018],       [30000, 50000, 70000, 90000]),
    ("Toyota",     "Yaris",     [2014, 2015, 2016, 2017, 2018],       [20000, 40000, 60000, 80000]),
    ("Toyota",     "Auris",     [2014, 2015, 2016, 2017, 2018],       [30000, 50000, 70000, 90000]),
    ("Toyota",     "Corolla",   [2015, 2016, 2017, 2018, 2019],       [20000, 40000, 60000, 80000]),
    ("Nissan",     "Micra",     [2014, 2015, 2016, 2017, 2018],       [20000, 40000, 60000, 80000]),
    ("Nissan",     "Juke",      [2014, 2015, 2016, 2017, 2018],       [30000, 50000, 70000, 90000]),
    ("Nissan",     "Qashqai",   [2014, 2015, 2016, 2017, 2018, 2019], [30000, 50000, 70000, 90000]),
    ("Honda",      "Civic",     [2014, 2015, 2016, 2017, 2018],       [30000, 50000, 70000, 90000]),
    ("Honda",      "Jazz",      [2014, 2015, 2016, 2017, 2018],       [20000, 40000, 60000, 80000]),
    ("Hyundai",    "I20",       [2014, 2015, 2016, 2017, 2018],       [20000, 40000, 60000, 80000]),
    ("Hyundai",    "I30",       [2014, 2015, 2016, 2017, 2018],       [30000, 50000, 70000, 90000]),
    ("Kia",        "Rio",       [2014, 2015, 2016, 2017, 2018],       [20000, 40000, 60000, 80000]),
    ("Kia",        "Ceed",      [2014, 2015, 2016, 2017, 2018],       [30000, 50000, 70000, 90000]),
    ("Kia",        "Sportage",  [2015, 2016, 2017, 2018, 2019],       [30000, 50000, 70000, 90000]),
    ("Seat",       "Ibiza",     [2014, 2015, 2016, 2017, 2018],       [20000, 40000, 60000, 80000]),
    ("Seat",       "Leon",      [2014, 2015, 2016, 2017, 2018],       [30000, 50000, 70000, 90000]),
    ("Skoda",      "Fabia",     [2014, 2015, 2016, 2017, 2018],       [20000, 40000, 60000, 80000]),
    ("Skoda",      "Octavia",   [2014, 2015, 2016, 2017, 2018],       [30000, 50000, 70000, 90000]),
    ("Fiat",       "500",       [2013, 2014, 2015, 2016, 2017, 2018], [20000, 40000, 60000, 80000]),
    ("Mini",       "Hatch",     [2014, 2015, 2016, 2017, 2018],       [20000, 40000, 60000, 80000]),
    ("Mini",       "Clubman",   [2015, 2016, 2017, 2018, 2019],       [20000, 40000, 60000, 80000]),
    ("Peugeot",    "208",       [2014, 2015, 2016, 2017, 2018],       [20000, 40000, 60000, 80000]),
    ("Peugeot",    "308",       [2014, 2015, 2016, 2017, 2018],       [30000, 50000, 70000, 90000]),
    ("Renault",    "Clio",      [2014, 2015, 2016, 2017, 2018],       [20000, 40000, 60000, 80000]),
    ("Renault",    "Megane",    [2014, 2015, 2016, 2017, 2018],       [30000, 50000, 70000, 90000]),
    ("Citroen",    "C3",        [2014, 2015, 2016, 2017, 2018],       [20000, 40000, 60000, 80000]),
    ("Mazda",      "Mazda3",    [2014, 2015, 2016, 2017, 2018],       [30000, 50000, 70000, 90000]),
    ("Volvo",      "V40",       [2014, 2015, 2016, 2017, 2018],       [30000, 50000, 70000, 90000]),
    ("Land Rover", "Discovery", [2014, 2015, 2016, 2017, 2018],       [40000, 60000, 80000, 100000]),
    ("Land Rover", "Freelander",[2012, 2013, 2014, 2015],             [40000, 60000, 80000, 100000]),
    ("Jeep",       "Renegade",  [2015, 2016, 2017, 2018],             [30000, 50000, 70000, 90000]),
]


# ==========================================
# TELEGRAM NOTIFICATION
# ==========================================
@celery.task
def notify_deal(deal_id: int):
    db = SessionLocal()
    try:
        from app.models import Deal
        deal = db.query(Deal).filter(Deal.id == deal_id).first()
        if not deal:
            return

        report = deal.report or {}
        pdf_buffer = generate_deal_pdf(deal, report.get("mot_full_data"))
        pdf_buffer.seek(0)

        caption = f"""
🚗 {deal.status.upper()} CONFIDENCE DEAL

{deal.title}

📍 Reg: {deal.reg or "N/A"}
📊 Mileage: {deal.mileage or "N/A"}

💰 Price: £{deal.listing_price}
📈 CAP Value: £{deal.market_value}
📊 Profit: £{deal.profit}

⚠️ Risk: £{deal.risk_penalty}
🎯 Score: {deal.score}

🔗 Listing: {report.get("listing_url", "N/A")}
"""
        send_telegram_document(
            pdf_buffer,
            filename=f"VehicleIntel_Report_{deal.id}.pdf",
            caption=caption
        )
    finally:
        db.close()


# ==========================================
# PREWARM VALUATION CACHE
# Runs nightly at 3am — fills Redis with
# market prices for all common UK cars so
# scan tasks hit cache instead of eBay API
# ==========================================
@celery.task
def prewarm_valuation_cache():
    from app.services.market_valuation_service import get_market_price_from_sold

    total = 0
    hits = 0
    skipped = 0

    print("🔥 Starting valuation cache prewarm...")

    for make, model, years, mileage_buckets in PREWARM_TARGETS:
        for year in years:
            for mileage in mileage_buckets:

                # Check if already cached — don't burn API calls unnecessarily
                cache_key = f"sold_cache:{make.title()}:{model.title()}:{year}:{mileage}"
                if redis_client.get(cache_key):
                    skipped += 1
                    continue

                try:
                    result = get_market_price_from_sold(
                        make=make,
                        model=model,
                        year=year,
                        mileage=mileage,
                    )
                    total += 1
                    if result:
                        hits += 1
                        print(f"✅ {make} {model} {year} {mileage}mi → £{result['market_price']}")
                    else:
                        print(f"⚠️ {make} {model} {year} {mileage}mi → no data")

                    # Small pause between valuations to avoid hammering eBay
                    time.sleep(2)

                except Exception as e:
                    print(f"❌ {make} {model} {year}: {e}")
                    continue

    print(f"🔥 Prewarm complete: {hits}/{total} valued, {skipped} already cached")
    return {"valued": hits, "total": total, "skipped": skipped}


# ==========================================
# SNIPER MODE
# ==========================================
@celery.task
def scan_sniper(dealer_id: int):
    return run_scan(
        dealer_id=dealer_id,
        sort="newlyListed",
        listings_to_pull=20,
        mode_name="sniper"
    )


# ==========================================
# VALUE SWEEP
# ==========================================
@celery.task
def scan_value_sweep(dealer_id: int):
    return run_scan(
        dealer_id=dealer_id,
        sort="newlyListed",
        listings_to_pull=40,
        mode_name="value_sweep",
        deep_sweep=True,
        sweep_start_offset=200
    )


# ==========================================
# SHARED SCAN ENGINE
# ==========================================
def run_scan(dealer_id: int, sort: str, listings_to_pull: int, mode_name: str, deep_sweep=False, sweep_start_offset=0):

    lock_key = f"scan_lock_{dealer_id}_{mode_name}"

    if mode_name == "value_sweep":
        max_expansions = VALUE_SWEEP_LIMIT
    else:
        max_expansions = SNIPER_LIMIT

    if redis_client.get(lock_key):
        print("⚠️ Scan already running — skipping")
        return {"skipped": True}

    redis_client.set(lock_key, "1", ex=1800)

    db = SessionLocal()

    try:
        dealer = db.query(Dealer).filter(Dealer.id == dealer_id).first()
        if not dealer:
            return {"error": "Dealer not found"}

        settings = db.query(DealerSettings).filter(
            DealerSettings.dealer_id == dealer.id
        ).first()

        if not settings:
            return {"error": "Dealer settings missing"}

        filters = {
            "min_year": settings.min_year,
            "max_year": settings.max_year,
            "max_mileage": settings.max_mileage,
            "max_price": settings.max_price if settings.max_price else 50000,
            "min_profit": settings.min_profit,
            "min_score": settings.min_score,
        }

        total_listings = 0
        total_deals = 0
        processed_ids = set()
        detail_expansions = 0

        for source_name in SOURCES:

            source = get_listing_source(source_name)
            items = []

            if deep_sweep:
                for page in range(sweep_start_offset, sweep_start_offset + 200, listings_to_pull):
                    page_items = source.search(
                        keywords="car",
                        entries=listings_to_pull,
                        min_price=None,
                        max_price=filters["max_price"],
                        min_year=filters["min_year"],
                        max_year=filters["max_year"],
                        sort=sort,
                        offset=page
                    )
                    items.extend(page_items)
            else:
                items = source.search(
                    keywords="car",
                    entries=listings_to_pull,
                    min_price=None,
                    max_price=filters["max_price"],
                    min_year=filters["min_year"],
                    max_year=filters["max_year"],
                    sort=sort
                )

            total_listings += len(items)

            for item in items:

                if detail_expansions >= max_expansions:
                    print("🛑 Expansion cap reached")
                    break

                external_id = item.get("id") or item.get("view_url")
                if not external_id or external_id in processed_ids:
                    continue

                processed_ids.add(external_id)

                rough_price = float(item.get("price", 0))
                if not rough_price:
                    continue

                if rough_price < 2000:
                    rough_costs = 450
                elif rough_price < 4000:
                    rough_costs = 650
                elif rough_price < 8000:
                    rough_costs = 800
                else:
                    rough_costs = 1000

                rough_estimated_value = rough_price * 1.20
                rough_profit = rough_estimated_value - rough_price - rough_costs

                if settings.min_profit and rough_profit < (settings.min_profit * 0.5):
                    continue

                deal = process_listing(
                    item,
                    dealer.id,
                    source=source_name,
                    filters=filters
                )

                detail_expansions += 1

                if not deal:
                    continue

                total_deals += 1
                notify_deal.delay(deal.id)

        scan = ScanRun(
            dealer_id=dealer.id,
            source=f"mode_{mode_name}",
            listings_found=total_listings,
            deals_saved=total_deals
        )

        db.add(scan)
        db.commit()

        return {
            "mode": mode_name,
            "listings_found": total_listings,
            "deals_saved": total_deals
        }

    finally:
        redis_client.delete(lock_key)
        db.close()