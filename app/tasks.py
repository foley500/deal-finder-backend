import os
import redis
from app.celery_app import celery
from app.database import SessionLocal
from app.models import Dealer, DealerSettings, ScanRun
from app.services.deal_engine import process_listing
from app.services.listing_sources.factory import get_listing_source
from app.services.pdf_service import generate_deal_pdf
from app.services.telegram_service import send_telegram_document


# ==========================================
# 🔧 SOURCES
# ==========================================
SOURCES = ["ebay_browse"]

TEST_MODE = True

REDIS_URL = os.getenv("CELERY_BROKER_URL")
redis_client = redis.from_url(REDIS_URL)

SNIPER_LIMIT = 20
VALUE_SWEEP_LIMIT = 150 


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

        pdf_buffer = generate_deal_pdf(
            deal,
            report.get("mot_full_data")
        )

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
# SNIPER MODE (Fast / Frequent)
# Prioritises newly listed
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
# VALUE SWEEP (Deeper / Slower)
# Finds older underpriced vehicles
# ==========================================
@celery.task
def scan_value_sweep(dealer_id: int):

    return run_scan(
        dealer_id=dealer_id,
        sort="newlyListed",
        listings_to_pull=40,
        mode_name="value_sweep",
        deep_sweep=True
    )


# ==========================================
# SHARED SCAN ENGINE
# ==========================================
def run_scan(dealer_id: int, sort: str, listings_to_pull: int, mode_name: str, deep_sweep=False):

    lock_key = f"scan_lock_{dealer_id}_{mode_name}"

    if mode_name == "value_sweep":
        max_expansions = VALUE_SWEEP_LIMIT
    else:
        max_expansions = SNIPER_LIMIT

    if redis_client.get(lock_key):
        print("⚠️ Scan already running — skipping")
        return {"skipped": True}

    redis_client.set(lock_key, "1", ex=540)

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
            "max_price": 4000,
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
                for page in range(40, 400, listings_to_pull):
                    page_items = source.search(
                        keywords="cars",
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
                    keywords="cars",
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

                rough_estimated_value = rough_price * 1.15
                rough_profit = rough_estimated_value - rough_price

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