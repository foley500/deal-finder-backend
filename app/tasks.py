import os

from app.celery_app import celery
from app.database import SessionLocal
from app.models import Dealer, DealerSettings, ScanRun
from app.services.deal_engine import process_listing
from app.services.listing_sources.factory import get_listing_source
from app.services.pdf_service import generate_deal_pdf
from app.services.telegram_service import send_telegram_document

# Facebook Email Ingestion
from app.services.facebook_email_ingestion import FacebookEmailIngestion
from app.services.facebook_listing_parser import parse_facebook_listing


# ==========================================
# 🔧 SOURCES TO SCAN (API / structured sources)
# ==========================================
SOURCES = ["ebay_browse"]

TEST_MODE = True


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
# MASTER SCAN TASK (API SOURCES)
# ==========================================
@celery.task
def scan_market_for_deals(dealer_id: int):

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

        # ------------------------------------------
        # Build filters from dashboard settings
        # ------------------------------------------
        filters = {
            "min_year": settings.min_year,
            "max_year": settings.max_year,
            "max_mileage": settings.max_mileage,
            "max_price": 4000,  # hardcoded for now (no DB column yet)
            "min_profit": settings.min_profit,
            "min_score": settings.min_score,
        }

        total_listings = 0
        total_deals = 0
        LISTINGS_TO_PULL = 150  # increase scan depth
        processed_ids = set()

        for source_name in SOURCES:

            source = get_listing_source(source_name)

            # =====================================
            # MODE 1 — SNIPER (Newest First)
            # =====================================
            sniper_items = source.search(
                keywords="cars",
                entries=LISTINGS_TO_PULL,
                min_price=None,
                max_price=filters["max_price"],
                min_year=filters["min_year"],
                max_year=filters["max_year"],
                sort="newly_listed"
            )

            # =====================================
            # MODE 2 — VALUE SWEEP (Broad)
            # =====================================
            value_items = source.search(
                keywords="cars",
                entries=LISTINGS_TO_PULL,
                min_price=None,
                max_price=filters["max_price"],
                min_year=filters["min_year"],
                max_year=filters["max_year"],
                sort="price_lowest"
            )

            items = sniper_items + value_items
            total_listings += len(items)

            for item in items:

                external_id = item.get("id") or item.get("view_url")
                if not external_id:
                    continue

                # Prevent duplicate processing in same run
                if external_id in processed_ids:
                    continue

                processed_ids.add(external_id)

                deal = process_listing(
                    item,
                    dealer.id,
                    source=source_name,
                    filters=filters
                )

                if not deal:
                    continue

                total_deals += 1

                notify_deal.delay(deal.id)

        # ------------------------------------------
        # Log scan run
        # ------------------------------------------
        scan = ScanRun(
            dealer_id=dealer.id,
            source="multi_mode_scan",
            listings_found=total_listings,
            deals_saved=total_deals
        )

        db.add(scan)
        db.commit()

        return {
            "listings_found": total_listings,
            "deals_saved": total_deals
        }

    finally:
        db.close()