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

        # 🔥 Dynamic filters from dashboard
        filters = {
            "min_year": settings.min_year,
            "max_year": settings.max_year,
            "max_mileage": settings.max_mileage,
            "max_price": settings.max_price or 4000,
            "min_profit": settings.min_profit,
            "min_score": settings.min_score,
        }

        total_listings = 0
        total_deals = 0

        # --------------------------------------
        # SCAN CONFIG
        # --------------------------------------
        SNIPER_PAGES = 2          # newest first
        VALUE_SWEEP_PAGES = 5     # deeper sweep
        LISTINGS_PER_PAGE = 40

        KEYWORDS = [
            "cars",
            "hatchback",
            "manual",
            "automatic",
            "petrol",
            "diesel",
            "salvage",
            "spares or repair",
            "non runner",
        ]

        for source_name in SOURCES:

            source = get_listing_source(source_name)

            # ==================================================
            # 🟢 PASS 1 — SNIPER MODE (NEWEST LISTINGS FIRST)
            # ==================================================
            print("🚀 SNIPER MODE — Newly Listed")

            for keyword in KEYWORDS:
                for page in range(1, SNIPER_PAGES + 1):

                    items = source.search(
                        keywords=keyword,
                        entries=LISTINGS_PER_PAGE,
                        page=page,
                        sort="newlyListed",
                        min_price=None,
                        max_price=filters["max_price"],
                        min_year=filters["min_year"],
                        max_year=filters["max_year"],
                    )

                    total_listings += len(items)

                    for item in items:

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

            # ==================================================
            # 🔵 PASS 2 — VALUE SWEEP MODE (PRICE SORTED)
            # ==================================================
            print("🔎 VALUE SWEEP MODE — Price Sorted")

            for keyword in KEYWORDS:
                for page in range(1, VALUE_SWEEP_PAGES + 1):

                    items = source.search(
                        keywords=keyword,
                        entries=LISTINGS_PER_PAGE,
                        page=page,
                        sort="price",
                        min_price=None,
                        max_price=filters["max_price"],
                        min_year=filters["min_year"],
                        max_year=filters["max_year"],
                    )

                    total_listings += len(items)

                    for item in items:

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

        # --------------------------------------
        # SAVE SCAN LOG
        # --------------------------------------
        scan = ScanRun(
            dealer_id=dealer.id,
            source="multi_source_api",
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