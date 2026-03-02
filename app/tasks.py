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
SOURCES = ["search_ebay_browse"]

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
            settings = DealerSettings(dealer_id=dealer.id)
            db.add(settings)
            db.commit()
            db.refresh(settings)

        filters = {
            "min_year": settings.min_year,
            "max_year": settings.max_year,
            "max_mileage": settings.max_mileage,
            "min_profit": settings.min_profit,
            "min_score": settings.min_score,
            "required_keywords": settings.required_keywords or [],
            "excluded_keywords": settings.excluded_keywords or [],
            "allowed_body_types": settings.allowed_body_types or [],
        }

        total_listings = 0
        total_deals = 0

        for source_name in SOURCES:

            source = get_listing_source(source_name)

            items = source.search(
                keywords="cars",
                entries=5,
                min_price=None,
                max_price=None,
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

                if TEST_MODE:
                    total_deals += 1
                    notify_deal.delay(deal.id)
                    continue

                if (
                    deal.status in ["high", "very_high"]
                    and deal.profit >= settings.min_profit
                    and deal.score >= settings.min_score
                ):
                    total_deals += 1
                    notify_deal.delay(deal.id)

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


# ==========================================
# FACEBOOK EMAIL INGESTION TASK
# ==========================================
@celery.task
def scan_facebook_email(dealer_id: int):

    db = SessionLocal()

    try:
        dealer = db.query(Dealer).filter(Dealer.id == dealer_id).first()
        if not dealer:
            return

        settings = db.query(DealerSettings).filter(
            DealerSettings.dealer_id == dealer.id
        ).first()

        if not settings:
            return

        filters = {
            "min_year": settings.min_year,
            "max_year": settings.max_year,
            "max_mileage": settings.max_mileage,
            "min_profit": settings.min_profit,
            "min_score": settings.min_score,
            "required_keywords": settings.required_keywords or [],
            "excluded_keywords": settings.excluded_keywords or [],
            "allowed_body_types": settings.allowed_body_types or [],
        }

        # 🔐 Load credentials safely from environment
        host = os.getenv("FACEBOOK_EMAIL_HOST")
        username = os.getenv("FACEBOOK_EMAIL_USER")
        password = os.getenv("FACEBOOK_EMAIL_PASS")

        if not all([host, username, password]):
            print("❌ Facebook email credentials missing")
            return

        ingestion = FacebookEmailIngestion(
            host=host,
            username=username,
            password=password,
        )

        urls = ingestion.fetch_listing_urls()

        total_deals = 0

        for url in urls:

            raw_item = parse_facebook_listing(url)

            deal = process_listing(
                raw_item,
                dealer.id,
                source="facebook_email",
                filters=filters
            )

            if not deal:
                continue

            if (
                deal.status in ["high", "very_high"]
                and deal.profit >= settings.min_profit
                and deal.score >= settings.min_score
            ):
                total_deals += 1
                notify_deal.delay(deal.id)

        scan = ScanRun(
            dealer_id=dealer.id,
            source="facebook_email",
            listings_found=len(urls),
            deals_saved=total_deals
        )

        db.add(scan)
        db.commit()

        return {"facebook_email_deals": total_deals}

    finally:
        db.close()