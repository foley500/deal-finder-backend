import base64
import requests
import os
import re
import io
import hashlib
import time

from PIL import Image
from app.routes import settings as settings_router
from fastapi import FastAPI, Depends, Request, Query, Body, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware

from sqlalchemy.orm import Session
from sqlalchemy import func

from app.database import engine, SessionLocal
from app.models import Base, Dealer, DealerSettings, Deal, ScanRun
from app.tasks import notify_deal, scan_sniper, scan_value_sweep
from app.services.deal_engine import process_listing
from app.services.ebay_browse_service import search_ebay_browse

# =====================================================
# APP SETUP
# =====================================================

Base.metadata.create_all(bind=engine)

app = FastAPI()
app.include_router(settings_router.router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")


# =====================================================
# DATABASE DEPENDENCY
# =====================================================

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# =====================================================
# NAV COUNTS — used by sidebar badges on every page
# =====================================================

def get_nav_counts(db: Session) -> dict:
    all_deals = db.query(Deal).count()
    ebay_deals = db.query(Deal).filter(Deal.source == "ebay_browse").count()
    facebook_deals = db.query(Deal).filter(Deal.source == "facebook_extension").count()
    van_deals = db.query(Deal).filter(Deal.source == "ebay_vans").count()
    price_drop_count = db.query(Deal).filter(
        Deal.report["deal_signals"]["is_price_drop_alert"].astext == "true"
    ).count()
    return {
        "all_deals": all_deals,
        "ebay_deals": ebay_deals,
        "facebook_deals": facebook_deals,
        "van_deals": van_deals,
        "price_drop_count": price_drop_count,
    }


# =====================================================
# UK PLATE HELPERS
# =====================================================

def normalise_uk_plate(raw_plate: str) -> str:
    return raw_plate.upper().strip()


def is_valid_uk_plate(plate: str) -> bool:
    return bool(re.match(r"^[A-Z]{2}[0-9]{2}[A-Z]{3}$", plate))


# =====================================================
# DASHBOARD
# =====================================================

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(get_db)):

    total_deals = db.query(Deal).count()
    average_profit = db.query(func.avg(Deal.profit)).scalar() or 0
    best_profit = db.query(func.max(Deal.profit)).scalar() or 0
    high_confidence = db.query(Deal).filter(
        Deal.status.in_(["high", "very_high"])
    ).count()

    last_scan = db.query(ScanRun).order_by(
        ScanRun.created_at.desc()
    ).first()

    last_scan_time = last_scan.created_at if last_scan else None

    deals = db.query(Deal).order_by(
        Deal.created_at.desc()
    ).limit(20).all()

    dealers = db.query(Dealer).all()

    # 🔥 LOAD OR CREATE SETTINGS
    settings = db.query(DealerSettings).filter(
        DealerSettings.dealer_id == 1
    ).first()

    if not settings:
        settings = DealerSettings(dealer_id=1)
        db.add(settings)
        db.commit()
        db.refresh(settings)

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "dealers": dealers,
            "deals": deals,
            "total_deals": total_deals,
            "average_profit": average_profit,
            "best_profit": best_profit,
            "high_confidence": high_confidence,
            "last_scan_time": last_scan_time,
            "settings": settings,   # 🔥 THIS IS CRITICAL
            "nav_counts": get_nav_counts(db),
        }
    )


# =====================================================
# ALL DEALS
# =====================================================

@app.get("/deals", response_class=HTMLResponse)
def all_deals(
    request: Request,
    sort: str | None = Query(None),
    confidence: str | None = Query(None),
    source: str | None = Query(None),
    page: int = Query(1, ge=1),
    db: Session = Depends(get_db)
):
    PAGE_SIZE = 25

    query = db.query(Deal)

    if source:
        query = query.filter(Deal.source == source)

    if confidence:
        query = query.filter(Deal.status == confidence)

    if sort == "profit_desc":
        query = query.order_by(Deal.profit.desc())
    elif sort == "profit_asc":
        query = query.order_by(Deal.profit.asc())
    elif sort == "score_desc":
        query = query.order_by(Deal.score.desc())
    else:
        query = query.order_by(Deal.created_at.desc())

    total_count = query.count()
    total_pages = max(1, (total_count + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(page, total_pages)

    deals = query.offset((page - 1) * PAGE_SIZE).limit(PAGE_SIZE).all()

    dealer_settings = db.query(DealerSettings).filter(DealerSettings.dealer_id == 1).first()

    return templates.TemplateResponse(
        "all_deals.html",
        {
            "request": request,
            "deals": deals,
            "nav_counts": get_nav_counts(db),
            "active_source": source,
            "page": page,
            "total_pages": total_pages,
            "total_count": total_count,
            "dealer_settings": dealer_settings,
        },
    )



# =====================================================
# VANS
# =====================================================

@app.get("/vans", response_class=HTMLResponse)
def van_deals(
    request: Request,
    sort: str | None = Query(None),
    page: int = Query(1, ge=1),
    db: Session = Depends(get_db)
):
    PAGE_SIZE = 25

    query = db.query(Deal).filter(Deal.source == "ebay_vans")

    if sort == "profit_desc":
        query = query.order_by(Deal.profit.desc())
    elif sort == "profit_asc":
        query = query.order_by(Deal.profit.asc())
    elif sort == "score_desc":
        query = query.order_by(Deal.score.desc())
    else:
        query = query.order_by(Deal.created_at.desc())

    total_count = query.count()
    total_pages = max(1, (total_count + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(page, total_pages)

    deals = query.offset((page - 1) * PAGE_SIZE).limit(PAGE_SIZE).all()

    return templates.TemplateResponse(
        "vans.html",
        {
            "request": request,
            "deals": deals,
            "nav_counts": get_nav_counts(db),
            "page": page,
            "total_pages": total_pages,
            "total_count": total_count,
        },
    )


# =====================================================
# DELETE DEALS
# =====================================================

@app.post("/deals/delete")
def delete_deals(
    deal_ids: list[int] = Form(...),
    db: Session = Depends(get_db)
):
    deals = db.query(Deal).filter(Deal.id.in_(deal_ids)).all()

    for deal in deals:
        db.delete(deal)

    db.commit()

    return RedirectResponse(url="/deals", status_code=303)


# =====================================================
# DEAL DETAIL
# =====================================================
@app.get("/deals/{deal_id}", response_class=HTMLResponse)
def deal_detail(
    deal_id: int,
    request: Request,
    db: Session = Depends(get_db)
):

    deal = db.query(Deal).filter(Deal.id == deal_id).first()

    if not deal:
        return {"error": "Deal not found"}

    raw_report = deal.report or {}

    normalized_report = {
        "financials": raw_report.get("financials") or {
            "listing_price": 0,
            "market_value": 0,
            "gross_profit": 0,
            "net_profit": 0,
            "est_transport": None,
            "est_prep": None,
            "est_warranty": None,
            "est_total_costs": None,
        },
        "market_model": raw_report.get("market_model") or {
            "market_price": None,
            "source": None,
            "sample_size": None,
        },
        "mot_summary": raw_report.get("mot_summary") or {
            "fail_count": 0,
            "advisory_count": 0,
            "mot_penalty": 0,
        },
        "mot_full_data": raw_report.get("mot_full_data") or [],
        "risk_breakdown": raw_report.get("risk_breakdown") or {
            "description_penalty": 0,
            "mot_penalty": 0,
            "total_risk_penalty": 0,
        },
        "scoring": raw_report.get("scoring") or {
            "score": 0,
            "confidence_level": "low",
        },
        "listing_details": raw_report.get("listing_details") or {},
        "listing_url": raw_report.get("listing_url"),
        "seller": raw_report.get("seller"),
        "location": raw_report.get("location"),
        "primary_image": raw_report.get("primary_image"),
        "deal_signals": raw_report.get("deal_signals") or {},
    }

    return templates.TemplateResponse(
        "deal_detail.html",
        {
            "request": request,
            "deal": deal,
            "report": normalized_report,
            "nav_counts": get_nav_counts(db),
        },
    )


# =====================================================
# EBAY BROWSE TEST ENDPOINT
# =====================================================

@app.get("/test-ebay-browse")
def test_ebay_browse():

    results = search_ebay_browse(
        keywords="BMW 3 Series",
        limit=5,
        min_price=1000,
        max_price=20000,
    )

    return {"results": results}


# =====================================================
# SETTINGS API — used by Chrome extension
# =====================================================

@app.get("/dealer/{dealer_id}/settings/json")
def get_settings_json(dealer_id: int, db: Session = Depends(get_db)):
    settings = db.query(DealerSettings).filter(
        DealerSettings.dealer_id == dealer_id
    ).first()
    if not settings:
        return {}
    return {
        "min_year": settings.min_year,
        "max_year": settings.max_year,
        "max_price": settings.max_price,
        "max_mileage": settings.max_mileage,
        "min_profit": settings.min_profit,
        "min_score": settings.min_score,
    }


@app.post("/dealer/{dealer_id}/settings/json")
def save_settings_json(dealer_id: int, data: dict = Body(...), db: Session = Depends(get_db)):
    settings = db.query(DealerSettings).filter(
        DealerSettings.dealer_id == dealer_id
    ).first()
    if not settings:
        settings = DealerSettings(dealer_id=dealer_id)
        db.add(settings)

    if data.get("min_year") is not None:
        settings.min_year = int(data["min_year"])
    if data.get("max_year") is not None:
        settings.max_year = int(data["max_year"])
    if data.get("max_price") is not None:
        settings.max_price = float(data["max_price"])
    if data.get("max_mileage") is not None:
        settings.max_mileage = int(data["max_mileage"])
    if data.get("min_profit") is not None:
        settings.min_profit = float(data["min_profit"])
    if data.get("min_score") is not None:
        settings.min_score = float(data["min_score"])
    if "search_radius_miles" in data:
        settings.search_radius_miles = int(data["search_radius_miles"]) if data["search_radius_miles"] is not None else None
    if "search_postcode" in data:
        settings.search_postcode = data["search_postcode"] or None

    db.commit()
    return {"status": "saved"}


# =====================================================
# FACEBOOK INGESTION — queues OCR + valuation on the Celery worker.
# Backend never loads EasyOCR — keeps backend memory lean.
# =====================================================

@app.post("/ingest/facebook")
def ingest_facebook(
    data: dict = Body(...),
    db: Session = Depends(get_db)
):
    from app.tasks import process_facebook_listing

    dealer_id = 1

    # Pre-screen cheaply before queuing — avoids burning worker cycles on obvious rejects
    settings = db.query(DealerSettings).filter(
        DealerSettings.dealer_id == dealer_id
    ).first()

    price = float(data.get("price", 0) or 0)

    if not price:
        return {"status": "filtered", "reason": "No price found on listing"}

    if price < 500:
        return {"status": "filtered", "reason": f"Price £{price} is below minimum £500"}

    if settings and settings.max_price and price > settings.max_price:
        return {"status": "filtered", "reason": f"Price £{price} exceeds your max price filter of £{settings.max_price}"}

    # Queue full OCR + DVSA + valuation pipeline on worker
    task = process_facebook_listing.delay(data, dealer_id)

    return {"status": "queued", "task_id": task.id}


# =====================================================
# HEALTH
# =====================================================

@app.get("/health")
def health():
    return {"status": "ok"}


# =====================================================
# EBAY MARKETPLACE DELETION COMPLIANCE
# =====================================================

VERIFICATION_TOKEN = "deal_finder_ebay_verify_2026_X9kLmP7qT2vR8z"
ENDPOINT_URL = "https://deal-finder-backend-mhrj.onrender.com/ebay/marketplace-deletion"


@app.api_route(
    "/ebay/marketplace-deletion",
    methods=["GET", "POST", "HEAD"]
)
async def ebay_marketplace_deletion(request: Request):

    if request.method == "HEAD":
        return JSONResponse(content={})

    challenge_code = request.query_params.get("challenge_code")

    if challenge_code:
        combined = challenge_code + VERIFICATION_TOKEN + ENDPOINT_URL
        challenge_response = hashlib.sha256(
            combined.encode("utf-8")
        ).hexdigest()

        return JSONResponse({"challengeResponse": challenge_response})

    try:
        data = await request.json()
        body_challenge = data.get("challengeCode")

        if body_challenge:
            combined = body_challenge + VERIFICATION_TOKEN + ENDPOINT_URL
            challenge_response = hashlib.sha256(
                combined.encode("utf-8")
            ).hexdigest()

            return JSONResponse({"challengeResponse": challenge_response})

    except Exception:
        pass

    return JSONResponse({"status": "received"})


@app.post("/ingest/ebay")
def ingest_ebay(db: Session = Depends(get_db)):

    dealer_id = 1

    listings = search_ebay_browse(
        limit=50,
        min_price=1000,
        max_price=50000
    )

    saved = 0

    for item in listings:

        deal = process_listing(
            raw_item=item,
            dealer_id=dealer_id,
            source="ebay",
            filters={
                "min_year": 2012,
                "max_mileage": 90000,
                "min_profit": 1000,
                "min_score": 30,
            }
        )

        if deal:
            saved += 1

    return {
        "fetched": len(listings),
        "saved": saved
    }


@app.get("/test-ebay-scan")
def test_ebay_scan():
    scan_sniper.delay(1)
    scan_value_sweep.delay(1)
    return {"status": "Both scans triggered"}


@app.post("/dealer/{dealer_id}/scan")
def run_market_scan(dealer_id: int):
    scan_sniper.delay(dealer_id)
    return RedirectResponse(url="/", status_code=303)


@app.post("/dealer/{dealer_id}/value-sweep")
def run_value_sweep(dealer_id: int):
    scan_value_sweep.delay(dealer_id)
    return RedirectResponse(url="/", status_code=303)


@app.get("/create-test-dealer")
def create_test_dealer():

    db = SessionLocal()

    try:
        # Check if dealer already exists
        existing = db.query(Dealer).filter(Dealer.name == "Test Dealer").first()
        if existing:
            return {"status": "Dealer already exists", "dealer_id": existing.id}

        dealer = Dealer(
            name="Test Dealer"
        )
        db.add(dealer)
        db.commit()
        db.refresh(dealer)

        settings = DealerSettings(dealer_id=dealer.id)
        db.add(settings)
        db.commit()

        return {
            "status": "Test dealer created",
            "dealer_id": dealer.id
        }

    finally:
        db.close()


@app.get("/wipe-ebay")
def wipe_ebay(db: Session = Depends(get_db)):
    db.query(Deal).filter(Deal.source == "ebay_browse").delete()
    db.commit()
    return {"status": "eBay deals deleted"}