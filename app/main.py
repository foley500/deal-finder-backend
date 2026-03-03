import base64
import requests
import os
import re
import io
import hashlib
import time

from PIL import Image
from app.routes import settings
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
app.include_router(settings.router)
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
    db: Session = Depends(get_db)
):

    query = db.query(Deal)

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

    deals = query.all()

    return templates.TemplateResponse(
        "all_deals.html",
        {"request": request, "deals": deals},
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
        return templates.TemplateResponse(
            "deal_detail.html",
            {
                "request": request,
                "deal": None,
                "report": {},
                "error": "Deal not found"
            },
        )

    raw_report = deal.report or {}

    normalized_report = {
        # Financial
        "financials": raw_report.get("financials") or {},

        # 🔥 NEW — required for eBay sold valuation
        "market_model": raw_report.get("market_model") or {},

        # MOT
        "mot_summary": raw_report.get("mot_summary") or {},
        "mot_full_data": raw_report.get("mot_full_data") or [],

        # Risk + scoring
        "risk_breakdown": raw_report.get("risk_breakdown") or {},
        "scoring": raw_report.get("scoring") or {},

        # Listing
        "listing_details": raw_report.get("listing_details") or {},
        "listing_url": raw_report.get("listing_url"),
        "seller": raw_report.get("seller"),
        "location": raw_report.get("location"),
    }

    return templates.TemplateResponse(
        "deal_detail.html",
        {
            "request": request,
            "deal": deal,
            "report": normalized_report,
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
# FACEBOOK INGESTION
# =====================================================

@app.post("/ingest/facebook")
def ingest_facebook(
    data: dict = Body(...),
    db: Session = Depends(get_db)
):

    dealer_id = 1
    detected_plate = None

    image_base64 = data.get("image_base64")
    PLATE_API_KEY = os.getenv("PLATE_API_KEY")

    if image_base64 and PLATE_API_KEY:
        try:
            header, encoded = image_base64.split(",", 1)
            image_bytes = base64.b64decode(encoded)

            response = requests.post(
                "https://api.platerecognizer.com/v1/plate-reader/",
                headers={"Authorization": f"Token {PLATE_API_KEY}"},
                files={"upload": ("image.jpg", image_bytes)},
                data={"regions": ["gb"]},
                timeout=20,
            )

            result = response.json()

            if result.get("results"):
                raw_plate = result["results"][0]["plate"].upper()
                if is_valid_uk_plate(raw_plate):
                    detected_plate = raw_plate

        except Exception:
            pass

    if detected_plate:
        data["registration"] = detected_plate

    deal = process_listing(
        raw_item=data,
        dealer_id=dealer_id,
        source="facebook_extension",
    )

    return {"status": "processed" if deal else "filtered"}


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

        # Create default settings
        settings = DealerSettings(
            dealer_id=dealer.id
        )

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