from fastapi import APIRouter, Request, Form, Depends
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import DealerSettings
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ===============================
# VIEW SETTINGS PAGE
# ===============================
@router.get("/settings/{dealer_id}")
def view_settings(request: Request, dealer_id: int, db: Session = Depends(get_db)):

    settings = db.query(DealerSettings).filter(
        DealerSettings.dealer_id == dealer_id
    ).first()

    if not settings:
        settings = DealerSettings(dealer_id=dealer_id)
        db.add(settings)
        db.commit()
        db.refresh(settings)

    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "settings": settings,
            "dealer_id": dealer_id
        }
    )


# ===============================
# UPDATE SETTINGS (Dashboard + Settings Page)
# ===============================
@router.post("/settings/{dealer_id}")
@router.post("/dealer/{dealer_id}/settings")
def update_settings(
    dealer_id: int,
    min_year: int = Form(...),
    max_year: int = Form(...),
    max_mileage: int = Form(...),
    min_profit: float = Form(...),
    min_score: float = Form(...),
    db: Session = Depends(get_db)
):

    settings = db.query(DealerSettings).filter(
        DealerSettings.dealer_id == dealer_id
    ).first()

    if not settings:
        settings = DealerSettings(dealer_id=dealer_id)
        db.add(settings)

    settings.min_year = min_year
    settings.max_year = max_year
    settings.max_mileage = max_mileage
    settings.min_profit = min_profit
    settings.min_score = min_score

    db.commit()

    return RedirectResponse(
        url="/",
        status_code=303
    )