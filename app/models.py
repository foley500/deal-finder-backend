from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, JSON
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

Base = declarative_base()


# ==========================================
# DEALER
# ==========================================

class Dealer(Base):
    __tablename__ = "dealers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)

    deals = relationship("Deal", back_populates="dealer")
    scans = relationship("ScanRun", back_populates="dealer")
    settings = relationship("DealerSettings", back_populates="dealer", uselist=False)


# ==========================================
# DEALER SETTINGS
# ==========================================

class DealerSettings(Base):
    __tablename__ = "dealer_settings"

    id = Column(Integer, primary_key=True, index=True)
    dealer_id = Column(Integer, ForeignKey("dealers.id"), unique=True)

    min_year = Column(Integer, default=2014)
    max_year = Column(Integer, default=2024)
    max_mileage = Column(Integer, default=120000)

    min_profit = Column(Float, default=500)
    min_score = Column(Float, default=10)

    required_keywords = Column(JSON, default=list)
    excluded_keywords = Column(JSON, default=list)
    allowed_body_types = Column(JSON, default=list)

    dealer = relationship("Dealer", back_populates="settings")

    # ==========================================
# VALUATIONS
# ==========================================

class Valuation(Base):
    __tablename__ = "valuations"

    id = Column(Integer, primary_key=True, index=True)
    registration = Column(String, unique=True, index=True)
    market_value = Column(Float)
    source = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


# ==========================================
# DEALS
# ==========================================

class Deal(Base):
    __tablename__ = "deals"

    id = Column(Integer, primary_key=True, index=True)

    dealer_id = Column(Integer, ForeignKey("dealers.id"))
    external_id = Column(String, index=True)

    title = Column(String)
    reg = Column(String)
    mileage = Column(Integer)

    listing_price = Column(Float)
    market_value = Column(Float)
    profit = Column(Float)
    risk_penalty = Column(Float)
    score = Column(Float)

    report = Column(JSON)

    source = Column(String)
    status = Column(String, default="new")

    created_at = Column(DateTime, default=datetime.utcnow)

    dealer = relationship("Dealer", back_populates="deals")


# ==========================================
# SCAN RUNS
# ==========================================

class ScanRun(Base):
    __tablename__ = "scan_runs"

    id = Column(Integer, primary_key=True, index=True)

    dealer_id = Column(Integer, ForeignKey("dealers.id"))
    source = Column(String)

    listings_found = Column(Integer)
    deals_saved = Column(Integer)

    created_at = Column(DateTime, default=datetime.utcnow)

    dealer = relationship("Dealer", back_populates="scans")