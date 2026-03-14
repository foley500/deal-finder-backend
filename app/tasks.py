import gc
import os
import redis
import time
import random
from app.celery_app import celery
from app.database import SessionLocal
from app.models import Dealer, DealerSettings, ScanRun
from app.services.deal_engine import process_listing
from app.services.listing_sources.factory import get_listing_source
from app.services.pdf_service import generate_deal_pdf
from app.services.telegram_service import send_telegram_document
from app.services.ebay_browse_service import sniper_search, search_sniper_windows


# ==========================================
# SOURCES
# ==========================================
SOURCES = ["ebay_browse"]
PRICE_TRACK_KEY = "listing_price"
REDIS_URL = os.getenv("CELERY_BROKER_URL")
redis_client = redis.from_url(REDIS_URL)

# API BUDGET — REAL CALL COSTS
#
# Prewarm: ~160 models × (2 search + up to 15 detail) = ~2,720 calls max
#   In practice ~8-12 detail calls per model (only year-missing listings expanded)
#   Realistic prewarm cost: ~160 × (2 + 10) = ~1,920 calls per cold run
#   With 70% skip threshold on warm models: ~576 calls/day
#
# Sniper: 48 runs/day × 1 search = 48 search calls
#   + up to 25 expansions/run × 80% cache hit = ~5 live valuations/run
#   Live valuation = 2 search + up to 15 detail = 17 calls max
#   Total sniper: ~48 + (48 × 5 × 17) = ~4,128 — but cache hit rate higher in practice
#
# Value sweep: 6 runs/day × 20 makes × 3 searches = 360 search calls
#   + up to 60 expansions/run × 80% cache hit = ~12 live valuations/run
#   Total sweep: ~360 + (6 × 12 × 17) = ~1,584 calls
#
# BUDGET SPLIT TARGET:
#   Prewarm:    ~600 calls/day  (runs 5hr schedule, skips warm models)
#   Scans:      ~2,000 calls/day
#   Buffer:     ~1,900 remaining
#   Total:      ~4,500 vs 5,000 limit ✅
#
# The key fix: _pre_expand_details now only expands on MISSING YEAR (not mileage).
# Missing mileage is handled gracefully by the filter layer (weight 1, not rejected).
# This cuts prewarm detail calls from ~60/model to ~10/model.
SNIPER_LIMIT = 10        # Expansions per sniper run — cache handles the rest
DAILY_API_BUDGET = 4500  # Hard ceiling — 500 buffer vs 5,000 eBay limit
DAILY_BUDGET_KEY = "ebay_daily_calls"
VALUE_SWEEP_LIMIT = 30   # Expansions per sweep run

# ==========================================
# SCAN ROTATION QUERIES
#
# Both scans rotate through these make groups on each run rather than
# using the static keyword "car". This ensures every scan sees a fresh
# slice of eBay inventory — eBay caches and stabilises results for
# broad generic queries, so "newlyListed" with keyword "car" returns
# the same ~20 cars every 10 minutes.
#
# Sniper: picks the next group in sequence on each run (Redis pointer).
# Value sweep: scans ALL groups per run (runs every 4hrs, has the budget).
#
# Group sizes kept small so eBay returns tight, relevant results.
# ==========================================
SCAN_QUERY_GROUPS = [
    "Ford",
    "Vauxhall",
    "Volkswagen",
    "Audi",
    "BMW",
    "Mercedes",
    "Toyota",
    "Nissan",
    "Honda",
    "Hyundai",
    "Kia",
    "Land Rover",
    "Peugeot",
    "Renault",
    "Skoda",
    "Seat",
    "Mazda",
    "Volvo",
    "Mini",
    "Citroen",
]

YEAR_SNIPER_QUERIES = [
    "2013",
    "2014",
    "2015",
    "2016",
    "2017",
    "2018",
]

GENERIC_SNIPER_QUERIES = [
    "cheap car",
    "good runner",
    "first car",
    "diesel car",
    "petrol car",
    "cheap vehicle",
    "cheap hatchback",
    "cheap automatic",
]

SNIPER_ROTATION_KEY = "sniper_query_rotation_idx"
# ==========================================
# VAN SCAN QUERY GROUPS
# Used by van sniper — rotated per run.
# ==========================================
VAN_SCAN_QUERY_GROUPS = [
    'Ford Transit',
    'Ford Transit Custom',
    'Ford Transit Connect',
    'Volkswagen Transporter',
    'Volkswagen Crafter',
    'Mercedes-Benz Sprinter',
    'Mercedes-Benz Vito',
    'Vauxhall Vivaro',
    'Vauxhall Movano',
    'Peugeot Expert',
    'Peugeot Boxer',
    'Citroen Dispatch',
    'Citroen Relay',
    'Renault Trafic',
    'Renault Master',
    'Nissan NV200',
    'Nissan NV400',
    'Toyota Proace',
    'Fiat Ducato',
    'Fiat Doblo',
]

VAN_SNIPER_ROTATION_KEY = "van_sniper_rotation_idx"

# ==========================================
# VAN PREWARM TARGETS
# Higher mileage buckets vs cars — vans
# regularly trade at 100-200k miles.
# ==========================================
VAN_PREWARM_TARGETS = [

    # ── FORD ──────────────────────────────────────────────────────────────
    ("Ford", "Transit",         [2014, 2015, 2016, 2017, 2018, 2019, 2020], [60000, 80000, 100000, 120000, 140000, 160000, 180000, 200000]),
    ("Ford", "Transit Custom",  [2014, 2015, 2016, 2017, 2018, 2019, 2020], [60000, 80000, 100000, 120000, 140000, 160000]),
    ("Ford", "Transit Connect", [2014, 2015, 2016, 2017, 2018, 2019],       [40000, 60000, 80000, 100000, 120000]),

    # ── VOLKSWAGEN ────────────────────────────────────────────────────────
    ("Volkswagen", "Transporter", [2013, 2014, 2015, 2016, 2017, 2018, 2019], [60000, 80000, 100000, 120000, 140000, 160000]),
    ("Volkswagen", "Crafter",     [2013, 2014, 2015, 2016, 2017, 2018],       [60000, 80000, 100000, 120000, 140000]),
    ("Volkswagen", "Caddy",       [2013, 2014, 2015, 2016, 2017, 2018],       [40000, 60000, 80000, 100000, 120000]),

    # ── MERCEDES-BENZ ─────────────────────────────────────────────────────
    ("Mercedes-Benz", "Sprinter", [2014, 2015, 2016, 2017, 2018, 2019],       [60000, 80000, 100000, 120000, 140000, 160000]),
    ("Mercedes-Benz", "Vito",     [2014, 2015, 2016, 2017, 2018, 2019],       [60000, 80000, 100000, 120000, 140000]),

    # ── VAUXHALL ──────────────────────────────────────────────────────────
    ("Vauxhall", "Vivaro",  [2014, 2015, 2016, 2017, 2018, 2019],             [60000, 80000, 100000, 120000, 140000]),
    ("Vauxhall", "Movano",  [2013, 2014, 2015, 2016, 2017, 2018],             [60000, 80000, 100000, 120000, 140000, 160000]),

    # ── PEUGEOT / CITROEN / FIAT ──────────────────────────────────────────
    ("Peugeot",  "Expert", [2014, 2015, 2016, 2017, 2018, 2019],              [60000, 80000, 100000, 120000, 140000]),
    ("Peugeot",  "Boxer",  [2013, 2014, 2015, 2016, 2017, 2018],              [60000, 80000, 100000, 120000, 140000, 160000]),
    ("Citroen",  "Dispatch",[2014, 2015, 2016, 2017, 2018, 2019],             [60000, 80000, 100000, 120000, 140000]),
    ("Citroen",  "Relay",   [2013, 2014, 2015, 2016, 2017, 2018],             [60000, 80000, 100000, 120000, 140000, 160000]),
    ("Fiat",     "Ducato",  [2013, 2014, 2015, 2016, 2017, 2018],             [60000, 80000, 100000, 120000, 140000, 160000]),
    ("Fiat",     "Doblo",   [2013, 2014, 2015, 2016, 2017, 2018],             [40000, 60000, 80000, 100000, 120000]),

    # ── RENAULT ───────────────────────────────────────────────────────────
    ("Renault",  "Trafic",  [2014, 2015, 2016, 2017, 2018, 2019],             [60000, 80000, 100000, 120000, 140000]),
    ("Renault",  "Master",  [2013, 2014, 2015, 2016, 2017, 2018],             [60000, 80000, 100000, 120000, 140000, 160000]),

    # ── NISSAN ────────────────────────────────────────────────────────────
    ("Nissan",   "NV200",   [2013, 2014, 2015, 2016, 2017, 2018],             [40000, 60000, 80000, 100000, 120000]),
    ("Nissan",   "NV400",   [2013, 2014, 2015, 2016, 2017, 2018],             [60000, 80000, 100000, 120000, 140000]),

    # ── TOYOTA ────────────────────────────────────────────────────────────
    ("Toyota",   "Proace",  [2016, 2017, 2018, 2019, 2020],                   [40000, 60000, 80000, 100000, 120000]),
]



# ==========================================
# FULL UK MARKET PREWARM TARGETS
#
# Format: (make, base_model, years, mileage_buckets)
#
# One eBay search per (make, base_model) entry — NOT per year/mileage.
# The prewarm fetches results once per model then filters across all
# year/mileage buckets at zero additional API cost.
#
# IMPORTANT: mileage buckets must be multiples of 20,000 to match
# the cache key rounding in market_valuation_service.py:
#   mileage_bucket = round(mileage / 20000) * 20000
# Any odd bucket (e.g. 30000, 50000) will never be looked up by scans.
#
# API budget: ~160 models × 2 calls = ~320 calls per cold prewarm.
# With 5hr schedule and skip-if-cached (70% warm threshold), daily
# refresh cost is typically ~80-120 calls leaving ~4,400/day for scanning.
# ==========================================
PREWARM_TARGETS = [

    # ── FORD ──────────────────────────────────────────────────────────────
    ("Ford", "Fiesta",   [2013, 2014, 2015, 2016, 2017, 2018, 2019],         [20000, 40000, 60000, 80000, 100000, 120000]),
    ("Ford", "Focus",    [2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019],   [20000, 40000, 60000, 80000, 100000, 120000, 140000, 160000]),
    ("Ford", "Ka",       [2010, 2011, 2012, 2013, 2014, 2015, 2016],         [20000, 40000, 60000, 80000, 100000]),
    ("Ford", "Kuga",     [2013, 2014, 2015, 2016, 2017, 2018, 2019],         [20000, 40000, 60000, 80000, 100000, 120000]),
    ("Ford", "Mondeo",   [2013, 2014, 2015, 2016, 2017, 2018],               [20000, 40000, 60000, 80000, 100000, 120000]),
    ("Ford", "EcoSport", [2014, 2015, 2016, 2017, 2018, 2019],               [20000, 40000, 60000, 80000]),
    ("Ford", "Puma",     [2019, 2020, 2021, 2022],                           [20000, 40000, 60000]),
    ("Ford", "S-Max",    [2013, 2014, 2015, 2016, 2017, 2018],               [20000, 40000, 60000, 80000, 100000, 120000]),
    ("Ford", "Galaxy",   [2013, 2014, 2015, 2016, 2017, 2018],               [20000, 40000, 60000, 80000, 100000, 120000]),
    ("Ford", "Ranger",   [2013, 2014, 2015, 2016, 2017, 2018, 2019],         [40000, 60000, 80000, 100000, 120000]),
    ("Ford", "Transit",  [2014, 2015, 2016, 2017, 2018, 2019],               [60000, 80000, 100000, 120000, 140000, 160000]),

    # ── VAUXHALL ──────────────────────────────────────────────────────────
    ("Vauxhall", "Corsa",     [2013, 2014, 2015, 2016, 2017, 2018, 2019],    [20000, 40000, 60000, 80000, 100000, 120000]),
    ("Vauxhall", "Astra",     [2013, 2014, 2015, 2016, 2017, 2018],          [20000, 40000, 60000, 80000, 100000, 120000]),
    ("Vauxhall", "Insignia",  [2013, 2014, 2015, 2016, 2017, 2018],          [40000, 60000, 80000, 100000, 120000]),
    ("Vauxhall", "Mokka",     [2013, 2014, 2015, 2016, 2017, 2018],          [20000, 40000, 60000, 80000, 100000]),
    ("Vauxhall", "Zafira",    [2012, 2013, 2014, 2015, 2016, 2017],          [40000, 60000, 80000, 100000]),
    ("Vauxhall", "Meriva",    [2012, 2013, 2014, 2015, 2016],                [20000, 40000, 60000, 80000, 100000]),
    ("Vauxhall", "Crossland", [2017, 2018, 2019, 2020],                      [20000, 40000, 60000, 80000]),
    ("Vauxhall", "Grandland", [2017, 2018, 2019, 2020],                      [20000, 40000, 60000, 80000]),
    ("Vauxhall", "Vivaro",    [2014, 2015, 2016, 2017, 2018, 2019],          [60000, 80000, 100000, 120000]),

    # ── VOLKSWAGEN ────────────────────────────────────────────────────────
    ("Volkswagen", "Golf",    [2013, 2014, 2015, 2016, 2017, 2018, 2019],    [20000, 40000, 60000, 80000, 100000]),
    ("Volkswagen", "Polo",    [2013, 2014, 2015, 2016, 2017, 2018],          [20000, 40000, 60000, 80000]),
    ("Volkswagen", "Passat",  [2013, 2014, 2015, 2016, 2017, 2018],          [40000, 60000, 80000, 100000, 120000]),
    ("Volkswagen", "Tiguan",  [2014, 2015, 2016, 2017, 2018, 2019],          [20000, 40000, 60000, 80000, 100000]),
    ("Volkswagen", "Up",      [2012, 2013, 2014, 2015, 2016, 2017, 2018],    [20000, 40000, 60000, 80000]),
    ("Volkswagen", "Touareg", [2013, 2014, 2015, 2016, 2017, 2018],          [40000, 60000, 80000, 100000]),
    ("Volkswagen", "Sharan",  [2012, 2013, 2014, 2015, 2016, 2017],          [40000, 60000, 80000, 100000]),
    ("Volkswagen", "Caddy",   [2013, 2014, 2015, 2016, 2017, 2018],          [40000, 60000, 80000, 100000]),
    ("Volkswagen", "Touran",  [2015, 2016, 2017, 2018, 2019],                [20000, 40000, 60000, 80000, 100000]),
    ("Volkswagen", "T-Roc",   [2018, 2019, 2020, 2021],                      [20000, 40000, 60000]),

    # ── AUDI ──────────────────────────────────────────────────────────────
    ("Audi", "A1", [2013, 2014, 2015, 2016, 2017, 2018],                     [20000, 40000, 60000, 80000]),
    ("Audi", "A3", [2013, 2014, 2015, 2016, 2017, 2018, 2019],               [20000, 40000, 60000, 80000, 100000]),
    ("Audi", "A4", [2013, 2014, 2015, 2016, 2017, 2018],                     [20000, 40000, 60000, 80000, 100000, 120000]),
    ("Audi", "A5", [2013, 2014, 2015, 2016, 2017, 2018],                     [20000, 40000, 60000, 80000, 100000]),
    ("Audi", "A6", [2013, 2014, 2015, 2016, 2017, 2018],                     [40000, 60000, 80000, 100000, 120000]),
    ("Audi", "Q2", [2016, 2017, 2018, 2019, 2020],                           [20000, 40000, 60000]),
    ("Audi", "Q3", [2013, 2014, 2015, 2016, 2017, 2018],                     [20000, 40000, 60000, 80000]),
    ("Audi", "Q5", [2013, 2014, 2015, 2016, 2017, 2018],                     [20000, 40000, 60000, 80000, 100000]),
    ("Audi", "TT", [2013, 2014, 2015, 2016, 2017, 2018],                     [20000, 40000, 60000, 80000]),

    # ── BMW ───────────────────────────────────────────────────────────────
    ("Bmw", "1 Series", [2012, 2013, 2014, 2015, 2016, 2017, 2018],          [20000, 40000, 60000, 80000, 100000, 120000]),
    ("Bmw", "2 Series", [2014, 2015, 2016, 2017, 2018, 2019],                [20000, 40000, 60000, 80000]),
    ("Bmw", "3 Series", [2012, 2013, 2014, 2015, 2016, 2017, 2018],          [20000, 40000, 60000, 80000, 100000, 120000]),
    ("Bmw", "4 Series", [2014, 2015, 2016, 2017, 2018],                      [20000, 40000, 60000, 80000]),
    ("Bmw", "5 Series", [2012, 2013, 2014, 2015, 2016, 2017],                [40000, 60000, 80000, 100000, 120000]),
    ("Bmw", "X1",       [2013, 2014, 2015, 2016, 2017, 2018],                [20000, 40000, 60000, 80000]),
    ("Bmw", "X3",       [2013, 2014, 2015, 2016, 2017, 2018],                [20000, 40000, 60000, 80000, 100000]),
    ("Bmw", "X5",       [2013, 2014, 2015, 2016, 2017],                      [40000, 60000, 80000, 100000]),

    # ── MERCEDES-BENZ ─────────────────────────────────────────────────────
    ("Mercedes-Benz", "A-Class",  [2013, 2014, 2015, 2016, 2017, 2018],      [20000, 40000, 60000, 80000]),
    ("Mercedes-Benz", "B-Class",  [2013, 2014, 2015, 2016, 2017, 2018],      [20000, 40000, 60000, 80000]),
    ("Mercedes-Benz", "C-Class",  [2013, 2014, 2015, 2016, 2017, 2018],      [20000, 40000, 60000, 80000, 100000]),
    ("Mercedes-Benz", "E-Class",  [2013, 2014, 2015, 2016, 2017, 2018],      [40000, 60000, 80000, 100000, 120000]),
    ("Mercedes-Benz", "GLA",      [2014, 2015, 2016, 2017, 2018],            [20000, 40000, 60000, 80000]),
    ("Mercedes-Benz", "GLC",      [2015, 2016, 2017, 2018, 2019],            [20000, 40000, 60000, 80000]),
    ("Mercedes-Benz", "Gle",      [2015, 2016, 2017, 2018],                  [20000, 40000, 60000, 80000, 100000]),
    ("Mercedes-Benz", "Cla",      [2013, 2014, 2015, 2016, 2017, 2018],      [20000, 40000, 60000, 80000]),
    ("Mercedes-Benz", "Sprinter", [2014, 2015, 2016, 2017, 2018],            [60000, 80000, 100000, 120000]),
    ("Mercedes-Benz", "Vito",     [2014, 2015, 2016, 2017, 2018],            [60000, 80000, 100000, 120000]),

    # ── TOYOTA ────────────────────────────────────────────────────────────
    ("Toyota", "Yaris",   [2012, 2013, 2014, 2015, 2016, 2017, 2018],        [20000, 40000, 60000, 80000]),
    ("Toyota", "Auris",   [2012, 2013, 2014, 2015, 2016, 2017, 2018],        [20000, 40000, 60000, 80000, 100000]),
    ("Toyota", "Corolla", [2015, 2016, 2017, 2018, 2019, 2020],              [20000, 40000, 60000, 80000]),
    ("Toyota", "Aygo",    [2014, 2015, 2016, 2017, 2018, 2019],              [20000, 40000, 60000]),
    ("Toyota", "C-HR",    [2016, 2017, 2018, 2019, 2020],                    [20000, 40000, 60000]),
    ("Toyota", "RAV4",    [2013, 2014, 2015, 2016, 2017, 2018],              [20000, 40000, 60000, 80000, 100000]),
    ("Toyota", "Prius",   [2013, 2014, 2015, 2016, 2017, 2018],              [40000, 60000, 80000, 100000]),
    ("Toyota", "Hilux",   [2013, 2014, 2015, 2016, 2017, 2018],              [40000, 60000, 80000, 100000, 120000]),

    # ── NISSAN ────────────────────────────────────────────────────────────
    ("Nissan", "Micra",   [2013, 2014, 2015, 2016, 2017, 2018],              [20000, 40000, 60000, 80000]),
    ("Nissan", "Juke",    [2013, 2014, 2015, 2016, 2017, 2018],              [20000, 40000, 60000, 80000, 100000]),
    ("Nissan", "Qashqai", [2013, 2014, 2015, 2016, 2017, 2018, 2019],        [20000, 40000, 60000, 80000, 100000]),
    ("Nissan", "Note",    [2013, 2014, 2015, 2016, 2017],                    [20000, 40000, 60000, 80000]),
    ("Nissan", "Leaf",    [2015, 2016, 2017, 2018, 2019],                    [20000, 40000, 60000]),
    ("Nissan", "X-Trail", [2014, 2015, 2016, 2017, 2018],                    [20000, 40000, 60000, 80000, 100000]),
    ("Nissan", "Navara",  [2013, 2014, 2015, 2016, 2017, 2018],              [40000, 60000, 80000, 100000, 120000]),

    # ── HONDA ─────────────────────────────────────────────────────────────
    ("Honda", "Civic", [2013, 2014, 2015, 2016, 2017, 2018],                 [20000, 40000, 60000, 80000, 100000]),
    ("Honda", "Jazz",  [2013, 2014, 2015, 2016, 2017, 2018],                 [20000, 40000, 60000, 80000]),
    ("Honda", "CR-V",  [2013, 2014, 2015, 2016, 2017, 2018],                 [20000, 40000, 60000, 80000, 100000]),
    ("Honda", "HR-V",  [2015, 2016, 2017, 2018, 2019],                       [20000, 40000, 60000, 80000]),

    # ── HYUNDAI ───────────────────────────────────────────────────────────
    ("Hyundai", "I10",    [2013, 2014, 2015, 2016, 2017, 2018],              [20000, 40000, 60000]),
    ("Hyundai", "I20",    [2013, 2014, 2015, 2016, 2017, 2018],              [20000, 40000, 60000, 80000]),
    ("Hyundai", "I30",    [2013, 2014, 2015, 2016, 2017, 2018],              [20000, 40000, 60000, 80000, 100000]),
    ("Hyundai", "Tucson", [2015, 2016, 2017, 2018, 2019],                    [20000, 40000, 60000, 80000]),
    ("Hyundai", "Ix35",   [2012, 2013, 2014, 2015, 2016],                    [20000, 40000, 60000, 80000, 100000]),

    # ── KIA ───────────────────────────────────────────────────────────────
    ("Kia", "Picanto",  [2013, 2014, 2015, 2016, 2017, 2018],                [20000, 40000, 60000]),
    ("Kia", "Rio",      [2013, 2014, 2015, 2016, 2017, 2018],                [20000, 40000, 60000, 80000]),
    ("Kia", "Ceed",     [2013, 2014, 2015, 2016, 2017, 2018],                [20000, 40000, 60000, 80000, 100000]),
    ("Kia", "Sportage", [2014, 2015, 2016, 2017, 2018, 2019],                [20000, 40000, 60000, 80000, 100000]),
    ("Kia", "Stinger",  [2017, 2018, 2019, 2020],                            [20000, 40000, 60000]),
    ("Kia", "Niro",     [2016, 2017, 2018, 2019, 2020],                      [20000, 40000, 60000]),
    ("Kia", "Sorento",  [2013, 2014, 2015, 2016, 2017, 2018],                [20000, 40000, 60000, 80000, 100000]),

    # ── SEAT ──────────────────────────────────────────────────────────────
    ("Seat", "Ibiza", [2013, 2014, 2015, 2016, 2017, 2018],                  [20000, 40000, 60000, 80000]),
    ("Seat", "Leon",  [2013, 2014, 2015, 2016, 2017, 2018],                  [20000, 40000, 60000, 80000, 100000]),
    ("Seat", "Arona", [2017, 2018, 2019, 2020],                              [20000, 40000, 60000]),
    ("Seat", "Ateca", [2016, 2017, 2018, 2019],                              [20000, 40000, 60000, 80000]),

    # ── SKODA ─────────────────────────────────────────────────────────────
    ("Skoda", "Fabia",   [2013, 2014, 2015, 2016, 2017, 2018],               [20000, 40000, 60000, 80000]),
    ("Skoda", "Octavia", [2013, 2014, 2015, 2016, 2017, 2018],               [20000, 40000, 60000, 80000, 100000]),
    ("Skoda", "Superb",  [2014, 2015, 2016, 2017, 2018],                     [20000, 40000, 60000, 80000, 100000]),
    ("Skoda", "Karoq",   [2017, 2018, 2019, 2020],                           [20000, 40000, 60000]),
    ("Skoda", "Kodiaq",  [2017, 2018, 2019, 2020],                           [20000, 40000, 60000, 80000]),
    ("Skoda", "Yeti",    [2012, 2013, 2014, 2015, 2016, 2017],               [20000, 40000, 60000, 80000, 100000]),

    # ── PEUGEOT ───────────────────────────────────────────────────────────
    ("Peugeot", "108",     [2014, 2015, 2016, 2017, 2018],                   [20000, 40000, 60000]),
    ("Peugeot", "208",     [2013, 2014, 2015, 2016, 2017, 2018, 2019],       [20000, 40000, 60000, 80000]),
    ("Peugeot", "308",     [2013, 2014, 2015, 2016, 2017, 2018],             [20000, 40000, 60000, 80000, 100000]),
    ("Peugeot", "3008",    [2013, 2014, 2015, 2016, 2017, 2018],             [20000, 40000, 60000, 80000, 100000]),
    ("Peugeot", "5008",    [2014, 2015, 2016, 2017, 2018],                   [20000, 40000, 60000, 80000]),
    ("Peugeot", "2008",    [2014, 2015, 2016, 2017, 2018],                   [20000, 40000, 60000, 80000]),
    ("Peugeot", "Partner", [2013, 2014, 2015, 2016, 2017, 2018],             [40000, 60000, 80000, 100000, 120000]),

    # ── RENAULT ───────────────────────────────────────────────────────────
    ("Renault", "Clio",   [2013, 2014, 2015, 2016, 2017, 2018],              [20000, 40000, 60000, 80000]),
    ("Renault", "Megane", [2013, 2014, 2015, 2016, 2017, 2018],              [20000, 40000, 60000, 80000, 100000]),
    ("Renault", "Kadjar", [2015, 2016, 2017, 2018, 2019],                    [20000, 40000, 60000, 80000]),
    ("Renault", "Captur", [2014, 2015, 2016, 2017, 2018],                    [20000, 40000, 60000, 80000]),
    ("Renault", "Zoe",    [2014, 2015, 2016, 2017, 2018, 2019],              [20000, 40000, 60000]),
    ("Renault", "Kangoo", [2013, 2014, 2015, 2016, 2017],                    [40000, 60000, 80000, 100000]),

    # ── CITROEN ───────────────────────────────────────────────────────────
    ("Citroen", "C1",       [2013, 2014, 2015, 2016, 2017, 2018],            [20000, 40000, 60000]),
    ("Citroen", "C3",       [2013, 2014, 2015, 2016, 2017, 2018],            [20000, 40000, 60000, 80000]),
    ("Citroen", "C4",       [2013, 2014, 2015, 2016, 2017],                  [20000, 40000, 60000, 80000, 100000]),
    ("Citroen", "C5",       [2012, 2013, 2014, 2015, 2016],                  [40000, 60000, 80000, 100000]),
    ("Citroen", "Berlingo", [2013, 2014, 2015, 2016, 2017, 2018],            [40000, 60000, 80000, 100000]),
    ("Citroen", "Dispatch", [2014, 2015, 2016, 2017, 2018],                  [40000, 60000, 80000, 100000, 120000]),

    # ── FIAT ──────────────────────────────────────────────────────────────
    ("Fiat", "500",   [2012, 2013, 2014, 2015, 2016, 2017, 2018],            [20000, 40000, 60000, 80000]),
    ("Fiat", "Punto", [2012, 2013, 2014, 2015, 2016],                        [20000, 40000, 60000, 80000, 100000]),
    ("Fiat", "Panda", [2012, 2013, 2014, 2015, 2016, 2017, 2018],            [20000, 40000, 60000, 80000]),
    ("Fiat", "500X",  [2015, 2016, 2017, 2018, 2019],                        [20000, 40000, 60000, 80000]),
    ("Fiat", "Tipo",  [2016, 2017, 2018, 2019],                              [20000, 40000, 60000, 80000]),

    # ── MINI ──────────────────────────────────────────────────────────────
    ("Mini", "Hatch",       [2013, 2014, 2015, 2016, 2017, 2018],            [20000, 40000, 60000, 80000]),
    ("Mini", "Clubman",     [2015, 2016, 2017, 2018, 2019],                  [20000, 40000, 60000, 80000]),
    ("Mini", "Countryman",  [2013, 2014, 2015, 2016, 2017, 2018],            [20000, 40000, 60000, 80000]),
    ("Mini", "Convertible", [2014, 2015, 2016, 2017, 2018],                  [20000, 40000, 60000]),

    # ── MAZDA ─────────────────────────────────────────────────────────────
    ("Mazda", "Mazda2", [2013, 2014, 2015, 2016, 2017, 2018],                [20000, 40000, 60000, 80000]),
    ("Mazda", "Mazda3", [2013, 2014, 2015, 2016, 2017, 2018],                [20000, 40000, 60000, 80000, 100000]),
    ("Mazda", "Mazda6", [2013, 2014, 2015, 2016, 2017, 2018],                [20000, 40000, 60000, 80000, 100000]),
    ("Mazda", "CX-3",   [2015, 2016, 2017, 2018, 2019],                      [20000, 40000, 60000, 80000]),
    ("Mazda", "CX-5",   [2013, 2014, 2015, 2016, 2017, 2018],                [20000, 40000, 60000, 80000, 100000]),

    # ── VOLVO ─────────────────────────────────────────────────────────────
    ("Volvo", "V40",  [2013, 2014, 2015, 2016, 2017, 2018],                  [20000, 40000, 60000, 80000, 100000]),
    ("Volvo", "V60",  [2013, 2014, 2015, 2016, 2017, 2018],                  [20000, 40000, 60000, 80000, 100000]),
    ("Volvo", "C30",  [2010, 2011, 2012, 2013, 2014],                        [20000, 40000, 60000, 80000, 100000]),
    ("Volvo", "XC40", [2017, 2018, 2019, 2020],                              [20000, 40000, 60000]),
    ("Volvo", "XC60", [2013, 2014, 2015, 2016, 2017, 2018],                  [20000, 40000, 60000, 80000, 100000]),
    ("Volvo", "XC90", [2015, 2016, 2017, 2018],                              [20000, 40000, 60000, 80000, 100000]),

    # ── LAND ROVER ────────────────────────────────────────────────────────
    ("Land Rover", "Discovery",          [2013, 2014, 2015, 2016, 2017, 2018], [40000, 60000, 80000, 100000, 120000]),
    ("Land Rover", "Discovery Sport",    [2015, 2016, 2017, 2018, 2019],        [20000, 40000, 60000, 80000]),
    ("Land Rover", "Freelander",         [2011, 2012, 2013, 2014, 2015],        [40000, 60000, 80000, 100000]),
    ("Land Rover", "Range Rover Evoque", [2013, 2014, 2015, 2016, 2017, 2018],  [20000, 40000, 60000, 80000]),
    ("Land Rover", "Range Rover Sport",  [2013, 2014, 2015, 2016, 2017],        [40000, 60000, 80000, 100000]),
    ("Land Rover", "Range Rover",        [2013, 2014, 2015, 2016, 2017],        [40000, 60000, 80000, 100000]),
    ("Land Rover", "Defender",           [2018, 2019, 2020, 2021],              [20000, 40000, 60000]),

    # ── JAGUAR ────────────────────────────────────────────────────────────
    ("Jaguar", "XE",     [2015, 2016, 2017, 2018, 2019],                     [20000, 40000, 60000, 80000]),
    ("Jaguar", "XF",     [2013, 2014, 2015, 2016, 2017, 2018],               [20000, 40000, 60000, 80000, 100000]),
    ("Jaguar", "F-Pace", [2016, 2017, 2018, 2019],                           [20000, 40000, 60000, 80000]),
    ("Jaguar", "E-Pace", [2017, 2018, 2019, 2020],                           [20000, 40000, 60000]),

    # ── SUBARU ────────────────────────────────────────────────────────────
    ("Subaru", "Outback",  [2013, 2014, 2015, 2016, 2017, 2018],             [20000, 40000, 60000, 80000, 100000]),
    ("Subaru", "Forester", [2013, 2014, 2015, 2016, 2017, 2018],             [20000, 40000, 60000, 80000, 100000]),
    ("Subaru", "Impreza",  [2013, 2014, 2015, 2016, 2017],                   [20000, 40000, 60000, 80000]),

    # ── MITSUBISHI ────────────────────────────────────────────────────────
    ("Mitsubishi", "Outlander", [2013, 2014, 2015, 2016, 2017, 2018],        [20000, 40000, 60000, 80000, 100000]),
    ("Mitsubishi", "ASX",       [2013, 2014, 2015, 2016, 2017, 2018],        [20000, 40000, 60000, 80000]),
    ("Mitsubishi", "L200",      [2013, 2014, 2015, 2016, 2017, 2018],        [40000, 60000, 80000, 100000, 120000]),
    ("Mitsubishi", "Eclipse",   [2017, 2018, 2019, 2020],                    [20000, 40000, 60000]),
    ("Mitsubishi", "Grandis",   [2004, 2005, 2006, 2007, 2008, 2009],        [60000, 80000, 100000, 120000]),

    # ── SUZUKI ────────────────────────────────────────────────────────────
    ("Suzuki", "Swift",        [2013, 2014, 2015, 2016, 2017, 2018],         [20000, 40000, 60000, 80000]),
    ("Suzuki", "Vitara",       [2015, 2016, 2017, 2018, 2019],               [20000, 40000, 60000, 80000]),
    ("Suzuki", "Jimny",        [2013, 2014, 2015, 2016, 2017, 2018],         [20000, 40000, 60000, 80000]),
    ("Suzuki", "Ignis",        [2016, 2017, 2018, 2019],                     [20000, 40000, 60000]),
    ("Suzuki", "Grand Vitara", [2010, 2011, 2012, 2013, 2014, 2015],         [40000, 60000, 80000, 100000, 120000]),
    ("Suzuki", "Baleno",       [2016, 2017, 2018, 2019],                     [20000, 40000, 60000]),

    # ── DACIA ─────────────────────────────────────────────────────────────
    ("Dacia", "Sandero", [2013, 2014, 2015, 2016, 2017, 2018],               [20000, 40000, 60000, 80000]),
    ("Dacia", "Duster",  [2013, 2014, 2015, 2016, 2017, 2018],               [20000, 40000, 60000, 80000]),
    ("Dacia", "Logan",   [2013, 2014, 2015, 2016, 2017],                     [20000, 40000, 60000, 80000, 100000]),

    # ── ALFA ROMEO ────────────────────────────────────────────────────────
    ("Alfa Romeo", "Giulietta", [2013, 2014, 2015, 2016, 2017, 2018],        [20000, 40000, 60000, 80000]),
    ("Alfa Romeo", "Giulia",    [2016, 2017, 2018, 2019],                    [20000, 40000, 60000]),
    ("Alfa Romeo", "Stelvio",   [2017, 2018, 2019],                          [20000, 40000, 60000]),

    # ── JEEP ──────────────────────────────────────────────────────────────
    ("Jeep", "Renegade", [2015, 2016, 2017, 2018, 2019],                     [20000, 40000, 60000, 80000]),
    ("Jeep", "Compass",  [2017, 2018, 2019, 2020],                           [20000, 40000, 60000]),
    ("Jeep", "Cherokee", [2014, 2015, 2016, 2017, 2018],                     [20000, 40000, 60000, 80000, 100000]),

    # ── TESLA ─────────────────────────────────────────────────────────────
    ("Tesla", "Model 3", [2019, 2020, 2021, 2022],                           [20000, 40000, 60000]),
    ("Tesla", "Model S", [2015, 2016, 2017, 2018, 2019],                     [20000, 40000, 60000, 80000]),
    ("Tesla", "Model X", [2016, 2017, 2018, 2019],                           [20000, 40000, 60000]),

    # ── LEXUS ─────────────────────────────────────────────────────────────
    ("Lexus", "IS", [2013, 2014, 2015, 2016, 2017, 2018],                    [20000, 40000, 60000, 80000]),
    ("Lexus", "CT", [2013, 2014, 2015, 2016, 2017],                          [20000, 40000, 60000, 80000, 100000]),
    ("Lexus", "NX", [2015, 2016, 2017, 2018],                                [20000, 40000, 60000]),
    ("Lexus", "RX", [2013, 2014, 2015, 2016, 2017, 2018],                    [20000, 40000, 60000, 80000, 100000]),

    # ── PORSCHE ───────────────────────────────────────────────────────────
    ("Porsche", "Cayenne", [2013, 2014, 2015, 2016, 2017, 2018],             [20000, 40000, 60000, 80000, 100000]),
    ("Porsche", "Macan",   [2014, 2015, 2016, 2017, 2018],                   [20000, 40000, 60000, 80000]),
    ("Porsche", "911",     [2013, 2014, 2015, 2016, 2017, 2018],             [20000, 40000, 60000]),

    # ── ISUZU ─────────────────────────────────────────────────────────────
    ("Isuzu", "D-Max", [2013, 2014, 2015, 2016, 2017, 2018],                 [40000, 60000, 80000, 100000, 120000]),

    # ── CITROEN DS ────────────────────────────────────────────────────────
    ("Citroen", "Ds3", [2010, 2011, 2012, 2013, 2014, 2015, 2016],           [20000, 40000, 60000, 80000, 100000, 120000]),
    ("Citroen", "Ds4", [2011, 2012, 2013, 2014, 2015, 2016],                 [20000, 40000, 60000, 80000, 100000]),
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

        financials = report.get("financials", {})
        gross_profit = financials.get("gross_profit", deal.profit)
        net_profit = financials.get("net_profit", "N/A")
        est_prep = financials.get("est_prep", "N/A")
        est_transport = financials.get("est_transport", "N/A")
        est_warranty = financials.get("est_warranty", "N/A")
        est_total = financials.get("est_total_costs", "N/A")

        caption = f"""
🚗 {deal.status.upper()} CONFIDENCE DEAL

{deal.title}

📍 Reg: {deal.reg or "N/A"}
📊 Mileage: {deal.mileage or "N/A"}

💰 Asking: £{deal.listing_price}
📈 Market Value: £{deal.market_value}
📊 Gross Profit: £{gross_profit}

🔧 Est. Costs:
  • Transport: £{est_transport}
  • Prep: £{est_prep}
  • Warranty: £{est_warranty}
  • Risk: £{deal.risk_penalty}
  • Total: £{est_total}

💵 Net Profit: £{net_profit}
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
#
# KEY DESIGN: one eBay search per (make, base_model) only.
# The valuation engine is called once per (make, model, year, mileage)
# combination but reuses the same eBay search results via the shared
# enriched summaries — no additional API calls per year/mileage bucket.
#
# IMPORTANT: mileage buckets must be multiples of 20,000 to match
# the cache key rounding in market_valuation_service.py.
#
# API budget: ~160 models × 2 calls = ~320 calls per cold prewarm.
# With 5hr schedule and skip-if-cached (70% warm threshold), daily
# refresh cost is typically ~80-120 calls.
# ==========================================
@celery.task
def prewarm_valuation_cache(targets_override=None):
    from app.services.market_valuation_service import (
        get_market_price_from_sold,
        get_sold_listings,
        _pre_expand_details,
        run_filter_layer,
        normalise_base_model,
        get_mileage_tolerances,
        EXTREME_MILEAGE_THRESHOLD,
        CACHE_TTL,
        redis_client as mvc_redis,
    )
    import json

    total_searches = 0
    total_cached = 0
    total_skipped = 0

    print("🔥 Starting valuation cache prewarm...")

    targets = targets_override if targets_override is not None else PREWARM_TARGETS
    for make, base_model, years, mileage_buckets in targets:

        make_title = make.strip().title()
        base_model_title = normalise_base_model(make_title, base_model.strip().title())

        # Check if ALL buckets for this model are already cached.
        # If most are warm, skip the whole model to save API calls.
        cached_count = 0

        engine_buckets = [
            1.0, 1.2, 1.4, 1.6, 
            1.8, 2.0, 2.2, 
            2.5, 3.0,
            None
        ]

        total_buckets = len(engine_buckets) * len(years) * len(mileage_buckets)

        for engine_bucket in engine_buckets:
            for year in years:
                for mileage in mileage_buckets:
                    ck = f"sold_cache:{make_title}:{base_model_title}:{engine_bucket}:{year}:{mileage}"
                    if mvc_redis.get(ck):
                        cached_count += 1

        if cached_count >= total_buckets:
            total_skipped += total_buckets
            print(f"⏭️  {make_title} {base_model_title} — all {total_buckets} buckets cached, skipping")
            continue

        warm_ratio = cached_count / total_buckets if total_buckets > 0 else 0
        if warm_ratio > 0.7:
            total_skipped += cached_count
            print(f"⏭️  {make_title} {base_model_title} — {cached_count}/{total_buckets} warm (>70%), skipping")
            continue

        print(f"🔎 Fetching: {make_title} {base_model_title} ({cached_count}/{total_buckets} already cached)")

        # ONE eBay search for this make/model — shared across all year/mileage combos
        query = f"{make_title} {base_model_title}"
        try:
            all_summaries = get_sold_listings(
                query,
                budget_fn=lambda n: _check_budget(n, "prewarm")
            )
            total_searches += 1
        except Exception as e:
            print(f"❌ Search failed for {query}: {e}")
            continue

        if not all_summaries:
            print(f"⚠️  No results for {query}")
            continue

        # Enrich summaries once — reused across all year/mileage combinations below
        try:
            enriched_summaries = _pre_expand_details(all_summaries, prewarm_mode=True)  # Year-only expansion, capped at 15
        except Exception as e:
            print(f"❌ Expansion failed for {query}: {e}")
            continue

        # Fan out: run filter layers for every year/mileage bucket combination
        # No additional API calls — just filtering the already-fetched summaries
        for engine_bucket in engine_buckets:
            for year in years:
                for mileage in mileage_buckets:

                    cache_key = f"sold_cache:{make_title}:{base_model_title}:{engine_bucket}:{year}:{mileage}"

                    if mvc_redis.get(cache_key):
                        total_skipped += 1
                        continue

                    l1_tolerance, l2_tolerance = get_mileage_tolerances(mileage)

                    result = None
                    for tolerance_config in [
                        {"year_tolerance": 2, "mileage_tolerance": l1_tolerance,         "source": "layer_1_strict",          "adjust_mileage": True},
                        {"year_tolerance": 2, "mileage_tolerance": l2_tolerance,         "source": "layer_2_relaxed_mileage", "adjust_mileage": True},
                        {"year_tolerance": 3, "mileage_tolerance": l2_tolerance + 5000,  "source": "layer_3_relaxed_year",    "adjust_mileage": True},
                        {"year_tolerance": 4, "mileage_tolerance": l2_tolerance + 15000, "source": "layer_4_wide",            "adjust_mileage": True},
                    ]:
                        result = run_filter_layer(
                            enriched_summaries,
                            target_year=year,
                            target_mileage=mileage,
                            engine_litre=engine_bucket,
                            year_tolerance=tolerance_config["year_tolerance"],
                            mileage_tolerance=tolerance_config["mileage_tolerance"],
                            adjust_mileage=tolerance_config["adjust_mileage"],
                            layer_name=tolerance_config["source"],
                        )
                        if result and result.get("sample_size", 0) >= 4:
                            if mileage > EXTREME_MILEAGE_THRESHOLD:
                                excess = mileage - EXTREME_MILEAGE_THRESHOLD
                                extra_blocks = min(excess / 10000, 15)
                                extreme_penalty_pct = min(0.025 * extra_blocks, 0.50)
                                original = result["market_price"]
                                result["market_price"] = round(original * (1 - extreme_penalty_pct), 2)
                                print(f"   🔻 Extreme mileage penalty: {mileage}mi → −{round(extreme_penalty_pct*100,1)}% → £{result['market_price']}")

                            result["source"] = tolerance_config["source"]
                            mvc_redis.set(cache_key, json.dumps(result), ex=CACHE_TTL)
                            total_cached += 1
                            print(f"   ✅ Cached: {make_title} {base_model_title} {year} {mileage}mi → £{result['market_price']}")
                            break

                    if not result:
                        print(f"   ⚠️  No result: {make_title} {base_model_title} {year} {mileage}mi")

        # Pause between models to respect rate limiter
        time.sleep(1)

    print(f"🔥 Prewarm complete: {total_searches} searches, {total_cached} buckets cached, {total_skipped} skipped")
    return {"searches": total_searches, "cached": total_cached, "skipped": total_skipped}


# ==========================================
# SNIPER MODE
# ==========================================
@celery.task
def scan_sniper(dealer_id: int):
    # Rotate through make groups — each 10-min cycle scans a fresh slice
    all_queries = SCAN_QUERY_GROUPS + YEAR_SNIPER_QUERIES + GENERIC_SNIPER_QUERIES
    idx = int(redis_client.incr(SNIPER_ROTATION_KEY) -1) % len(all_queries)
    query = all_queries[idx]
    print(f"🎯 Sniper rotation [{idx+1}/{len(all_queries)}]: '{query}'")
    return run_scan(
        dealer_id=dealer_id,
        mode_name="sniper",
        listings_to_pull=50,
        keywords=query,
        sort="newlyListed",
    )


# ==========================================
# VALUE SWEEP
# Dual-strategy bulk scan — runs every 4hrs.
# Catches two distinct populations the sniper misses:
#
# Strategy A — sort=price, offset=80 then 120, 2 pages (80 listings per group)
#   Targets cars priced below market that have been sitting on eBay
#   for days/weeks. Offsets 80 & 120 skip the obvious front-page cheap cars
#   everyone sees, landing in the overlooked mid-tier. Must be multiples
#   of limit (40) per eBay Browse API requirement.
#
# Strategy B — sort=bestMatch, offset=0, 1 page (40 listings per group)
#   Targets recently price-dropped or relisted cars. eBay's bestMatch
#   algorithm boosts listings where the seller has recently modified
#   the price or relisted — these never appear in newlyListed (not a
#   new listing) but are exactly the motivated sellers we want.
#
# Both strategies feed into the same deduped item pool per run.
# Budget: 8 groups × 3 searches = 24 search calls per sweep run.
# ==========================================
@celery.task
def scan_value_sweep(dealer_id: int):
    return run_scan(
        dealer_id=dealer_id,
        mode_name="value_sweep",
        listings_to_pull=40,
        keywords=None,  # None = all 20 make groups
    )





# ==========================================
# VAN SNIPER
# ==========================================
@celery.task
def scan_van_sniper(dealer_id: int):
    idx = int(redis_client.incr(VAN_SNIPER_ROTATION_KEY) - 1) % len(VAN_SCAN_QUERY_GROUPS)
    query = VAN_SCAN_QUERY_GROUPS[idx]
    print(f"🚐 Van sniper rotation [{idx+1}/{len(VAN_SCAN_QUERY_GROUPS)}]: '{query}'")
    return run_scan(
        dealer_id=dealer_id,
        mode_name="sniper",
        listings_to_pull=50,
        keywords=query,
        sort="newlyListed",
        source_override="ebay_vans",
    )


# ==========================================
# VAN VALUE SWEEP
# ==========================================
@celery.task
def scan_van_sweep(dealer_id: int):
    return run_scan(
        dealer_id=dealer_id,
        mode_name="value_sweep",
        listings_to_pull=40,
        keywords=None,
        query_groups_override=VAN_SCAN_QUERY_GROUPS,
        source_override="ebay_vans",
    )


# ==========================================
# VAN PREWARM
# ==========================================
@celery.task
def prewarm_van_valuation_cache():
    return prewarm_valuation_cache(targets_override=VAN_PREWARM_TARGETS)



# ==========================================
# DAILY API BUDGET GUARD
# Tracks eBay API calls in Redis with a 24hr TTL.
# Stops all scans once DAILY_API_BUDGET is reached.
# Resets automatically at midnight UTC when key expires.
# ==========================================
# Per-task soft budget allocations — prevents any single task from
# consuming the entire daily budget and starving other tasks.
# These are SOFT limits logged as warnings, not hard stops.
# The DAILY_API_BUDGET is the only hard stop.
BUDGET_ALLOCATIONS = {
    "prewarm":     1000,   # Once/day cold run
    "van_prewarm":  200,   # Once/day cold run
    "sniper":      1500,   # 48 runs/day × ~31 calls avg
    "van_sniper":   700,   # 24 runs/day × ~29 calls avg
    "sweep":       1000,   # 6 runs/day × ~136 calls avg
    "van_sweep":    700,   # 4 runs/day × ~136 calls avg
}
TASK_BUDGET_KEY_PREFIX = "ebay_task_calls"

def _check_budget(calls_needed: int = 1, task_name: str = None) -> bool:
    """Returns True if budget allows, increments counter. False = stop scanning."""
    pipe = redis_client.pipeline()
    pipe.incrby(DAILY_BUDGET_KEY, calls_needed)
    pipe.ttl(DAILY_BUDGET_KEY)
    result = pipe.execute()
    current_count = result[0]
    ttl = result[1]

    # Set 24hr expiry on first write
    if ttl < 0:
        redis_client.expire(DAILY_BUDGET_KEY, 86400)

    remaining = DAILY_API_BUDGET - current_count
    if current_count % 500 == 0:
        print(f"📊 Budget: {current_count}/{DAILY_API_BUDGET} used ({remaining} remaining)")

    if current_count > DAILY_API_BUDGET:
        print(f"🛑 Daily budget exceeded: {current_count}/{DAILY_API_BUDGET} calls used")
        return False

    # Soft per-task warning
    if task_name and task_name in BUDGET_ALLOCATIONS:
        task_key = f"{TASK_BUDGET_KEY_PREFIX}:{task_name}"
        task_count = int(redis_client.incr(task_key))
        if ttl < 0:
            redis_client.expire(task_key, 86400)
        soft_limit = BUDGET_ALLOCATIONS[task_name]
        if task_count > soft_limit:
            print(f"⚠️ [{task_name}] soft budget exceeded: {task_count}/{soft_limit} — continuing but investigate")

    return True


# ==========================================
# SHARED SCAN ENGINE
# ==========================================
def run_scan(dealer_id: int, mode_name: str, listings_to_pull: int, keywords=None, sort="newlyListed", source_override=None, query_groups_override=None):
    """
    Unified scan engine.

    Sniper:      single make group, sort=newlyListed, offset=0 — freshest listings.
    Value sweep: all make groups, dual strategy per group:
                   A) sort=price, offset=100 + 40 — stale cheap stock
                   B) sort=bestMatch, offset=0    — recently price-dropped/relisted
    """

    lock_key = f"scan_lock_{dealer_id}_{mode_name}"
    max_expansions = VALUE_SWEEP_LIMIT if mode_name == "value_sweep" else SNIPER_LIMIT

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

        base_groups = query_groups_override if query_groups_override is not None else SCAN_QUERY_GROUPS
        query_groups = [keywords] if keywords is not None else base_groups

        total_listings = 0
        total_deals = 0
        processed_ids = set()
        detail_expansions = 0

        for source_name in SOURCES:

            source = get_listing_source(source_name)
            items = []

            for query in query_groups:
                print(f"🔍 [{mode_name}] Searching: '{query}'")

                if mode_name == "value_sweep":
                    budget_ok = True
                    # -------------------------------------------------------
                    # Strategy A: price-ascending, offset 80 then 120
                    # Lands in stale/overlooked cheap stock beyond page 1.
                    # Offsets must be multiples of limit (40) per eBay API.
                    # -------------------------------------------------------
                    for offset in [40, 80, 120, 160]:

                        task_name = "van_sweep" if source_override == "ebay_vans" else "sweep"
                        if not _check_budget(1, task_name):
                            print("🛑 Daily API budget reached — stopping sweep (strategy A)")
                            budget_ok = False
                            break

                        page_items = source.search(
                            keywords=query,
                            entries=listings_to_pull,
                            min_price=None,
                            max_price=filters["max_price"],
                            sort="price",
                            offset=offset,
                        )

                        items.extend(page_items)

                    # -------------------------------------------------------
                    # Strategy B: bestMatch, offset 0
                    # eBay boosts recently price-dropped or relisted cars.
                    # Catches motivated sellers that newlyListed won't show.
                    # -------------------------------------------------------
                    if budget_ok:
                        task_name = "van_sweep" if source_override == "ebay_vans" else "sweep"
                        if not _check_budget(1, task_name):
                            print("🛑 Daily API budget reached — stopping sweep (strategy B)")
                        else:
                            page_items = source.search(
                                keywords=query,
                                entries=listings_to_pull,
                                min_price=None,
                                max_price=filters["max_price"],
                                sort="bestMatch",
                                offset=0,
                            )
                            items.extend(page_items)


                else:
                    # Sniper: multi-window search strategy
                    task_name = "van_sniper" if source_override == "ebay_vans" else "sniper"

                    if source_name == "ebay_browse":
                        if not _check_budget(1, task_name):
                            print("Daily API budget reached - stopping sniper")
                            break

                        sniper_items = search_sniper_windows(query, "")
                        items.extend(sniper_items)

                    else:
                        page_items = source.search(
                            keywords=query,
                            entries=listings_to_pull,
                            min_price=None,
                            max_price=filters["max_price"],
                            sort=sort,
                            offset=0,
                        )
                        items.extend(page_items)

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

                item_id = item.get("id")
                price_key = f"{PRICE_TRACK_KEY}:{item_id}"

                previous_price = redis_client.get(price_key)

                if previous_price:
                    previous_price = float(previous_price)

                    drop_pct = (previous_price - rough_price) / previous_price

                    if drop_pct > 0.07:
                        print(f"📉 Price drop detected: {previous_price} → {rough_price}")

                redis_client.set(price_key, rough_price, ex=86400)

                if not rough_price:
                    continue

                if rough_price > filters["max_price"]:
                    print(f"⛔ Pre-screen: £{rough_price} exceeds max £{filters['max_price']} — skipping")
                    continue

                task_name = "van_sweep" if source_override == "ebay_vans" else "sweep"
                if not _check_budget(1, task_name):
                    print("🛑 Daily API budget reached — stopping expansions")
                    break

                deal = process_listing(
                    item,
                    dealer.id,
                    source=source_override or source_name,
                    filters=filters,
                    budget_fn=lambda n: _check_budget(n, mode_name), # Routes all valuation eBay calls through daily budget guard
                )

                detail_expansions += 1
                gc.collect()  # Free image/OCR memory between expansions

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


# ==========================================
# FACEBOOK LISTING PROCESSOR
# Runs OCR + DVSA + valuation off the backend process.
# Backend never loads EasyOCR — queues this task instead.
# ==========================================
@celery.task
def process_facebook_listing(data: dict, dealer_id: int = 1):
    from app.services.ocr_service import extract_plate_from_base64

    image_base64 = data.get("image_base64")
    if image_base64:
        try:
            detected_plate = extract_plate_from_base64(image_base64)
            if detected_plate:
                data["registration"] = detected_plate
        except Exception as e:
            print(f"Facebook OCR error: {e}")

    deal = process_listing(
        raw_item=data,
        dealer_id=dealer_id,
        source="facebook_extension",
    )

    if deal:
        notify_deal.delay(deal.id)
        return {
            "status": "accepted",
            "deal_id": deal.id,
            "profit": deal.profit,
            "score": deal.score,
            "market_value": deal.market_value,
            "reg": deal.reg or "Not detected",
        }

    return {"status": "filtered"}