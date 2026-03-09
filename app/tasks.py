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
# API budget: ~160 models × 6 calls = ~960 calls per cold prewarm.
# With 5hr schedule and skip-if-cached (70% warm threshold), daily
# refresh cost is typically ~200-300 calls leaving ~4,700/day for scanning.
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
#
# KEY DESIGN: one eBay search per (make, base_model) only.
# The valuation engine is called once per (make, model, year, mileage)
# combination but reuses the same eBay search results via the shared
# enriched summaries — no additional API calls per year/mileage bucket.
#
# IMPORTANT: mileage buckets must be multiples of 20,000 to match
# the cache key rounding in market_valuation_service.py.
#
# API budget: ~160 models × 6 calls = ~960 calls per cold prewarm.
# With 5hr schedule and skip-if-cached (70% warm threshold), daily
# refresh cost is typically ~200-300 calls leaving ~4,700/day for scanning.
# ==========================================
@celery.task
def prewarm_valuation_cache():
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

    for make, base_model, years, mileage_buckets in PREWARM_TARGETS:

        make_title = make.strip().title()
        base_model_title = normalise_base_model(make_title, base_model.strip().title())

        # Check if ALL buckets for this model are already cached.
        # If most are warm, skip the whole model to save API calls.
        cached_count = 0
        total_buckets = len(years) * len(mileage_buckets)
        for year in years:
            for mileage in mileage_buckets:
                ck = f"sold_cache:{make_title}:{base_model_title}:{year}:{mileage}"
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
            all_summaries = get_sold_listings(query)
            total_searches += 1
        except Exception as e:
            print(f"❌ Search failed for {query}: {e}")
            continue

        if not all_summaries:
            print(f"⚠️  No results for {query}")
            continue

        # Enrich summaries once — reused across all year/mileage combinations below
        try:
            enriched_summaries = _pre_expand_details(all_summaries)
        except Exception as e:
            print(f"❌ Expansion failed for {query}: {e}")
            continue

        # Fan out: run filter layers for every year/mileage bucket combination
        # No additional API calls — just filtering the already-fetched summaries
        for year in years:
            for mileage in mileage_buckets:

                cache_key = f"sold_cache:{make_title}:{base_model_title}:{year}:{mileage}"

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
                        year_tolerance=tolerance_config["year_tolerance"],
                        mileage_tolerance=tolerance_config["mileage_tolerance"],
                        adjust_mileage=tolerance_config["adjust_mileage"],
                        layer_name=tolerance_config["source"],
                    )
                    if result:
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
        time.sleep(3)

    print(f"🔥 Prewarm complete: {total_searches} searches, {total_cached} buckets cached, {total_skipped} skipped")
    return {"searches": total_searches, "cached": total_cached, "skipped": total_skipped}


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

                if rough_price > filters["max_price"]:
                    print(f"⛔ Pre-screen: £{rough_price} exceeds max £{filters['max_price']} — skipping")
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