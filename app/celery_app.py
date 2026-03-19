import os
from celery import Celery
from celery.schedules import crontab
from datetime import timedelta

BROKER_URL = os.getenv("CELERY_BROKER_URL")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND")

celery = Celery(
    "vehicleintel",
    broker=BROKER_URL,
    backend=RESULT_BACKEND,
)

celery.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    worker_pool="solo",          # Single-process, no forking — critical for low-memory deploys.
                                 # Prefork spawns child processes that each load the OCR model
                                 # independently, blowing past the 2GB limit.
)

celery.conf.beat_schedule = {

    # ==========================================
    # CAR SCAN TASKS
    # ==========================================
    "sniper-scan-every-30-minutes": {
    "task": "app.tasks.scan_sniper",
    "schedule": timedelta(minutes=30),
    "args": (1,),
    },

    "value-sweep-every-4-hours": {
    "task": "app.tasks.scan_value_sweep",
    "schedule": timedelta(hours=4),
    "args": (1,),
    "options": {"expires": 3600},
    },

    # ==========================================
    # CAR PREWARM — 7:10 AM UTC daily
    # eBay quota resets at 7 AM GMT — run prewarm 10 min after reset
    # so it gets first access to fresh quota before sniper/sweep burn it.
    # Van prewarm at 8:30 AM UTC — offset so they don't overlap.
    # ==========================================
    "prewarm-valuation-cache-daily": {
        "task": "app.tasks.prewarm_valuation_cache",
        "schedule": crontab(hour=7, minute=10),
        "options": {"expires": 7200},
    },

    # ==========================================
    # VAN SCAN TASKS
    # Van sniper every 60min (not 30) — vans move slower,
    # halves van sniper API spend with negligible deal loss.
    # ==========================================
    "van-sniper-every-60-minutes": {
        "task": "app.tasks.scan_van_sniper",
        "schedule": timedelta(minutes=120),
        "args": (1,),
        "options": {"expires": 3000},
    },
    "van-sweep-every-6-hours": {
        "task": "app.tasks.scan_van_sweep",
        "schedule": timedelta(hours=6),
        "args": (1,),
        "options": {"expires": 3600},
    },

    # ==========================================
    # VAN PREWARM — 8:30 AM UTC, after car prewarm finishes
    # ==========================================
    "prewarm-van-valuation-cache-daily": {
        "task": "app.tasks.prewarm_van_valuation_cache",
        "schedule": crontab(hour=8, minute=30),
        "options": {"expires": 7200},
    },
}

import app.tasks