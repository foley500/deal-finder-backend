import os
from celery import Celery
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
    "sniper-scan-every-60-minutes": {
    "task": "app.tasks.scan_sniper",
    "schedule": timedelta(hours=1),
    "args": (1,),
    },

    "value-sweep-every-12-hours": {
    "task": "app.tasks.scan_value_sweep",
    "schedule": timedelta(hours=12),
    "args": (1,),
    "options": {"expires": 3600},
    },

    # ==========================================
    # CAR PREWARM — once per day at 2am UTC
    # Running every 5hrs was burning ~2,700 calls/day alone.
    # Cache TTL is 6hrs so entries stay warm across the day
    # after a single morning refresh.
    # ==========================================
    "prewarm-valuation-cache-daily": {
        "task": "app.tasks.prewarm_valuation_cache",
        "schedule": timedelta(hours=24),
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
    # VAN PREWARM — once per day, offset 1hr from car prewarm
    # ==========================================
    "prewarm-van-valuation-cache-daily": {
        "task": "app.tasks.prewarm_van_valuation_cache",
        "schedule": timedelta(hours=24),
        "options": {"expires": 7200},
    },
}

import app.tasks