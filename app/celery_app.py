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
    # SCAN TASKS
    # ==========================================
    "sniper-scan-every-10-minutes": {
        "task": "app.tasks.scan_sniper",
        "schedule": timedelta(minutes=10),
        "args": (1,),
    },
    # Value sweep runs every 4 hours — catches listings where
    # the seller dropped the price after the sniper passed over them.
    "value-sweep-every-4-hours": {
        "task": "app.tasks.scan_value_sweep",
        "schedule": timedelta(hours=4),
        "args": (1,),
        "options": {"expires": 3600},
    },

    # ==========================================
    # CACHE PREWARM — every 5 hours
    # ==========================================
    "prewarm-valuation-cache": {
        "task": "app.tasks.prewarm_valuation_cache",
        "schedule": timedelta(hours=5),
        "options": {"expires": 7200},
    },
}

import app.tasks