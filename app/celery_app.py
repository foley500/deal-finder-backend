import os
from celery import Celery

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
)

# ==========================================
# INTELLIGENT MULTI-MODE SCHEDULING
# ==========================================

celery.conf.beat_schedule = {

    # 🔥 SNIPER MODE
    # Runs frequently to catch newly listed vehicles
    "sniper-scan-every-3-minutes": {
        "task": "app.tasks.scan_sniper",
        "schedule": 180.0,
        "args": (1,)  # dealer_id
    },

    # 🔍 VALUE SWEEP
    # Runs slower to catch older underpriced vehicles
    "value-sweep-every-30-minutes": {
        "task": "app.tasks.scan_value_sweep",
        "schedule": 1800.0,
        "args": (1,)  # dealer_id
    },
}

import app.tasks