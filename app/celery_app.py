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
)

# ==========================================
# INTELLIGENT MULTI-MODE SCHEDULING
# ==========================================

celery.conf.beat_schedule = {
    "sniper-scan-every-10-minutes": {
        "task": "app.tasks.scan_sniper",
        "schedule": timedelta(minutes=10),
        "args": (1,),
    },

    "value-sweep-every-30-minutes": {
        "task": "app.tasks.scan_value_sweep",
        "schedule": timedelta(minutes=30),
        "args": (1,),
        "options": {"expires": 1800},
    },
}

import app.tasks

