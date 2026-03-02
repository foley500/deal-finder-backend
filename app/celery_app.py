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

celery.conf.beat_schedule = {
    "scan-every-2-minutes": {
        "task": "app.tasks.scan_market_for_deals",
        "schedule": 120.0,
        "args": (1,)  # dealer_id
    },
}

import app.tasks