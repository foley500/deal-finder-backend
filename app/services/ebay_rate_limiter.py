import time
import redis
import os

REDIS_URL = os.getenv("CELERY_BROKER_URL")
redis_client = redis.from_url(REDIS_URL)

# Allow 1 request every 0.6 seconds (~100 requests per minute max)
MIN_REQUEST_INTERVAL = 0.6


def throttle_ebay():
    """
    Global eBay API rate limiter.
    Ensures only one request every MIN_REQUEST_INTERVAL seconds
    across the entire system.
    """

    key = "ebay_global_last_request"

    while True:
        last_request = redis_client.get(key)

        now = time.time()

        if not last_request:
            redis_client.set(key, now)
            return

        last_request = float(last_request)
        elapsed = now - last_request

        if elapsed >= MIN_REQUEST_INTERVAL:
            redis_client.set(key, now)
            return

        sleep_time = MIN_REQUEST_INTERVAL - elapsed
        time.sleep(sleep_time)