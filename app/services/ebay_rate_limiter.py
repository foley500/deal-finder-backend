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
    Atomic lock ensures only one worker fires at a time.
    Prevents race condition causing 429s under multi-worker load.
    """
    key = "ebay_global_last_request"
    lock_key = "ebay_throttle_lock"

    while True:
        now = time.time()
        last_raw = redis_client.get(key)

        if last_raw:
            elapsed = now - float(last_raw)
            if elapsed < MIN_REQUEST_INTERVAL:
                time.sleep(MIN_REQUEST_INTERVAL - elapsed)
                continue

        # Atomic lock — only one worker proceeds at a time
        acquired = redis_client.set(lock_key, now, nx=True, ex=2)
        if not acquired:
            time.sleep(0.05)
            continue

        redis_client.set(key, now)
        redis_client.delete(lock_key)
        return