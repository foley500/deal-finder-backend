import os
import requests
import time
import re
import redis
import json
from datetime import datetime

DVSA_CLIENT_ID = os.getenv("DVSA_CLIENT_ID")
DVSA_CLIENT_SECRET = os.getenv("DVSA_CLIENT_SECRET")
DVSA_API_KEY = os.getenv("DVSA_API_KEY")
DVSA_TOKEN_URL = os.getenv("DVSA_TOKEN_URL")
DVSA_SCOPE_URL = os.getenv("DVSA_SCOPE_URL")

MOT_TRADE_URL = "https://history.mot.api.gov.uk/v1/trade/vehicles/registration"

REDIS_URL = os.getenv("CELERY_BROKER_URL")
redis_client = redis.from_url(REDIS_URL)

DVSA_TOKEN_KEY = "dvsa:access_token"
DVSA_CACHE_TTL = 86400  # 24 hours


def get_dvsa_token():
    cached = redis_client.get(DVSA_TOKEN_KEY)
    if cached:
        return cached.decode()

    if not DVSA_CLIENT_ID or not DVSA_CLIENT_SECRET:
        print("❌ DVSA credentials missing")
        return None

    payload = {
        "client_id": DVSA_CLIENT_ID,
        "client_secret": DVSA_CLIENT_SECRET,
        "scope": DVSA_SCOPE_URL,
        "grant_type": "client_credentials"
    }

    try:
        response = requests.post(
            DVSA_TOKEN_URL,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10
        )

        print("🔐 DVSA TOKEN STATUS:", response.status_code)

        if response.status_code != 200:
            print("❌ DVSA token error:", response.text)
            return None

        token_data = response.json()
        token = token_data.get("access_token")
        expires_in = int(token_data.get("expires_in", 3600)) - 60

        redis_client.set(DVSA_TOKEN_KEY, token, ex=expires_in)
        return token

    except Exception as e:
        print("❌ DVSA token exception:", e)
        return None


def get_mot_data(registration: str, asking_price: float = None):

    if not registration:
        return build_empty_response()

    clean_reg = re.sub(r"[^A-Z0-9]", "", registration.upper())

    # Check Redis cache first — MOT data doesn't change hourly
    cache_key = f"dvsa:{clean_reg}"
    cached = redis_client.get(cache_key)
    if cached:
        print(f"✅ DVSA cache hit: {clean_reg}")
        return json.loads(cached)

    token = get_dvsa_token()
    if not token:
        return build_empty_response()

    url = f"{MOT_TRADE_URL}/{clean_reg}"

    headers = {
        "Authorization": f"Bearer {token}",
        "x-api-key": DVSA_API_KEY,
        "Accept": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)

        print("🚗 DVSA MOT Status:", response.status_code)

        if response.status_code == 200:
            result = parse_mot_trade_response(response.json(), asking_price=asking_price)
            # Use expiry-aware TTL: cache until MOT expiry + 7 days so we don't
            # serve stale data right after an MOT test. Fall back to 24h if unknown.
            mot_expiry = result.get("mot_expiry_date")
            ttl = DVSA_CACHE_TTL
            if mot_expiry:
                try:
                    from datetime import date
                    expiry_date = datetime.strptime(mot_expiry, "%Y-%m-%d").date()
                    days_until = (expiry_date - date.today()).days + 7
                    if days_until > 0:
                        ttl = days_until * 86400
                except Exception:
                    pass
            redis_client.set(cache_key, json.dumps(result), ex=ttl)
            return result
        else:
            print("❌ DVSA MOT error body:", response.text)

    except Exception as e:
        print("DVSA MOT exception:", e)

    return build_empty_response()


def is_same_day_retest(mot_tests: list, failed_test: dict) -> bool:
    """
    Returns True if a FAILED test was retested and PASSED on the same date.
    This is a normal same-day retest — the failure was resolved immediately
    and should not be penalised as a real risk.
    """
    fail_date = failed_test.get("completedDate", "")[:10]  # YYYY-MM-DD

    for test in mot_tests:
        if test is failed_test:
            continue
        if test.get("testResult") == "PASSED":
            pass_date = test.get("completedDate", "")[:10]
            if pass_date == fail_date:
                return True

    return False


def parse_mot_trade_response(data, asking_price=None):

    if not data or not isinstance(data, dict):
        return build_empty_response()

    mot_tests = data.get("motTests", [])

    first_used_date = data.get("firstUsedDate")
    vehicle_year = None

    if first_used_date:
        try:
            vehicle_year = int(first_used_date.split("-")[0])
        except:
            pass

    current_year = datetime.utcnow().year
    vehicle_age = current_year - vehicle_year if vehicle_year else 10

    recent_window_years = 3
    medium_window_years = 6

    recent_fails = 0
    recent_advisories = 0
    medium_fails = 0
    medium_advisories = 0

    # Advisory penalty per item scales down with vehicle age.
    # A 14-year-old car with 18 advisories is normal wear — not risk.
    # A 3-year-old car with 5 advisories is genuinely concerning.
    if vehicle_age <= 5:
        advisory_rate_recent = 20
        advisory_rate_medium = 8
    elif vehicle_age <= 10:
        advisory_rate_recent = 12
        advisory_rate_medium = 5
    elif vehicle_age <= 15:
        advisory_rate_recent = 7
        advisory_rate_medium = 3
    else:
        advisory_rate_recent = 4
        advisory_rate_medium = 2

    for test in mot_tests:

        test_date = test.get("completedDate")
        if not test_date:
            continue

        try:
            test_year = int(test_date.split("-")[0])
        except:
            continue

        years_ago = current_year - test_year
        is_failed = test.get("testResult") == "FAILED"

        advisory_count_this_test = sum(
            1 for defect in test.get("defects", [])
            if defect.get("type") == "ADVISORY"
        )

        if years_ago <= recent_window_years:
            if is_failed:
                # Only penalise if this wasn't a same-day retest that passed
                if is_same_day_retest(mot_tests, test):
                    print(f"   ℹ️ MOT fail on {test_date[:10]} was same-day retest — not penalised")
                else:
                    recent_fails += 1
            recent_advisories += advisory_count_this_test

        elif years_ago <= medium_window_years:
            if is_failed:
                if is_same_day_retest(mot_tests, test):
                    print(f"   ℹ️ MOT fail on {test_date[:10]} was same-day retest — not penalised")
                else:
                    medium_fails += 1
            medium_advisories += advisory_count_this_test

    fail_penalty = (recent_fails * 200) + (medium_fails * 100)
    advisory_penalty = (recent_advisories * advisory_rate_recent) + (medium_advisories * advisory_rate_medium)

    # Cap advisories — even on a new car, advisory penalty shouldn't dominate
    advisory_penalty = min(advisory_penalty, 500)

    raw_penalty = fail_penalty + advisory_penalty

    # --- Chronic failure bonus penalty ---
    # A car that fails its MOT in consecutive years is a structural risk,
    # not a one-off problem. Count real (non-same-day) fails across all history.
    all_real_fails = []
    for test in mot_tests:
        if test.get("testResult") == "FAILED" and not is_same_day_retest(mot_tests, test):
            test_date = test.get("completedDate", "")[:10]
            if test_date:
                try:
                    all_real_fails.append(int(test_date[:4]))
                except:
                    pass

    # Count how many years had at least one real fail
    years_with_fails = set(all_real_fails)
    consecutive_fail_years = 0
    if len(years_with_fails) >= 2:
        sorted_fail_years = sorted(years_with_fails)
        # Walk through and count runs of consecutive years
        run = 1
        max_run = 1
        for i in range(1, len(sorted_fail_years)):
            if sorted_fail_years[i] - sorted_fail_years[i-1] <= 1:
                run += 1
                max_run = max(max_run, run)
            else:
                run = 1
        consecutive_fail_years = max_run

    chronic_bonus = 0
    if consecutive_fail_years >= 4:
        # 4+ consecutive years of failing = serious chronic issues
        chronic_bonus = 300
        print(f"   🔴 Chronic failer: {consecutive_fail_years} consecutive years with real MOT fails → +£{chronic_bonus} penalty")
    elif consecutive_fail_years >= 2:
        chronic_bonus = 150
        print(f"   🟡 Repeat failer: {consecutive_fail_years} consecutive years with real MOT fails → +£{chronic_bonus} penalty")

    # --- Clean history discount ---
    # If the car has ZERO real failures across its entire MOT history,
    # it has a clean record — reduce the advisory penalty slightly.
    # This differentiates a genuinely well-maintained car from a problem car
    # that happened to pass its most recent test.
    clean_history = len(all_real_fails) == 0 and len(mot_tests) >= 2
    clean_discount = 0
    if clean_history:
        # Reduce advisory penalty by 20% — clean car, advisories are routine wear
        clean_discount = round(advisory_penalty * 0.20, 2)
        print(f"   ✅ Clean MOT history (0 real fails, {len(mot_tests)} tests) → advisory discount £{clean_discount}")

    raw_penalty = fail_penalty + advisory_penalty + chronic_bonus - clean_discount

    # Age factor: older cars get a further overall reduction
    # because some risk is already priced in at the purchase price
    if vehicle_age <= 5:
        age_factor = 1.0
    elif vehicle_age <= 10:
        age_factor = 0.85
    elif vehicle_age <= 15:
        age_factor = 0.65
    else:
        age_factor = 0.45

    adjusted_penalty = raw_penalty * age_factor

    # Cap penalty relative to vehicle value — never more than 25% of asking
    # (was 30% — reduced because the per-advisory rates are now more accurate)
    if asking_price and asking_price > 0:
        value_cap = asking_price * 0.25
    else:
        value_cap = 2000

    final_penalty = min(adjusted_penalty, value_cap)

    print(f"   🔧 MOT penalty calc: age={vehicle_age}yr, recent_fails={recent_fails}, medium_fails={medium_fails}")
    print(f"   🔧 recent_advisories={recent_advisories}, medium_advisories={medium_advisories}")
    print(f"   🔧 fail_penalty=£{fail_penalty}, advisory_penalty=£{round(advisory_penalty,2)}, age_factor={age_factor}")
    print(f"   🔧 raw=£{round(raw_penalty,2)}, adjusted=£{round(adjusted_penalty,2)}, final=£{round(final_penalty,2)}")

    # Find the most recent MOT expiry date for cache TTL calculation
    mot_expiry_date = None
    passed_tests = [t for t in mot_tests if t.get("testResult") == "PASSED"]
    if passed_tests:
        latest_pass = max(passed_tests, key=lambda t: t.get("completedDate", ""))
        mot_expiry_date = latest_pass.get("expiryDate")
        if mot_expiry_date:
            # DVSA returns expiryDate in format YYYY-MM-DD
            mot_expiry_date = str(mot_expiry_date)[:10]

    return {
        "mot_summary": {
            "fail_count": recent_fails + medium_fails,
            "advisory_count": recent_advisories + medium_advisories,
            "mot_penalty": round(final_penalty, 2),
            "clean_history": clean_history,
        },
        "mot_full_data": mot_tests,
        "mot_expiry_date": mot_expiry_date,
        "vehicle_data": {
            "make": data.get("make"),
            "model": data.get("model"),
            "first_used_date": data.get("firstUsedDate"),
            "fuel_type": data.get("fuelType"),
            "engine_size": data.get("engineSize"),
            "colour": data.get("primaryColour"),
        }
    }


def build_empty_response():
    return {
        "mot_summary": {
            "fail_count": 0,
            "advisory_count": 0,
            "mot_penalty": 0,
        },
        "mot_full_data": [],
        "vehicle_data": {}
    }