import os
import requests


def get_cap_valuation(registration: str):
    """
    Fetch valuation data from CAP API.
    Returns structured dict or None.
    """

    if not registration:
        return None

    username = os.getenv("CAP_USERNAME")
    password = os.getenv("CAP_PASSWORD")
    url = os.getenv("CAP_URL")

    if not username or not password or not url:
        print("CAP credentials missing.")
        return None

    try:
        response = requests.post(
            url,
            auth=(username, password),
            json={"registration": registration},
            timeout=10
        )

        response.raise_for_status()

        data = response.json()

        # 🔥 IMPORTANT:
        # Adjust these keys once CAP gives you real schema.
        return {
            "clean": data.get("capTradeClean"),
            "retail": data.get("capRetail"),
            "trade": data.get("capTrade"),
        }

    except requests.RequestException as e:
        print("CAP API request error:", e)
        return None

    except ValueError:
        print("CAP returned invalid JSON.")
        return None