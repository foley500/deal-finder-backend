from typing import Dict


def parse_facebook_listing(url: str) -> Dict:
    """
    Replace this with however you legally obtain
    listing data from a Facebook URL.
    """

    # Placeholder example
    return {
        "id": url.split("/")[-1],
        "title": "Facebook Vehicle Listing",
        "price": 5000,
        "description": "",
        "image_url": None,
        "view_url": url,
        "seller": None,
        "location": None,
        "aspects": {},
    }