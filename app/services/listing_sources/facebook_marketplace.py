# app/services/listing_sources/facebook_marketplace.py

from typing import List, Dict


class FacebookMarketplaceSource:

    def search(
        self,
        keywords: str,
        entries: int = 50,
        min_price=None,
        max_price=None,
    ) -> List[Dict]:

        """
        Must return normalized raw_item dictionaries.

        Replace the body of this function with however you
        legally obtain Marketplace listing data.
        """

        listings = []

        # Example placeholder (proves pipeline works)
        example = {
            "id": "fb_demo_001",
            "title": "2018 Audi A4 2.0 TDI S Line",
            "price": 7200,
            "description": "Full service history. Clean car.",
            "image_url": None,
            "view_url": "https://facebook.com/example",
            "seller": "Private Seller",
            "location": "Manchester",
            "aspects": {},
        }

        listings.append(example)

        return listings