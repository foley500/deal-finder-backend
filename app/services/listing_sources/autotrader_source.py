from app.services.listing_sources.base import ListingSource
from app.services.autotrader_service import search_autotrader


class AutoTraderSource(ListingSource):

    def search(
        self,
        keywords: str,
        entries: int = 50,
        min_price=None,
        max_price=None,
        min_year=None,
        max_year=None,
        max_mileage=None,
        **kwargs
    ):
        return search_autotrader(
            keywords=keywords,
            min_price=min_price,
            max_price=max_price,
            min_year=min_year,
            max_year=max_year,
            max_mileage=max_mileage,
        )