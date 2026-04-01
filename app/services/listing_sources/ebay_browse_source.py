from app.services.listing_sources.base import ListingSource
from app.services.ebay_browse_service import search_ebay_browse


class EbayBrowseSource(ListingSource):

    def search(
        self,
        keywords: str,
        entries: int = 20,
        min_price=None,
        max_price=None,
        sort="newlyListed",
        offset=0,
        buyer_postcode=None,
        radius_miles=None,
        start_time_filter=None,
        **kwargs
    ):
        return search_ebay_browse(
            keywords=keywords,
            limit=entries,
            min_price=min_price or 0,
            max_price=max_price or 50000,
            sort=sort,
            offset=offset,
            buyer_postcode=buyer_postcode,
            radius_miles=radius_miles,
            start_time_filter=start_time_filter,
        )