from app.services.listing_sources.ebay_browse_source import EbayBrowseSource


def get_listing_source(name: str):

    if name == "ebay_browse":
        return EbayBrowseSource()

    raise ValueError(f"Unknown listing source: {name}")