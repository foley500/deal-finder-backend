from abc import ABC, abstractmethod

class ListingSource(ABC):

    @abstractmethod
    def search(self, keywords: str, entries: int = 30):
        pass
