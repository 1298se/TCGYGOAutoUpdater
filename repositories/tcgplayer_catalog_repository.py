from typing import Optional

from sqlalchemy.orm.session import Session

from models import Base
from services.tcgplayer_api_service import TCGPlayerApiService


class TCGPlayerCatalogRepository:

    def __init__(self, api_service: TCGPlayerApiService, session: Session):
        self.api_service: TCGPlayerApiService = api_service
        self.session = session

    def fetch_card_printings(self):
        response = self.api_service.get_card_printings()
        return response.get('results', [])

    def fetch_card_conditions(self):
        response = self.api_service.get_card_conditions()
        return response.get('results', [])

    def fetch_card_rarities(self):
        response = self.api_service.get_card_rarities()
        return response.get('results', [])

    def fetch_card_sets(self, offset, limit) -> Optional[list]:
        response = self.api_service.get_sets(offset, limit)
        return response.get('results', [])

    def fetch_total_card_set_count(self) -> Optional[int]:
        response = self.api_service.get_sets(offset=0, limit=1)
        return response.get('totalItems', 0)

    def fetch_cards(self, offset, limit, set_id=None) -> Optional[list]:
        response = self.api_service.get_cards(offset=offset, limit=limit, set_id=set_id)
        return response.get('results', [])

    def fetch_total_card_count(self, set_id=None) -> Optional[int]:
        response = self.api_service.get_cards(offset=0, limit=1, set_id=set_id)
        return response.get('totalItems', 0)

    def insert_printings(self, printings):
        self._insert_or_update(printings)

    def insert_conditions(self, conditions):
        self._insert_or_update(conditions)

    def insert_rarities(self, rarities):
        self._insert_or_update(rarities)

    def insert_sets(self, sets):
        self._insert_or_update(sets)

    def insert_cards(self, cards):
        self._insert_or_update(cards)

    def insert_skus(self, skus):
        self._insert_or_update(skus)

    def _insert_or_update(self, models: list):
        for model in models:
            self.session.merge(model)
