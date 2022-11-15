import asyncio
from datetime import datetime

from sqlalchemy.orm import sessionmaker, Session

from models.rarity import Rarity
from repositories.tcgplayer_catalog_repository import TCGPlayerCatalogRepository
from services import paginate


class DatabaseSyncWorker:
    def __init__(self, catalog_repository: TCGPlayerCatalogRepository, session: Session):
        self.catalog_repository: TCGPlayerCatalogRepository = catalog_repository
        self.session = session

    def _add_updated_set_models(self, outdated_sets: list, set_responses: list):
        from models.set import Set

        for set_response in set_responses:
            response_set_model = Set.from_tcgplayer_response(set_response)

            existing_set_model: Set = self.session.get(Set, response_set_model.id)

            if (existing_set_model is None or
                    existing_set_model.modified_date < response_set_model.modified_date):
                outdated_sets.append(response_set_model)

    def _convert_and_insert_cards_and_skus(self, card_responses):
        from models.card import Card
        from models.sku import Sku

        self.catalog_repository.insert_cards(
            map(lambda card_response: Card.from_tcgplayer_response(card_response, self.session), card_responses)
        )

        for card_response in card_responses:
            self.catalog_repository.insert_skus(
                map(lambda sku_response: Sku.from_tcgplayer_response(sku_response), card_response['skus'])
            )

    def _fetch_cards_in_set(self, set_id: int) -> list:
        print(set_id)
        set_card_count = self.catalog_repository.fetch_total_card_count(set_id)
        set_cards = []

        paginate(
            total=set_card_count,
            paginate_fn=lambda offset, limit: self.catalog_repository.fetch_cards(offset, limit, set_id),
            on_paginated=lambda card_responses: set_cards.extend(card_responses)
        )

        return set_cards

    def update_card_database(self):
        from models.condition import Condition
        from models.printing import Printing

        print(f'{self.__class__.__name__} started at {datetime.now()}')

        printing_responses = self.catalog_repository.fetch_card_printings()
        condition_responses = self.catalog_repository.fetch_card_conditions()
        rarity_responses = self.catalog_repository.fetch_card_rarities()

        condition_models = list(map(lambda x: Condition.from_tcgplayer_response(x), condition_responses))
        printing_models = list(map(lambda x: Printing.from_tcgplayer_response(x), printing_responses))
        rarity_models = list(map(lambda x: Rarity.from_tcgplayer_response(x), rarity_responses))

        self.catalog_repository.insert_conditions(condition_models)
        self.catalog_repository.insert_printings(printing_models)
        self.catalog_repository.insert_rarities(rarity_models)

        set_total_count = self.catalog_repository.fetch_total_card_set_count()

        outdated_sets = []
        paginate(
            total=set_total_count,
            paginate_fn=self.catalog_repository.fetch_card_sets,
            on_paginated=lambda set_responses: self._add_updated_set_models(outdated_sets, set_responses)
        )

        print(f'{len(outdated_sets)} sets are oudated: {[outdated_set.name for outdated_set in outdated_sets]}')

        self.catalog_repository.insert_sets(outdated_sets)

        # We want to "paginate" on all the card sets and fetch the cards in each set. Hence, we call paginate
        # with pagination_size=1.
        paginate(
            total=len(outdated_sets),
            paginate_fn=lambda offset, limit: self._fetch_cards_in_set(outdated_sets[offset].id),
            on_paginated=lambda card_responses: self._convert_and_insert_cards_and_skus(card_responses),
            num_parallel_requests=1,
            pagination_size=1,
        )

        self.session.commit()

        print(f'{self.__class__.__name__} done at {datetime.now()}')
