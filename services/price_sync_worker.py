from datetime import datetime
from typing import Optional

from flask.ctx import AppContext

from repositories.tcgplayer_price_repository import TCGPlayerPriceRepository
from services import paginate


class PriceSyncWorker:
    def __init__(self):
        self.app_context = None
        self.price_repository: Optional[TCGPlayerPriceRepository] = None

    def init(self, price_repository: TCGPlayerPriceRepository, app_context: AppContext):
        self.price_repository = price_repository
        self.app_context = app_context

    def paginate_and_fetch_sku_prices(self, offset, limit):
        from models.sku import Sku

        cur_page = offset / limit + 1
        with self.app_context:
            skus = Sku.query.paginate(page=cur_page, per_page=limit).items

        sku_ids = [sku.id for sku in skus]
        return self.price_repository.fetch_sku_prices(sku_ids)

    async def update_prices(self):
        from models.sku import Sku

        print(f'{self.__class__.__name__} running at {datetime.now()}')

        with self.app_context:
            sku_total_count = Sku.query.count()

        job_start_date = datetime.now()

        await paginate(
            total=sku_total_count,
            paginate_fn=lambda offset, limit: self.paginate_and_fetch_sku_prices(offset, limit),
            on_paginated=lambda sku_price_responses: self.price_repository.update_sku_prices(
                sku_price_responses,
                job_start_date
            )
        )

        print(f'Price sync done at {datetime.now()}')
