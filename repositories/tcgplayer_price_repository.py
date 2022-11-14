from typing import Optional

from services.tcgplayer_api_service import TCGPlayerApiService


class TCGPlayerPriceRepository:

    def __init__(self, api_service: TCGPlayerApiService):
        self.api_service: TCGPlayerApiService = api_service

    async def fetch_sku_prices(self, sku_ids: list[int]):
        response = await self.api_service.get_sku_prices(sku_ids)
        return response.get('results', [])

    """def update_sku_prices(self, sku_price_responses, date):
        from models.sku import Sku
        from models.sku_pricing_info import SkuPricingInfo

        with self.app_context:
            for response in sku_price_responses:
                sku_id = response['skuId']

                sku: Sku = Sku.query.get(sku_id)

                sku.current_lowest_base_price = response['lowPrice']
                sku.current_lowest_shipping_price = response['lowestShipping']
                sku.current_lowest_listing_price = response['lowestListingPrice'],
                sku.current_market_price = response['marketPrice']

                # Only save pricing info for Near Mint cards because price trends for other conditions
                # don't really make sense.
                if sku.condition_id == 1:
                    sku_pricing_info = SkuPricingInfo.from_tcgplayer_response(response, date)

                    self.db.session.add(sku_pricing_info)

            self.db.session.commit()"""
