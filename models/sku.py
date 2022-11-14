from sqlalchemy import Column, Integer, ForeignKey, SmallInteger
from sqlalchemy.orm import relationship

from models import Base


class Sku(Base):
    __tablename__ = "sku"

    id = Column(Integer, primary_key=True)
    card_id = Column(Integer, ForeignKey('card.id'), nullable=False)
    card = relationship("Card", back_populates="skus")
    printing_id = Column(SmallInteger, ForeignKey('printing.id'))
    printing = relationship("Printing")
    condition_id = Column(SmallInteger, ForeignKey('condition.id'))
    condition = relationship("Condition")

    @staticmethod
    def from_tcgplayer_response(response: dict):
        return Sku(
            id=response['skuId'],
            card_id=response['productId'],
            printing_id=response['printingId'],
            condition_id=response.get('conditionId'),
        )
