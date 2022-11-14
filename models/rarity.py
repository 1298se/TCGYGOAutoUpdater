from sqlalchemy import Column, SmallInteger, String

from models import Base


class Rarity(Base):
    __tablename__ = "rarity"

    id = Column(SmallInteger, primary_key=True)
    name = Column(String(255), index=True)

    @staticmethod
    def from_tcgplayer_response(response: dict):
        return Rarity(
            id=response['rarityId'],
            name=response['displayText'],
        )
