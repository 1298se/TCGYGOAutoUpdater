from sqlalchemy import SmallInteger, Column, String

from models import Base


class Condition(Base):
    __tablename__ = "condition"

    id = Column(SmallInteger, primary_key=True)
    name = Column(String(255))
    abbreviation = Column(String(255))
    order = Column(SmallInteger)

    @staticmethod
    def from_tcgplayer_response(response: dict):
        return Condition(
            id=response['conditionId'],
            name=response['name'],
            abbreviation=response['abbreviation'],
            order=response['displayOrder'],
        )
