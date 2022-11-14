from sqlalchemy import Column, SmallInteger, String

from models import Base


class Printing(Base):
    __tablename__ = "printing"

    id = Column(SmallInteger, primary_key=True)
    name = Column(String(255))
    order = Column(SmallInteger)

    @staticmethod
    def from_tcgplayer_response(response: dict):
        return Printing(
            id=response['printingId'],
            name=response['name'],
            order=response['displayOrder'],
        )
