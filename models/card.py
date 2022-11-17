from sqlalchemy import Column, Integer, String, ForeignKey, SmallInteger, Text
from sqlalchemy.orm import relationship, Session

from models import Base


class Card(Base):
    __tablename__ = "card"

    id = Column(Integer, primary_key=True)
    # Blue-Eyes White Dragon
    name = Column(String(255), index=True)
    # name but without hyphens, semicolons, etc
    clean_name = Column(String(255), index=True)
    image_url = Column(String(255))
    set_id = Column(Integer, ForeignKey("set.id"), nullable=False)
    set = relationship("Set", back_populates="cards")
    number = Column(String(255), index=True)
    # Ultra rare
    rarity_id = Column(SmallInteger, ForeignKey("rarity.id"), nullable=True)
    rarity = relationship("Rarity")
    # LIGHT
    attribute = Column(String(255))
    # Normal Monster
    card_type = Column(String(255))
    # Dragon
    monster_type = Column(String(255))
    attack = Column(String(255))
    defense = Column(String(255))
    description = Column(Text)
    skus = relationship("Sku", back_populates="card")

    @staticmethod
    def parse_extended_data(extended_data: list) -> dict:
        return {data['name']: data['value'] for data in extended_data}

    @staticmethod
    def from_tcgplayer_response(response: dict, session: Session):
        from models.rarity import Rarity

        card_metadata = Card.parse_extended_data(response['extendedData'])
        rarity = session.query(Rarity).filter_by(name=card_metadata.get('Rarity')).one_or_none()
        print(rarity)
        return Card(
            id=response['productId'],
            name=response['name'],
            clean_name=response['cleanName'],
            image_url=response['imageUrl'],
            set_id=response['groupId'],
            number=card_metadata.get('Number'),
            rarity_id=rarity.id if rarity is not None else None,
            attribute=card_metadata.get('Attribute'),
            card_type=card_metadata.get('Card Type'),
            monster_type=card_metadata.get('MonsterType'),
            attack=card_metadata.get('Attack'),
            defense=card_metadata.get('Defense'),
            description=card_metadata.get('Description'),
        )
