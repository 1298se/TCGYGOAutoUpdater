import os

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as DBSession

from models import Base
# We need to import new models to have them automatically created
from models import card, condition, printing, rarity, set, sku
from repositories.tcgplayer_catalog_repository import TCGPlayerCatalogRepository
from services.db_sync_worker import DatabaseSyncWorker
from services.tcgplayer_api_service import TCGPlayerApiService

# noinspection PyStatementEffect
card, condition, printing, rarity, set, sku

load_dotenv()

SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URI")

engine = create_engine(SQLALCHEMY_DATABASE_URI, echo=True, future=True)

db_session = DBSession(engine)
tcgplayer_api_service = TCGPlayerApiService()
tcgplayer_catalog_repository = TCGPlayerCatalogRepository(tcgplayer_api_service, db_session)
db_sync_worker = DatabaseSyncWorker(tcgplayer_catalog_repository, db_session)

Base.metadata.create_all(engine)

if __name__ == '__main__':
    db_sync_worker.update_card_database()