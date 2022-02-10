"""
bzfunds.dbm
~~~~~~~~~~~

Database manager to persist data requests.
"""

import logging
from typing import Optional

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError


__all__ = ("Manager",)


logger = logging.getLogger(__name__)


class Manager:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 27017,
        db: str = "bzfundsDB",
        collection: str = "funds",
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.db = db
        self.collection = collection

        try:
            self.client = MongoClient(
                host=host
                port=port,
                username=username,
                password=password,
            )
        except ServerSelectionTimeoutError as e:
            logger.error(f"Failed to connect to database - {e}")
        else:
            self.db = self.client[db_name]
            self.collection = self.db[collection]
