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


DEFAULT_CLIENT_SETTINGS = {
    "connectTimeoutMS": 2500,
    "serverSelectionTimeoutMS": 2500,
    "retryWrites": True,
}


class Manager:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 27017,
        *,
        db: str = "bzfundsDB",
        collection: str = "funds",
        username: Optional[str] = None,
        password: Optional[str] = None,
        **client_settings,
    ):
        self.host = host
        self.port = port
        self.db = db
        self.collection = collection
        self.client_settings = {**DEFAULT_CLIENT_SETTINGS, **client_settings}

        self.connect()

    def connect(self):
        try:
            self.client = MongoClient(
                host=host,
                port=port,
                username=username,
                password=password,
                **self.client_settings,
            )
        except ServerSelectionTimeoutError as e:
            logger.error(f"Failed to connect to database - {e}")
        else:
            self.db = self.client[db]
            self.collection = self.db[collection]
