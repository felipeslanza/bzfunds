"""
bzfunds.dbm
~~~~~~~~~~~

Database manager to persist data requests.
"""

import logging
from typing import Optional

import pandas as pd
import pymongo
from pymongo import MongoClient


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
        self.host = self.parse_host(host)
        self.port = port
        self.db = db
        self.collection = collection
        self.username = username
        self.password = password

        self.client_settings = {
            **DEFAULT_CLIENT_SETTINGS,
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "password": self.password,
            **client_settings,
        }

        self.setup()

    @staticmethod
    def parse_host(host: str) -> str:
        if len(host.split("//")) > 1:
            # Prefix is present
            return host
        elif host.endswith("mongodb.net"):
            # Single host resolving to multiple hosts (e.g. Atlas)
            return f"mongodb+srv://{host}"
        else:
            # Single host
            return f"mongodb://{host}"

    def setup(self):
        try:
            self.client = MongoClient(**self.client_settings)
        except pymongo.errors.ServerSelectionTimeoutError as e:
            logger.error(f"Failed to setup to database - {e}")
        else:
            self.db = self.client[self.db]
            self.collection = self.db[self.collection]
            self.collection.create_index("date")
            self.collection.create_index("fund_cnpj")
            self.collection.create_index(
                [
                    ("date", pymongo.DESCENDING),
                    ("fund_cnpj", pymongo.ASCENDING),
                ],
                unique=True,
            )

    def write_df(self, df: pd.DataFrame, orient: str = "records"):
        try:
            self.collection.insert_many(df.to_dict(orient=orient), ordered=False)
        except pymongo.errors.BulkWriteError as e:
            for err_obj in e.details["writeErrors"]:
                logger.error(err_obj["errmsg"])
