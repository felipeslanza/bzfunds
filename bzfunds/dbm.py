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
        self.host = host if "localhost" in host else f"mongodb+srv://{host}"
        self.port = port
        self.db = db
        self.collection = collection
        self.username = username
        self.password = password
        self.client_settings = {**DEFAULT_CLIENT_SETTINGS, **client_settings}

        self.setup()

    def setup(self):
        try:
            self.client = MongoClient(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                **self.client_settings,
            )
        except pymongo.errors.ServerSelectionTimeoutError as e:
            logger.error(f"Failed to setup to database - {e}")
        else:
            self.db = self.client[self.db]
            self.collection = self.db[self.collection]
            self.collection.create_index(
                [
                    ("date", pymongo.ASCENDING),
                    ("fund_cnpj", pymongo.ASCENDING),
                ],
                unique=True,
            )

    def write_df(self, df: pd.DataFrame):
        try:
            self.collection.insert_many(df.to_dict(orient="records"), ordered=False)
        except pymongo.errors.BulkWriteError as e:
            for err_obj in e.details["writeErrors"]:
                logger.error(err_obj["errmsg"])
