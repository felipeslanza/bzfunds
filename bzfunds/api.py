"""
bzfunds.api
~~~~~~~~~~~

This module implements an interface to conveniently store and search
funds' data.
"""

import logging
import sys
from datetime import datetime
from typing import Optional, Union

import pandas as pd
import pymongo
from typeguard import typechecked

from . import settings
from .constants import API_FIRST_VALID_DATE
from .data import get_history
from .dbm import Manager


__all__ = ("download_data", "get_data")


logging.basicConfig(
    stream=sys.stdout,
    level=settings.LOGGING_LEVEL,
    format=settings.LOGGING_FORMAT,
)
logger = logging.getLogger(__name__)


# Globals
# ----
DEFAULT_DB_MANAGER = Manager(**settings.MONGODB)


@typechecked
def download_data(update_only: bool = True, manager: Manager = DEFAULT_DB_MANAGER):
    """Download available data and insert it into the configured database.

    ...

    Parameters
    ----------
    update_only : `bool`
        if True, will use the last available date in `manager` as the starting
        query date (this is not a `diff` against the database!)
    """
    last_db_date = API_FIRST_VALID_DATE
    if update_only:
        cursor = manager.collection.find().limit(1).sort("date", pymongo.DESCENDING)
        try:
            last_db_date = cursor[0]["date"]
        except (IndexError, KeyError):
            logger.warning("No previous data found. Querying all available history.")

    try:
        _ = get_history(
            # start_dt=pd.to_datetime("2021-12-1"),
            start_dt=last_db_date,
            end_dt=datetime.today(),
            commit=True,
            manager=manager,
        )
    except ValueError as e:
        logger.error(e)


@typechecked
def get_data(
    funds: list = Optional[None],
    start_date: Optional[Union[str, datetime]] = None,
    end_date: Optional[Union[str, datetime]] = None,
    manager: Manager = DEFAULT_DB_MANAGER,
) -> Optional[pd.DataFrame]:
    """Easily query the configured database.

    ...

    Parameters
    ----------
    funds : `list`
    start_date : `str or `datetime`
    end_date : `str or `datetime`
    """
    pass


if __name__ == "__main__":
    pass
