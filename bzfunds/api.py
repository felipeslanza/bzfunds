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
def download_data(
    *,
    start_year: Optional[Union[str, float]] = None,
    update_only: bool = True,
    manager: Manager = DEFAULT_DB_MANAGER,
):
    """Download available data and insert it into the configured database.

    ...

    Parameters
    ----------
    start_year : str or float
        starting year to query data. If not provided, defaults to last 5 years
    update_only : `bool`
        if True, will use the last available date in `manager` as the starting
        query date (this is not a `diff` against the database!)
    manager : Manager
        loaded instance of database manager
    """
    assert start_year or update_only, "Must provide a `start_year` or `update_only` flag"

    if update_only:
        cursor = manager.collection.find().limit(1).sort("date", pymongo.DESCENDING)
        try:
            start_dt = cursor[0]["date"]
        except (IndexError, KeyError):
            logger.warning("No previous data found. Querying all available history.")
    else:
        # Defaults to last 5 years if not provided
        if not start_year:
            start_year = (datetime.today() - pd.Timedelta(f"{int(365 * 5)}D")).year
        start_dt = pd.to_datetime(f"{start_year}-01-01")

    try:
        _ = get_history(
            start_dt=start_dt,
            end_dt=datetime.today(),
            commit=True,
            manager=manager,
        )
    except ValueError as e:
        logger.error(e)


@typechecked
def get_data(
    funds: Optional[Union[str, list]] = None,
    start_dt: Optional[Union[str, datetime]] = None,
    end_dt: Optional[Union[str, datetime]] = None,
    manager: Manager = DEFAULT_DB_MANAGER,
) -> Optional[pd.DataFrame]:
    """Easily query the configured database.

    ...

    Parameters
    ----------
    funds : `str` or `list`
    start_dt : str or datetime
        string must be in YYYY-MM-DD format
    end_dt : str or datetime
        string must be in YYYY-MM-DD format
    manager : Manager
        loaded instance of database manager
    """
    if isinstance(funds, str):
        funds = [funds]

    search = {}
    if funds:
        search["fund_cnpj"] = {"$in": funds}

    if start_dt or end_dt:
        search["date"] = {}
        if start_dt:
            search["date"]["$gte"] = pd.to_datetime(start_dt)
        if end_dt:
            search["date"]["$lte"] = pd.to_datetime(end_dt)

    cursor = list(manager.collection.find(search))
    if cursor:
        df = pd.DataFrame(cursor).set_index("date").sort_index().drop("_id", axis=1)

        return df
