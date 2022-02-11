# +++++++++++++++++++++++++++++++++++++++++++++++++++
# TODO list:
# +++++++++++++++++++++++++++++++++++++++++++++++++++
# - add caching when DB is implemented
# - add options to i) force query, ii) commit results
# +++++++++++++++++++++++++++++++++++++++++++++++++++

"""
bzfunds.data
~~~~~~~~~~~~~

This module implements functions to GET and parse data from CVM's
daily funds database_.

.. _database: http://dados.cvm.gov.br/dataset/fi-doc-inf_diario
"""

import glob
import logging
import os
import shutil
from datetime import datetime
from io import StringIO
from tempfile import TemporaryDirectory
from typing import Optional

import pandas as pd
import requests
from joblib import Parallel, delayed
from typeguard import typechecked

from .constants import API_FIRST_VALID_DATE, API_LAST_ZIPPED_DATE
from .utils import get_url_from_date, parse_csv


__all__ = ("get_monthly_data", "get_history")


logger = logging.getLogger(__name__)


# Constants
# ----
FILENAME = "temp.zip"
TOMORROW = datetime.today() + pd.Timedelta("1d")


# Private Helpers
# ----
@typechecked
def _handle_csv_request(date: datetime) -> Optional[pd.DataFrame]:
    """Takes a `date`, requests a monthly `csv` file and return a parsed `DataFrame`"""
    url = get_url_from_date(date, zipped=False)
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.ConnectionError as e:
        logger.error("Connection error")
    except (requests.exceptions.Timeout, requests.exceptions.HTTPError) as e:
        logger.error("Service unavailable. Try again later")
    else:
        # pd.to_pickle(response, f"tests/sample_responseponse_{url[-10:-4]}.pkl")
        if response is not None:
            csv_buffer = StringIO(response.content.decode("utf-8"))
            return parse_csv(csv_buffer)


@typechecked
def _handle_zip_request(date: datetime) -> Optional[pd.DataFrame]:
    """Takes a `date`, requests an annual `zip` file, unzips it, and return a
    parsed `DataFrame (with annual data)`
    """
    url = get_url_from_date(date, zipped=True)
    with TemporaryDirectory() as temp_dir:
        filepath = os.path.join(temp_dir, FILENAME)

        # 1. Download bulk/zipped file
        try:
            with requests.get(url, stream=True) as res:
                res.raise_for_status()
                with open(filepath, "wb") as fp:
                    shutil.copyfileobj(res.raw, fp)
        except requests.exceptions.ConnectionError as e:
            logger.error("Connection error")
        except (requests.exceptions.Timeout, requests.exceptions.HTTPError) as e:
            logger.error("Service unavailable. Try again later")

        # 2. Unzip and parse monthly files
        try:
            shutil.unpack_archive(filepath, temp_dir)
        except FileNotFoundError:
            logger.error("Failed to download bulk file")
        else:
            df_list = [parse_csv(p) for p in glob.glob(f"{temp_dir}/*.csv")]
            if df_list:
                return pd.concat(df_list, axis=0).sort_index()


# Public Functions
# ----
@typechecked
def get_monthly_data(
    date: datetime,
    full_year: bool = False,
) -> Optional[pd.DataFrame]:
    """Get data for a single month.

    ...

    Parameters
    ----------
    date : `datetime`
    full_year : `bool`
        if True and `date < API_LAST_ZIPPED_DATE`, will return data for the whole year
    """

    if (date < API_FIRST_VALID_DATE) or (date >= TOMORROW):
        # Don't bother
        return
    elif date > API_LAST_ZIPPED_DATE:
        # New-format dates, i.e. directly thru single-month `csv` file
        return _handle_csv_request(date)
    else:
        # Old-format dates, i.e. zipped file with whole-year data
        df = _handle_zip_request(date)
        if df is not None:
            if full_year:
                return df
            return df.loc[df.index.month == date.month]


@typechecked
def get_history(
    start_dt: datetime,
    end_dt: datetime,
    n_jobs: int = -1,
) -> Optional[pd.DataFrame]:
    """Get all monthly data available from `start_dt` to `end_dt`

    ...

    Parameters
    ----------
    start_dt : `datetime`
    end_dt : `datetime`
    n_jobs : `int`
        # of jobs forwarded to `joblib.Parallel` call
    """
    if start_dt >= end_dt:
        raise ValueError("`start_dt` must be < `end_dt`")

    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # TODO: range not getting right bound
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # TODO: handle old-format requests (query whole-year only once)
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    dates = pd.date_range(start_dt, end_dt, freq="m")
    queue = Parallel(n_jobs=n_jobs)(delayed(get_monthly_data)(date) for date in dates)
    df_list = [df for df in queue if df is not None]
    if df_list:
        return pd.concat(df_list, axis=0).sort_index()
