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
from .dbm import Manager
from .utils import get_url_from_date, parse_csv


__all__ = ("get_monthly_data", "get_history")


logger = logging.getLogger(__name__)


# Constants
# ----
FILENAME = "temp.zip"
TOMORROW = datetime.today() + pd.Timedelta("1d")


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


@typechecked
def get_monthly_data(
    date: datetime,
    *,
    full_year: bool = False,
    commit: bool = True,
    manager: Optional[Manager] = None,
) -> Optional[pd.DataFrame]:
    """Get data for a single month.

    ...

    Parameters
    ----------
    date : `datetime`
    full_year : `bool`
        if `True` and `date < API_LAST_ZIPPED_DATE`, will return data for the whole year
    commit : `bool`
        if True, will write data to provided `manager` and return `None`
    """
    if (date < API_FIRST_VALID_DATE) or (date >= TOMORROW):
        # Don't bother
        return

    if date > API_LAST_ZIPPED_DATE:
        # New-format dates, i.e. directly thru single-month `csv` file
        df = _handle_csv_request(date)
    else:
        # Old-format dates, i.e. zipped file with whole-year data
        df = _handle_zip_request(date)
        if df is not None:
            if not full_year:
                df = df.loc[df.index.month == date.month]

    if df is not None and not df.empty:
        if commit:
            assert manager is not None, "Requires a `manager` to `commit`"
            manager.write_df(df.reset_index())  # `date` must be a column
        else:
            return df


@typechecked
def get_history(
    start_dt: datetime,
    end_dt: datetime,
    *,
    commit: bool = True,
    manager: Optional[Manager] = None,
    n_jobs: int = -1,
) -> Optional[pd.DataFrame]:
    """Get all monthly data available from `start_dt` to `end_dt`

    ...

    Parameters
    ----------
    start_dt : `datetime`
    end_dt : `datetime`
    manager : `Manager`
    commit : bool
        if True, will write data to provided `manager`
    n_jobs : `int`
        # of jobs forwarded to `joblib.Parallel` call
    """
    if start_dt >= end_dt:
        raise ValueError("`start_dt` must be < `end_dt`")
    elif end_dt < API_LAST_ZIPPED_DATE and end_dt.month != 12:
        raise ValueError("`end_dt` must be be annual when querying old dates")

    if not commit:
        logger.warning("Running without committing might require a lot of memory!")

    dates = pd.period_range(start_dt, end_dt, freq="m").to_timestamp().to_series()

    # Redundant to get one month at a time with old-format (already parses whole year)
    pre_dates = dates.loc[:API_LAST_ZIPPED_DATE].resample("y").last().index
    pre_queue = Parallel(n_jobs=n_jobs, backend="threading")(
        delayed(get_monthly_data)(date, full_year=True, commit=commit, manager=manager)
        for date in pre_dates
    )

    post_dates = dates.loc[API_LAST_ZIPPED_DATE:].index
    post_queue = Parallel(n_jobs=n_jobs, backend="threading")(
        delayed(get_monthly_data)(date, full_year=False, commit=commit, manager=manager)
        for date in post_dates
    )

    # List should be empty when `commit=True`
    df_list = [df for df in (*pre_queue, *post_queue) if df is not None]
    if df_list:
        df = pd.concat(df_list, axis=0).sort_index()

        # Must re-`loc` to ensure non-annual `start_dt` or `end_dt` are respected
        # when querying old-format dates
        start_month = start_dt.strftime("%Y-%m")
        end_month = end_dt.strftime("%Y-%m")

        return df.loc[start_month:end_month]
