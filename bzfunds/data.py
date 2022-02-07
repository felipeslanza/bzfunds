"""
bzfunds.data
~~~~~~~~~~~~~

This module implements functions to GET and parse data from CVM's
daily funds database_.

.. _database: http://dados.cvm.gov.br/dataset/fi-doc-inf_diario
"""

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
import requests
from typeguard import typechecked

from .utils import get_url_from_date, parse_data_from_response


__all__ = ("get_monthly_data", "get_history")


logger = logging.getLogger(__name__)


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++
# TODO: add caching when DB is implemented
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++
# TODO: add options to i) force query, ii) commit results
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++


@typechecked
def get_monthly_data(date: datetime) -> Optional[pd.DataFrame]:
    """Get data for a single month."""
    url = get_url_from_date(date)
    try:
        res = requests.get(url)
        res.raise_for_status()
    except requests.exceptions.ConnectionError as e:
        logger.error("Connection error")
    except (requests.exceptions.Timeout, requests.exceptions.HTTPError) as e:
        logger.error("Service unavailable. Try again later")
    else:
        # pd.to_pickle(res, f"tests/sample_response_{url[-10:-4]}.pkl")
        return parse_data_from_response(res)


@typechecked
def get_history(start_dt: datetime, end_dt: datetime) -> Optional[pd.DataFrame]:
    """Get all monthly data available from :start_dt: to :end_dt:"""
    assert start_dt < end_dt, "Invalid dates"

    dates = pd.date_range(start_dt, end_dt, freq="m")
    df_list = []
    for date in dates:
        df = get_monthly_data(date)
        if df is not None:
            df_list.append(df)

    if df_list:
        return pd.concat(hist, axis=0).sort_index()
