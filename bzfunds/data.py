import logging
from datetime import datetime
from typing import Optional

import pandas as pd
import requests
from typeguard import typechecked

from .utils import get_url_from_date, parse_data_from_response


__all__ = ("get_monthly_data", "get_history")


logger = logging.getLogger(__name__)


@typechecked
def get_monthly_data(date: datetime) -> Optional[pd.DataFrame]:
    url = get_url_from_date(date)
    try:
        res = requests.get(url)
        res.raise_for_status()
    except requests.exceptions.ConnectionError as e:
        logger.error("Connection error")
    except (requests.exceptions.Timeout, requests.exceptions.HTTPError) as e:
        logger.error("Service unavailable. Try again later")
    else:
        pd.to_pickle(res, f"tests/sample_response_{url[-10:-4]}.pkl")
        return parse_data_from_response(res)


@typechecked
def get_history(start_dt: datetime, end_dt: datetime) -> Optional[pd.DataFrame]:
    assert start_dt < end_dt, "Invalid dates"

    dates = pd.date_range(start_dt, end_dt, freq="m")
    df_list = []
    for date in dates:
        df = get_monthly_data(date)
        if df is not None:
            df_list.append(df)

    if df_list:
        return pd.concat(hist, axis=0).sort_index()
