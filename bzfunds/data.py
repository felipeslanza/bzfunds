import logging
from datetime import datetime

import pandas as pd
import requests
from typeguard import typechecked

from .utils import get_url_from_date, parse_data_from_response


__all__ = ("get_monthly_data", "get_history")


logger = logging.getLogger(__name__)


@typechecked
def get_monthly_data(date: datetime) -> pd.DataFrame:
    url = get_url_from_date(date)
    try:
        res = requests.get(url)
        res.raise_for_status()
    except requests.exceptions.ConnectionError as e:
        logger.error("Connection error")
    except (requests.exceptions.Timeout, requests.exceptions.HttpError) as e:
        logger.error("Service unavailable. Try again later")
    else:
        return parse_data_from_response(res)


def get_history(start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    start_dt = ...
    pass
