"""
bzfunds.utils
~~~~~~~~~~~~~

General utils used within bzfunds
"""

from datetime import datetime
from io import StringIO

import pandas as pd
import requests

from .constants import API_DATE_FORMAT, API_ENDPOINT, API_FILENAME_PREFIX, API_COLUMNS_MAP


__all__ = ("get_url_from_date", "parse_data_from_response")


def get_url_from_date(date: datetime) -> str:
    date_str = date.strftime(API_DATE_FORMAT)
    return f"{API_ENDPOINT}/{API_FILENAME_PREFIX}{date_str}.csv"


def parse_data_from_response(res: requests.Response) -> pd.DataFrame:
    csv_buffer = StringIO(res.content.decode("utf-8"))
    df = pd.read_csv(csv_buffer, sep=";")
    df.rename(API_COLUMNS_MAP, axis=1, errors="ignore", inplace=True)
    df = df.set_index("date")
    df.index = pd.to_datetime(df.index, format="%Y-%m-%d")

    return df
