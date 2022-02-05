from datetime import datetime
from io import StringIO

import pandas as pd
import requests

from .constants import API_DATE_FORMAT, API_ENDPOINT, API_FILENAME_PREFIX


__all__ = ("get_url_from_date",)


def get_url_from_date(date: datetime) -> str:
    date_str = date.strftime(API_DATE_FORMAT)
    return f"{API_ENDPOINT}/{API_FILENAME_PREFIX}/{date_str}.csv"


def parse_data_from_response(res: requests.Response) -> pd.DataFrame:
    csv_buffer = StringIO(res.content.decode("utf-8"))
    df = pd.read_csv(csv_buffer)

    return df
