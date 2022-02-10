"""
bzfunds.utils
~~~~~~~~~~~~~

General utils used within bzfunds
"""

from datetime import datetime
from io import StringIO
from typing import Union, TextIO

import pandas as pd
import requests

from . import constants


__all__ = ("get_url_from_date", "parse_csv")


def get_url_from_date(date: datetime, zipped: bool = False) -> str:
    """Return a formatted `url` from a `date`

    **Note**: Because data is stored differently before and after `2016-12-31`,
    the `url` will have two different formats. In particular, dates
    before the cutoff date require downloading a zipped file for the
    whole year.

    Parameters
    ----------
    date : datetime
    zipped : bool
    """
    date_str = date.strftime(constants.API_DATE_FORMAT)
    if zipped:
        # Zipped folder with all monthly CSV files for that year
        url = f"{constants.API_ENDPOINT}/HIST/{constants.API_FILENAME_PREFIX}{date.year}.zip"
    else:
        # Monthly CSV file
        url = f"{constants.API_ENDPOINT}/{constants.API_FILENAME_PREFIX}{date_str}.csv"

    return url


def parse_csv(csv: Union[str, TextIO]) -> pd.DataFrame:
    df = pd.read_csv(csv, sep=";")
    df.rename(constants.API_COLUMNS_MAP, axis=1, errors="ignore", inplace=True)
    df = df.set_index("date")
    df.index = pd.to_datetime(df.index, format="%Y-%m-%d")

    return df
