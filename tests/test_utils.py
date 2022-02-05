import pandas as pd

from bzfunds.constants import API_DATE_FORMAT
from bzfunds.utils import *


def test_get_url_from_date():
    date = pd.to_datetime("2021-1-1")
    date_str = date.strftime(API_DATE_FORMAT)
    assert get_url_from_date(date).endswith(f"{date_str}.csv")
