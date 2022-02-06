import glob

import pandas as pd

from bzfunds.constants import API_DATE_FORMAT
from bzfunds.utils import *


def test_get_url_from_date():
    date = pd.to_datetime("2021-1-1")
    date_str = date.strftime(API_DATE_FORMAT)
    assert get_url_from_date(date).endswith(f"{date_str}.csv")


def test_parse_response_from_df():
    for filepath in glob.glob("/tests/sample_responses/*pkl"):
        res = pd.read_pickle(filepath)
        df = parse_data_from_response(res)
        assert df.index.name == "date"
        assert "fund_cnpj" in df.columns
        assert "total_portfolio" in df.columns
