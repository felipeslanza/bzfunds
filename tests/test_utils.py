import glob
from io import StringIO

import pandas as pd

from bzfunds.constants import API_DATE_FORMAT, ROOT_DIR
from bzfunds.utils import *


def test_get_url_from_date():
    date = pd.to_datetime("2021-1-1")
    date_str = date.strftime(API_DATE_FORMAT)
    assert get_url_from_date(date).endswith(f"{date_str}.csv")


def test_parse_csv():
    for filepath in glob.glob(f"{ROOT_DIR}/tests/sample_responses/*pkl"):
        res = pd.read_pickle(filepath)
        csv_buffer = StringIO(res.content.decode("utf-8"))
        df = parse_csv(csv_buffer)
        assert df.index.name == "date"
        assert "fund_cnpj" in df.columns
        assert "total_portfolio" in df.columns
