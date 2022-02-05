import pandas as pd
import pytest

from bzfunds.data import *
from bzfunds.utils import get_url_from_date


# Globals
date_str = "2021-01-01"
date = pd.to_datetime(date_str)


def assert_get_data_is_typed():
    with pytest.raises(TypeError, match=".*dateitme.*"):
        get_monthly_data(date_str)


def assert_get_data_only_parses_successul_response():
    pass


def test_get_history():
    pass
