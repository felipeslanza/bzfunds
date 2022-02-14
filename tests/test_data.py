from functools import partial

import pandas as pd
import pytest
from unittest.mock import Mock, patch

import requests

from bzfunds import constants
from bzfunds.data import *
from bzfunds.dbm import *
from bzfunds.utils import get_url_from_date


# Globals
date_str = "2021-01-01"
date = pd.to_datetime(date_str)
manager = Manager()
get_history = partial(get_history, manager=manager, commit=False)


def test_assert_get_data_is_typed():
    with pytest.raises(TypeError, match=".*datetime.*"):
        get_monthly_data(123)
        get_monthly_data(date_str)
        get_monthly_data({})


def test_assert_get_data_only_parses_successul_response():
    errors = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.HTTPError,
    )

    mocked_get = Mock(side_effect=errors)
    with patch("bzfunds.data.requests.get", mocked_get):
        assert get_monthly_data(date) is None
        assert get_monthly_data(date) is None
        assert get_monthly_data(date) is None


def test_assert_get_history_is_typed():
    with pytest.raises(TypeError, match=".*datetime.*"):
        get_history(123, 456)
        get_history(date_str, date_str)
        get_history({}, [])


def test_get_history_date_range():
    d1, d2 = pd.to_datetime(["1910-9-1", "1910-12-1"])
    d3, d4 = pd.to_datetime(["2110-1-1", "2110-3-1"])
    assert get_history(d1, d2) is None
    assert get_history(d3, d4) is None
