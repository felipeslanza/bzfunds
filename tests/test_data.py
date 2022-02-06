import pandas as pd
import pytest
from unittest.mock import Mock, patch

import requests

from bzfunds.data import *
from bzfunds.utils import get_url_from_date


# Globals
date_str = "2021-01-01"
date = pd.to_datetime(date_str)


def test_assert_get_data_is_typed():
    with pytest.raises(TypeError, match=".*datetime.*"):
        get_monthly_data(date_str)


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


def test_get_history():
    pass
