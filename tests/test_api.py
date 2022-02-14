import pytest

from bzfunds.api import download_data, get_data


def test_download_data_raises_on_bad_arguments():
    with pytest.raises(ValueError):
        _ = download_data(start_year="2020", update_only=True)
        _ = download_data(start_year=None, update_only=False)


def test_get_data_missed_query():
    res = get_data(funds="123456")
    assert res is None

    res = get_data(start_dt="2050-1-1")
    assert res is None


def test_get_data_success_query():
    df = get_data(funds="13.001.211/0001-90")
    assert df.size
    assert df.index.name == "date"
    assert "fund_cnpj" in df.columns
