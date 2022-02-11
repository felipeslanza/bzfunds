from bzfunds.api import download_data
from bzfunds.data import get_monthly_data, get_history
from bzfunds.dbm import Manager


if __name__ == "__main__":
    # dbm = Manager()

    # Testing single
    # ----
    # d1 = pd.to_datetime("2017-1-1")
    # d2 = pd.to_datetime("2019-1-1")
    # d3 = pd.to_datetime("2021-1-1")

    # df1 = get_monthly_data(d1).reset_index()
    # df1 = get_monthly_data(d1)
    # df2 = get_monthly_data(d2)
    # df3 = get_monthly_data(d3)

    # Testing hist
    # ----
    # d1 = pd.to_datetime("2017-1-1")
    # d2 = pd.to_datetime("2017-4-1")

    # df = get_history(d1, d2)

    # Test interface
    # ----
    download_data(update_only=True)
