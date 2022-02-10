from bzfunds.data import *


if __name__ == "__main__":
    d1 = pd.to_datetime("2017-1-1")
    # d2 = pd.to_datetime("2019-1-1")
    # d3 = pd.to_datetime("2021-1-1")

    # Testing single
    # ----
    df1 = get_monthly_data(d1)
    # df2 = get_monthly_data(d2)
    # df3 = get_monthly_data(d3)

    # Testing hist
    # ----
    #  df = get_history(d1, d2)
