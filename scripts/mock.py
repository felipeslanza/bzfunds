from bzfunds.data import *


if __name__ == "__main__":
    d1 = pd.to_datetime("2021-1-1")
    d2 = pd.to_datetime("2022-1-1")

    # Testing single
    # df = get_monthly_data(d1)

    # Testing hist
    df = get_history(d1, d2)
