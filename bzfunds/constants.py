"""
bzfunds.constants
~~~~~~~~~~~~~~~~~

This module defines all global constants.
"""

import os
import pandas as pd


# General
# ----
ROOT_DIR = os.path.abspath(__file__ + "/../..")


# API settings
# ----
API_ENDPOINT = r"http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS"
API_FILENAME_PREFIX = "inf_diario_fi_"
API_DATE_FORMAT = "%Y%m"

# Endpoint has two conditional paths:
#   i) dates > `API_LAST_ZIPPED_DATE` can be handled directly thru the endpoint
#   ii) dates <= `API_LAST_ZIPPED_DATE` must me adjusted and use the historical endpoint
API_LAST_ZIPPED_DATE = pd.to_datetime("2016-12-31")

API_FIRST_VALID_DATE = pd.to_datetime("2005-01-01")

API_COLUMNS_MAP = {
    "TP_FUNDO": "fund_type",
    "CNPJ_FUNDO": "fund_cnpj",
    "DT_COMPTC": "date",
    "VL_TOTAL": "total_portfolio",
    "VL_QUOTA": "nav",
    "VL_PATRIM_LIQ": "total_equity",
    "CAPTC_DIA": "subscriptions",
    "RESG_DIA": "redemptions",
    "NR_COTST": "n_shareholders",
}
