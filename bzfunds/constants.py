API_ENDPOINT = r"http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS"
API_FILENAME_PREFIX = "inf_diario_fi_"
API_DATE_FORMAT = "%Y%m"
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
