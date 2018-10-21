from .queries import *

cfg = {
    "postgres":
        {
            "secmaster_db": "secmasterdb",
            "secmaster_user": "postgres",
            "secmaster_password": "postgres",
            "secmaster_host": "192.168.178.8",
            "secmaster_port": "5432"
        },
    "non_ticker_type": ["INDEX", "CMDTY", "FOREX", "CRYPTO"]
}

available_fundamentals = {
    "equity": {
        "stock": {
            "balance_sheet": eq_balance_sheet,
            "earnings_history": eq_earnings_history,
            "earnings_trend": eq_earnings_trend,
            "cash_flow": eq_cash_flow,
            "income_statement": eq_income_statement,
            "key_metrics_highlights": eq_key_metrics_highlights,
            "key_metrics_valuation": eq_key_metrics_valuation,
            "key_metrics_technicals": eq_key_metrics_technicals,
            "key_metrics_splitdiv": eq_key_metrics_splitdiv
        },
        "etp": {
            "key_metrics_highlights": fund_key_metrics_highlights,
            "key_metrics_valuation": fund_key_metrics_valuation,
            "key_metrics_technicals": fund_key_metrics_technicals,
            "key_metrics_holdings": fund_key_metrics_holdings
        }
    },
    "debt": {}
}
