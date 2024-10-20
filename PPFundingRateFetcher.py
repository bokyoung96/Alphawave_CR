import logging
import pandas as pd
from FundingRateFetcher import *


class PPFundingRateFetcher(FundingRateFetcher):
    def __init__(self, mkts, top_n=10, max_workers=20):
        super().__init__(mkts, top_n, max_workers)

    def format_dataframe_as_text(self, df: pd.DataFrame):
        formatted_rows = []

        col_widths = {
            'exchange': 10,
            'symbol': 20,
            'fundingRate (%)': 10,
            'fundingDatetime': 20,
            'position': 8,
            'price': 8,
            'volume': 10,
            'bid': 8,
            'ask': 8,
            'spread': 8,
            'ask_bid_ratio': 10,
            'volumeSpread': 12
        }

        headers = []
        for col in df.columns:
            headers.append(f"{col:<{col_widths[col]}}")
        formatted_rows.append(" | ".join(headers))

        for _, row in df.iterrows():
            formatted_row = []
            for col in df.columns:
                formatted_row.append(f"{str(row[col]):<{col_widths[col]}}")
            formatted_rows.append(" | ".join(formatted_row))

        return "\n".join(formatted_rows)

    def get_funding_rate_mdstr(self):
        res = self.run()
        logging.debug(f"Raw DataFrame: {res}")

        table_text = self.format_dataframe_as_text(res)
        logging.debug(f"Formatted text table: {table_text}")

        return f"```\n{table_text}\n```"


if __name__ == "__main__":
    mkts = ['bybit', 'gateio', 'mexc', 'okx']
    fetcher = PPFundingRateFetcher(mkts, top_n=10, max_workers=10)
    text = fetcher.get_funding_rate_mdstr()
    print(text)
