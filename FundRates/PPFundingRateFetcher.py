import pandas as pd

from FundingRateFetcher import *


class PPFundingRateFetcher(FundingRateFetcher):
    def __init__(self, mkts, top_n=10, max_workers=20):
        super().__init__(mkts, top_n, max_workers)

    def format_dataframe_as_text(self, df: pd.DataFrame):
        formatted_rows = []

        col_widths = {
            'exch': 10,
            'symb': 20,
            'FR (%)': 8,
            'FD': 12,
            'pos': 4,
            'p': 8,
            'vol': 10,
            'bid': 8,
            'ask': 8,
            'spr': 8,
            'ab_r': 8,
            'volspr': 8
        }

        headers = [f"{col:<{col_widths[col]}}" for col in df.columns]
        formatted_rows.append(" | ".join(headers))

        for _, row in df.iterrows():
            formatted_row = []
            for col in df.columns:
                value = row[col] if pd.notna(row[col]) else ''
                formatted_row.append(f"{str(value):<{col_widths[col]}}")
            formatted_rows.append(" | ".join(formatted_row))

        return "\n".join(formatted_rows)

    def get_funding_rate_mdstr(self):
        try:
            res = self.run()
            table_text = self.format_dataframe_as_text(res)
            return f"```\n{table_text}\n```"
        except Exception as e:
            return f"Error generating funding rate data: {str(e)}"

    def get_additional_data_by_symbol_mdstr(self, symbol):
        try:
            res = self.get_additional_data_by_symbol(symbol)
            table_text = self.format_dataframe_as_text(res)
            return f"```\n{table_text}\n```"
        except Exception as e:
            return f"Error generating addtitional symbol data: {str(e)}"


if __name__ == "__main__":
    mkts = ['bybit', 'gateio', 'mexc', 'okx']
    fetcher = PPFundingRateFetcher(mkts, top_n=10, max_workers=10)
    text = fetcher.get_funding_rate_mdstr()
    print(text)
