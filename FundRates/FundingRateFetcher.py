import ccxt
import pytz
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


class FundingRateFetcher:
    def __init__(self, mkts, top_n=10, max_workers=20):
        self.mkts = mkts
        self.top_n = top_n
        self.max_workers = max_workers
        self.kst = pytz.timezone('Asia/Seoul')
        self.funding_rates = pd.DataFrame()
        self.funding_rates_per_exchange = pd.DataFrame()
        self.deduped_top_funding_rates = pd.DataFrame()
        self.additional_data = pd.DataFrame()
        self.exchanges = {}
        self._initialize_exchanges()

    def __len__(self):
        return len(self.funding_rates)

    def _initialize_exchanges(self):
        for mkt in self.mkts:
            try:
                exchange_class = getattr(ccxt, mkt)
                exchange = exchange_class({'enableRateLimit': True})
                exchange.load_markets()
                self.exchanges[mkt] = exchange
                print(f"Initialized exchange: {mkt}")
            except Exception as e:
                print(f"Error initializing exchange {mkt}: {str(e)}")

    def fetch_funding_rates(self):
        funding_rates = []

        def fetch_rate(mkt, exchange, symbol):
            try:
                rate = exchange.fetch_funding_rate(symbol)
                funding_rate = rate['fundingRate']
                funding_timestamp = rate.get('fundingTimestamp')
                funding_datetime = self.convert_timestamp_to_kst(
                    timestamp=funding_timestamp) if funding_timestamp else 'Unknown'
                return {
                    'exchange': mkt,
                    'symbol': symbol,
                    'fundingRate': funding_rate,
                    'fundingDatetime': funding_datetime,
                }
            except Exception:
                return None

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for mkt, exchange in self.exchanges.items():
                swap_symbols = [
                    symbol for symbol in exchange.symbols
                    if 'swap' in exchange.markets[symbol].get('type', '').lower()
                ]
                for symbol in swap_symbols:
                    futures.append(executor.submit(
                        fetch_rate, mkt, exchange, symbol))
            for future in as_completed(futures):
                result = future.result()
                if result:
                    funding_rates.append(result)

        self.funding_rates = pd.DataFrame(funding_rates)
        print(
            f"Fetched {len(self.funding_rates)} funding rates from {len(self.mkts)} exchanges.")

    def get_funding_rates_per_exchange(self):
        if self.funding_rates.empty:
            self.fetch_funding_rates()
        df = self.funding_rates.assign(
            absFundingRate=self.funding_rates['fundingRate'].abs())
        self.funding_rates_per_exchange = (
            df.sort_values(['exchange', 'absFundingRate'],
                           ascending=[True, False])
            .groupby('exchange')
            .head(self.top_n)
            .drop(columns=['absFundingRate'])
            .reset_index(drop=True)
        )
        print(f"Selected top {self.top_n} funding rates per exchange.")

    def fetch_additional_data(self):
        if self.funding_rates_per_exchange.empty:
            self.get_funding_rates_per_exchange()
        additional_data = []

        def fetch_additional(row):
            try:
                exchange = self.exchanges.get(row['exchange'])
                if not exchange:
                    return None
                ticker = exchange.fetch_ticker(row['symbol'])
                order_book = exchange.fetch_order_book(row['symbol'], limit=1)
                position = 'L' if row['fundingRate'] < 0 else 'S'
                price = ticker.get('last', 0.0)
                volume = ticker.get('baseVolume', 0.0)
                bid = ticker.get('bid', 0.0)
                ask = ticker.get('ask', 0.0)
                spread = (
                    ask - bid) if (bid is not None and ask is not None) else 0.0
                volume_spread = (
                    (order_book['asks'][0][1] if order_book['asks'] else 0.0) -
                    (order_book['bids'][0][1] if order_book['bids'] else 0.0)
                )
                ask_bid_ratio = (ask / bid) if bid != 0 else None
                return {
                    'exchange': row['exchange'],
                    'symbol': row['symbol'],
                    'fundingRate': row['fundingRate'],
                    'fundingDatetime': row['fundingDatetime'],
                    'position': position,
                    'price': price,
                    'volume': volume,
                    'bid': bid,
                    'ask': ask,
                    'spread': spread,
                    'ask_bid_ratio': ask_bid_ratio,
                    'volumeSpread': volume_spread,
                }
            except Exception:
                return None

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(fetch_additional, row)
                       for _, row in self.funding_rates_per_exchange.iterrows()]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    additional_data.append(result)
        self.additional_data = pd.DataFrame(additional_data)
        print(
            f"Fetched additional data for {len(self.additional_data)} symbols.")

    def deduplicate_symbols_by_volume(self):
        if self.additional_data.empty:
            self.fetch_additional_data()
        df = self.additional_data.assign(
            absFundingRate=self.additional_data['fundingRate'].abs())
        if df.duplicated(subset=['symbol']).any():
            deduped = (
                df.sort_values('volume', ascending=False)
                .drop_duplicates(subset=['symbol'])
                .sort_values('absFundingRate', ascending=False)
                .head(self.top_n)
                .drop(columns=['absFundingRate'])
                .reset_index(drop=True)
            )
            self.deduped_top_funding_rates = deduped
            print(
                f"Deduplicated to top {self.top_n} funding rates based on volume.")
        else:
            top_n = (
                df.sort_values('absFundingRate', ascending=False)
                .head(self.top_n)
                .drop(columns=['absFundingRate'])
                .reset_index(drop=True)
            )
            self.deduped_top_funding_rates = top_n
            print(
                f"No duplicate symbols found. Selected top {self.top_n} funding rates by absolute value.")

    def run(self):
        self.fetch_funding_rates()
        self.get_funding_rates_per_exchange()
        self.fetch_additional_data()
        self.deduplicate_symbols_by_volume()
        self.main_df = self.deduped_top_funding_rates.copy()
        self.main_df = self.main_df.round({
            'fundingRate': 4,
            'price': 4,
            'volume': 4,
            'bid': 4,
            'ask': 4,
            'spread': 4,
            'ask_bid_ratio': 4,
            'volumeSpread': 4
        })
        self.main_df = self.format_dataframe(self.main_df)
        print("Final top funding rates obtained.")
        return self.main_df

    def get_additional_data_by_symbol(self, symbol):
        if self.additional_data.empty:
            self.fetch_additional_data()
        df = self.additional_data[self.additional_data['symbol'] == symbol]
        if df.empty:
            print(f"No data found for coin symbol: {symbol}")
            return pd.DataFrame()
        df = df.round({
            'fundingRate': 4,
            'price': 6,
            'volume': 4,
            'bid': 4,
            'ask': 4,
            'spread': 6,
            'ask_bid_ratio': 4,
            'volumeSpread': 4
        })
        res = self.format_dataframe(df.reset_index(drop=True))
        res = res.rename(columns=self.format_cols)
        print(f"Data for {symbol}:")
        return res

    def convert_timestamp_to_kst(self, timestamp):
        if timestamp:
            return datetime.fromtimestamp(timestamp / 1000, tz=pytz.utc).astimezone(self.kst).strftime('%m-%d %H:%M')
        else:
            return 'Unknown'

    def format_volume(self, x):
        try:
            x = float(x)
        except (ValueError, TypeError):
            x = 0.0

        if x >= 1e9:
            return f"{x/1e9:.2f}B"
        elif x >= 1e6:
            return f"{x/1e6:.2f}M"
        else:
            return f"{x:.2f}"

    def format_dataframe(self, df):
        if 'fundingRate' in df.columns:
            df['fundingRate (%)'] = (df['fundingRate'] * 100).round(2)
            df.drop('fundingRate', axis=1, inplace=True)

        if 'volume' in df.columns:
            df['volume'] = pd.to_numeric(
                df['volume'], errors='coerce').fillna(0.0)
            df['volume'] = df['volume'].apply(self.format_volume)

        desired_order = ['exchange', 'symbol', 'fundingRate (%)', 'fundingDatetime', 'position',
                         'price', 'volume', 'bid', 'ask', 'spread', 'ask_bid_ratio', 'volumeSpread']
        existing_columns = [col for col in desired_order if col in df.columns]
        df = df[existing_columns]
        df = df.rename(columns=self.format_cols)
        return df

    @property
    def format_cols(self):
        cols = {
            'exchange': 'exch',
            'symbol': 'symb',
            'fundingRate (%)': 'FR (%)',
            'fundingDatetime': 'FD',
            'position': 'pos',
            'price': 'p',
            'volume': 'vol',
            'bid': 'bid',
            'ask': 'ask',
            'spread': 'spr',
            'ask_bid_ratio': 'ab_r',
            'volumeSpread': 'volspr'
        }
        return cols


if __name__ == "__main__":
    mkts = ['bybit', 'gateio', 'mexc', 'okx']
    fetcher = FundingRateFetcher(mkts, top_n=10, max_workers=10)
    df = fetcher.run()
    print("\nFinal Top Funding Rates:")
    print(df)

    coin = 'APE/USDT:USDT'
    coin_data = fetcher.get_additional_data_by_symbol(coin)
    if not coin_data.empty:
        print(f"\nFunding Rates for {coin}:")
        print(coin_data)
