import ccxt
import json
import logging


class OKXClient:
    def __init__(self, config_file_path: str = 'config_okx.json'):
        self.config = self.load_config(config_file_path)
        if not self.config:
            logging.error("Configuration not loaded. Exiting...")
            raise Exception("Configuration not loaded.")

        self.okx = ccxt.okx({
            'apiKey': self.config.get('apiKey'),
            'secret': self.config.get('secret'),
            'password': self.config.get('password'),
            'enableRateLimit': True,
            'adjustForTimeDifference': True
        })
        self.okx.load_markets()

    def load_config(self, file_path: str) -> dict:
        try:
            with open(file_path, 'r') as file:
                config = json.load(file)
            return config
        except FileNotFoundError:
            logging.error(f"Configuration file {file_path} not found.")
            return None
        except json.JSONDecodeError:
            logging.error("Error decoding JSON from the configuration file.")
            return None

    def get_balance(self) -> dict:
        try:
            balance = self.okx.fetch_balance()
            return balance
        except ccxt.BaseError as e:
            logging.error(
                f"An error occurred while fetching balance: {str(e)}")
            return None

    def place_order(self, symbol: str, order_type: str, side: str, amount: float, price: float = None, params: dict = {}):
        try:
            order = self.okx.create_order(
                symbol=symbol,
                type=order_type,
                side=side,
                amount=amount,
                price=price,
                params=params
            )
            return order
        except ccxt.BaseError as e:
            logging.error(f"An error occurred while placing order: {str(e)}")
            return None

    def close_position(self, symbol: str, params: dict = {}):
        try:
            positions = self.okx.fetch_positions([symbol])
            for position in positions:
                if position['symbol'] == symbol and float(position['contracts']) != 0:
                    amount = abs(float(position['contracts']))
                    side = 'sell' if position['side'] == 'long' else 'buy'
                    order_params = {
                        'tdMode': 'isolated',
                        'posSide': position['side'],
                    }
                    order_params.update(params)
                    order = self.place_order(
                        symbol, 'market', side, amount, params=order_params)
                    return order
            logging.info(f"No open position found for {symbol}")
            return None
        except ccxt.BaseError as e:
            logging.error(
                f"An error occurred while closing position: {str(e)}")
            return None
