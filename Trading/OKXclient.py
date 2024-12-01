import ccxt.async_support as ccxt
import json
import logging
import asyncio


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
        self.initialized = False

    async def initialize(self):
        await self.okx.load_markets()
        self.initialized = True

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

    async def get_balance(self) -> dict:
        try:
            balance = await self.okx.fetch_balance()
            return balance
        except ccxt.BaseError as e:
            logging.error(
                f"An error occurred while fetching balance: {str(e)}")
            return {}

    async def place_order(self, symbol: str, order_type: str, side: str, amount: float, price: float = None, params: dict = {}):
        try:
            order = await self.okx.create_order(
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

    async def close_position(self, position: dict):
        """
        특정 포지션을 청산하는 메서드.
        """
        try:
            symbol = position['symbol']
            side = 'sell' if position['side'] == 'long' else 'buy'
            amount = abs(float(position['contracts']))
            order_params = {
                'tdMode': 'isolated',
                'posSide': 'long' if side == 'buy' else 'short',
                'reduceOnly': True
            }

            order = await self.place_order(
                symbol=symbol,
                order_type='market',
                side=side,
                amount=amount,
                params=order_params
            )
            if order is None:
                return None

            logging.info(
                f"Position closed: Side={side}, Amount={amount}, Order ID={order['id']}"
            )

            order_id = order['id']
            await asyncio.sleep(0.5)
            detailed_order = await self.okx.fetch_order(order_id, symbol)

            exit_price = (
                detailed_order['average']
                if detailed_order['average']
                else detailed_order['price']
            )
            if exit_price is None:
                logging.error("Failed to retrieve exit price.")
                return None

            profit_loss = (
                float(exit_price) - position['entry_price']
            ) * position['amount']
            if position['side'] == 'sell':
                profit_loss *= -1

            logging.info(
                f"Closed position P/L: {profit_loss}, Exit Price: {exit_price}"
            )

            return order
        except ccxt.BaseError as e:
            logging.error(
                f"An error occurred while closing position: {str(e)}")
            return None

    async def close_all_positions(self, positions: list):
        closed_orders = []
        for position in positions:
            order = await self.close_position(position)
            if order:
                closed_orders.append(order)
        return closed_orders

    async def close(self):
        await self.okx.close()
