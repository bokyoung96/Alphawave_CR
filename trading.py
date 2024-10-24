import ccxt
import logging
import threading
import time
import asyncio
from strategy import AbstractStrategy, strategy_pool, StrategyType
from sender import TelegramSender


class Trading:
    def __init__(
        self,
        client,
        symbol: str,
        strategy_type: StrategyType,
        leverage: int = 1,
        timeframe: str = '1m',
        amount: float = 1.0,
        max_positions: int = 5,
        take_profit: float = None,
        stop_loss: float = None,
        signal_interval: int = 60,
        telegram_sender: TelegramSender = None,
        **strategy_kwargs
    ):
        self.lock = threading.Lock()
        self.client = client
        self.symbol = symbol
        self.leverage = leverage
        self.timeframe = timeframe
        self.amount = amount
        self.max_positions = max_positions
        self.take_profit = take_profit
        self.stop_loss = stop_loss
        self.signal_interval = signal_interval
        self.telegram_sender = telegram_sender
        self.strategy = strategy_pool(strategy_type, **strategy_kwargs)
        self.positions = []

        self.set_leverage()

    def set_leverage(self):
        """레버리지 설정"""
        try:
            self.client.okx.set_leverage(self.leverage, self.symbol)
            logging.info(f"Leverage set to {self.leverage}x for {self.symbol}")
        except ccxt.BaseError as e:
            logging.error(f"Failed to set leverage: {str(e)}")

    def fetch_price_data(self) -> list:
        try:
            ohlcv = self.client.okx.fetch_ohlcv(
                self.symbol, timeframe=self.timeframe, limit=self.strategy.period
            )
            prices = [candle[4] for candle in ohlcv]
            return prices
        except ccxt.BaseError as e:
            logging.error(f"Failed to fetch price data: {str(e)}")
            return []

    async def execute_trade(self, side: str):
        if len(self.positions) >= self.max_positions:
            logging.info(
                "Maximum number of positions reached. Holding position."
            )
            return None

        try:
            order_params = {
                'tdMode': 'isolated',
                'posSide': 'long' if side == 'buy' else 'short',
            }

            order = self.client.okx.create_order(
                symbol=self.symbol,
                type='market',
                side=side,
                amount=self.amount,
                params=order_params
            )
            logging.info(
                f"Order executed: Side={side}, Amount={self.amount}, "
                f"Order ID={order['id']}"
            )

            order_id = order['id']
            await asyncio.sleep(0.5)
            detailed_order = self.client.okx.fetch_order(order_id, self.symbol)

            entry_price = (
                detailed_order['average']
                if detailed_order['average']
                else detailed_order['price']
            )
            if entry_price is None:
                logging.error("Failed to retrieve entry price.")
                return None

            position = {
                'side': side,
                'entry_price': float(entry_price),
                'amount': self.amount,
                'timestamp': detailed_order['timestamp']
            }
            self.positions.append(position)

            if self.telegram_sender:
                message = (
                    f"Executed {side.upper()} order for {self.amount} "
                    f"{self.symbol} at {entry_price}."
                )
                await self.telegram_sender.send_message(message)

            return order
        except ccxt.BaseError as e:
            logging.error(f"Failed to execute trade: {str(e)}")
            return None

    async def close_position(self, position):
        side = 'sell' if position['side'] == 'buy' else 'buy'
        try:
            order_params = {
                'tdMode': 'isolated',
                'posSide': 'long' if side == 'buy' else 'short',
            }

            order = self.client.okx.create_order(
                symbol=self.symbol,
                type='market',
                side=side,
                amount=position['amount'],
                params=order_params
            )
            logging.info(
                f"Position closed: Side={side}, Amount={position['amount']}, "
                f"Order ID={order['id']}"
            )

            order_id = order['id']
            await asyncio.sleep(0.5)
            detailed_order = self.client.okx.fetch_order(order_id, self.symbol)

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

            self.positions.remove(position)

            logging.info(
                f"Closed position P/L: {profit_loss}, "
                f"Exit Price: {exit_price}"
            )

            if self.telegram_sender:
                message = (
                    f"Closed position: {side.upper()} {self.amount} "
                    f"{self.symbol} at {exit_price}. P/L: {profit_loss}"
                )
                await self.telegram_sender.send_message(message)

            return order
        except ccxt.BaseError as e:
            logging.error(f"Failed to close position: {str(e)}")
            return None

    def check_take_profit_stop_loss(self, current_price):
        for position in self.positions[:]:
            entry_price = position['entry_price']
            side = position['side']
            if entry_price is None:
                continue
            if side == 'buy':
                profit = (current_price - entry_price) / entry_price * 100
            else:
                profit = (entry_price - current_price) / entry_price * 100

            if self.take_profit and profit >= self.take_profit:
                logging.info("Take profit level reached.")
                asyncio.create_task(self.close_position(position))
            elif self.stop_loss and profit <= -self.stop_loss:
                logging.info("Stop loss level reached.")
                asyncio.create_task(self.close_position(position))

    async def manage_position(self):
        with self.lock:
            prices = self.fetch_price_data()
            if len(prices) < self.strategy.period:
                logging.warning("Not enough data to generate a signal.")
                return

            signal = self.strategy.generate_signal(prices)
            current_price = prices[-1]
            logging.info(
                f"Generated signal: {signal}, Current price: {current_price}"
            )

            if self.telegram_sender:
                message = (
                    f"Generated signal: {signal}, Current price: {current_price}"
                )
                await self.telegram_sender.send_message(message)

            self.check_take_profit_stop_loss(current_price)

            if signal == 'buy' or signal == 'sell':
                await self.execute_trade(signal)
            else:
                logging.info("No action taken. Holding position.")

    async def run(self, time_limit: int):
        self.running = True
        start_time = time.time()
        while True:
            with self.lock:
                if not self.running or (time.time() - start_time >= time_limit):
                    break
            await self.manage_position()
            await asyncio.sleep(self.signal_interval)
        logging.info("Trading session ended.")

        if self.positions:
            logging.info("Closing any remaining open positions.")
            with self.lock:
                for position in self.positions[:]:
                    await self.close_position(position)

    def get_balance_info(self):
        balance = self.client.get_balance()
        if balance:
            usdt_balance = balance.get('USDT', {})
            total = usdt_balance.get('total', 'N/A')
            free = usdt_balance.get('free', 'N/A')
            used = usdt_balance.get('used', 'N/A')
            return f"Balance:\nTotal: {total}\nFree: {free}\nUsed: {used}"
        else:
            return "Failed to retrieve balance."

    def get_positions_info(self):
        positions_info = "Open Positions:\n"
        if not self.positions:
            positions_info += "No open positions."
        else:
            for position in self.positions:
                side = position['side']
                amount = position['amount']
                entry_price = position['entry_price']
                positions_info += (
                    f"{side.upper()} {amount} {self.symbol} at {entry_price}\n"
                )
        return positions_info

    async def close_all_positions(self):
        with self.lock:
            logging.info("Closing all open positions upon user request.")
            for position in self.positions[:]:
                await self.close_position(position)

            self.running = False

        if self.telegram_sender:
            message = "All positions have been closed as per your request."
            await self.telegram_sender.send_message(message)
