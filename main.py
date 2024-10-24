import logging
import argparse
import threading
import asyncio
from trading import Trading
from OKXclient import OKXClient
from strategy import StrategyType
from sender import TelegramSender
from handler import TelegramHandler


def parse_arguments():
    parser = argparse.ArgumentParser(description="Automated Trading Bot")
    parser.add_argument('--symbol', type=str, default='ALPHA/USDT:USDT',
                        help='Trading symbol (e.g., BTC/USDT:USDT)')
    parser.add_argument('--amount', type=float, default=1, help='Trade amount')
    parser.add_argument('--time_limit', type=int, default=3600,
                        help='Trading execution time in seconds')
    parser.add_argument('--timeframe', type=str, default='5m',
                        help='Candle timeframe (e.g., 1m, 15m, 1h)')
    parser.add_argument('--strategy', type=str, default='KaufmanAMA',
                        help='Strategy to use (e.g., KaufmanAMA, MovingAverageCross)')
    parser.add_argument('--max_positions', type=int, default=5,
                        help='Maximum number of open positions')
    parser.add_argument('--take_profit', type=float, default=None,
                        help='Take profit percentage (e.g., 5 for 5%)')
    parser.add_argument('--stop_loss', type=float, default=None,
                        help='Stop loss percentage (e.g., 5 for 5%)')
    parser.add_argument('--signal_interval', type=float, default=60.0,
                        help='Signal detection interval in seconds')
    parser.add_argument('--use_telegram', action='store_true',
                        help='Enable Telegram notifications')
    return parser.parse_args()


def run_trading_system(args):
    client = OKXClient()

    strategy_type = StrategyType(args.strategy)

    strategy_kwargs = {}
    if strategy_type == StrategyType.KAUFMAN_AMA:
        strategy_kwargs = {'period': 10, 'fast_period': 2, 'slow_period': 30}
    elif strategy_type == StrategyType.MA_CROSS:
        strategy_kwargs = {'short_window': 5, 'long_window': 20}

    if args.use_telegram:
        telegram_sender = TelegramSender()
    else:
        telegram_sender = None

    trader = Trading(
        client=client,
        symbol=args.symbol,
        strategy_type=strategy_type,
        timeframe=args.timeframe,
        amount=args.amount,
        max_positions=args.max_positions,
        take_profit=args.take_profit,
        stop_loss=args.stop_loss,
        signal_interval=args.signal_interval,
        telegram_sender=telegram_sender,
        **strategy_kwargs
    )

    if args.use_telegram:
        telegram_handler = TelegramHandler(trading_bot=trader)
        threading.Thread(target=telegram_handler.start_bot,
                         daemon=True).start()

    asyncio.run(trader.run(time_limit=args.time_limit))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s:%(message)s',
        handlers=[
            logging.FileHandler("trading_bot.log"),
            logging.StreamHandler()
        ]
    )
    args = parse_arguments()
    run_trading_system(args)

# python main.py --symbol 'APE/USDT:USDT' --amount 1000 --time_limit 1800 --timeframe '1m' --strategy KaufmanAMA --max_positions 1000 --take_profit 5 --stop_loss 2 --signal_interval 60.0
# python main.py --symbol 'APE/USDT:USDT' --amount 1 --time_limit 1800 --timeframe '1m' --strategy KaufmanAMA --max_positions 1 --take_profit 0.5 --stop_loss 0.3 --signal_interval 60.0
