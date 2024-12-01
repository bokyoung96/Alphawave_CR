import json
import logging
import argparse
import asyncio
import signal
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


async def run_trading_system(args, shutdown_event):
    client = OKXClient()
    await client.initialize()

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
        telegram_handler = TelegramHandler(
            trading_bot=trader, telegram_sender=telegram_sender)
        bot_task = asyncio.create_task(telegram_handler.start_bot())
    else:
        bot_task = None

    trading_task = asyncio.create_task(trader.run(time_limit=args.time_limit))

    await shutdown_event.wait()

    logging.info("Shutting down...")

    trader.running = False
    if args.use_telegram:
        await telegram_handler.application.shutdown()
        await telegram_sender.bot.close()

    await client.close()

    trading_task.cancel()
    if bot_task:
        bot_task.cancel()

    try:
        await asyncio.gather(trading_task, bot_task, return_exceptions=True)
    except Exception as e:
        logging.error(f"Error during shutdown: {str(e)}")


async def main():
    args = parse_arguments()
    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()

    def shutdown_signal():
        logging.info("Received shutdown signal.")
        asyncio.create_task(shutdown_event.set())

    for signame in {'SIGINT', 'SIGTERM'}:
        try:
            loop.add_signal_handler(getattr(signal, signame), shutdown_signal)
        except NotImplementedError:
            pass

    try:
        await run_trading_system(args, shutdown_event)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
    finally:
        logging.info("Bot stopped.")


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s:%(message)s',
        handlers=[
            logging.FileHandler("trading_bot.log"),
            logging.StreamHandler()
        ]
    )
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Trading bot stopped manually.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")


# python main.py --symbol 'APE/USDT:USDT' --amount 1 --time_limit 1800 --timeframe '1m' --strategy KaufmanAMA --max_positions 1 --take_profit 5 --stop_loss 2 --signal_interval 60.0 --use_telegram
# python main.py --symbol 'ETH/USDT:USDT' --amount 1 --time_limit 1800 --timeframe '1m' --strategy KaufmanAMA --max_positions 1 --take_profit 5 --stop_loss 2 --signal_interval 60.0 --use_telegram
