import json
import logging
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters


class TelegramHandler:
    def __init__(self, trading_bot, telegram_sender, config_file_path='config_trading.json'):
        self.trading_bot = trading_bot
        self.telegram_sender = telegram_sender
        self.config = self.load_config(config_file_path)
        if not self.config:
            logging.error("Telegram configuration not loaded. Exiting...")
            raise Exception("Telegram configuration not loaded.")

        self.token = self.config.get('kamp_alphawave_bot_token')

        self.application = ApplicationBuilder().token(self.token).build()

        self.application.add_handler(CommandHandler('start', self.start))
        self.application.add_handler(
            CommandHandler('balance', self.get_balance))
        self.application.add_handler(
            CommandHandler('positions', self.get_positions))
        self.application.add_handler(CommandHandler('exit', self.exit_trading))
        self.application.add_handler(
            MessageHandler(filters.COMMAND, self.unknown))

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

    async def start(self, update, context: ContextTypes.DEFAULT_TYPE):
        message = "Trading bot is running."
        await self.telegram_sender.send_message(message)

    async def get_balance(self, update, context: ContextTypes.DEFAULT_TYPE):
        balance_info = self.trading_bot.get_balance_info()
        await self.telegram_sender.send_message(balance_info)

    async def get_positions(self, update, context: ContextTypes.DEFAULT_TYPE):
        positions_info = self.trading_bot.get_positions_info()
        await self.telegram_sender.send_message(positions_info)

    async def exit_trading(self, update, context: ContextTypes.DEFAULT_TYPE):
        await self.telegram_sender.send_message("Exiting all positions and stopping trading.")
        await self.trading_bot.close_all_positions()
        self.trading_bot.running = False

    async def unknown(self, update, context: ContextTypes.DEFAULT_TYPE):
        message = "Unknown command. Available commands: /start, /balance, /positions, /exit."
        await self.telegram_sender.send_message(message)

    async def start_bot(self):
        await self.application.run_polling()
