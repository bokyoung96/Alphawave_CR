import logging
import json
from telegram import Bot
from telegram.error import TelegramError


class TelegramSender:
    def __init__(self, config_file_path: str = 'config_trading.json'):
        self.config = self.load_config(config_file_path)
        if not self.config:
            logging.error("Telegram configuration not loaded. Exiting...")
            raise Exception("Telegram configuration not loaded.")

        self.token = self.config.get('kamp_alphawave_bot_token')
        self.chat_id = self.config.get('bot_myself_chat_id')
        self.bot = Bot(token=self.token)

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

    async def send_message(self, message: str):
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=message)
            logging.info("Message sent to Telegram.")
        except TelegramError as e:
            logging.error(f"Failed to send message to Telegram: {str(e)}")
