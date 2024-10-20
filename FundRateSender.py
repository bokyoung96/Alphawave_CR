import json
import logging
import asyncio
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler

from PPFundingRateFetcher import *

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


def load_config(file_path):
    try:
        with open(file_path, 'r') as file:
            config = json.load(file)
        return config
    except FileNotFoundError:
        logging.error(f"Configuration file {file_path} not found.")
        return {}
    except json.JSONDecodeError:
        logging.error("Error decoding JSON from the configuration file.")
        return {}


config = load_config('./config.json')

kamp_alphawave_bot_token = config.get('kamp_alphawave_bot_token')
bot_myself_chat_id = config.get('bot_myself_chat_id')
alphawave_cr_group_chat_id = config.get('alphawave_cr_group_chat_id')

mkts = ['bybit', 'gateio', 'mexc', 'okx']
top_n = 10
max_workers = 10

fetcher = PPFundingRateFetcher(mkts=mkts, top_n=top_n, max_workers=max_workers)


async def send_funding_rate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        logging.info("Fetching funding rate...")
        loop = asyncio.get_running_loop()
        text = await loop.run_in_executor(None, fetcher.get_funding_rate_mdstr)
        logging.info("Funding rate fetched successfully.")

        for i in range(0, len(text), 4000):
            await context.bot.send_message(
                chat_id=alphawave_cr_group_chat_id,
                text=text[i:i+4000],
                parse_mode='Markdown'
            )
        logging.info("Funding rate messages sent successfully.")
    except Exception as e:
        logging.error(f"Error sending funding rate: {e}", exc_info=True)
        await context.bot.send_message(
            chat_id=alphawave_cr_group_chat_id,
            text="Error while receiving funding rate!",
            parse_mode='Markdown'
        )


def main():
    try:
        application = ApplicationBuilder().token(kamp_alphawave_bot_token).build()

        send_fund_rate_handler = CommandHandler('on', send_funding_rate)
        application.add_handler(send_fund_rate_handler)

        application.run_polling()
    except Exception as e:
        logging.error(f"An error occurred in main: {e}", exc_info=True)


if __name__ == "__main__":
    main()
