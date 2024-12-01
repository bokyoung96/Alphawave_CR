# pip install "python-telegram-bot[job-queue]"

import json
import logging
import asyncio
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, ConversationHandler, filters, JobQueue
from datetime import datetime, timedelta
import pytz

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

SYMBOL = range(1)

last_funding_rate_data = None
last_funding_rate_time = None


async def send_funding_rate(update: Update, context: ContextTypes.DEFAULT_TYPE, update_data=True):
    global last_funding_rate_data, last_funding_rate_time
    try:
        logging.info("Fetching funding rate...")
        if update_data or not last_funding_rate_data:
            loop = asyncio.get_running_loop()
            last_funding_rate_data = await loop.run_in_executor(None, fetcher.get_funding_rate_mdstr)
            last_funding_rate_time = datetime.now()
            logging.info("Funding rate fetched successfully.")

        for i in range(0, len(last_funding_rate_data), 4000):
            await context.bot.send_message(
                chat_id=alphawave_cr_group_chat_id,
                text=last_funding_rate_data[i:i+4000],
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


async def periodic_funding_rate_update(context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now(pytz.timezone('Asia/Seoul'))
    if now.minute < 30:
        next_run = now.replace(minute=30, second=0, microsecond=0)
    else:
        next_run = now.replace(
            minute=0, second=0, microsecond=0) + timedelta(hours=1)

    wait_time = (next_run - now).total_seconds()

    await context.bot.send_message(
        chat_id=alphawave_cr_group_chat_id,
        text=f"Next funding rate update scheduled at {next_run.strftime('%Y-%m-%d %H:%M:%S')} (KST)",
        parse_mode='Markdown'
    )

    logging.info(
        f"Next update at {next_run}. Waiting {wait_time / 60:.2f} minutes...")

    await asyncio.sleep(wait_time)
    await send_funding_rate(update=None, context=context, update_data=True)

    context.job_queue.run_repeating(
        periodic_funding_rate_update, interval=30*60, first=next_run)


async def on_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_funding_rate(update, context, update_data=True)


async def prev_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global last_funding_rate_data
    if last_funding_rate_data:
        for i in range(0, len(last_funding_rate_data), 4000):
            await context.bot.send_message(
                chat_id=alphawave_cr_group_chat_id,
                text=last_funding_rate_data[i:i+4000],
                parse_mode='Markdown'
            )
    else:
        await update.message.reply_text("No previous data available. Please try /on to get the latest data.")


async def ask_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global last_funding_rate_data

    if last_funding_rate_data is None:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="No funding rate data available. Please run /on or wait for the 30-minute cycle.",
            parse_mode='Markdown'
        )
        return ConversationHandler.END

    await update.message.reply_text("Which symbol would you like to check?")
    return SYMBOL


async def send_symbol_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global last_funding_rate_data
    if last_funding_rate_data is None:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="No funding rate data available. Please run /on or wait for the 30-minute cycle.",
            parse_mode='Markdown'
        )
        return

    symbol = update.message.text
    try:
        text = fetcher.get_additional_data_by_symbol_mdstr(symbol)
        if text.startswith("Error"):
            await update.message.reply_text(f"No data found for symbol: {symbol}")
        else:
            for i in range(0, len(text), 4000):
                await context.bot.send_message(
                    chat_id=alphawave_cr_group_chat_id,
                    text=text[i:i+4000],
                    parse_mode='Markdown'
                )
            logging.info(f"Symbol data for {symbol} sent successfully.")
    except Exception as e:
        logging.error(f"Error fetching symbol data: {e}", exc_info=True)
        await update.message.reply_text(f"Error fetching data for symbol: {symbol}")

    return ConversationHandler.END


async def send_symbol_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global last_funding_rate_data

    if last_funding_rate_data is None:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="No funding rate data available. Please run /on or wait for the 30-minute cycle.",
            parse_mode='Markdown'
        )
        return

    try:
        symbols = []

        lines = last_funding_rate_data.split("\n")

        for line in lines[1:]:
            if "|" in line:
                columns = line.split("|")
                if len(columns) > 2:
                    exchange = columns[0].strip()
                    symbol = columns[1].strip()
                    symbols.append(f"{exchange}: {symbol}")

        symbols_text = "\n".join(symbols)
        for i in range(0, len(symbols_text), 4000):
            await context.bot.send_message(
                chat_id=alphawave_cr_group_chat_id,
                text=symbols_text[i:i + 4000],
                parse_mode='Markdown'
            )
        logging.info("Symbol list sent successfully.")
    except Exception as e:
        logging.error(f"Error fetching symbol list: {e}", exc_info=True)
        await context.bot.send_message(
            chat_id=alphawave_cr_group_chat_id,
            text="Error while fetching symbol list!",
            parse_mode='Markdown'
        )


async def send_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    info_message = (
        "Command Manual:\n\n"
        "/on - Fetches the latest funding rate data and updates the stored data.\n"
        "/prev - Shows the most recent funding rate data previously fetched.\n"
        "/symbol - Lets you input a symbol to get detailed funding rate information for that specific symbol.\n"
        "/symbol_list - Displays a list of symbols for which funding rate data is available.\n\n"
        "Notes:\n"
        "- The funding rate data updates every 30 minutes (at half-past and on the hour).\n"
        "- You can use /prev to view the previously fetched data.\n"
        "- If you want to fetch new data immediately, use /on to trigger a manual update.\n"
        "- If the server was just started or if it's before the next 30-minute update, use /on to get the latest data.\n"
    )

    try:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=info_message
        )
    except Exception as e:
        logging.error(f"Error sending info message: {e}")


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Action cancelled.")
    return ConversationHandler.END


def main():
    try:
        application = ApplicationBuilder().token(kamp_alphawave_bot_token).build()

        send_fund_rate_handler = CommandHandler('on', on_command)
        prev_fund_rate_handler = CommandHandler('prev', prev_command)
        symbol_list_handler = CommandHandler('symbol_list', send_symbol_list)
        info_handler = CommandHandler('info', send_info)

        symbol_handler = ConversationHandler(
            entry_points=[CommandHandler('symbol', ask_symbol)],
            states={
                SYMBOL: [MessageHandler(
                    filters.TEXT & ~filters.COMMAND, send_symbol_data)]
            },
            fallbacks=[CommandHandler('cancel', cancel)]
        )

        application.add_handler(send_fund_rate_handler)
        application.add_handler(prev_fund_rate_handler)
        application.add_handler(symbol_list_handler)
        application.add_handler(info_handler)
        application.add_handler(symbol_handler)

        job_queue = application.job_queue
        now = datetime.now(pytz.timezone('Asia/Seoul'))
        if now.minute < 30:
            first_run = now.replace(minute=30, second=0, microsecond=0)
        else:
            first_run = now.replace(
                minute=0, second=0, microsecond=0) + timedelta(hours=1)
        job_queue.run_once(periodic_funding_rate_update,
                           when=(first_run - now).total_seconds())

        application.run_polling()
    except Exception as e:
        logging.error(f"An error occurred in main: {e}", exc_info=True)


if __name__ == "__main__":
    main()
