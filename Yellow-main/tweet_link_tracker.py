import logging
from telegram import Update
from telegram.ext import MessageHandler, filters, ContextTypes

logger = logging.getLogger(__name__)


async def handle_tweet_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Minimal handler that responds when a tweet link is detected (stub)."""
    if update.message:
        await update.message.reply_text("Tweet link detected (stub).")
        logger.info(f"Tweet link handler triggered: {update.message.text}")


# Export a MessageHandler so the main bot can add it.
tweet_link_handler = MessageHandler(filters.Regex(r'https?://(mobile\.)?twitter.com/|https?://t.co/'), handle_tweet_link)
