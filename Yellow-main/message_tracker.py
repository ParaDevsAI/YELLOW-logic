import logging
from telegram import Update
from telegram.ext import MessageHandler, filters, ContextTypes

logger = logging.getLogger(__name__)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Minimal message tracker stub that acknowledges text messages."""
    if update.message:
        await update.message.reply_text("Message received (stub).")
        logger.info(f"Message from user_id={getattr(update.effective_user, 'id', None)}: {update.message.text}")


# Export a MessageHandler used by main_bot
message_handler = MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message)
