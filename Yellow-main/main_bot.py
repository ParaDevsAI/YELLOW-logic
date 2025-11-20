import os
import logging
from dotenv import load_dotenv

# Carrega as variáveis de ambiente como a primeira ação.
# Isso garante que elas estejam disponíveis para todos os módulos importados abaixo.
load_dotenv()

from telegram import Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters

# Importar os handlers corretos dos nossos módulos
from registration_handler import start_handler
from tweet_link_tracker import tweet_link_handler
from message_tracker import message_handler

# Configuração de logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

async def unhandled_updates_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Loga todas as atualizações não tratadas para diagnóstico."""
    # Este log é crucial para ver se o bot está recebendo mensagens do grupo
    logger.warning(f"UPDATE NÃO TRATADO: {update}")

def main() -> None:
    """Inicia o bot e configura todos os handlers."""
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_BOT_TOKEN:
        logger.error("A variável de ambiente TELEGRAM_BOT_TOKEN não foi encontrada!")
        return

    # Cria a aplicação do bot
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # --- Configuração dos Handlers com Grupos de Prioridade ---
    # Grupo 0: Handlers principais que devem rodar primeiro.
    # O ConversationHandler para registro de novos usuários.
    application.add_handler(start_handler, group=0)
    logger.info("Handler de registro ativado.")

    # O MessageHandler para pontuação de atividade.
    # Ele deve processar TODAS as mensagens de texto válidas.
    application.add_handler(message_handler, group=0)
    logger.info("Handler de captura de mensagens para pontuação ativado.")

    # Grupo 1: Handlers secundários que rodam após o grupo 0.
    # O MessageHandler para rastrear links de tweets.
    # Ele rodará na mesma mensagem que o message_handler se ela contiver um link.
    application.add_handler(tweet_link_handler, group=1)
    logger.info("Handler de rastreamento de links de tweet ativado.")

    # Handler para atualizações não tratadas (deve ter a menor prioridade)
    # É uma boa prática colocá-lo em um grupo alto para que ele só pegue o que realmente sobrou.
    application.add_handler(MessageHandler(filters.ALL, unhandled_updates_handler), group=99)
    logger.info("Handler de diagnóstico 'espião' ativado.")
    
    # Inicia o bot
    logger.info("Bot principal iniciado. Todos os recursos estão ativos.")
    # Drop pending updates on startup to avoid processing a backlog.
    # Note: this does not resolve conflicts if another instance is actively
    # polling or a webhook is set. See troubleshooting notes below.
    application.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main() 