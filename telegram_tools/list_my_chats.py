import os
import asyncio
import logging
from dotenv import load_dotenv
from telethon import TelegramClient

# --- Configuração de Logging ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Carregar Variáveis de Ambiente ---
load_dotenv()
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "my_user_session")

async def main():
    """Conecta e lista todos os chats (diálogos) do usuário."""
    logger.info("--- Iniciando Script para Listar Chats do Telegram ---")
    if not all([API_ID, API_HASH]):
        logger.critical("Variáveis de ambiente TELEGRAM_API_ID ou TELEGRAM_API_HASH não encontradas. Abortando.")
        return

    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)

    async with client:
        logger.info("Cliente Telegram conectado. Buscando diálogos...")
        
        print("\n" + "="*50)
        print("          LISTA DE CHATS E SEUS IDs          ")
        print("="*50 + "\n")
        
        try:
            async for dialog in client.iter_dialogs():
                # O dialog.id já vem no formato que a API espera.
                print(f"Nome do Chat: '{dialog.name}'   |   ID: {dialog.id}")

            print("\n" + "="*50)
            print("Copie o ID do grupo 'Yellow' exatamente como apareceu acima")
            print("e cole no seu arquivo .env na variável SCORING_GROUP_ID.")
            print("="*50 + "\n")

        except Exception as e:
            logger.error(f"Ocorreu um erro ao buscar os diálogos: {e}")

    logger.info("--- Listagem de Chats Concluída ---")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main()) 