"""
test_download_debug.py
Script para debugar o problema do download de mensagens
"""
import asyncio
import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from telethon import TelegramClient

# Configuração de logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Carregar variáveis
load_dotenv()
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "my_user_session")
SCORING_GROUP_ID = int(os.getenv("SCORING_GROUP_ID", 0))

async def test_connection():
    """Testa conexão e acesso aos grupos."""
    print("🔍 Testando conexão e grupos...")
    
    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)
    
    try:
        async with client:
            print("✅ Cliente conectado!")
            
            # Verificar se está autorizado
            if not await client.is_user_authorized():
                print("❌ Usuário não autorizado!")
                return
            
            print("✅ Usuário autorizado!")
            
            # Testar acesso ao grupo de scoring
            print(f"🎯 Testando grupo de scoring: {SCORING_GROUP_ID}")
            
            if SCORING_GROUP_ID == 0:
                print("❌ SCORING_GROUP_ID é 0! Verifique a variável de ambiente.")
                return
            
            try:
                group = await client.get_entity(SCORING_GROUP_ID)
                print(f"✅ Grupo de scoring acessado: '{group.title}'")
                
                # Testar buscar uma mensagem
                print("📥 Testando busca de mensagens...")
                message_count = 0
                async for message in client.iter_messages(group, limit=5):
                    message_count += 1
                    print(f"  📨 Mensagem {message_count}: {message.text[:50]}...")
                
                print(f"✅ Conseguiu buscar {message_count} mensagens!")
                
            except Exception as e:
                print(f"❌ Erro ao acessar grupo de scoring: {e}")
            
            # Testar grupo de tweets
            TWEETS_GROUP_ID = -1002330680602
            print(f"🎯 Testando grupo de tweets: {TWEETS_GROUP_ID}")
            
            try:
                group = await client.get_entity(TWEETS_GROUP_ID)
                print(f"✅ Grupo de tweets acessado: '{group.title}'")
                
                # Testar buscar uma mensagem
                print("📥 Testando busca de mensagens...")
                message_count = 0
                async for message in client.iter_messages(group, limit=5):
                    message_count += 1
                    print(f"  📨 Mensagem {message_count}: {message.text[:50]}...")
                
                print(f"✅ Conseguiu buscar {message_count} mensagens!")
                
            except Exception as e:
                print(f"❌ Erro ao acessar grupo de tweets: {e}")
    
    except Exception as e:
        print(f"❌ Erro geral: {e}")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(test_connection()) 