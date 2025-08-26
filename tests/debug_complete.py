"""
debug_complete.py
Diagnóstico completo do problema de download
"""
import asyncio
import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from telethon import TelegramClient

# Configuração de logging detalhado
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG  # DEBUG para ver tudo
)
logger = logging.getLogger(__name__)

# Carregar variáveis
load_dotenv()
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "my_user_session")
SCORING_GROUP_ID = int(os.getenv("SCORING_GROUP_ID", 0))
TWEETS_GROUP_ID = -1002330680602

print("🔍 DIAGNÓSTICO COMPLETO")
print("="*50)
print(f"API_ID: {API_ID}")
print(f"API_HASH: {'***' if API_HASH else 'None'}")
print(f"SESSION_NAME: {SESSION_NAME}")
print(f"SCORING_GROUP_ID: {SCORING_GROUP_ID}")
print(f"TWEETS_GROUP_ID: {TWEETS_GROUP_ID}")

async def test_complete():
    """Teste completo de conexão e acesso."""
    print("\n🚀 Iniciando teste completo...")
    
    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)
    
    try:
        print("📡 Conectando ao Telegram...")
        async with client:
            print("✅ Cliente conectado!")
            
            # Verificar autorização
            if not await client.is_user_authorized():
                print("❌ Usuário não autorizado!")
                return
            print("✅ Usuário autorizado!")
            
            # Testar grupo de scoring
            print(f"\n🎯 Testando grupo de scoring: {SCORING_GROUP_ID}")
            if SCORING_GROUP_ID == 0:
                print("❌ SCORING_GROUP_ID é 0!")
            else:
                try:
                    group = await client.get_entity(SCORING_GROUP_ID)
                    print(f"✅ Grupo de scoring: '{group.title}'")
                    
                    # Testar busca de mensagens
                    print("📥 Testando busca de mensagens...")
                    message_count = 0
                    async for message in client.iter_messages(group, limit=3):
                        message_count += 1
                        print(f"  📨 Mensagem {message_count}: {message.text[:30]}...")
                    
                    print(f"✅ Busca bem-sucedida: {message_count} mensagens")
                    
                except Exception as e:
                    print(f"❌ Erro no grupo de scoring: {e}")
            
            # Testar grupo de tweets
            print(f"\n🐦 Testando grupo de tweets: {TWEETS_GROUP_ID}")
            try:
                group = await client.get_entity(TWEETS_GROUP_ID)
                print(f"✅ Grupo de tweets: '{group.title}'")
                
                # Testar busca de mensagens
                print("📥 Testando busca de mensagens...")
                message_count = 0
                async for message in client.iter_messages(group, limit=3):
                    message_count += 1
                    print(f"  📨 Mensagem {message_count}: {message.text[:30]}...")
                
                print(f"✅ Busca bem-sucedida: {message_count} mensagens")
                
            except Exception as e:
                print(f"❌ Erro no grupo de tweets: {e}")
            
            print("\n✅ Teste completo finalizado!")
            
    except Exception as e:
        print(f"❌ Erro geral: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(test_complete()) 