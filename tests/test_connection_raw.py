"""
test_connection_raw.py
Teste de conexão em modo RAW para ver tudo que acontece
"""
import asyncio
import os
import logging
from dotenv import load_dotenv
from telethon import TelegramClient

# Configurar logging em modo RAW
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG  # DEBUG para ver TUDO
)
logger = logging.getLogger(__name__)

# Carregar variáveis
load_dotenv()
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "my_user_session")

async def test_connection_raw():
    """Teste de conexão em modo RAW."""
    print("🔍 TESTE DE CONEXÃO RAW")
    print("="*50)
    
    print("1️⃣ Criando cliente...")
    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)
    
    print("2️⃣ Conectando...")
    try:
        await client.connect()
        print("✅ Conectado!")
        
        print("3️⃣ Verificando autorização...")
        authorized = await client.is_user_authorized()
        print(f"✅ Autorizado: {authorized}")
        
        if authorized:
            print("4️⃣ Testando operação...")
            me = await client.get_me()
            print(f"✅ Usuário: {me.first_name}")
            
            print("5️⃣ Testando grupos...")
            SCORING_GROUP_ID = int(os.getenv("SCORING_GROUP_ID", 0))
            print(f"🎯 Tentando grupo: {SCORING_GROUP_ID}")
            
            try:
                group = await client.get_entity(SCORING_GROUP_ID)
                print(f"✅ Grupo: {group.title}")
                
                print("6️⃣ Testando mensagens...")
                message_count = 0
                async for message in client.iter_messages(group, limit=3):
                    message_count += 1
                    print(f"  📨 Mensagem {message_count}: {message.text[:30]}...")
                
                print(f"✅ Busca: {message_count} mensagens")
                
            except Exception as e:
                print(f"❌ Erro no grupo: {e}")
        
        print("7️⃣ Desconectando...")
        await client.disconnect()
        print("✅ Desconectado!")
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
    
    print("🏁 Teste completo!")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(test_connection_raw()) 