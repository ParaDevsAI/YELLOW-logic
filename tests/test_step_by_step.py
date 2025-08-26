"""
test_step_by_step.py
Teste passo a passo para identificar onde o script para
"""
import asyncio
import os
from dotenv import load_dotenv
from telethon import TelegramClient

# Carregar variáveis
load_dotenv()
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "my_user_session")
SCORING_GROUP_ID = int(os.getenv("SCORING_GROUP_ID", 0))

async def test_step_by_step():
    """Teste passo a passo para identificar o problema."""
    print("🔍 TESTE PASSO A PASSO")
    print("="*50)
    
    print("1️⃣ Criando cliente...")
    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)
    
    print("2️⃣ Tentando conectar...")
    try:
        await client.connect()
        print("✅ Conectado!")
    except Exception as e:
        print(f"❌ Erro na conexão: {e}")
        return
    
    print("3️⃣ Verificando autorização...")
    try:
        authorized = await client.is_user_authorized()
        if authorized:
            print("✅ Autorizado!")
        else:
            print("❌ Não autorizado!")
            return
    except Exception as e:
        print(f"❌ Erro na verificação: {e}")
        return
    
    print("4️⃣ Tentando acessar grupo...")
    try:
        group = await client.get_entity(SCORING_GROUP_ID)
        print(f"✅ Grupo acessado: '{group.title}'")
    except Exception as e:
        print(f"❌ Erro ao acessar grupo: {e}")
        return
    
    print("5️⃣ Tentando buscar mensagens...")
    try:
        message_count = 0
        async for message in client.iter_messages(group, limit=3):
            message_count += 1
            print(f"  📨 Mensagem {message_count}: {message.text[:30]}...")
        
        print(f"✅ Busca bem-sucedida: {message_count} mensagens")
    except Exception as e:
        print(f"❌ Erro na busca: {e}")
        return
    
    print("6️⃣ Desconectando...")
    try:
        await client.disconnect()
        print("✅ Desconectado!")
    except Exception as e:
        print(f"❌ Erro na desconexão: {e}")
    
    print("🏁 Teste completo!")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(test_step_by_step()) 