"""
test_connection_simple.py
Teste de conexão simples sem timeout
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

async def test_connection_simple():
    """Testa conexão simples sem timeout."""
    print("🔍 TESTE DE CONEXÃO SIMPLES")
    print("="*50)
    
    print("1️⃣ Criando cliente...")
    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)
    
    print("2️⃣ Conectando SEM timeout...")
    try:
        # CONEXÃO SIMPLES - SEM TIMEOUT
        await client.connect()
        print("✅ Conectado com sucesso!")
        
        print("3️⃣ Verificando autorização...")
        authorized = await client.is_user_authorized()
        if authorized:
            print("✅ Autorizado!")
        else:
            print("❌ Não autorizado!")
            return
        
        print("4️⃣ Testando operação simples...")
        me = await client.get_me()
        print(f"✅ Usuário: {me.first_name} (@{me.username})")
        
        print("5️⃣ Desconectando...")
        await client.disconnect()
        print("✅ Desconectado!")
        
    except Exception as e:
        print(f"❌ Erro: {e}")
    
    print("🏁 Teste completo!")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(test_connection_simple()) 