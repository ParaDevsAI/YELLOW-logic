"""
test_connection_timeout.py
Teste de conexão com timeout para resolver o problema de travamento
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

async def test_connection_with_timeout():
    """Testa conexão com timeout explícito."""
    print("🔍 TESTE DE CONEXÃO COM TIMEOUT")
    print("="*50)
    
    print("1️⃣ Criando cliente...")
    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)
    
    print("2️⃣ Tentando conectar com timeout de 30 segundos...")
    try:
        # TIMEOUT EXPLÍCITO - SOLUÇÃO PARA O PROBLEMA
        await asyncio.wait_for(client.connect(), timeout=30)
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
        
    except asyncio.TimeoutError:
        print("❌ TIMEOUT: Conexão demorou mais de 30 segundos")
        print("💡 SOLUÇÃO: Problema de rede ou servidor lento")
        
    except Exception as e:
        print(f"❌ Erro: {e}")
    
    print("🏁 Teste completo!")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(test_connection_with_timeout()) 