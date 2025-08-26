"""
debug_simple.py
Script simples para debugar o problema
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

print(f"🔍 DEBUG INFO:")
print(f"API_ID: {API_ID}")
print(f"API_HASH: {'***' if API_HASH else 'None'}")
print(f"SESSION_NAME: {SESSION_NAME}")
print(f"SCORING_GROUP_ID: {SCORING_GROUP_ID}")

async def test():
    print("\n🚀 Testando conexão...")
    
    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)
    
    try:
        async with client:
            print("✅ Conectado!")
            
            if SCORING_GROUP_ID == 0:
                print("❌ SCORING_GROUP_ID é 0!")
                return
            
            print(f"🎯 Tentando acessar grupo: {SCORING_GROUP_ID}")
            
            try:
                group = await client.get_entity(SCORING_GROUP_ID)
                print(f"✅ Grupo acessado: '{group.title}'")
            except Exception as e:
                print(f"❌ Erro ao acessar grupo: {e}")
                
    except Exception as e:
        print(f"❌ Erro geral: {e}")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(test()) 