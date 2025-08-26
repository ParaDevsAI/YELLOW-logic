"""
test_telegram_session.py
Script simples para testar se a sessão do Telegram está funcionando
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

async def test_session():
    """Testa se consegue acessar o grupo."""
    print("🔍 Testando sessão do Telegram...")
    
    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)
    
    try:
        async with client:
            print("✅ Cliente conectado!")
            
            # Testar acesso ao grupo
            print(f"🎯 Tentando acessar grupo ID: {SCORING_GROUP_ID}")
            group = await client.get_entity(SCORING_GROUP_ID)
            print(f"✅ Sucesso! Grupo: '{group.title}'")
            
            # Testar buscar uma mensagem
            print("📥 Testando busca de mensagens...")
            message_count = 0
            async for message in client.iter_messages(group, limit=5):
                message_count += 1
                
            print(f"✅ Conseguiu buscar {message_count} mensagens!")
            print("🎉 Sessão está funcionando perfeitamente!")
            
    except Exception as e:
        print(f"❌ Erro: {e}")
        print("💡 Solução: Delete os arquivos .session e execute novamente")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(test_session()) 