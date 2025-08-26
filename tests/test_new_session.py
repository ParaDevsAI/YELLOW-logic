"""
test_new_session.py
Teste com NOVA sessão
"""
import asyncio
import os
import logging
from dotenv import load_dotenv
from telethon import TelegramClient

# Configurar logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Carregar variáveis
load_dotenv()
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")

async def test_new_session():
    """Teste com NOVA sessão."""
    print("🔄 TESTE COM NOVA SESSÃO")
    print("="*50)
    
    # NOVA SESSÃO - NOME DIFERENTE!
    NEW_SESSION_NAME = "test_session_fresh"
    
    print(f"1️⃣ Criando cliente com sessão: {NEW_SESSION_NAME}")
    client = TelegramClient(NEW_SESSION_NAME, int(API_ID), API_HASH)
    
    print("2️⃣ Conectando...")
    try:
        await client.connect()
        print("✅ Conectado!")
        
        print("3️⃣ Verificando autorização...")
        authorized = await client.is_user_authorized()
        print(f"✅ Autorizado: {authorized}")
        
        if not authorized:
            print("❌ Nova sessão não autorizada - precisa autenticar")
            print("🔐 Iniciando autenticação...")
            
            # Autenticação
            phone = input("📱 Digite seu número de telefone (ex: +5511999999999): ")
            await client.send_code_request(phone)
            
            code = input("🔢 Digite o código recebido no Telegram: ")
            try:
                await client.sign_in(phone, code)
            except Exception as e:
                if "Two-steps verification" in str(e):
                    password = input("🔐 Digite sua senha 2FA: ")
                    await client.sign_in(password=password)
                else:
                    raise e
            
            print("✅ Autenticação completa!")
        
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
    
    asyncio.run(test_new_session()) 