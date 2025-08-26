"""
reauthenticate.py
Script para reautenticar a sessão do Telegram
"""
import asyncio
import os
from dotenv import load_dotenv
from telethon import TelegramClient

# Carregar variáveis
load_dotenv()
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "new_one")

async def reauthenticate():
    """Reautentica a sessão do Telegram."""
    print("🔐 REAUTENTICAÇÃO DO TELEGRAM")
    print("="*50)
    
    print("1️⃣ Criando cliente...")
    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)
    
    print("2️⃣ Conectando...")
    await client.connect()
    
    print("3️⃣ Verificando autorização...")
    if not await client.is_user_authorized():
        print("❌ Não autorizado - iniciando autenticação...")
        
        print("📱 Digite seu número de telefone (ex: +5511999999999):")
        phone = input("Telefone: ")
        
        print("📤 Enviando código...")
        await client.send_code_request(phone)
        
        print("🔢 Digite o código recebido no Telegram:")
        code = input("Código: ")
        
        try:
            await client.sign_in(phone, code)
            print("✅ Autenticação bem-sucedida!")
        except Exception as e:
            print(f"❌ Erro na autenticação: {e}")
            
            # Se precisar de senha 2FA
            if "2FA" in str(e) or "password" in str(e):
                print("🔐 Digite sua senha 2FA:")
                password = input("Senha 2FA: ")
                await client.sign_in(password=password)
                print("✅ Autenticação 2FA bem-sucedida!")
    else:
        print("✅ Já autorizado!")
    
    print("4️⃣ Testando conexão...")
    me = await client.get_me()
    print(f"✅ Conectado como: {me.first_name} (@{me.username})")
    
    print("5️⃣ Desconectando...")
    await client.disconnect()
    
    print("🏁 Reautenticação completa!")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(reauthenticate()) 