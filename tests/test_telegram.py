"""
test_telegram.py

Este é um script de teste único para verificar se a conexão com a API do Telegram
usando a biblioteca Telethon está funcionando corretamente.

Ele executa os seguintes passos:
1. Carrega as variáveis de ambiente do arquivo .env.
2. Lê as credenciais da API do Telegram (API_ID, API_HASH) e o nome da sessão.
3. Cria um cliente Telethon.
4. Conecta-se à API do Telegram. Na primeira execução, ele pedirá
   autenticação (número de telefone, código, senha de 2FA, se houver).
   Ele criará um arquivo .session para logins futuros.
5. Busca os detalhes do próprio usuário autenticado.
6. Imprime os detalhes no console.
7. Desconecta-se da API.
"""

import asyncio
import os
from dotenv import load_dotenv
from telethon import TelegramClient

async def main():
    """Função principal para conectar e testar."""
    print("--- Iniciando teste de conexão com o Telegram via Telethon ---")
    
    # 1. Carregar variáveis de ambiente
    load_dotenv()
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    session_name = os.getenv("TELEGRAM_SESSION_NAME", "my_user_session")

    if not api_id or not api_hash:
        print("\nERRO: TELEGRAM_API_ID ou TELEGRAM_API_HASH não foram encontrados no arquivo .env.")
        print("Por favor, configure as variáveis de ambiente e tente novamente.")
        return

    print("Credenciais da API carregadas com sucesso.")

    # 2. Criar e conectar o cliente
    # O arquivo .session será criado no mesmo diretório deste script.
    client = TelegramClient(session_name, int(api_id), api_hash)
    
    print("Tentando conectar e autenticar no Telegram...")
    print("Se for a primeira vez, siga as instruções no console para fazer o login.")
    
    try:
        # 3. Conectar e autenticar
        # O método .start() gerencia todo o fluxo de login automaticamente.
        await client.start()
        
        # 4. Obter e imprimir os dados do usuário autenticado
        me = await client.get_me()
        
        print("\n--- SUCESSO! ---")
        print("Conexão com o Telegram autenticada com sucesso.")
        print(f"ID do Usuário: {me.id}")
        print(f"Nome: {me.first_name}")
        if me.last_name:
            print(f"Sobrenome: {me.last_name}")
        if me.username:
            print(f"Username: @{me.username}")
        print("------------------\n")

    except Exception as e:
        print(f"\n--- ERRO DURANTE A EXECUÇÃO ---")
        print(f"Ocorreu um erro: {e}")
        print("Verifique suas credenciais e a conexão com a internet.")
        print("---------------------------------\n")

    finally:
        # 5. Desconectar o cliente
        if client.is_connected():
            await client.disconnect()
            print("Cliente desconectado.")
            
    print("--- Teste de conexão concluído ---")

if __name__ == "__main__":
    # Usar asyncio.run para executar a função assíncrona
    asyncio.run(main()) 