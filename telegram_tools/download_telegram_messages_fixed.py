"""
download_telegram_messages_fixed.py

Versão corrigida do script de download com melhor tratamento de erros
e logs detalhados para identificar problemas.
"""
import asyncio
import os
import json
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from telethon import TelegramClient
from pathlib import Path

# --- Configuração de Logging Melhorada ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Carregar Variáveis de Ambiente ---
load_dotenv()
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "my_user_session")
SCORING_GROUP_ID = int(os.getenv("SCORING_GROUP_ID", 0))
TWEETS_GROUP_ID = -1002330680602

# --- Configurações do Script ---
START_DATE = datetime(2025, 7, 29, tzinfo=timezone.utc)
END_DATE = datetime(2025, 8, 3, tzinfo=timezone.utc)

# --- Criar estrutura de pastas ---
BASE_DIR = Path("data")
SCORING_DIR = BASE_DIR / "scoring_group"
TWEETS_DIR = BASE_DIR / "tweets_group"

def create_directories():
    """Cria as pastas necessárias para organizar os dados."""
    BASE_DIR.mkdir(exist_ok=True)
    SCORING_DIR.mkdir(exist_ok=True)
    TWEETS_DIR.mkdir(exist_ok=True)
    logger.info(f"Estrutura de pastas criada em: {BASE_DIR.absolute()}")

def format_message_for_json(message):
    """Converte uma mensagem do Telethon para formato JSON serializável."""
    return {
        "message_id": message.id,
        "sender_id": getattr(message, 'sender_id', None),
        "date": message.date.isoformat() if message.date else None,
        "text": message.text,
        "has_media": bool(message.media),
        "reply_to_msg_id": message.reply_to.reply_to_msg_id if message.reply_to else None,
        "is_forward": bool(message.forward),
        "edit_date": message.edit_date.isoformat() if message.edit_date else None
    }

async def download_messages_for_day(client, group, group_name, target_dir, target_date):
    """Baixa todas as mensagens de um grupo para um dia específico."""
    day_str = target_date.strftime('%Y-%m-%d')
    json_file = target_dir / f"{day_str}.json"
    
    # Se o arquivo já existe, pular
    if json_file.exists():
        logger.info(f"📁 Arquivo já existe: {json_file.name}, pulando...")
        return
    
    logger.info(f"📥 Baixando mensagens de {group_name} para {day_str}...")
    
    day_start = target_date
    day_end = target_date + timedelta(days=1)
    
    messages_data = {
        "date": day_str,
        "group_id": group.id,
        "group_name": group_name,
        "download_timestamp": datetime.now(timezone.utc).isoformat(),
        "messages": []
    }
    
    message_count = 0
    
    try:
        logger.info(f"🔍 Iniciando busca de mensagens para {day_str}...")
        async for message in client.iter_messages(
            group,
            offset_date=day_end,
            reverse=True
        ):
            # Para quando sair do período do dia
            if message.date < day_start:
                logger.info(f"⏰ Saindo do período do dia {day_str}")
                break
                
            # Converter mensagem para JSON
            message_json = format_message_for_json(message)
            messages_data["messages"].append(message_json)
            message_count += 1
            
            # Log a cada 100 mensagens
            if message_count % 100 == 0:
                logger.info(f"  📊 Processadas {message_count} mensagens...")
    
    except Exception as e:
        logger.error(f"❌ Erro ao baixar mensagens para {day_str}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return
    
    # Salvar no arquivo JSON
    try:
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(messages_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✅ Salvo: {json_file.name} ({message_count} mensagens)")
        
    except Exception as e:
        logger.error(f"❌ Erro ao salvar arquivo {json_file}: {e}")

async def download_group_messages(client, group_id, group_name, target_dir):
    """Baixa mensagens de um grupo para todos os dias do período."""
    logger.info(f"🎯 Iniciando download do grupo: {group_name} (ID: {group_id})")
    
    try:
        logger.info(f"🔍 Obtendo entidade do grupo {group_id}...")
        group = await client.get_entity(group_id)
        actual_group_name = group.title
        logger.info(f"✅ Conectado ao grupo: '{actual_group_name}' (ID: {group_id})")
        
        current_date = START_DATE
        days_processed = 0
        
        while current_date.date() < END_DATE.date():
            logger.info(f"📅 Processando dia: {current_date.strftime('%Y-%m-%d')}")
            await download_messages_for_day(
                client, group, actual_group_name, target_dir, current_date
            )
            current_date += timedelta(days=1)
            days_processed += 1
            
        logger.info(f"🏁 Concluído download do grupo: {actual_group_name} ({days_processed} dias processados)")
        
    except Exception as e:
        logger.error(f"❌ Erro ao processar grupo {group_id}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

async def main():
    """Função principal que coordena o download."""
    logger.info("🚀 Iniciando Download de Mensagens do Telegram (VERSÃO CORRIGIDA)")
    logger.info(f"📅 Período: {START_DATE.strftime('%d/%m/%Y')} até {(END_DATE - timedelta(days=1)).strftime('%d/%m/%Y')}")
    
    # Verificar variáveis de ambiente
    if not all([API_ID, API_HASH, SCORING_GROUP_ID]):
        logger.critical("❌ Variáveis de ambiente não encontradas: TELEGRAM_API_ID, TELEGRAM_API_HASH, SCORING_GROUP_ID")
        return
    
    logger.info(f"🔧 Configuração: API_ID={API_ID}, SCORING_GROUP_ID={SCORING_GROUP_ID}")
    
    # Criar estrutura de pastas
    create_directories()
    
    # Conectar ao Telegram
    logger.info("📡 Conectando ao Telegram...")
    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)
    
    try:
        async with client:
            logger.info("✅ Cliente Telegram conectado")
            
            # Verificar autorização
            if not await client.is_user_authorized():
                logger.error("❌ Usuário não autorizado!")
                return
            
            logger.info("✅ Usuário autorizado")
            
            # 1. Download do grupo de scoring (activity scores)
            logger.info("\n" + "="*60)
            logger.info("📊 BAIXANDO MENSAGENS DO GRUPO DE SCORING")
            logger.info("="*60)
            await download_group_messages(
                client, SCORING_GROUP_ID, "Scoring Group", SCORING_DIR
            )
            
            # 2. Download do grupo de tweets
            logger.info("\n" + "="*60)
            logger.info("🐦 BAIXANDO MENSAGENS DO GRUPO DE TWEETS")
            logger.info("="*60)
            await download_group_messages(
                client, TWEETS_GROUP_ID, "Tweets Group", TWEETS_DIR
            )
    
    except Exception as e:
        logger.error(f"❌ Erro geral na execução: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    # Exibir resumo final
    print_download_summary()

def print_download_summary():
    """Exibe um resumo dos arquivos baixados."""
    print("\n" + "="*60)
    print("📊 RESUMO DO DOWNLOAD")
    print("="*60)
    
    # Contar arquivos no scoring group
    scoring_files = list(SCORING_DIR.glob("*.json"))
    tweets_files = list(TWEETS_DIR.glob("*.json"))
    
    print(f"📊 Grupo de Scoring: {len(scoring_files)} arquivos")
    for file in sorted(scoring_files):
        print(f"   📁 {file.name}")
    
    print(f"\n🐦 Grupo de Tweets: {len(tweets_files)} arquivos")
    for file in sorted(tweets_files):
        print(f"   📁 {file.name}")
    
    print(f"\n📂 Pasta base: {BASE_DIR.absolute()}")
    print("="*60)

if __name__ == "__main__":
    # Configurar loop de eventos para Windows
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main()) 