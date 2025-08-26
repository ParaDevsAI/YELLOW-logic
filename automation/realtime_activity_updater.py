"""
realtime_activity_updater.py

Este script é projetado para ser executado periodicamente (e.g., via cron job
ou GitHub Actions a cada X horas) para atualizar a atividade do Telegram.

Lógica:
1.  Gerencia um arquivo de estado ('telegram_tracker_state.json') para saber
    qual foi a última mensagem processada.
2.  Conecta-se à API do Telegram como um usuário usando Telethon.
3.  Lê o ID da última mensagem processada do arquivo de estado.
4.  Busca todas as mensagens no grupo alvo que chegaram *depois* daquele ID.
5.  Aplica a lógica de pontuação por sessão (bônus, limites) para as novas mensagens.
6.  Salva os novos registros de atividade no Supabase.
7.  Atualiza o arquivo de estado com o ID da mensagem mais recente processada.
8.  Em caso de qualquer falha crítica, envia uma notificação de erro para
    um administrador via Telegram.
"""
import asyncio
import os
import json
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from telethon import TelegramClient

from bot.author_manager import get_supabase_client, get_all_ambassador_telegram_ids

# --- Configuração ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "my_user_session")
SCORING_GROUP_ID = int(os.getenv("SCORING_GROUP_ID", 0))
SCORING_BONUS_MULTIPLIER = float(os.getenv("SCORING_BONUS_MULTIPLIER", 1.25))
ADMIN_TELEGRAM_ID = int(os.getenv("ADMIN_TELEGRAM_ID", 1091994845))
STATE_FILE = 'telegram_tracker_state.json'

# --- Funções de Lógica e Banco de Dados (Reutilizadas/Adaptadas) ---

def get_session_from_date(dt: datetime) -> str:
    hour = dt.hour
    session_start = (hour // 3) * 3
    session_end = session_start + 3
    return f"{session_start:02d}-{session_end:02d}"

async def save_bulk_activity_to_supabase(activity_records: list):
    if not activity_records:
        logger.info("Nenhum novo registro de atividade para salvar.")
        return
    supabase = await get_supabase_client()
    if not supabase:
        raise Exception("Falha ao obter cliente Supabase para salvar atividade.")
    
    logger.info(f"Preparando para salvar {len(activity_records)} registros de atividade diária no Supabase...")
    response = await asyncio.to_thread(
        supabase.table('user_activity').upsert(activity_records, on_conflict='user_id,activity_date').execute
    )
    if not response.data:
         logger.warning(f"O Supabase não retornou dados após o upsert, o que pode indicar um problema. Resposta: {response}")

    logger.info(f"Lote de {len(activity_records)} registros salvo/atualizado com sucesso!")


def get_last_processed_id() -> int:
    if not os.path.exists(STATE_FILE):
        return 0
    try:
        with open(STATE_FILE, 'r') as f:
            data = json.load(f)
            return data.get('last_message_id', 0)
    except (json.JSONDecodeError, IOError):
        return 0

def save_last_processed_id(message_id: int):
    with open(STATE_FILE, 'w') as f:
        json.dump({'last_message_id': message_id}, f)

# --- Função Principal ---

async def run_update():
    """Função principal que orquestra a atualização."""
    logger.info("--- Iniciando Atualização de Atividade do Telegram ---")

    # 1. Obter último ID processado
    last_id = get_last_processed_id()
    logger.info(f"Último ID de mensagem processado: {last_id}")

    # 2. Conectar e buscar novas mensagens
    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)
    await client.connect()
    
    target_group = await client.get_entity(SCORING_GROUP_ID)
    ambassador_ids = await get_all_ambassador_telegram_ids()
    
    if not ambassador_ids:
        logger.warning("Nenhum embaixador encontrado. Finalizando.")
        await client.disconnect()
        return

    logger.info(f"Buscando novas mensagens no grupo '{target_group.title}' após o ID {last_id}...")
    
    new_messages = []
    async for message in client.iter_messages(target_group, min_id=last_id):
        new_messages.append(message)

    if not new_messages:
        logger.info("Nenhuma mensagem nova encontrada. Finalizando.")
        await client.disconnect()
        return

    logger.info(f"Encontradas {len(new_messages)} novas mensagens para processar.")
    
    # Ordenar mensagens da mais antiga para a mais nova
    new_messages.sort(key=lambda m: m.id)

    # 3. Processar mensagens em memória
    activity_data = {}
    
    for message in new_messages:
        if not message.from_id or not hasattr(message.from_id, 'user_id'):
            continue
        
        user_id = message.from_id.user_id
        if user_id not in ambassador_ids:
            continue
        
        msg_dt = message.date
        date_str = msg_dt.strftime('%Y-%m-%d')
        session_id = get_session_from_date(msg_dt)

        activity_data.setdefault(user_id, {}).setdefault(date_str, {})
        session_state = activity_data[user_id][date_str].setdefault(session_id, {'messages': 0, 'score': 0.0, 'last_score': 0.0})

        if session_state['messages'] >= 10:
            continue
        
        current_msg_score = 1.0 if session_state['messages'] == 0 else session_state['last_score'] * SCORING_BONUS_MULTIPLIER
        
        session_state['messages'] += 1
        session_state['score'] += current_msg_score
        session_state['last_score'] = current_msg_score

    logger.info("Processamento concluído. Agregando dados para o banco de dados...")

    # 4. Preparar dados para o Supabase
    # Primeiro, buscar dados existentes do DB para os dias afetados para não sobrescrever
    supabase = await get_supabase_client()
    if not supabase:
        raise Exception("Não foi possível obter o cliente Supabase antes de agregar os dados.")

    affected_users_dates = []
    for user_id, dates in activity_data.items():
        for date_str in dates.keys():
            affected_users_dates.append((user_id, date_str))

    # Construir uma consulta para buscar todos os registros existentes de uma vez
    if affected_users_dates:
        query_filter = ",".join([f"(user_id.eq.{uid},activity_date.eq.{dstr})" for uid, dstr in affected_users_dates])
        
        response = await asyncio.to_thread(
            supabase.table('user_activity')
            .select('*')
            .or_(query_filter)
            .execute
        )
        
        existing_data = {(item['user_id'], item['activity_date']): item for item in response.data}
    else:
        existing_data = {}


    records_to_save = []
    for user_id, daily_data in activity_data.items():
        for date_str, session_updates in daily_data.items():
            # Mergear com dados existentes do DB
            day_key = (user_id, date_str)
            final_sessions = existing_data.get(day_key, {}).get('intervals_details_json', {})
            
            for session_id, new_data in session_updates.items():
                final_sessions[session_id] = {'messages': new_data['messages'], 'score': round(new_data['score'], 4)}

            total_day_score = sum(details.get('score', 0.0) for details in final_sessions.values())

            records_to_save.append({
                'user_id': user_id,
                'activity_date': date_str,
                'total_day_score': round(total_day_score, 4),
                'intervals_details_json': final_sessions
            })

    # 5. Salvar em lote e atualizar o estado
    if records_to_save:
        await save_bulk_activity_to_supabase(records_to_save)
        
    latest_message_id = new_messages[-1].id
    save_last_processed_id(latest_message_id)
    logger.info(f"Estado atualizado. Último ID de mensagem processado: {latest_message_id}")

    await client.disconnect()
    logger.info("--- Atualização de Atividade Concluída com Sucesso ---")

# --- Orquestrador com Tratamento de Erro ---

async def main():
    client = None
    try:
        await run_update()
    except Exception as e:
        logger.critical(f"ERRO CRÍTICO no script de atualização de atividade: {e}", exc_info=True)
        # Tenta enviar uma notificação de falha
        try:
            client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)
            await client.connect()
            error_message = f"⚠️ ALERTA: O script 'realtime_activity_updater.py' falhou com um erro crítico.\n\nErro: {e}"
            await client.send_message(ADMIN_TELEGRAM_ID, error_message)
            logger.info(f"Notificação de erro enviada para o administrador {ADMIN_TELEGRAM_ID}.")
        except Exception as notify_e:
            logger.error(f"Falha ao tentar enviar a notificação de erro para o administrador: {notify_e}")
        finally:
            if client and client.is_connected():
                await client.disconnect()


if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main()) 