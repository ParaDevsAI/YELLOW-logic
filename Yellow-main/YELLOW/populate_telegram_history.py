"""
populate_telegram_history.py

Este script é uma ferramenta de uso único para popular a tabela 'user_activity'
com dados históricos de mensagens do Telegram. Ele não é um bot em tempo real.

Lógica:
1. Conecta-se à API do Telegram como uma conta de usuário (não um bot) usando Telethon.
   - Isso é necessário para ler o histórico de mensagens de um grupo.
2. Define um período de tempo (data de início até hoje).
3. Busca todos os IDs de Telegram dos embaixadores registrados no Supabase.
4. Para um número limitado de embaixadores (para teste), ele itera por cada dia do período.
5. Para cada dia e cada embaixador, ele busca todas as mensagens enviadas por eles no grupo alvo.
6. Ele aplica a MESMA lógica de pontuação do 'message_tracker.py':
   - Sessões de 3 horas.
   - Bônus multiplicativo por mensagem.
   - Limite de 10 mensagens pontuadas por sessão.
7. Agrega os dados por dia e salva o resultado (score total e detalhes por intervalo)
   na tabela 'user_activity' do Supabase usando 'upsert'.
"""
import asyncio
import os
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from telethon import TelegramClient
import json

# Importando o cliente Supabase centralizado e outras funções
from author_manager import get_supabase_client, get_all_ambassador_telegram_ids

# --- Configuração de Logging ---
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
SCORING_BONUS_MULTIPLIER = float(os.getenv("SCORING_BONUS_MULTIPLIER", 1.25))

# --- Configurações do Script ---
START_DATE = datetime(2025, 4, 23, tzinfo=timezone.utc)
# AMBASSADOR_TEST_LIMIT = 5 # Limite removido para processar todos
# MESSAGE_FETCH_LIMIT = 5 # Limite removido para teste rápido

def get_session_from_date(dt: datetime) -> str:
    """Determina a sessão de 3 horas para uma data/hora específica."""
    hour = dt.hour
    session_start = (hour // 3) * 3
    session_end = session_start + 3
    return f"{session_start:02d}-{session_end:02d}"

async def save_activity_to_supabase(user_id: int, activity_date: str, total_score: float, details_json: dict):
    """Salva os dados de atividade diária de um usuário no Supabase."""
    supabase = await get_supabase_client()
    if not supabase:
        logger.error("Falha ao obter cliente Supabase para salvar atividade.")
        return

    record_to_save = {
        'user_id': user_id,
        'activity_date': activity_date,
        'total_day_score': total_score,
        'intervals_details_json': details_json
    }

    try:
        await asyncio.to_thread(
            supabase.table('user_activity').upsert(record_to_save).execute
        )
        logger.info(f"Salvo com sucesso a atividade para o usuário {user_id} na data {activity_date} com score {total_score:.2f}")
    except Exception as e:
        logger.error(f"FALHA CRÍTICA ao salvar atividade para o usuário {user_id} na data {activity_date}. Erro: {e}")

async def main():
    """Função principal que orquestra a busca e o cálculo do histórico."""
    logger.info("--- Iniciando Script de População de Histórico do Telegram ---")

    if not all([API_ID, API_HASH, SCORING_GROUP_ID]):
        logger.critical("Variáveis de ambiente TELEGRAM_API_ID, TELEGRAM_API_HASH ou SCORING_GROUP_ID não encontradas. Abortando.")
        return

    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)

    async with client:
        logger.info("Cliente Telegram conectado.")

        # Buscar o grupo alvo
        try:
            target_group = await client.get_entity(SCORING_GROUP_ID)
            logger.info(f"Grupo alvo encontrado: '{target_group.title}'")
        except Exception as e:
            logger.critical(f"Não foi possível encontrar o grupo com ID {SCORING_GROUP_ID}. Erro: {e}. Abortando.")
            return

        # Buscar todos os embaixadores
        ambassador_ids = await get_all_ambassador_telegram_ids()
        if not ambassador_ids:
            logger.warning("Nenhum ID de embaixador encontrado no banco de dados. Abortando.")
            return

        logger.info(f"Encontrados {len(ambassador_ids)} embaixadores. Processando histórico completo para todos.")
        
        # Iterar por cada embaixador da lista
        for i, user_id in enumerate(list(ambassador_ids)):
            logger.info(f"--- Processando Embaixador {i+1}/{len(ambassador_ids)} (ID: {user_id}) ---")

            # Iterar por cada dia desde a data de início até hoje
            current_date = START_DATE
            while current_date.date() <= datetime.now(timezone.utc).date():
                
                day_str = current_date.strftime('%Y-%m-%d')
                logger.info(f"Verificando data: {day_str} para o usuário {user_id}...")

                day_start = current_date
                day_end = current_date + timedelta(days=1)
                
                daily_activity = {} # Estrutura: { 'HH-HH': {'messages': X, 'score': Y, 'last_score': Z} }

                messages_in_day = []
                # Busca as mensagens do usuário naquele dia, na ordem da mais antiga para a mais nova
                try:
                    async for message in client.iter_messages(
                        target_group,
                        from_user=user_id,
                        offset_date=day_end, # Começa do fim do dia e vai para trás
                        reverse=True # Reverte para processar em ordem cronológica
                    ):
                        # O loop para quando a data da mensagem for anterior ao início do dia
                        if message.date < day_start:
                            break
                        messages_in_day.append(message)
                
                except Exception as e:
                    logger.error(f"Erro ao buscar mensagens para o usuário {user_id} no dia {day_str}: {e}")
                    # Pula para o próximo dia para este usuário
                    current_date += timedelta(days=1)
                    continue

                if not messages_in_day:
                    # Se não houver mensagens, apenas avança para o próximo dia
                    current_date += timedelta(days=1)
                    continue

                logger.info(f"-> Encontradas {len(messages_in_day)} mensagens para {user_id} em {day_str}")

                # Processar as mensagens do dia
                for message in messages_in_day:
                    session_id = get_session_from_date(message.date)

                    # Inicializa a sessão se for a primeira vez no dia
                    if session_id not in daily_activity:
                        daily_activity[session_id] = {'messages': 0, 'score': 0.0, 'last_score': 0.0}

                    session_state = daily_activity[session_id]

                    # Aplicar limite de 10 mensagens por sessão
                    if session_state['messages'] >= 10:
                        continue
                    
                    # Calcular pontuação
                    current_msg_score = 0.0
                    if session_state['messages'] == 0:
                        current_msg_score = 1.0
                    else:
                        current_msg_score = session_state['last_score'] * SCORING_BONUS_MULTIPLIER
                    
                    # Atualizar estado da sessão
                    session_state['messages'] += 1
                    session_state['score'] += current_msg_score
                    session_state['last_score'] = current_msg_score

                # Após processar todas as mensagens do dia, preparar para salvar no DB
                total_day_score = sum(details.get('score', 0.0) for details in daily_activity.values())
                
                # O JSON armazenado não precisa do 'last_score'
                intervals_details_json = {
                    session: {'messages': data['messages'], 'score': data['score']}
                    for session, data in daily_activity.items()
                }

                await save_activity_to_supabase(
                    user_id=user_id,
                    activity_date=day_str,
                    total_score=total_day_score,
                    details_json=intervals_details_json
                )

                # Avança para o próximo dia
                current_date += timedelta(days=1)

    logger.info("--- Script de População de Histórico Concluído ---")


if __name__ == "__main__":
    # Garante que o loop de eventos asyncio seja gerenciado corretamente
    # no Windows para evitar RuntimeError.
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main()) 