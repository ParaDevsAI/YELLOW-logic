"""
populate_from_json.py

Este script popula a tabela 'user_activity' usando um arquivo JSON
exportado do histórico do Telegram ('result_yellow.json').

Lógica:
1.  Carrega um conjunto de IDs de Telegram de embaixadores registrados do Supabase.
2.  Lê o arquivo 'result_yellow.json' que contém o histórico de mensagens.
3.  Itera sobre cada mensagem no arquivo JSON.
4.  Para cada mensagem, verifica se o autor é um embaixador registrado.
5.  Se for, aplica a lógica de pontuação por sessão (sessões de 3 horas,
    bônus multiplicativo, limite de 10 mensagens) e armazena os resultados
    em uma estrutura de dados em memória.
6.  Após processar todas as mensagens, o script agrega os dados por usuário e por dia.
7.  Finalmente, ele salva (usando 'upsert') cada registro diário de atividade
    na tabela 'user_activity' do Supabase.
"""
import asyncio
import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv

# Importando funções do author_manager
from author_manager import get_supabase_client, get_all_ambassador_telegram_ids

# --- Configuração de Logging ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Carregar Variáveis de Ambiente ---
load_dotenv()
SCORING_BONUS_MULTIPLIER = float(os.getenv("SCORING_BONUS_MULTIPLIER", 1.25))
JSON_FILE_PATH = 'result_yellow.json' # Nome do arquivo de histórico

def get_session_from_date(dt: datetime) -> str:
    """Determina a sessão de 3 horas para uma data/hora específica."""
    hour = dt.hour
    session_start = (hour // 3) * 3
    session_end = session_start + 3
    return f"{session_start:02d}-{session_end:02d}"

async def save_bulk_activity_to_supabase(activity_records: list):
    """Salva uma lista de registros de atividade diária no Supabase."""
    if not activity_records:
        logger.info("Nenhum registro de atividade para salvar.")
        return

    supabase = await get_supabase_client()
    if not supabase:
        logger.error("Falha ao obter cliente Supabase para salvar atividade.")
        return

    try:
        logger.info(f"Preparando para salvar {len(activity_records)} registros de atividade diária no Supabase...")
        await asyncio.to_thread(
            supabase.table('user_activity').upsert(activity_records).execute
        )
        logger.info(f"Lote de {len(activity_records)} registros salvo com sucesso!")
    except Exception as e:
        logger.critical(f"FALHA CRÍTICA ao salvar lote de atividades. Erro: {e}")

async def main():
    """Função principal que orquestra a leitura do JSON, cálculo e salvamento."""
    logger.info("--- Iniciando Script de População de Histórico via JSON ---")

    # 1. Verificar se o arquivo JSON existe
    if not os.path.exists(JSON_FILE_PATH):
        logger.critical(f"Arquivo de histórico '{JSON_FILE_PATH}' não encontrado. Abortando.")
        return

    # 2. Buscar IDs dos embaixadores
    ambassador_ids = await get_all_ambassador_telegram_ids()
    if not ambassador_ids:
        logger.warning("Nenhum ID de embaixador encontrado no banco de dados. Abortando.")
        return
    logger.info(f"Encontrados {len(ambassador_ids)} embaixadores registrados.")

    # 3. Ler o arquivo JSON
    try:
        with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
            history_data = json.load(f)
        messages = history_data.get('messages', [])
        logger.info(f"Arquivo JSON lido com sucesso. Encontradas {len(messages)} mensagens para processar.")
    except (json.JSONDecodeError, IOError) as e:
        logger.critical(f"Erro ao ler ou decodificar o arquivo JSON: {e}. Abortando.")
        return
    
    # 4. Processar mensagens em memória
    # Estrutura: {user_id: {date_str: {session_id: {'messages': X, 'score': Y, 'last_score': Z}}}}
    activity_data = {}

    for message in messages:
        from_id_str = message.get('from_id')
        if not from_id_str or not from_id_str.startswith('user'):
            continue
        
        try:
            user_id = int(from_id_str.replace('user', ''))
        except (ValueError, TypeError):
            continue

        # Verificar se o autor é um embaixador
        if user_id not in ambassador_ids:
            continue
        
        # Obter data da mensagem
        try:
            # Formato esperado: "2025-04-23T01:24:17"
            msg_dt = datetime.fromisoformat(message['date'])
        except (KeyError, ValueError):
            continue
            
        date_str = msg_dt.strftime('%Y-%m-%d')
        session_id = get_session_from_date(msg_dt)

        # Inicializar estruturas de dados se for a primeira vez
        activity_data.setdefault(user_id, {}).setdefault(date_str, {})
        session_state = activity_data[user_id][date_str].setdefault(session_id, {'messages': 0, 'score': 0.0, 'last_score': 0.0})

        # Aplicar limite de 5 mensagens por sessão
        if session_state['messages'] >= 5:
            continue
        
        # Calcular pontuação
        current_msg_score = 1.0 if session_state['messages'] == 0 else session_state['last_score'] * SCORING_BONUS_MULTIPLIER
        
        # Atualizar estado
        session_state['messages'] += 1
        session_state['score'] += current_msg_score
        session_state['last_score'] = current_msg_score

    logger.info("Processamento de todas as mensagens concluído. Agregando resultados...")

    # 5. Preparar dados para o Supabase
    records_to_save = []
    for user_id, daily_data in activity_data.items():
        for date_str, session_data in daily_data.items():
            total_day_score = sum(details.get('score', 0.0) for details in session_data.values())
            
            # O JSON armazenado não precisa do 'last_score'
            intervals_details_json = {
                session: {'messages': data['messages'], 'score': round(data['score'], 4)}
                for session, data in session_data.items()
            }

            records_to_save.append({
                'user_id': user_id,
                'activity_date': date_str,
                'total_day_score': round(total_day_score, 4),
                'intervals_details_json': intervals_details_json
            })

    # 6. Salvar em lote no banco de dados
    await save_bulk_activity_to_supabase(records_to_save)

    logger.info("--- Script de População de Histórico via JSON Concluído ---")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main()) 