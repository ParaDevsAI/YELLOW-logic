import asyncio
import os
import logging
from dotenv import load_dotenv
from collections import defaultdict
from datetime import datetime

# Importando o cliente Supabase
from bot.author_manager import get_supabase_client

# --- Configuração de Logging ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

async def analyze_history():
    """
    Analisa os dados na tabela leaderboard_history para mostrar a evolução
    dos usuários e contar dias com pontuação zerada.
    Este script é somente leitura.
    """
    supabase = await get_supabase_client()
    if not supabase:
        logger.critical("Não foi possível conectar ao Supabase. Abortando.")
        return

    # Passo 1: Buscar todos os dados da tabela de histórico com paginação
    logger.info("Buscando todos os dados da tabela 'leaderboard_history'...")
    all_history_data = []
    page = 0
    page_size = 1000
    while True:
        try:
            start_index = page * page_size
            response = await asyncio.to_thread(
                supabase.table('leaderboard_history')
                .select('*')
                .order('snapshot_timestamp', desc=False) # Garante ordem cronológica
                .range(start_index, start_index + page_size - 1)
                .execute
            )
            if response.data:
                all_history_data.extend(response.data)
                if len(response.data) < page_size:
                    break
                page += 1
            else:
                break
        except Exception as e:
            logger.error(f"Erro ao buscar dados do histórico (página {page}): {e}")
            break
    
    if not all_history_data:
        logger.warning("A tabela 'leaderboard_history' está vazia ou não pôde ser lida.")
        logger.warning("Execute o script 'populate_full_history.py' primeiro.")
        return

    # Passo 2: Processar os dados para as análises
    
    # Análise 1: Evolução do Usuário
    user_evolution = defaultdict(lambda: {
        'name': 'N/A',
        'first_score_date': None,
        'final_score': 0,
        'active_days': 0,
        'last_score': 0
    })
    
    data_by_user = defaultdict(list)
    for row in all_history_data:
        data_by_user[row['user_id']].append(row)

    for user_id, records in data_by_user.items():
        user_info = user_evolution[user_id]
        user_info['name'] = records[0].get('telegram_name') or records[0].get('twitter_username', str(user_id))

        for record in records:
            score = record.get('grand_total_score', 0)
            date = datetime.fromisoformat(record['snapshot_timestamp']).date()
            
            # Registra a primeira data com pontuação
            if score > 0 and user_info['first_score_date'] is None:
                user_info['first_score_date'] = date
            
            # Conta dias ativos (quando a pontuação aumentou)
            if score > user_info['last_score']:
                user_info['active_days'] += 1
            
            user_info['last_score'] = score
        
        user_info['final_score'] = user_info['last_score']

    # Análise 2: Datas com Pontuação Zerada
    total_score_by_date = defaultdict(float)
    for row in all_history_data:
        date = datetime.fromisoformat(row['snapshot_timestamp']).date()
        total_score_by_date[date] += row.get('grand_total_score', 0)
        
    zero_score_dates_count = 0
    for date, total_score in total_score_by_date.items():
        if total_score == 0:
            zero_score_dates_count += 1

    # Passo 3: Exibir o relatório de análise
    print("\n" + "="*80)
    print("                Relatório de Análise do Histórico do Leaderboard")
    print("="*80)
    
    print("\n[ 1 ] Análise da Evolução dos Embaixadores (Top 10 por pontuação final)")
    print("-" * 80)
    # Ordena os usuários pela pontuação final para mostrar os mais relevantes
    sorted_users = sorted(user_evolution.items(), key=lambda item: item[1]['final_score'], reverse=True)
    
    for user_id, data in sorted_users[:10]:
        print(f"  -> Usuário: {data['name']}")
        print(f"     - Pontuação Final: {data['final_score']:.0f}")
        if data['first_score_date']:
            print(f"     - Primeira Pontuação em: {data['first_score_date'].strftime('%Y-%m-%d')}")
            print(f"     - Total de Dias Ativos (pontuação aumentou): {data['active_days']} dias")
        else:
            print("     - Nenhuma pontuação registrada.")
        print("-" * 40)

    print("\n[ 2 ] Análise de Datas com Pontuação Zerada")
    print("-" * 80)
    print(f"  - Total de dias no histórico: {len(total_score_by_date)}")
    print(f"  - Número de dias em que a pontuação de TODOS os embaixadores era zero: {zero_score_dates_count}")
    print("\n" + "="*80)

async def main():
    load_dotenv()
    await analyze_history()

if __name__ == "__main__":
    asyncio.run(main()) 