import asyncio
import os
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

# Importando o cliente Supabase centralizado
from bot.author_manager import get_supabase_client

# --- Configuração de Logging ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Autores para o Teste ---
# Adicionamos os novos usuários que você pediu.
TEST_AUTHOR_TWITTER_IDS = ['799992467332407296', '22130734', '3044807060']
TEST_AUTHOR_TELEGRAM_IDS = ['1578956859', '1635941845']

async def get_authors_for_test(supabase, twitter_ids, telegram_ids):
    """Busca os dados dos autores com base nos twitter_ids e telegram_ids."""
    try:
        # Usamos uma cláusula OR para buscar por qualquer um dos tipos de ID
        filter_query = f"twitter_id.in.({','.join(twitter_ids)}),telegram_id.in.({','.join(telegram_ids)})"
        response = await asyncio.to_thread(
            supabase.table('authors')
            .select('twitter_id, telegram_id, twitter_username')
            .or_(filter_query)
            .execute
        )
        return response.data if response.data else []
    except Exception as e:
        logger.error(f"Erro ao buscar dados dos autores de teste: {e}")
        return []

async def run_history_simulation_test():
    """
    Simula a geração do leaderboard histórico para um grupo de autores.
    Não modifica nenhum dado no banco. Apenas lê e imprime a simulação.
    """
    supabase = await get_supabase_client()
    if not supabase:
        logger.critical("Não foi possível conectar ao Supabase. Abortando.")
        return

    logger.info("Passo 1: Buscando dados dos autores para o teste...")
    authors_to_test = await get_authors_for_test(supabase, TEST_AUTHOR_TWITTER_IDS, TEST_AUTHOR_TELEGRAM_IDS)
    if not authors_to_test:
        logger.error("Não foi possível encontrar os autores de teste no banco de dados. Verifique os IDs.")
        return
        
    test_author_telegram_ids = [str(author['telegram_id']) for author in authors_to_test]
    all_author_twitter_ids = [str(author['twitter_id']) for author in authors_to_test]
    telegram_id_to_username_map = {str(author['telegram_id']): author['twitter_username'] for author in authors_to_test}

    logger.info("O arquivo 'test_leaderboard_history.sql' deve ser executado manualmente no Supabase SQL Editor.")
    
    logger.info("Passo 2: Encontrando o período de tempo para a simulação...")
    try:
        response = await asyncio.to_thread(
            supabase.table('ambassador_engagements')
            .select('created_at')
            .in_('interacting_user_id', all_author_twitter_ids)
            .order('created_at', desc=False)
            .limit(1)
            .single()
            .execute
        )
        first_interaction_date = datetime.fromisoformat(response.data['created_at']).date()
        today = datetime.now(timezone.utc).date()
    except Exception as e:
        logger.warning(f"Não foi possível determinar a data de início exata. Usando os últimos 30 dias. Erro: {e}")
        today = datetime.now(timezone.utc).date()
        first_interaction_date = today - timedelta(days=30)

    logger.info(f"Período da simulação: de {first_interaction_date} até {today}")

    print("\n" + "="*80)
    print(f"      Simulação do Histórico de Pontuação DETALHADA")
    print("  Regras Atuais (máx. 4 pts/tweet) | Nenhum dado está sendo salvo no banco.")
    print("="*80)

    # Passo 3: Loop diário
    current_date = first_interaction_date
    while current_date <= today:
        date_str_for_rpc = current_date.strftime('%Y-%m-%d 23:59:59')
        print(f"\n--- DATA: {current_date.strftime('%Y-%m-%d')} ---")
        
        try:
            response = await asyncio.to_thread(
                supabase.rpc(
                    'calculate_leaderboard_for_date',
                    {'target_date': date_str_for_rpc}
                ).execute
            )
            
            if response.data:
                authors_in_response = [
                    item for item in response.data 
                    if str(item.get('telegram_id')) in test_author_telegram_ids
                ]

                if not authors_in_response:
                     print("  - Nenhum dos autores de teste teve pontuação registrada nesta data.")
                else:
                    for item in authors_in_response:
                        telegram_id = str(item.get('telegram_id'))
                        username = telegram_id_to_username_map.get(telegram_id, "N/A")
                        
                        print(f"  -> Autor: @{username} (ID: {telegram_id})")
                        print(f"     - Pontos de Tweets:        {item.get('total_score_from_tweets', 0):.0f}")
                        print(f"     - Pontos de Engajamento:   {item.get('total_score_from_engagements', 0):.0f}")
                        print(f"     - Pontos de Telegram:      {item.get('total_score_from_telegram', 0):.0f}")
                        print(f"     - Pontos de Contribuições: {item.get('total_score_from_contributions', 0):.0f}")
                        print(f"     ---------------------------------------------")
                        print(f"     - PONTUAÇÃO TOTAL:         {item.get('grand_total_score', 0):.0f}")
            else:
                print("  - Nenhum dado de leaderboard retornado para esta data.")
        except Exception as e:
            print(f"  - Erro ao chamar a função RPC para esta data: {e}")

        current_date += timedelta(days=1)
        
    print("\n" + "="*80)

async def main():
    load_dotenv()
    await run_history_simulation_test()

if __name__ == "__main__":
    asyncio.run(main()) 