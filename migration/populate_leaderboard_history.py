import asyncio
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

from bot.author_manager import get_supabase_client

# --- Configuração de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÃO ---
# A data de início para popular o histórico, conforme solicitado.
START_DATE = datetime(2025, 4, 23, tzinfo=timezone.utc).date()


async def populate_history():
    """
    Calcula e popula a tabela `leaderboard_history` dia a dia, desde a data de início
    até a data atual, usando a estrutura de tabela definida em create_schema.sql.
    """
    supabase = await get_supabase_client()
    if not supabase:
        logger.critical("Não foi possível conectar ao Supabase. Abortando.")
        return

    logger.info("="*80)
    logger.info("Iniciando o script para popular a tabela 'leaderboard_history'.")
    logger.info("Este processo irá calcular a pontuação para cada dia e salvá-la no banco.")
    logger.info(f"O histórico será preenchido a partir de: {START_DATE.strftime('%Y-%m-%d')}")
    logger.info("="*80)

    # Passo 1: Loop diário desde a data de início até hoje
    today = datetime.now(timezone.utc).date()
    current_date = START_DATE
    total_days = (today - START_DATE).days + 1
    day_count = 0

    while current_date <= today:
        day_count += 1
        
        logger.info(f"Processando Dia {day_count}/{total_days}: {current_date.strftime('%Y-%m-%d')}")

        try:
            # Passo 2: Limpar registros existentes para garantir idempotência.
            # Como a tabela não tem UNIQUE(data, user), deletamos antes de inserir.
            date_start_str = current_date.strftime('%Y-%m-%d 00:00:00')
            date_end_str = current_date.strftime('%Y-%m-%d 23:59:59')
            logger.info(f"Limpando registros existentes para {current_date.strftime('%Y-%m-%d')}...")
            await asyncio.to_thread(
                supabase.table('leaderboard_history')
                .delete()
                .gte('snapshot_timestamp', date_start_str)
                .lte('snapshot_timestamp', date_end_str)
                .execute
            )

            # Passo 3: Chamar a função que calcula o leaderboard para a data específica
            date_str_for_rpc = current_date.strftime('%Y-%m-%d 23:59:59')
            response = await asyncio.to_thread(
                supabase.rpc(
                    'calculate_leaderboard_for_date',
                    {'target_date': date_str_for_rpc}
                ).execute
            )

            if not response.data:
                logger.warning(f"Nenhum dado de leaderboard retornado para {current_date}. Pulando para o próximo dia.")
                current_date += timedelta(days=1)
                continue

            # Passo 4: Preparar os dados para inserção, mapeando para as colunas corretas.
            records_to_insert = []
            
            # Ordenar por pontuação para calcular o rank
            sorted_leaderboard = sorted(response.data, key=lambda x: x.get('grand_total_score', 0), reverse=True)
            
            for rank, item in enumerate(sorted_leaderboard, 1):
                user_id = item.get('telegram_id')
                if not user_id:
                    logger.warning(f"Registro pulado para {current_date} por não ter telegram_id: {item}")
                    continue

                # Mapeamento 1-para-1 com a tabela leaderboard_history do create_schema.sql
                record = {
                    "snapshot_timestamp": date_str_for_rpc,
                    "user_id": user_id,
                    "rank": rank,
                    "telegram_name": item.get('telegram_name'),
                    "twitter_username": item.get('twitter_username'),
                    "count_tweets_text_only": item.get('count_tweets_text_only', 0),
                    "count_tweets_image": item.get('count_tweets_image', 0),
                    "count_tweets_thread": item.get('count_tweets_thread', 0),
                    "count_tweets_video": item.get('count_tweets_video', 0),
                    "total_score_from_tweets": item.get('total_score_from_tweets', 0),
                    "count_retweets_made": item.get('count_retweets_made', 0),
                    "count_comments_made": item.get('count_comments_made', 0),
                    "total_score_from_engagements": item.get('total_score_from_engagements', 0),
                    "total_score_from_telegram": item.get('total_score_from_telegram', 0),
                    "count_partner_introduction": item.get('count_partner_introduction', 0),
                    "count_hosting_ama": item.get('count_hosting_ama', 0),
                    "count_recruitment_ambassador": item.get('count_recruitment_ambassador', 0),
                    "count_product_feedback": item.get('count_product_feedback', 0),
                    "count_recruitment_investor": item.get('count_recruitment_investor', 0),
                    "total_score_from_contributions": item.get('total_score_from_contributions', 0),
                    "grand_total_score": item.get('grand_total_score', 0)
                }
                records_to_insert.append(record)
            
            if not records_to_insert:
                logger.info(f"Nenhum registro válido para salvar em {current_date}.")
                current_date += timedelta(days=1)
                continue

            # Passo 5: Fazer o 'insert' dos dados na tabela de histórico
            await asyncio.to_thread(
                supabase.table('leaderboard_history')
                .insert(records_to_insert)
                .execute
            )
            logger.info(f"Sucesso! {len(records_to_insert)} registros salvos para {current_date}.")

        except Exception as e:
            logger.error(f"Ocorreu um erro ao processar a data {current_date}: {e}")
            # Decide se quer parar ou continuar em caso de erro. Vamos continuar por enquanto.
        
        # Avança para o próximo dia
        current_date += timedelta(days=1)
    
    logger.info("="*80)
    logger.info("População do histórico do leaderboard concluída com sucesso!")
    logger.info("A tabela 'leaderboard_history' agora contém os dados diários.")
    logger.info("="*80)


async def main():
    load_dotenv()
    # Adicionando uma confirmação final do usuário
    print("Este script irá apagar e recriar o conteúdo da tabela 'leaderboard_history'.")
    print(f"O processo começará a partir de {START_DATE.strftime('%Y-%m-%d')}.")
    
    # Comentado para automação, mas útil para execução manual
    # user_confirmation = input("Você tem certeza que deseja continuar? (s/n): ")
    # if user_confirmation.lower() != 's':
    #     print("Operação cancelada pelo usuário.")
    #     return
        
    await populate_history()

if __name__ == "__main__":
    asyncio.run(main()) 