import asyncio
import os
import logging
from dotenv import load_dotenv
from datetime import datetime, timezone

from bot.author_manager import get_supabase_client

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

async def update_leaderboard():
    """
    Atualiza a tabela principal 'leaderboard' com os dados mais recentes.
    1. Limpa a tabela atual.
    2. Chama a função `calculate_leaderboard` para obter os scores totais.
    3. Insere os novos dados na tabela.
    4. Atualiza os rankings.
    """
    supabase = await get_supabase_client()
    if not supabase:
        logger.critical("Não foi possível conectar ao Supabase. Abortando.")
        return

    logger.info("="*80)
    logger.info("Iniciando a atualização da tabela principal 'leaderboard'.")
    logger.info("="*80)

    try:
        # Passo 1: Limpar a tabela 'leaderboard' completamente
        logger.info("Passo 1/4: Limpando a tabela 'leaderboard' existente...")
        # A forma mais segura de limpar a tabela via API é deletar todas as linhas.
        # Um filtro trivial como 'neq' em uma coluna não-nula garante que tudo seja deletado.
        await asyncio.to_thread(
            supabase.table('leaderboard').delete().neq('user_id', -9999999).execute
        )
        logger.info("Tabela 'leaderboard' limpa com sucesso.")

        # Passo 2: Chamar a função principal que calcula o placar total
        logger.info("Passo 2/4: Calculando a pontuação mais recente de todos os embaixadores...")
        response = await asyncio.to_thread(
            supabase.rpc('calculate_leaderboard', {}).execute
        )

        if not response.data:
            logger.error("A função 'calculate_leaderboard' não retornou dados. A tabela não será atualizada.")
            return
        
        logger.info(f"Cálculo concluído. {len(response.data)} embaixadores processados.")

        # Passo 3: Preparar e inserir os novos dados
        logger.info("Passo 3/4: Inserindo os novos dados na tabela 'leaderboard'...")
        records_to_insert = []
        now_utc = datetime.now(timezone.utc).isoformat()

        for item in response.data:
            user_id = item.get('telegram_id')
            if not user_id:
                logger.warning(f"Registro pulado por não ter telegram_id: {item}")
                continue
            
            # O 'rank' será nulo inicialmente e preenchido no próximo passo.
            record = {
                "user_id": user_id,
                "last_updated": now_utc,
                "rank": None, 
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
        
        await asyncio.to_thread(
            supabase.table('leaderboard').insert(records_to_insert).execute
        )
        logger.info(f"{len(records_to_insert)} registros inseridos com sucesso.")

        # Passo 4: Chamar a função para calcular e atualizar os ranks
        logger.info("Passo 4/5: Atualizando os ranks na tabela 'leaderboard'...")
        await asyncio.to_thread(
            supabase.rpc('update_leaderboard_ranks', {}).execute
        )
        logger.info("Ranks atualizados com sucesso.")

        # Passo 5: Criar snapshot histórico do dia na tabela 'leaderboard_history'
        logger.info("Passo 5/5: Criando snapshot histórico do leaderboard do dia...")
        history_records = []
        snapshot_timestamp = now_utc

        for item in response.data:
            user_id = item.get('telegram_id')
            if not user_id:
                continue
            
            history_record = {
                "snapshot_timestamp": snapshot_timestamp,
                "user_id": user_id,
                "rank": None,  # Será preenchido depois
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
            history_records.append(history_record)
        
        # Ordenar por pontuação para calcular os ranks no histórico
        history_records_sorted = sorted(history_records, key=lambda x: x.get('grand_total_score', 0), reverse=True)
        for rank, record in enumerate(history_records_sorted, 1):
            record['rank'] = rank

        # Inserir os dados históricos
        await asyncio.to_thread(
            supabase.table('leaderboard_history').insert(history_records_sorted).execute
        )
        logger.info(f"Snapshot histórico criado com {len(history_records_sorted)} registros.")

    except Exception as e:
        logger.error(f"Ocorreu um erro ao atualizar a tabela 'leaderboard': {e}")

    logger.info("="*80)
    logger.info("Atualização das tabelas 'leaderboard' e 'leaderboard_history' concluída!")
    logger.info("="*80)

async def main():
    load_dotenv()
    await update_leaderboard()

if __name__ == "__main__":
    asyncio.run(main()) 