import asyncio
import logging
from bot.author_manager import get_supabase_client
from dotenv import load_dotenv

# Configuração de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def find_orphan_data():
    """
    Diagnostica o banco de dados em busca de dados órfãos que causam o erro user_id = -1.
    Dados órfãos são registros em tabelas de "eventos" (engajamentos, contribuições, etc.)
    que se referem a um autor que não existe na tabela `authors`.
    """
    supabase = await get_supabase_client()
    if not supabase:
        logging.critical("Falha ao conectar com o Supabase. Abortando.")
        return

    logging.info("Iniciando diagnóstico de dados órfãos...")

    try:
        # 1. Buscar todos os IDs de autores válidos
        logging.info("Buscando todos os autores válidos da tabela 'authors'...")
        authors_response = await asyncio.to_thread(
            supabase.table('authors').select('twitter_id, telegram_id').execute
        )
        if not authors_response.data:
            logging.error("Nenhum autor encontrado na tabela 'authors'. Isso é inesperado.")
            return

        valid_twitter_ids = {str(author['twitter_id']) for author in authors_response.data if author.get('twitter_id')}
        valid_telegram_ids = {str(author['telegram_id']) for author in authors_response.data if author.get('telegram_id')}
        logging.info(f"Encontrados {len(valid_twitter_ids)} twitter_ids e {len(valid_telegram_ids)} telegram_ids válidos.")

        total_orphans_found = 0

        # 2. Verificar 'ambassador_engagements'
        logging.info("Verificando a tabela 'ambassador_engagements' por engajamentos órfãos...")
        engagements_response = await asyncio.to_thread(
            supabase.table('ambassador_engagements').select('interacting_user_id, tweet_id').execute
        )
        orphan_engagements = []
        if engagements_response.data:
            for engagement in engagements_response.data:
                user_id = str(engagement.get('interacting_user_id'))
                if user_id not in valid_twitter_ids:
                    orphan_engagements.append(engagement)
        
        if orphan_engagements:
            logging.warning(f"Encontrados {len(orphan_engagements)} engajamentos órfãos em 'ambassador_engagements':")
            for orphan in orphan_engagements:
                print(f"  - Engajamento Órfão: Usuário Twitter ID {orphan['interacting_user_id']} (no tweet {orphan['tweet_id']}) não está na tabela 'authors'.")
            total_orphans_found += len(orphan_engagements)
        else:
            logging.info("'ambassador_engagements' está limpa. Nenhum engajamento órfão encontrado.")

        # 3. Verificar 'manual_contributions'
        logging.info("Verificando a tabela 'manual_contributions' por contribuições órfãs...")
        contributions_response = await asyncio.to_thread(
            supabase.table('manual_contributions').select('author_telegram_id, description').execute
        )
        orphan_contributions = []
        if contributions_response.data:
            for contribution in contributions_response.data:
                user_id = str(contribution.get('author_telegram_id'))
                if user_id not in valid_telegram_ids:
                    orphan_contributions.append(contribution)

        if orphan_contributions:
            logging.warning(f"Encontradas {len(orphan_contributions)} contribuições órfãs em 'manual_contributions':")
            for orphan in orphan_contributions:
                print(f"  - Contribuição Órfã: Usuário Telegram ID {orphan['author_telegram_id']} (descrição: '{orphan['description']}') não está na tabela 'authors'.")
            total_orphans_found += len(orphan_contributions)
        else:
            logging.info("'manual_contributions' está limpa. Nenhuma contribuição órfã encontrada.")

        # 4. Verificar 'tweet_engagement_metrics' (autores dos tweets originais)
        logging.info("Verificando a tabela 'tweet_engagement_metrics' por tweets órfãos...")
        tweets_response = await asyncio.to_thread(
            supabase.table('tweet_engagement_metrics').select('author_id, tweet_id').execute
        )
        orphan_tweets = []
        if tweets_response.data:
            for tweet in tweets_response.data:
                author_id = str(tweet.get('author_id'))
                if author_id not in valid_twitter_ids:
                    orphan_tweets.append(tweet)

        if orphan_tweets:
            logging.warning(f"Encontrados {len(orphan_tweets)} tweets órfãos em 'tweet_engagement_metrics':")
            for orphan in orphan_tweets:
                print(f"  - Tweet Órfão: O autor do tweet {orphan['tweet_id']} (ID: {orphan['author_id']}) não está na tabela 'authors'.")
            total_orphans_found += len(orphan_tweets)
        else:
            logging.info("'tweet_engagement_metrics' está limpa. Nenhum tweet órfão encontrado.")

        # --- Conclusão ---
        print("\n" + "="*50)
        if total_orphans_found > 0:
            logging.warning(f"Diagnóstico concluído. Total de {total_orphans_found} registros órfãos encontrados.")
            print("Ação recomendada: Para cada ID órfão, você deve:")
            print("1. Adicionar o autor correspondente na tabela 'authors' (se for um embaixador válido).")
            print("2. Ou apagar o registro órfão da tabela de origem (se for um erro ou lixo de dados).")
            print("\nO leaderboard não poderá ser gerado corretamente até que estes dados sejam corrigidos.")
        else:
            logging.info("Diagnóstico concluído. NENHUM DADO ÓRFÃO ENCONTRADO.")
            print("Seu banco de dados parece estar consistente. O erro de 'user_id = -1' pode ter outra causa, mas dados órfãos eram a mais provável.")
        print("="*50)

    except Exception as e:
        logging.error(f"Ocorreu um erro durante o diagnóstico: {e}")

async def main():
    load_dotenv()
    await find_orphan_data()

if __name__ == "__main__":
    asyncio.run(main()) 