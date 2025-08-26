import asyncio
import os
import pandas as pd
import httpx
from dotenv import load_dotenv
import logging
from datetime import datetime
import re

from bot.author_manager import get_supabase_client

# --- Configuração de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("population.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Funções Reutilizadas (Adaptadas de tweet_link_tracker.py) ---

def extract_tweet_id_from_url(text: str) -> str | None:
    """
    Usa regex para encontrar um link do Twitter/X.com e extrair o ID do tweet.
    """
    if not isinstance(text, str):
        return None
    match = re.search(r"https?://(?:twitter|x)\.com/(?:\w+)/status/(\d+)", text)
    if match:
        return match.group(1)
    return None

async def get_twitter_id_from_telegram_id(supabase_client, telegram_id: int) -> int | None:
    """Busca o twitter_id de um autor usando seu telegram_id."""
    try:
        response = await asyncio.to_thread(
            supabase_client.table('authors').select('twitter_id').eq('telegram_id', telegram_id).single().execute
        )
        if response.data:
            return response.data.get('twitter_id')
    except Exception as e:
        logger.error(f"Erro ao buscar twitter_id para telegram_id {telegram_id}: {e}")
    return None

async def save_full_tweet_data(supabase_client, tweet_data: dict, telegram_id: int) -> bool:
    """Salva todos os dados do tweet e do autor no banco de dados Supabase."""
    author_twitter_id = await get_twitter_id_from_telegram_id(supabase_client, telegram_id)
    if not author_twitter_id:
        logger.error(f"CRITICAL: Não foi possível encontrar o twitter_id para o telegram_id {telegram_id}. O salvamento do tweet foi abortado.")
        return False
        
    author_api_data = tweet_data.get('author', {})
    entities = tweet_data.get('entities', {})
    
    try:
        if author_api_data.get('id'):
            author_update_record = {
                'twitter_username': author_api_data.get('userName'),
                'twitter_name': author_api_data.get('name'),
                'twitter_description': author_api_data.get('description'),
                'twitter_followers': author_api_data.get('followers'),
                'twitter_following': author_api_data.get('following'),
                'twitter_statusescount': author_api_data.get('statusesCount'),
                'twitter_mediacount': author_api_data.get('mediaCount'),
                'twitter_createdat': author_api_data.get('createdAt'),
                'twitter_isblueverified': author_api_data.get('isBlueVerified', False),
                'twitter_profilepicture': author_api_data.get('profilePicture'),
                'sync_timestamp': datetime.utcnow().isoformat()
            }
            await asyncio.to_thread(
                supabase_client.table('authors').update(author_update_record).eq('telegram_id', telegram_id).execute
            )

        media_url = None
        if tweet_data.get('extendedEntities', {}).get('media'):
            media_url = tweet_data['extendedEntities']['media'][0].get('media_url_https')

        tweet_record = {
            'tweet_id': tweet_data.get('id'),
            'author_id': author_twitter_id,
            'twitter_url': tweet_data.get('twitterUrl'),
            'text': tweet_data.get('text'),
            'createdat': tweet_data.get('createdAt'),
            'views': tweet_data.get('viewCount'),
            'likes': tweet_data.get('likeCount'),
            'retweets': tweet_data.get('retweetCount'),
            'replies': tweet_data.get('replyCount'),
            'quotes': tweet_data.get('quoteCount'),
            'bookmarks': tweet_data.get('bookmarkCount'),
            'content_type': tweet_data.get('extendedEntities', {}).get('media', [{}])[0].get('type'),
            'media_url': media_url
        }
        await asyncio.to_thread(
            supabase_client.table('tweets').upsert(tweet_record).execute
        )

        await asyncio.to_thread(
            supabase_client.table('tweet_entities').delete().eq('tweet_id', tweet_data.get('id')).execute
        )
        
        entities_to_insert = []
        if entities.get('user_mentions'):
            for mention in entities['user_mentions']:
                entities_to_insert.append({'tweet_id': tweet_data.get('id'),'entity_type': 'user_mention','text_in_tweet': mention.get('screen_name'),'mentioned_user_id': mention.get('id_str')})
        if entities.get('hashtags'):
            for hashtag in entities['hashtags']:
                 entities_to_insert.append({'tweet_id': tweet_data.get('id'),'entity_type': 'hashtag','text_in_tweet': hashtag.get('text')})
        if entities.get('urls'):
            for url_entity in entities['urls']:
                 entities_to_insert.append({'tweet_id': tweet_data.get('id'),'entity_type': 'url','text_in_tweet': url_entity.get('url'),'expanded_url': url_entity.get('expanded_url')})

        if entities_to_insert:
            await asyncio.to_thread(
                supabase_client.table('tweet_entities').insert(entities_to_insert).execute
            )
        logger.info(f"Tweet {tweet_data.get('id')} salvo com sucesso para o autor com telegram_id {telegram_id}.")
        return True
    except Exception as e:
        logger.error(f"FALHA CRÍTICA no Supabase ao salvar o tweet {tweet_data.get('id')}. Erro: {e}")
        return False

# --- Função Principal de População ---

async def fetch_and_populate_tweets(supabase_client, api_key: str, csv_path: str):
    """
    Lê um CSV, busca os detalhes de cada tweet e popula o banco de dados.
    """
    logger.info("--- Iniciando o processo de população de tweets a partir do CSV ---")
    
    try:
        df_tweets = pd.read_csv(csv_path)
        total_rows_in_csv = len(df_tweets)
        logger.info(f"Lidos {total_rows_in_csv} tweets do arquivo '{csv_path}'.")
    except FileNotFoundError:
        logger.error(f"ERRO: O arquivo '{csv_path}' não foi encontrado.")
        return

    # Contadores para o resumo final
    success_count = 0
    failure_count = 0

    # Mapear twitter_username para telegram_id para evitar buscas repetidas no DB
    logger.info("Mapeando twitter_usernames para telegram_ids...")
    response = await asyncio.to_thread(supabase_client.table('authors').select('twitter_username, telegram_id').execute)
    if not response.data:
        logger.error("Nenhum autor encontrado no Supabase. Abortando.")
        return
    author_map = {author['twitter_username'].lower(): author['telegram_id'] for author in response.data if author['twitter_username']}
    logger.info(f"Mapa de {len(author_map)} autores criado.")

    async with httpx.AsyncClient(timeout=40.0) as http_client:
        for index, row in df_tweets.iterrows():
            is_successful_iteration = False
            try:
                tweet_url = row.get('tweet_url')
                author_username = row.get('author_twitter_username')

                if not tweet_url or not author_username:
                    logger.warning(f"Pulando linha {index+2}: Faltando URL do tweet ou username do autor.")
                    continue
                
                telegram_id = author_map.get(str(author_username).lower())
                if not telegram_id:
                    logger.warning(f"Pulando tweet {tweet_url}: Autor @{author_username} não encontrado no banco de dados.")
                    continue

                tweet_id = extract_tweet_id_from_url(tweet_url)
                if not tweet_id:
                    logger.warning(f"  -> AVISO: Não foi possível extrair o ID do tweet da URL: {tweet_url}")
                    continue

                logger.info(f"Processando tweet ({index + 1}/{total_rows_in_csv}): ID {tweet_id} de {tweet_url}")
                
                url_api = "https://api.twitterapi.io/twitter/tweets"
                params = {"tweet_ids": tweet_id}
                headers = {"X-API-Key": api_key}
                
                api_response = await http_client.get(url_api, params=params, headers=headers)
                    
                if api_response.status_code == 200:
                    data = api_response.json()
                    tweets = data.get('tweets', [])
                    if tweets:
                        logger.info(f"  -> Sucesso! API retornou dados para o tweet ID {tweet_id}. Salvando...")
                        if await save_full_tweet_data(supabase_client, tweets[0], telegram_id):
                            is_successful_iteration = True
                    else:
                        logger.warning(f"  -> AVISO: A API retornou 200 OK, mas nenhum tweet foi encontrado para o ID {tweet_id} (URL: {tweet_url}).")
                else:
                    logger.error(f"  -> ERRO na API ao buscar o ID {tweet_id}. Status: {api_response.status_code}, Resposta: {api_response.text[:200]}")
            
            except Exception as e:
                logger.error(f"  -> ERRO inesperado ao processar o ID {tweet_id} (URL: {tweet_url}): {e}")
            
            finally:
                if is_successful_iteration:
                    success_count += 1
                else:
                    failure_count += 1

    logger.info("\n\n" + "="*50)
    logger.info("--- RELATÓRIO FINAL DE POPULAÇÃO ---")
    logger.info(f"Total de linhas no CSV: {total_rows_in_csv}")
    logger.info(f"Tweets salvos com sucesso: {success_count}")
    logger.info(f"Falhas ou tweets não processados: {failure_count}")
    logger.info("="*50)

async def main():
    """Função principal que orquestra todo o processo."""
    load_dotenv()
    
    api_key = os.getenv("TWITTER_API_KEY")
    if not api_key:
        logger.critical("A variável de ambiente TWITTER_API_KEY não foi encontrada. O script não pode continuar.")
        return

    supabase = await get_supabase_client()
    if not supabase:
        logger.critical("Falha ao conectar com o Supabase. Verifique as credenciais.")
        return

    csv_file = 'found_tweets.csv'
    await fetch_and_populate_tweets(supabase, api_key, csv_file)

    logger.info("\n--- PROCESSO DE POPULAÇÃO CONCLUÍDO ---")

if __name__ == "__main__":
    asyncio.run(main()) 