import asyncio
import os
import httpx
import json
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta

from author_manager import get_supabase_client

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

async def get_all_ambassador_twitter_ids() -> set:
    supabase = await get_supabase_client()
    if not supabase:
        return set()
    try:
        response = await asyncio.to_thread(
            supabase.table('authors').select('twitter_id').execute
        )
        if response.data:
            return {item['twitter_id'] for item in response.data}
        return set()
    except Exception as e:
        logger.error(f"Erro no Supabase ao buscar IDs de embaixadores: {e}")
        return set()

async def get_all_tweets_from_db() -> list:
    supabase = await get_supabase_client()
    if not supabase:
        return []
    
    cutoff_date = datetime.utcnow() - timedelta(days=3)
    cutoff_date_iso = cutoff_date.isoformat()

    logger.info(f"Buscando tweets criados desde: {cutoff_date_iso}")
    
    try:
        response = await asyncio.to_thread(
            supabase.table('tweets')
            .select('tweet_id, author_id')
            .gte('createdat', cutoff_date_iso)
            .execute
        )
        return response.data if response.data else []
    except Exception as e:
        logger.error(f"Erro no Supabase ao buscar tweets recentes: {e}")
        return []

async def fetch_with_pagination(session: httpx.AsyncClient, url: str, params: dict, headers: dict, data_key: str) -> list:
    all_data = []
    while True:
        try:
            response = await session.get(url, params=params, headers=headers, timeout=40.0)
            if response.status_code != 200:
                logger.error(f"API retornou erro {response.status_code} para {url} com params {params}. Resposta: {response.text}")
                break
            
            data = response.json()
            all_data.extend(data.get(data_key, []))

            if data.get('has_next_page') and data.get('next_cursor'):
                params['cursor'] = data.get('next_cursor')
                logger.info(f"Paginando para {url}... Encontrados {len(data.get(data_key, []))} itens.")
                await asyncio.sleep(1)
            else:
                break
        except (httpx.RequestError, json.JSONDecodeError) as e:
            logger.error(f"Erro na requisição para {url}: {e}")
            break
        except Exception as e:
            logger.error(f"Erro inesperado na requisição para {url}: {e}")
            break
    
    return all_data

async def fetch_replies_and_quotes(session: httpx.AsyncClient, tweet_id: str, api_key: str) -> list:
    url = "https://api.twitterapi.io/twitter/tweet/advanced_search"
    params = {
        'query': f'(conversation_id:{tweet_id}) OR (quoted_tweet_id:{tweet_id})',
        'queryType': 'Latest'
    }
    headers = {"X-API-Key": api_key}
    
    return await fetch_with_pagination(session, url, params, headers, 'tweets')

async def fetch_retweeters(session: httpx.AsyncClient, tweet_id: str, api_key: str) -> list:
    url = "https://api.twitterapi.io/twitter/tweet/retweeters"
    params = {'tweetId': tweet_id}
    headers = {"X-API-Key": api_key}
    
    return await fetch_with_pagination(session, url, params, headers, 'users')

async def process_tweet_engagements(session: httpx.AsyncClient, tweet_id: str, author_id: str, api_key: str, ambassador_ids: set) -> list:
    engagements = []
    
    logger.info(f"--- Processando tweet: {tweet_id} (Autor: {author_id}) ---")
    
    logger.info(f"Buscando respostas/citações para o tweet {tweet_id}...")
    replies_quotes = await fetch_replies_and_quotes(session, tweet_id, api_key)
    
    logger.info(f"Buscando retweeters para o tweet {tweet_id}...")
    retweeters = await fetch_retweeters(session, tweet_id, api_key)
    
    for reply in replies_quotes:
        user_id = reply.get('author_id')
        if user_id and user_id in ambassador_ids and user_id != author_id:
            engagements.append({
                'tweet_id': tweet_id,
                'tweet_author_id': author_id,
                'interacting_user_id': user_id,
                'action_type': 'reply',
                'points_awarded': 2,
                'created_at': datetime.utcnow().isoformat()
            })
    
    for retweet in retweeters:
        user_id = retweet.get('id')
        if user_id and user_id in ambassador_ids and user_id != author_id:
            engagements.append({
                'tweet_id': tweet_id,
                'tweet_author_id': author_id,
                'interacting_user_id': user_id,
                'action_type': 'retweet_or_quote',
                'points_awarded': 1,
                'created_at': datetime.utcnow().isoformat()
            })
    
    return engagements

async def save_engagements_batch(supabase, engagements: list) -> bool:
    if not engagements:
        return True
    
    try:
        columns = ['points_awarded', 'tweet_author_id', 'tweet_id', 'interacting_user_id', 'action_type', 'created_at']
        engagement_records = [{col: engagement[col] for col in columns} for engagement in engagements]
        
        response = await asyncio.to_thread(
            supabase.table('ambassador_engagements').upsert(engagement_records, on_conflict='tweet_id,interacting_user_id,action_type').execute
        )
        
        if response.data:
            logger.info(f"Lote de {len(engagements)} engajamentos salvo com sucesso")
            return True
        else:
            logger.error("Falha ao salvar lote de engajamentos: resposta vazia da API")
            return False
            
    except Exception as e:
        logger.error(f"FALHA CRÍTICA ao salvar lote de engajamentos no Supabase: {e}")
        return False

async def main():
    logger.info("--- Script de Rastreamento de Engajamento Cruzado Iniciado ---")
    
    load_dotenv()
    api_key = os.getenv("TWITTER_API_KEY")
    
    if not api_key:
        logger.error("TWITTER_API_KEY não encontrada no ambiente")
        return
    
    ambassador_ids = await get_all_ambassador_twitter_ids()
    if not ambassador_ids:
        logger.error("Nenhum embaixador encontrado no banco de dados")
        return
    
    logger.info(f"Encontrados {len(ambassador_ids)} embaixadores para análise")
    
    tweets = await get_all_tweets_from_db()
    if not tweets:
        logger.info("Nenhum tweet recente encontrado para análise")
        return
    
    logger.info(f"Analisando {len(tweets)} tweets para engajamentos cruzados")
    
    supabase = await get_supabase_client()
    if not supabase:
        logger.error("Falha ao conectar com Supabase")
        return
    
    total_engagements = 0
    
    async with httpx.AsyncClient() as session:
        for tweet in tweets:
            tweet_id = tweet.get('tweet_id')
            author_id = tweet.get('author_id')
            
            if not tweet_id or not author_id:
                continue
            
            engagements = await process_tweet_engagements(session, tweet_id, author_id, api_key, ambassador_ids)
            
            if engagements:
                success = await save_engagements_batch(supabase, engagements)
                if success:
                    total_engagements += len(engagements)
                
                await asyncio.sleep(0.5)
    
    logger.info(f"--- Script de Rastreamento de Engajamento Cruzado Concluído ---")
    logger.info(f"Total de engajamentos processados: {total_engagements}")
    
    return {'engagements_captured': total_engagements}

if __name__ == "__main__":
    asyncio.run(main()) 