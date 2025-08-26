"""
metrics_snapshot.py

This script is designed to be run periodically to capture the performance
metrics of all tweets stored in the database.

It operates as follows:
1.  Fetches all unique tweet IDs from the 'tweets' table in Supabase.
2.  Groups these IDs into batches of a specified size (e.g., 100).
3.  For each batch, it calls the GET /twitter/tweets endpoint to get the
    latest metrics (views, likes, retweets, etc.).
4.  For each tweet returned by the API, it inserts a new record (a "snapshot")
    into the `tweet_metrics_history` table.
5.  It also updates the main `tweets` table with these latest metrics, ensuring
    it always reflects the most recent data.
"""
import asyncio
import os
import httpx
import json
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta
import argparse

# Importando o cliente Supabase centralizado
from bot.author_manager import get_supabase_client

# --- Logging Configuration ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Constants and Configuration ---
# Based on our tests, the API comfortably handles batches of 100.
BATCH_SIZE = 100

# --- Database Functions (Refactored for Supabase) ---

async def get_tweet_ids_to_update(frequency: str) -> list[str]:
    """
    Busca IDs de tweets do Supabase com base na frequência de atualização.
    '6_hours': tweets com menos de 3 dias.
    'daily': tweets entre 3 e 7 dias.
    'weekly': tweets com mais de 7 dias.
    """
    supabase = await get_supabase_client()
    if not supabase:
        logger.error("Falha ao obter cliente Supabase. Não foi possível buscar os IDs dos tweets.")
        return []

    logger.info(f"Buscando IDs de tweets para a frequência de atualização: '{frequency}'")
    
    now = datetime.utcnow()
    query = supabase.table('tweets').select('tweet_id')

    if frequency == '6_hours':
        three_days_ago = now - timedelta(days=3)
        query = query.gte('createdat', three_days_ago.isoformat())
        logger.info("Filtro aplicado: tweets criados nos últimos 3 dias.")
    elif frequency == 'daily':
        seven_days_ago = now - timedelta(days=7)
        three_days_ago = now - timedelta(days=3)
        query = query.lte('createdat', three_days_ago.isoformat()).gte('createdat', seven_days_ago.isoformat())
        logger.info("Filtro aplicado: tweets criados entre 3 e 7 dias atrás.")
    elif frequency == 'weekly':
        seven_days_ago = now - timedelta(days=7)
        query = query.lte('createdat', seven_days_ago.isoformat())
        logger.info("Filtro aplicado: tweets criados há mais de 7 dias.")
    else:
        logger.warning(f"Frequência '{frequency}' não reconhecida. Nenhum tweet será processado.")
        return []
        
    try:
        response = await asyncio.to_thread(query.execute)
        if response.data:
            return [row['tweet_id'] for row in response.data]
        return []
    except Exception as e:
        logger.error(f"Erro no Supabase ao buscar IDs de tweets para frequência '{frequency}': {e}")
        return []

async def process_metrics_batch_supabase(tweets_data: list):
    """
    Saves a batch of tweet metrics to the history table and updates the main tweets table in Supabase.
    This version uses an upsert for both tables for efficiency and correctness.
    """
    if not tweets_data:
        return

    supabase = await get_supabase_client()
    if not supabase:
        logger.error("Failed to get Supabase client in process_metrics_batch. Aborting batch.")
        return

    history_records = []
    tweet_update_records = []
    snapshot_time = datetime.utcnow().isoformat()

    # --- 1. Prepare records for both tables ---
    for tweet in tweets_data:
        author_id = tweet.get('author', {}).get('id')
        tweet_id = tweet.get('id')

        if not author_id or not tweet_id:
            logger.warning(f"Skipping tweet in batch due to missing author_id or tweet_id. Data: {tweet}")
            continue

        # Record for the history table (new entry every time)
        history_records.append({
            'tweet_id': tweet_id,
            'snapshot_at': snapshot_time,
            'views': tweet.get('viewCount', 0),
            'likes': tweet.get('likeCount', 0),
            'retweets': tweet.get('retweetCount', 0),
            'replies': tweet.get('replyCount', 0),
            'quotes': tweet.get('quoteCount', 0),
            'bookmarks': tweet.get('bookmarkCount', 0)
        })
        
        # Record for the main tweets table (update existing entry)
        tweet_update_records.append({
            'tweet_id': tweet_id,
            'author_id': author_id, # Must be included for the upsert to work if the tweet is new
            'createdat': tweet.get('createdAt'),
            'views': tweet.get('viewCount', 0),
            'likes': tweet.get('likeCount', 0),
            'retweets': tweet.get('retweetCount', 0),
            'replies': tweet.get('replyCount', 0),
            'quotes': tweet.get('quoteCount', 0),
            'bookmarks': tweet.get('bookmarkCount', 0),
        })


    try:
        # --- 2. Bulk insert into history table ---
        if history_records:
            await asyncio.to_thread(
                supabase.table('tweet_metrics_history').insert(history_records).execute
            )
            logger.info(f"Successfully inserted {len(history_records)} records into tweet_metrics_history.")

        # --- 3. Bulk upsert into main tweets table ---
        if tweet_update_records:
            await asyncio.to_thread(
                supabase.table('tweets').upsert(tweet_update_records, on_conflict='tweet_id').execute
            )
            logger.info(f"Successfully upserted metrics for {len(tweet_update_records)} tweets in the main table.")

    except Exception as e:
        logger.error(f"CRITICAL failure when saving metrics batch to Supabase: {e}")


# --- Main Orchestration ---

async def main(frequency: str):
    """Main function to orchestrate the entire snapshot process."""
    logger.info(f"--- Iniciando Script de Métricas de Tweets (Frequência: {frequency}) ---")
    
    api_key = os.getenv("TWITTER_API_KEY")
    if not api_key:
        logger.critical("TWITTER_API_KEY não encontrada. Abortando.")
        return

    all_tweet_ids = await get_tweet_ids_to_update(frequency)
    if not all_tweet_ids:
        logger.warning("Nenhum tweet encontrado no banco de dados para a frequência especificada. Encerrando.")
        return

    logger.info(f"Encontrados {len(all_tweet_ids)} tweets únicos para processar.")
    
    # Split IDs into batches
    id_batches = [all_tweet_ids[i:i + BATCH_SIZE] for i in range(0, len(all_tweet_ids), BATCH_SIZE)]
    logger.info(f"Split into {len(id_batches)} batches of up to {BATCH_SIZE} IDs each.")

    url = "https://api.twitterapi.io/twitter/tweets"
    headers = {"X-API-Key": api_key}

    async with httpx.AsyncClient(timeout=40.0) as client:
        for i, batch in enumerate(id_batches):
            logger.info(f"--- Processing Batch {i+1}/{len(id_batches)} ---")
            
            params = {"tweet_ids": ",".join(batch)}

            try:
                response = await client.get(url, headers=headers, params=params)

                if response.status_code == 200:
                    # CORREÇÃO: a função .json() é síncrona, não precisa de await.
                    data = response.json()
                    tweets_returned = data.get('tweets', [])
                    logger.info(f"API returned {len(tweets_returned)} tweets for batch.")
                    await process_metrics_batch_supabase(tweets_returned)
                else:
                    logger.error(f"API request for batch {i+1} failed with status {response.status_code}. Response: {response.text[:200]}...")

            except httpx.RequestError as e:
                logger.error(f"Request failed for batch {i+1}: {e}")
            except json.JSONDecodeError:
                logger.error(f"Failed to decode JSON for batch {i+1}.")
            
            # Courtesy delay between batches
            if i < len(id_batches) - 1:
                await asyncio.sleep(2)
            
    logger.info(f"--- Script de Métricas de Tweets (Frequência: {frequency}) Concluído ---")

if __name__ == '__main__':
    # Load .env variables for standalone execution
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="Executa o snapshot de métricas de tweets com base na frequência.")
    parser.add_argument(
        '--frequency',
        type=str,
        choices=['6_hours', 'daily', 'weekly'],
        required=True,
        help="A frequência de execução para determinar quais tweets atualizar."
    )
    args = parser.parse_args()
    
    asyncio.run(main(args.frequency)) 