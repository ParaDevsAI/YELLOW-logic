import os
import httpx
from dotenv import load_dotenv
import logging
import time
from datetime import datetime, timedelta

import sys
sys.path.append('..')
from author_manager import initialize_supabase_client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("thread_identifier.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def check_if_thread_via_api(client: httpx.Client, api_key: str, tweet_id: str) -> bool | None:
    url_api = "https://api.twitterapi.io/twitter/tweet/thread_context"
    params = {"tweetId": tweet_id}
    headers = {"X-API-Key": api_key}
    
    try:
        response = client.get(url_api, params=params, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            thread_tweets = data.get('tweets', [])
            num_tweets_in_context = len(thread_tweets)
            
            is_a_thread = num_tweets_in_context >= 3
            logger.info(f" -> Contexto tem {num_tweets_in_context} tweets. É uma thread? {is_a_thread}")
            return is_a_thread
        else:
            logger.error(f" -> Erro na API para o tweet {tweet_id}. Status: {response.status_code}, Resposta: {response.text[:200]}")
            return None

    except httpx.RequestError as e:
        logger.error(f" -> Erro de requisição ao tentar contatar a API para o tweet {tweet_id}: {e}")
        return None
    except Exception as e:
        logger.error(f" -> Ocorreu um erro inesperado ao processar o tweet {tweet_id}: {e}")
        return None

async def main():
    logger.info("--- Iniciando Script de Identificação de Threads ---")
    load_dotenv()

    api_key = os.getenv("TWITTER_API_KEY")
    if not api_key:
        logger.critical("TWITTER_API_KEY não encontrada nas variáveis de ambiente. Abortando.")
        return {'threads_identified': 0}

    supabase = initialize_supabase_client()
    if not supabase:
        logger.error("Falha ao conectar com Supabase")
        return {'threads_identified': 0}

    logger.info("Buscando tweets recentes que ainda não foram verificados...")
    
    three_days_ago = datetime.utcnow() - timedelta(days=3)
    
    try:
        response = supabase.table("tweets").select("tweet_id").eq('is_thread_checked', "false").gte('createdat', three_days_ago.isoformat()).execute()
    except Exception as e:
        logger.critical(f"Falha crítica ao buscar tweets do Supabase: {e}")
        return {'threads_identified': 0}

    if not response.data:
        logger.info("Nenhum tweet não verificado encontrado.")
        return {'threads_identified': 0}

    logger.info(f"Encontrados {len(response.data)} tweets para verificar.")

    with httpx.Client(timeout=30.0) as client:
        threads_identified = 0
        for tweet_record in response.data:
            tweet_id = tweet_record.get("tweet_id")
            if not tweet_id:
                continue

            logger.info(f"Verificando tweet {tweet_id}...")
            
            is_thread = check_if_thread_via_api(client, api_key, tweet_id)
            
            if is_thread is not None:
                try:
                    update_data = {
                        'is_thread_checked': True,
                        'content_type': 'thread' if is_thread else 'text'
                    }
                    
                    supabase.table("tweets").update(update_data).eq("tweet_id", tweet_id).execute()
                    
                    if is_thread:
                        threads_identified += 1
                        logger.info(f" -> Tweet {tweet_id} identificado como thread e atualizado.")
                    else:
                        logger.info(f" -> Tweet {tweet_id} não é uma thread e foi marcado como verificado.")
                        
                except Exception as e:
                    logger.error(f" -> Falha ao atualizar tweet {tweet_id} no Supabase: {e}")
            else:
                logger.warning(f" -> Tweet {tweet_id} não pôde ser verificado devido a erro na API.")

            time.sleep(0.5)

    logger.info(f"--- Script de Identificação de Threads Concluído ---")
    logger.info(f"Total de threads identificadas: {threads_identified}")
    
    return {'threads_identified': threads_identified}

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
