"""
thread_identifier.py

Este script é projetado para ser executado periodicamente a fim de
identificar quais tweets são parte de uma thread.

Lógica de Execução:
1.  Busca um lote de tweets no Supabase que ainda não foram verificados
    (onde a coluna `is_thread_checked` é `false`).
2.  Para cada tweet no lote, chama o endpoint da API do Twitter
    `GET /twitter/tweet/thread_context`.
3.  Analisa a resposta da API:
    - Se a resposta contém outros tweets (respostas) cujo autor é o mesmo
      do tweet original, o tweet é considerado o início de uma thread.
4.  Se for uma thread, atualiza o campo `content_type` do tweet para 'thread'.
5.  Independentemente de ser uma thread ou não, atualiza o campo
    `is_thread_checked` para `true`, garantindo que o tweet não seja
    verificado novamente em execuções futuras.
"""
import os
import httpx
from dotenv import load_dotenv
import logging
import time
from datetime import datetime, timedelta

from author_manager import initialize_supabase_client

# --- Configuração de Logging ---
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
    """
    Verifica se um tweet é uma thread usando a API externa.
    Retorna True, False, ou None em caso de erro na API.
    """
    url_api = "https://api.twitterapi.io/twitter/tweet/thread_context"
    params = {"tweetId": tweet_id}
    headers = {"X-API-Key": api_key}
    
    try:
        response = client.get(url_api, params=params, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            thread_tweets = data.get('tweets', [])
            num_tweets_in_context = len(thread_tweets)
            
            # A thread é definida se o contexto tiver 3 ou mais tweets.
            is_a_thread = num_tweets_in_context >= 3
            logger.info(f" -> Contexto tem {num_tweets_in_context} tweets. É uma thread? {is_a_thread}")
            return is_a_thread
        else:
            logger.error(f" -> Erro na API para o tweet {tweet_id}. Status: {response.status_code}, Resposta: {response.text[:200]}")
            return None # Retorna None para indicar falha na API

    except httpx.RequestError as e:
        logger.error(f" -> Erro de requisição ao tentar contatar a API para o tweet {tweet_id}: {e}")
        return None
    except Exception as e:
        logger.error(f" -> Ocorreu um erro inesperado ao processar o tweet {tweet_id}: {e}")
        return None

def main():
    """
    Função principal para identificar e atualizar status de thread para tweets não verificados.
    """
    logger.info("--- Iniciando Script de Identificação de Threads ---")
    load_dotenv()

    api_key = os.getenv("TWITTER_API_KEY")
    if not api_key:
        logger.critical("TWITTER_API_KEY não encontrada nas variáveis de ambiente. Abortando.")
        return

    supabase = initialize_supabase_client()
    if not supabase:
        # A inicialização já loga o erro, então só retornamos.
        return

    logger.info("Buscando tweets recentes que ainda não foram verificados...")
    
    three_days_ago = datetime.utcnow() - timedelta(days=3)
    
    try:
        response = supabase.table("tweets").select("tweet_id").eq('is_thread_checked', "false").gte('createdat', three_days_ago.isoformat()).execute()
    except Exception as e:
        logger.critical(f"Falha crítica ao buscar tweets do Supabase: {e}")
        return

    if not response.data:
        logger.info("Nenhum tweet novo para verificar. Finalizando.")
        return

    tweets_to_check = response.data
    total_tweets = len(tweets_to_check)
    logger.info(f"Encontrados {total_tweets} tweets para verificar.")
    
    updated_count = 0
    failed_count = 0

    with httpx.Client(timeout=40.0) as client:
        for i, tweet in enumerate(tweets_to_check):
            tweet_id = tweet['tweet_id']
            logger.info(f"Processando tweet {i + 1}/{total_tweets} (ID: {tweet_id})...")
            
            is_thread_result = check_if_thread_via_api(client, api_key, tweet_id)
            
            if is_thread_result is not None:
                # Se a chamada de API foi bem-sucedida (não retornou None)
                try:
                    update_response = supabase.table("tweets").update({
                        'is_thread': is_thread_result,
                        'is_thread_checked': True
                    }).eq('tweet_id', tweet_id).execute()
                    
                    if len(update_response.data) > 0:
                        updated_count += 1
                    else:
                        logger.error(f"Falha ao atualizar o tweet {tweet_id} no banco de dados (nenhuma linha retornada).")
                        failed_count += 1

                except Exception as e:
                    logger.error(f"Exceção ao atualizar o tweet {tweet_id} no Supabase: {e}")
                    failed_count += 1
            else:
                # A API falhou, log já foi registrado na função.
                failed_count += 1

            time.sleep(1.5) # Pausa para ser cortês com a API

    logger.info("--- Processo de Identificação de Threads Concluído ---")
    logger.info(f"Resumo: {updated_count} tweets atualizados com sucesso, {failed_count} falhas ou pulados.")

if __name__ == "__main__":
    main() 