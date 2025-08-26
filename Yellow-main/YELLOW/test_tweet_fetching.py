import os
import httpx
import logging
import asyncio
from dotenv import load_dotenv
import re
import pandas as pd
import json
import sys

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÕES ---
FOUND_TWEETS_CSV = 'found_tweets.csv' 
# Endpoint CORRETO, copiado do historical_importer.py que funcionou.
API_ENDPOINT = "https://api.twitterapi.io/twitter/tweet/advanced_search" 

def extract_tweet_id_from_url(url: str) -> str | None:
    """Extrai o ID do tweet de uma URL do Twitter."""
    match = re.search(r'/status/(\d+)', url)
    return match.group(1) if match else None

async def get_tweet_details(tweet_url: str) -> dict | None:
    """
    Busca os detalhes de um tweet usando o endpoint de BUSCA, como no script 'historical_importer.py'.
    """
    load_dotenv()
    api_key = os.getenv("TWITTER_API_KEY")
    if not api_key:
        logger.error("TWITTER_API_KEY não encontrada nas variáveis de ambiente.")
        return None

    headers = {"X-API-Key": api_key}
    # A query agora é a própria URL do tweet.
    params = {"query": tweet_url, "queryType": "Latest"}
    
    logger.info(f"Buscando tweet com a query: '{tweet_url}' no endpoint: {API_ENDPOINT}")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(API_ENDPOINT, headers=headers, params=params)
        
        response.raise_for_status() 

        response_json = response.json()
        
        # A resposta de uma busca é uma lista de tweets
        tweets_found = response_json.get('tweets', [])
        if tweets_found:
            # Assumimos que o primeiro resultado é o que queremos
            logger.info(f"Tweet encontrado com sucesso para a URL: {tweet_url}")
            return tweets_found[0]
        else:
            logger.warning(f"A busca pela URL '{tweet_url}' não retornou nenhum tweet.")
            logger.debug(f"Resposta completa: {response.text}")
            return None

    except httpx.HTTPStatusError as e:
        logger.error(f"Erro de status da API ao buscar a URL {tweet_url}. Status: {e.response.status_code}, Resposta: {e.response.text}")
        return None
    except httpx.RequestError as e:
        logger.error(f"Erro de conexão com a API do Twitter para a URL {tweet_url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Um erro inesperado ocorreu ao buscar a URL {tweet_url}: {e}")
        return None

def print_mapping(tweet_data: dict):
    """
    Imprime o payload recebido e o mapeamento para as tabelas do Supabase.
    """
    print("\n" + "="*80)
    print("                PASSO 1: PAYLOAD RECEBIDO DA API (advanced_search)")
    print("="*80)
    print(json.dumps(tweet_data, indent=2, ensure_ascii=False))
    
    # A estrutura da resposta é uma suposição baseada no formato retornado pela busca.
    tweet_id = tweet_data.get('id')
    author_id = tweet_data.get('author', {}).get('id')
    username = tweet_data.get('author', {}).get('userName')
    created_at_str = tweet_data.get('createdAt')
    text = tweet_data.get('text')
    
    # Métricas
    like_count = tweet_data.get('likes', 0)
    reply_count = tweet_data.get('replies', 0)
    retweet_count = tweet_data.get('retweets', 0)
    view_count = tweet_data.get('views', 0)
    quote_count = tweet_data.get('quotes', 0)

    print("\n" + "="*80)
    print("     PASSO 2: MAPEAMENTO PROPOSTO PARA AS TABELAS DO SUPABASE")
    print("="*80)

    print("\n--- Tabela: public.tweets ---\n")
    print(f"{'Coluna no Supabase':<25} | {'Valor Extraído da API':<50}")
    print(f"{'-'*25} | {'-'*50}")
    print(f"{'tweet_id':<25} | {tweet_id}")
    print(f"{'author_id':<25} | {author_id}")
    print(f"{'created_at':<25} | {created_at_str}")
    print(f"{'twitter_url':<25} | https://twitter.com/{username}/status/{tweet_id}")
    print(f"{'text':<25} | {text.replace(chr(10), ' ') if text else 'N/A'}")

    print("\n\n--- Tabela: public.tweet_metrics_history ---\n")
    print(f"{'Coluna no Supabase':<25} | {'Valor Extraído da API':<50}")
    print(f"{'-'*25} | {'-'*50}")
    print(f"{'tweet_id':<25} | {tweet_id}")
    print(f"{'like_count':<25} | {like_count}")
    print(f"{'reply_count':<25} | {reply_count}")
    print(f"{'retweet_count':<25} | {retweet_count}")
    print(f"{'quote_count':<25} | {quote_count}")
    print(f"{'view_count':<25} | {view_count}")
    
    print("\n" + "="*80)
    print("FIM DO TESTE PARA ESTE TWEET. Nenhuma escrita no banco de dados foi realizada.")
    print("="*80)

async def main():
    """
    Função principal que executa o teste de busca para os 5 primeiros tweets.
    """
    logger.info("--- INICIANDO TESTE DE BUSCA DE TWEET (5 PRIMEIROS) ---")

    try:
        df = pd.read_csv(FOUND_TWEETS_CSV)
        sample_urls = df['tweet_url'].head(5).tolist()
        logger.info(f"Testando com {len(sample_urls)} URLs de amostra...")
    except (FileNotFoundError, IndexError):
        logger.error(f"Arquivo '{FOUND_TWEETS_CSV}' não encontrado ou vazio.")
        return

    for i, url in enumerate(sample_urls):
        logger.info(f"\n--- Processando Amostra {i+1}/{len(sample_urls)}: {url} ---")
        
        # Passamos a URL inteira para a função de busca
        tweet_data = await get_tweet_details(url)
        
        if tweet_data:
            print_mapping(tweet_data)
        else:
            tweet_id = extract_tweet_id_from_url(url)
            logger.error(f"Não foi possível obter os dados para o tweet de amostra (ID: {tweet_id}).")

    logger.info("\n--- TESTE FINALIZADO ---")

if __name__ == '__main__':
    if os.name == 'nt':
        sys.stdout.reconfigure(encoding='utf-8')
    asyncio.run(main()) 