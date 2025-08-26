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
        logging.FileHandler("population_missing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÃO ---
DRY_RUN = True  # True: Apenas exibe o que seria feito. False: Executa as operações no DB.

# --- Funções de Apoio (reutilizadas e adaptadas) ---

def extract_tweet_id_from_url(text: str) -> str | None:
    if not isinstance(text, str):
        return None
    match = re.search(r'status/(\d+)', text)
    return match.group(1) if match else None

async def get_twitter_id_from_telegram_id(supabase_client, telegram_id: int) -> int | None:
    try:
        response = await asyncio.to_thread(
            supabase_client.table('authors').select('twitter_id').eq('telegram_id', telegram_id).single().execute
        )
        if response.data:
            return response.data['twitter_id']
        return None
    except Exception as e:
        logger.error(f"Erro ao buscar twitter_id para telegram_id {telegram_id}: {e}")
        return None

async def upsert_author_and_get_id(supabase_client, author_data: dict, existing_authors_map: dict):
    """Verifica se um autor existe. Se não, cria. Retorna o twitter_id."""
    author_twitter_id = str(author_data.get('id'))
    author_username = author_data.get('username')

    author_payload = {
        'twitter_id': author_twitter_id,
        'twitter_username': author_username,
        'name': author_data.get('name'),
        'profile_image_url': author_data.get('profile_image_url'),
        'followers_count': author_data.get('followers_count', {}).get('value'),
        'following_count': author_data.get('following_count', {}).get('value'),
        'tweet_count': author_data.get('tweet_count', {}).get('value'),
        'verified': author_data.get('verified'),
        'last_updated': datetime.now().isoformat()
    }

    print("-" * 20)
    if author_twitter_id in existing_authors_map:
        print(f"Autor [EXISTENTE]: @{author_username} (ID: {author_twitter_id})")
        print("PAYLOAD DE ATUALIZAÇÃO (authors):")
        # Remover a chave primária para o payload de atualização
        update_payload = author_payload.copy()
        del update_payload['twitter_id']
        print(update_payload)
        
        if not DRY_RUN:
            await asyncio.to_thread(
                supabase_client.table('authors').update(update_payload).eq('twitter_id', author_twitter_id).execute
            )
    else:
        print(f"Autor [NOVO]: @{author_username} (ID: {author_twitter_id})")
        print("PAYLOAD DE CRIAÇÃO (authors):")
        print(author_payload)
        if not DRY_RUN:
            # Não incluir telegram_id na criação
            await asyncio.to_thread(
                supabase_client.table('authors').insert(author_payload).execute
            )
        # Adiciona ao mapa local para que não seja recriado na mesma execução
        existing_authors_map[author_twitter_id] = {'twitter_username': author_username}
        
    return author_twitter_id

async def prepare_and_save_tweet_data(supabase_client, tweet_data: dict, author_twitter_id: str):
    """Prepara os payloads do tweet e entidades e, se não for DRY_RUN, salva no DB."""
    
    # Payload do Tweet
    tweet_payload = {
        'tweet_id': tweet_data['id'],
        'author_id': author_twitter_id,
        'url': f"https://twitter.com/{tweet_data.get('author',{}).get('username','user')}/status/{tweet_data['id']}",
        'twitter_url': f"https://twitter.com/{tweet_data.get('author',{}).get('username','user')}/status/{tweet_data['id']}",
        'text': tweet_data.get('text'),
        'createdat': tweet_data.get('created_at'),
        'views': tweet_data.get('views', {}).get('value', 0),
        'likes': tweet_data.get('likes', {}).get('value', 0),
        'retweets': tweet_data.get('retweets', {}).get('value', 0),
        'replies': tweet_data.get('replies', {}).get('value', 0),
        'quotes': tweet_data.get('quotes', {}).get('value', 0),
        'bookmarks': tweet_data.get('bookmarks', {}).get('value', 0)
    }
    print("\nPAYLOAD (tweets):")
    print(tweet_payload)

    # Payloads das Entidades
    entities = tweet_data.get('entities', {})
    entity_payloads = []
    if entities:
        for entity_type, entity_list in entities.items():
            for item in entity_list:
                entity_payloads.append({
                    'tweet_id': tweet_data['id'],
                    'type': entity_type,
                    'value': item.get('username') or item.get('tag') or item.get('url')
                })
    
    if entity_payloads:
        print("\nPAYLOAD (tweet_entities):")
        print(entity_payloads)

    if DRY_RUN:
        return True

    # --- Execução no Banco de Dados (se DRY_RUN for False) ---
    try:
        await asyncio.to_thread(
            supabase_client.table('tweets').upsert(tweet_payload, on_conflict='tweet_id').execute
        )
        if entity_payloads:
            # Deleta as antigas e insere as novas para garantir consistência
            await asyncio.to_thread(
                supabase_client.table('tweet_entities').delete().eq('tweet_id', tweet_data['id']).execute
            )
            await asyncio.to_thread(
                supabase_client.table('tweet_entities').insert(entity_payloads).execute
            )
        return True
    except Exception as e:
        logger.error(f"  -> ERRO no DB ao salvar tweet {tweet_data['id']}: {e}")
        return False

async def populate_missing_tweets(supabase_client, api_key: str):
    if DRY_RUN:
        logger.warning("--- EXECUTANDO EM MODO DE SIMULAÇÃO (DRY_RUN = True) ---")
        logger.warning("Nenhuma alteração será feita no banco de dados.")
    else:
        logger.info("--- Iniciando o processo de população de tweets FALTANTES ---")
    
    # --- 1. Ler os IDs dos tweets faltantes ---
    try:
        with open('missing_from_db.txt', 'r') as f:
            missing_tweet_ids = {line.strip() for line in f if line.strip()}
        if not missing_tweet_ids:
            logger.info("O arquivo 'missing_from_db.txt' está vazio. Nenhum tweet a ser processado.")
            return
        logger.info(f"Encontrados {len(missing_tweet_ids)} IDs no arquivo 'missing_from_db.txt'.")
    except FileNotFoundError:
        logger.error("ERRO: O arquivo 'missing_from_db.txt' não foi encontrado.")
        return

    # --- 2. Buscar TODOS os autores existentes para um mapa local ---
    try:
        authors_response = await asyncio.to_thread(supabase_client.table('authors').select('twitter_id, twitter_username').execute)
        existing_authors_map = {str(author['twitter_id']): author for author in authors_response.data}
        logger.info(f"Mapeamento local de {len(existing_authors_map)} autores existentes criado.")
    except Exception as e:
        logger.critical(f"Falha ao buscar autores do Supabase: {e}")
        return

    # --- 3. Criar mapa de tweet_id -> url a partir do found_tweets.csv ---
    try:
        df_found = pd.read_csv('found_tweets.csv')
        df_found.dropna(subset=['tweet_url'], inplace=True)
        df_found['tweet_id'] = df_found['tweet_url'].apply(extract_tweet_id_from_url)
        df_found.dropna(subset=['tweet_id'], inplace=True)
        
        tweet_to_url_map = df_found.set_index('tweet_id')['tweet_url'].to_dict()
        logger.info(f"Mapeamento de URLs criado a partir de 'found_tweets.csv'.")
    except FileNotFoundError:
        logger.critical("ERRO: O arquivo 'found_tweets.csv' é necessário para mapear URLs, mas não foi encontrado.")
        return
    except KeyError:
        logger.critical("ERRO: Coluna 'tweet_url' não encontrada em 'found_tweets.csv'.")
        return
    
    # --- 4. Processar cada tweet faltante ---
    success_count = 0
    failure_count = 0
    total_to_process = len(missing_tweet_ids)
    
    async with httpx.AsyncClient() as client:
        for i, tweet_id in enumerate(missing_tweet_ids):
            is_success = False
            print("\n" + "="*50)
            print(f"PROCESSANDO TWEET {i+1}/{total_to_process}: {tweet_id}")
            print("="*50)

            try:
                tweet_url = tweet_to_url_map.get(tweet_id)
                if not tweet_url:
                    logger.warning(f"  -> AVISO: URL para o tweet_id {tweet_id} não encontrada em 'found_tweets.csv'.")
                    continue

                # Buscar na API
                response = await client.get(
                    "https://api.twitterapi.io/twitter/tweet/advanced_search",
                    params={"query": tweet_url, "queryType": "Latest"},
                    headers={"x-api-key": api_key}
                )
                response.raise_for_status()
                
                api_response_data = response.json()
                if not api_response_data or not api_response_data.get('data'):
                    logger.warning(f"  -> AVISO: A API não retornou dados para a URL: {tweet_url}")
                    continue
                
                tweet_data = api_response_data['data'][0]
                author_data = tweet_data.get('author', {})
                if not author_data:
                     logger.warning(f"  -> AVISO: Dados do autor não encontrados na resposta da API para o tweet {tweet_id}")
                     continue

                # Passo 1: Garantir que o autor existe no DB (ou criar em modo DRY RUN)
                author_twitter_id = await upsert_author_and_get_id(supabase_client, author_data, existing_authors_map)
                
                # Passo 2: Preparar e salvar (ou exibir) os dados do tweet
                is_success = await prepare_and_save_tweet_data(supabase_client, tweet_data, author_twitter_id)
                

            except httpx.HTTPStatusError as e:
                logger.error(f"  -> ERRO HTTP ao buscar tweet {tweet_id}: {e.response.status_code} - {e.response.text}")
            except Exception as e:
                logger.error(f"  -> ERRO inesperado ao processar tweet {tweet_id}: {e}")
            finally:
                if is_success:
                    success_count += 1
                else:
                    failure_count += 1

    # --- 5. Relatório Final ---
    logger.info("\n" + "="*50)
    logger.info("--- RELATÓRIO FINAL ---")
    logger.info(f"Modo de Execução: {'SIMULAÇÃO (DRY RUN)' if DRY_RUN else 'PRODUÇÃO'}")
    logger.info(f"Total de tweets para processar: {total_to_process}")
    logger.info(f"Tweets processados com sucesso: {success_count}")
    logger.info(f"Falhas ou tweets não processados: {failure_count}")
    logger.info("="*50 + "\n")


async def main():
    load_dotenv()
    api_key = os.getenv("TWITTER_API_KEY")
    if not api_key:
        logger.critical("A variável de ambiente TWITTER_API_KEY não está definida.")
        return

    supabase = await get_supabase_client()
    if not supabase:
        return

    await populate_missing_tweets(supabase, api_key)

if __name__ == "__main__":
    asyncio.run(main()) 