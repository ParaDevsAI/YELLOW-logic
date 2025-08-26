import asyncio
import os
import pandas as pd
from dotenv import load_dotenv
import logging
import re
import numpy as np

from author_manager import get_supabase_client

# --- CONFIGURAÇÃO ---
DRY_RUN = False  # True: Apenas exibe o que seria feito. False: Executa as operações no DB.

# --- Configuração de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("population_legacy.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Funções de Apoio ---
def extract_username_from_url(text: str) -> str | None:
    if not isinstance(text, str):
        return None
    # Padrão para URLs como https://x.com/username/status/... ou https://twitter.com/username/status/...
    match = re.search(r'(?:twitter|x)\.com/([^/]+)/status', text)
    return match.group(1) if match else None

async def populate_from_legacy_csv(supabase_client):
    if DRY_RUN:
        logger.warning("--- EXECUTANDO EM MODO DE SIMULAÇÃO (DRY_RUN = True) ---")
        logger.warning("Nenhuma alteração será feita no banco de dados.")
    else:
        logger.info("--- Iniciando o processo de população a partir do CSV Legado ---")

    # --- 1. Ler os IDs dos tweets faltantes ---
    try:
        with open('missing_from_db.txt', 'r') as f:
            missing_tweet_ids = {line.strip() for line in f if line.strip()}
        logger.info(f"Encontrados {len(missing_tweet_ids)} IDs no arquivo 'missing_from_db.txt'.")
    except FileNotFoundError:
        logger.error("ERRO CRÍTICO: O arquivo 'missing_from_db.txt' não foi encontrado.")
        return

    # --- 2. Ler o CSV legado completo ---
    try:
        df_legacy = pd.read_csv('tweets_rows.csv', dtype={'tweet_id': str, 'author_id': str})
        df_legacy.replace({np.nan: None}, inplace=True) # Substitui NaN por None para compatibilidade com JSON
        logger.info(f"Lido com sucesso o arquivo 'tweets_rows.csv' com {len(df_legacy)} linhas.")
    except FileNotFoundError:
        logger.error("ERRO CRÍTICO: O arquivo 'tweets_rows.csv' não foi encontrado.")
        return
    
    # --- 3. Filtrar apenas os tweets que estão faltando ---
    df_missing = df_legacy[df_legacy['tweet_id'].isin(missing_tweet_ids)].copy()
    logger.info(f"Encontradas {len(df_missing)} linhas correspondentes aos IDs faltantes no CSV legado.")

    # --- 4. Buscar autores existentes do Supabase ---
    try:
        authors_response = await asyncio.to_thread(supabase_client.table('authors').select('twitter_id').execute)
        existing_author_ids = {str(author['twitter_id']) for author in authors_response.data}
        logger.info(f"Encontrados {len(existing_author_ids)} autores existentes no Supabase.")
    except Exception as e:
        logger.critical(f"Falha ao buscar autores do Supabase: {e}")
        return

    # --- 5. Processar cada tweet e autor faltante ---
    new_authors_to_create = []
    tweets_to_insert = []
    
    df_missing['parsed_username'] = df_missing['twitter_url'].apply(extract_username_from_url)

    # Identificar novos autores
    unique_authors_in_missing = df_missing[['author_id', 'parsed_username']].drop_duplicates()
    for index, author in unique_authors_in_missing.iterrows():
        author_id = author['author_id']
        username = author['parsed_username']
        if author_id not in existing_author_ids:
            author_payload = {
                'twitter_id': author_id,
                'twitter_username': username
            }
            new_authors_to_create.append(author_payload)

    # Preparar tweets para inserção
    TWEET_TABLE_COLUMNS = [
        'tweet_id', 'author_id', 'twitter_url', 'text', 'createdat', 
        'views', 'likes', 'retweets', 'replies', 'quotes', 'bookmarks', 
        'content_type', 'media_url'
    ]
    for index, row in df_missing.iterrows():
        # Filtra o dicionário da linha para conter apenas as colunas que existem na tabela de tweets
        tweet_payload = {key: value for key, value in row.to_dict().items() if key in TWEET_TABLE_COLUMNS}
        tweets_to_insert.append(tweet_payload)

    # --- 6. Exibir a Pré-População (Dry Run) ---
    print("\n" + "="*60)
    print("--- PRÉ-POPULAÇÃO (DRY RUN) ---")
    print("="*60)

    if not new_authors_to_create and not tweets_to_insert:
        print("\nNenhuma ação necessária. Todos os autores e tweets parecem estar no lugar.")
        return
        
    if new_authors_to_create:
        print(f"\nAutores a serem CRIADOS: {len(new_authors_to_create)}")
        for author in new_authors_to_create:
            print(f"  - [NOVO AUTOR] {author}")
    else:
        print("\nNenhum autor novo a ser criado.")

    if tweets_to_insert:
        print(f"\nTweets a serem INSERIDOS: {len(tweets_to_insert)}")
        for tweet in tweets_to_insert:
            print(f"  - [NOVO TWEET] ID: {tweet.get('tweet_id')}, Autor ID: {tweet.get('author_id')}")
            # print(f"    Payload: {tweet}") # Descomente para ver o payload completo
    else:
        print("\nNenhum tweet novo a ser inserido.")
    
    print("\n" + "="*60)
    
    if DRY_RUN:
        logger.info("Fim da simulação. Para executar, mude DRY_RUN para False.")
        return

    # --- 7. Executar Inserções (se DRY_RUN for False) ---
    try:
        if new_authors_to_create:
            logger.info(f"Inserindo {len(new_authors_to_create)} novos autores...")
            await asyncio.to_thread(
                supabase_client.table('authors').insert(new_authors_to_create).execute
            )
            logger.info("Novos autores inseridos com sucesso.")

        if tweets_to_insert:
            logger.info(f"Inserindo {len(tweets_to_insert)} novos tweets...")
            # Usar 'upsert' para evitar falhas caso algum tweet já exista por alguma razão
            await asyncio.to_thread(
                supabase_client.table('tweets').upsert(tweets_to_insert).execute
            )
            logger.info("Novos tweets inseridos com sucesso.")

        logger.info("--- Processo de população a partir do CSV Legado concluído com sucesso! ---")

    except Exception as e:
        logger.critical(f"Ocorreu um erro durante a inserção no banco de dados: {e}")


async def main():
    load_dotenv()
    supabase = await get_supabase_client()
    if not supabase:
        return
    await populate_from_legacy_csv(supabase)

if __name__ == "__main__":
    asyncio.run(main()) 