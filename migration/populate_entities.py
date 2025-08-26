import asyncio
import os
import pandas as pd
from dotenv import load_dotenv
import logging
import numpy as np

from bot.author_manager import get_supabase_client

# --- Configuração de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("population_entities.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Colunas que existem na tabela 'tweet_entities' do Supabase, com base no schema.sql
# A coluna 'id' é autogerada, então não a incluímos.
VALID_COLUMNS = [
    'tweet_id', 
    'entity_type', 
    'text_in_tweet', 
    'mentioned_user_id', 
    'expanded_url'
]

async def populate_entities_from_csv(supabase_client):
    logger.info("--- Iniciando o processo de população da tabela 'tweet_entities' ---")
    
    # --- 1. Ler o arquivo CSV ---
    csv_path = 'tweet_entities_rows.csv'
    try:
        df = pd.read_csv(csv_path, dtype={'tweet_id': str, 'mentioned_user_id': str})
        df.replace({np.nan: None}, inplace=True) # Substitui NaN por None para o Supabase
        logger.info(f"Lido com sucesso o arquivo '{csv_path}' com {len(df)} linhas.")
    except FileNotFoundError:
        logger.error(f"ERRO CRÍTICO: O arquivo '{csv_path}' não foi encontrado.")
        return
    except Exception as e:
        logger.error(f"ERRO ao ler o arquivo CSV: {e}")
        return

    # --- 2. Remover duplicatas antes de processar ---
    original_rows = len(df)
    # A constraint UNIQUE no banco é (tweet_id, entity_type, text_in_tweet)
    df.drop_duplicates(subset=['tweet_id', 'entity_type', 'text_in_tweet'], inplace=True)
    if len(df) < original_rows:
        logger.info(f"Removidas {original_rows - len(df)} linhas duplicadas do CSV em memória.")

    # --- 3. Preparar dados para inserção ---
    # Filtrar o DataFrame para conter apenas as colunas válidas
    df_filtered = df[[col for col in VALID_COLUMNS if col in df.columns]].copy()
    
    # Validar se temos as colunas essenciais
    if 'tweet_id' not in df_filtered.columns or 'entity_type' not in df_filtered.columns:
        logger.error("ERRO CRÍTICO: O CSV precisa conter no mínimo as colunas 'tweet_id' e 'entity_type'.")
        return

    # Converter o DataFrame para uma lista de dicionários
    entities_to_insert = df_filtered.to_dict(orient='records')
    
    if not entities_to_insert:
        logger.info("Nenhuma entidade válida encontrada no arquivo CSV para inserir.")
        return

    # Obter lista de tweet_ids únicos para a limpeza
    unique_tweet_ids = list(df_filtered['tweet_id'].unique())
    logger.info(f"Serão processadas {len(entities_to_insert)} entidades para {len(unique_tweet_ids)} tweets únicos.")

    # --- 4. Executar operações no banco de dados ---
    try:
        # Passo 4.1: Limpeza Preventiva - Deletar entidades existentes para estes tweets
        logger.info(f"Limpando registros antigos de 'tweet_entities' para {len(unique_tweet_ids)} tweets...")
        await asyncio.to_thread(
            supabase_client.table('tweet_entities').delete().in_('tweet_id', unique_tweet_ids).execute
        )
        logger.info("Limpeza concluída com sucesso.")

        # Passo 4.2: Inserção em Massa
        logger.info(f"Inserindo {len(entities_to_insert)} novas entidades...")
        await asyncio.to_thread(
            supabase_client.table('tweet_entities').insert(entities_to_insert).execute
        )
        logger.info("--- Processo de população de entidades concluído com sucesso! ---")

    except Exception as e:
        logger.critical(f"Ocorreu um erro durante a operação no banco de dados: {e}")


async def main():
    load_dotenv()
    supabase = await get_supabase_client()
    if not supabase:
        return
    await populate_entities_from_csv(supabase)

if __name__ == "__main__":
    asyncio.run(main()) 