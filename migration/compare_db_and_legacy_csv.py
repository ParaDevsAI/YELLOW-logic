import asyncio
import os
import pandas as pd
from dotenv import load_dotenv
import logging

from bot.author_manager import get_supabase_client

# Configuração de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def compare_data():
    """
    Compara os tweets no Supabase com um CSV legado e imprime um relatório.
    """
    load_dotenv()
    
    supabase = await get_supabase_client()
    if not supabase:
        logger.critical("Falha ao conectar com o Supabase. Verifique as credenciais.")
        return

    try:
        # --- 1. Obter TODOS os dados do Supabase com Paginação ---
        logger.info("Buscando todos os tweet_ids do banco de dados Supabase (com paginação)...")
        
        # Primeiro, pegamos a contagem total
        count_response = await asyncio.to_thread(
            supabase.table('tweets').select('tweet_id', count='exact').limit(0).execute
        )
        total_count = count_response.count
        logger.info(f"Contagem total de tweets no Supabase: {total_count}")

        all_tweets_data = []
        page_size = 1000  # O limite padrão/máximo da API
        for i in range(0, total_count, page_size):
            logger.info(f"Buscando página de tweets: {i} a {i + page_size - 1}")
            page_response = await asyncio.to_thread(
                supabase.table('tweets').select('tweet_id').range(i, i + page_size - 1).execute
            )
            if page_response.data:
                all_tweets_data.extend(page_response.data)
        
        df_supabase = pd.DataFrame(all_tweets_data)
        logger.info(f"Total de tweets efetivamente baixados do Supabase: {len(df_supabase)}")

        # --- 2. Obter dados do CSV Legado ---
        legacy_csv_path = 'tweets_rows.csv'
        logger.info(f"Lendo o arquivo CSV legado: '{legacy_csv_path}'...")
        
        try:
            df_legacy = pd.read_csv(legacy_csv_path, dtype={'tweet_id': str})
            # Remover linhas onde tweet_id é nulo ou vazio
            df_legacy.dropna(subset=['tweet_id'], inplace=True)
            logger.info(f"Encontrados {len(df_legacy)} tweets no CSV legado.")
        except FileNotFoundError:
            logger.error(f"ERRO: O arquivo CSV legado '{legacy_csv_path}' não foi encontrado.")
            return

        # --- 3. Comparar os conjuntos de IDs ---
        logger.info("Comparando os conjuntos de IDs...")
        
        supabase_ids_set = set(df_supabase['tweet_id'].astype(str))
        legacy_ids_set = set(df_legacy['tweet_id'].astype(str))

        common_ids = supabase_ids_set.intersection(legacy_ids_set)
        missing_in_supabase = legacy_ids_set - supabase_ids_set
        new_in_supabase = supabase_ids_set - legacy_ids_set

        # --- 4. Salvar resultados em arquivos ---
        logger.info("Salvando listas de IDs em arquivos de texto...")
        with open('missing_from_db.txt', 'w') as f:
            for tweet_id in sorted(list(missing_in_supabase)):
                f.write(f"{tweet_id}\n")
        
        with open('new_in_db.txt', 'w') as f:
            for tweet_id in sorted(list(new_in_supabase)):
                f.write(f"{tweet_id}\n")

        # --- 5. Imprimir relatório ---
        print("\n" + "="*50)
        print("--- RELATÓRIO DE COMPARAÇÃO FINAL ---")
        print("="*50)
        print(f"Total de Tweets no Supabase: {len(supabase_ids_set)}")
        print(f"Total de Tweets no CSV Legado ('tweets_rows.csv'): {len(legacy_ids_set)}")
        print(f"Tweets em Comum: {len(common_ids)}")
        print("-" * 50)
        print(f"Tweets Faltando no Supabase (presentes no CSV): {len(missing_in_supabase)}")
        print("   (IDs salvos em 'missing_from_db.txt')")
        print(f"Tweets Novos no Supabase (não estão no CSV): {len(new_in_supabase)}")
        print("   (IDs salvos em 'new_in_db.txt')")
        print("="*50)

    except Exception as e:
        logger.critical(f"Ocorreu um erro inesperado durante a comparação: {e}")
    finally:
        logger.info("Processo de comparação concluído.")

async def main():
    await compare_data()

if __name__ == "__main__":
    asyncio.run(main()) 