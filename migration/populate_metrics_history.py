import csv
import os
import logging
from dotenv import load_dotenv
from supabase import create_client, Client

# --- Setup ---
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("population_metrics.log"), logging.StreamHandler()])

# --- Environment and Supabase Client ---
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")

if not supabase_url or not supabase_key:
    logging.error("Supabase URL ou Chave não encontrados. Certifique-se de configurá-los no seu arquivo .env.")
    exit(1)

try:
    supabase: Client = create_client(supabase_url, supabase_key)
    logging.info("Conectado com sucesso ao Supabase.")
except Exception as e:
    logging.error(f"Falha ao conectar ao Supabase: {e}")
    exit(1)

# --- Configuration ---
CSV_FILE_PATH = 'tweet_metrics_history_rows.csv'
TABLE_NAME = 'tweet_metrics_history'
BATCH_SIZE = 500

def populate_metrics_history():
    """
    Lê as métricas de tweets de um arquivo CSV e as insere na tabela 'tweet_metrics_history' do Supabase.
    Processa o arquivo em lotes para lidar com grandes volumes de dados de forma eficiente.
    """
    logging.info(f"Iniciando a população da tabela '{TABLE_NAME}' a partir de '{CSV_FILE_PATH}'.")
    
    try:
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            batch = []
            total_rows = 0
            successful_inserts = 0
            failed_inserts = 0

            header = reader.fieldnames
            logging.info(f"Cabeçalhos do CSV encontrados: {header}")

            # Colunas que esperamos que sejam inteiros
            int_columns = ['views', 'likes', 'retweets', 'replies', 'quotes', 'bookmarks']

            for row in reader:
                total_rows += 1
                
                # Prepara os dados para inserção
                insert_data = {
                    'tweet_id': row.get('tweet_id'),
                    'snapshot_at': row.get('snapshot_at')
                }

                # Converte com segurança as colunas de métricas para inteiros
                for col in int_columns:
                    try:
                        value = row.get(col)
                        insert_data[col] = int(float(value)) if value not in [None, ''] else None
                    except (ValueError, TypeError):
                        logging.warning(f"Não foi possível converter o valor '{row.get(col)}' para int na linha {total_rows} para a coluna '{col}'. Definindo como NULL.")
                        insert_data[col] = None

                # O CSV pode ter uma coluna 'id' de um dump anterior do banco de dados. Não precisamos dela.
                if 'id' in row:
                    pass # não adicionamos ao insert_data
                    
                batch.append(insert_data)

                if len(batch) >= BATCH_SIZE:
                    try:
                        # Use upsert para lidar com conflitos potenciais na chave primária 'id'
                        # A ação padrão do upsert é ignorar se o registro já existe (baseado na PK)
                        response = supabase.table(TABLE_NAME).upsert(batch).execute()
                        
                        # A API do Supabase pode não retornar dados em sucesso de inserção em lote,
                        # ou pode retornar um subconjunto. A ausência de exceção é o principal indicador.
                        successful_inserts += len(batch) # Assumimos sucesso se nenhuma exceção for lançada
                        logging.info(f"Lote inserido com sucesso. Total de linhas processadas até agora: {total_rows}")

                    except Exception as e:
                        logging.error(f"Erro ao inserir o lote: {e}")
                        failed_inserts += len(batch)
                    finally:
                        batch = []

            # Insere as linhas restantes
            if batch:
                try:
                    # Usa upsert também para o lote final
                    response = supabase.table(TABLE_NAME).upsert(batch).execute()
                    successful_inserts += len(batch)
                except Exception as e:
                    logging.error(f"Erro ao inserir o lote final: {e}")
                    failed_inserts += len(batch)
            
            logging.info("--- Resumo da População ---")
            logging.info(f"Total de linhas lidas do CSV: {total_rows}")
            logging.info(f"Linhas inseridas com sucesso (estimativa): {successful_inserts}")
            logging.info(f"Falha ao inserir linhas (estimativa): {failed_inserts}")
            logging.info("--------------------------")

    except FileNotFoundError:
        logging.error(f"Arquivo CSV não encontrado em '{CSV_FILE_PATH}'.")
    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado: {e}")

if __name__ == "__main__":
    populate_metrics_history() 