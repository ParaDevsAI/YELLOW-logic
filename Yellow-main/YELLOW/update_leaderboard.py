import os
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
from postgrest import APIResponse

# --- Setup ---
load_dotenv()

# Configuração do logging para registrar informações e erros
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("leaderboard_update.log"), logging.StreamHandler()])

# --- Conexão com o Supabase ---
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")

if not supabase_url or not supabase_key:
    logging.error("As variáveis de ambiente SUPABASE_URL e SUPABASE_KEY não foram encontradas.")
    exit(1)

try:
    supabase: Client = create_client(supabase_url, supabase_key)
    logging.info("Conexão com o Supabase estabelecida com sucesso.")
except Exception as e:
    logging.error(f"Falha ao conectar ao Supabase: {e}")
    exit(1)

def update_leaderboard_table():
    """
    Atualiza a tabela 'leaderboard' com os dados mais recentes da 'leaderboard_history'.
    
    Esta função executa as seguintes etapas:
    1. Encontra a data do snapshot mais recente na tabela 'leaderboard_history'.
    2. Busca todos os registros correspondentes a essa data.
    3. Limpa a tabela 'leaderboard' para garantir que não haja dados antigos.
    4. Insere os registros mais recentes na tabela 'leaderboard'.
    5. Invoca a função RPC 'update_leaderboard_ranks' para calcular os ranks.
    """
    logging.info("Iniciando o processo de atualização da tabela 'leaderboard'.")

    try:
        # 1. Encontrar a data do snapshot mais recente
        logging.info("Buscando a data do snapshot mais recente da tabela 'leaderboard_history'...")
        response = supabase.table('leaderboard_history').select('snapshot_timestamp').order('snapshot_timestamp', desc=True).limit(1).execute()
        
        if not response.data:
            logging.warning("Nenhum dado encontrado na tabela 'leaderboard_history'. Abortando a atualização.")
            return

        latest_snapshot_date = response.data[0]['snapshot_timestamp']
        logging.info(f"Snapshot mais recente encontrado: {latest_snapshot_date}")

        # 2. Buscar todos os registros dessa data
        logging.info(f"Buscando todos os registros do snapshot de {latest_snapshot_date}...")
        history_response = supabase.table('leaderboard_history').select('*').eq('snapshot_timestamp', latest_snapshot_date).execute()

        if not history_response.data:
            logging.warning(f"Nenhum registro encontrado para o snapshot {latest_snapshot_date}. Abortando.")
            return

        latest_records = history_response.data
        logging.info(f"{len(latest_records)} registros encontrados para o snapshot mais recente.")

        # 3. Preparar os dados para inserção na tabela 'leaderboard'
        leaderboard_data = []
        for record in latest_records:
            # Remove chaves que não existem na tabela 'leaderboard'
            record.pop('id', None)
            
            # Renomeia 'snapshot_timestamp' para 'last_updated'
            record['last_updated'] = record.pop('snapshot_timestamp')
            
            # Adiciona o registro à lista de dados a serem inseridos
            leaderboard_data.append(record)

        # 4. Limpar a tabela 'leaderboard'
        logging.info("Limpando a tabela 'leaderboard' para receber os novos dados...")
        # Usamos delete() pois TRUNCATE geralmente não é permitido via API por segurança
        supabase.table('leaderboard').delete().neq('user_id', -1).execute() # Deleta todos os registros
        logging.info("Tabela 'leaderboard' limpa com sucesso.")

        # 5. Inserir os novos dados
        logging.info(f"Inserindo {len(leaderboard_data)} registros na tabela 'leaderboard'...")
        insert_response = supabase.table('leaderboard').insert(leaderboard_data).execute()

        # Verifica se houve algum erro na inserção
        if isinstance(insert_response, APIResponse) and (insert_response.data is None or len(insert_response.data) == 0):
             if hasattr(insert_response, 'error') and insert_response.error:
                 raise Exception(f"Erro na API do Supabase ao inserir dados: {insert_response.error}")


        logging.info("Dados inseridos com sucesso na tabela 'leaderboard'.")

        # 6. Chamar a RPC para atualizar os ranks
        logging.info("Invocando a função RPC 'update_leaderboard_ranks' para calcular os ranks...")
        supabase.rpc('update_leaderboard_ranks').execute()
        logging.info("Ranks atualizados com sucesso.")

    except Exception as e:
        logging.error(f"Ocorreu um erro durante a atualização do leaderboard: {e}", exc_info=True)
        return

    logging.info("Processo de atualização do leaderboard concluído com sucesso!")

if __name__ == "__main__":
    update_leaderboard_table() 