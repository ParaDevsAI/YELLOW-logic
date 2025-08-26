"""
generate_leaderboard.py (Supabase Refactor)

This script calculates the comprehensive scores for all ambassadors and
populates both the live and historical leaderboards by leveraging
functions (RPCs) directly within the Supabase database.

Execution Flow:
1.  Connects to Supabase.
2.  Calls the `calculate_leaderboard` RPC function. This function performs
    all the complex calculations on the database server.
3.  Receives the calculated data from the RPC.
4.  Processes the data in Python to prepare it for insertion:
    a) Adds a `snapshot_timestamp` for the history table.
    b) Adds a `last_updated` timestamp for the live table.
    c) Maps the columns from the RPC result to the table columns.
5.  Saves the data in batches:
    a) Inserts into `leaderboard_history`.
    b) Upserts into `leaderboard`.
6.  Calls two more RPC functions (`update_leaderboard_ranks` and 
    `update_leaderboard_history_ranks`) to calculate and set the final
    rank for each user in both tables.
"""
import asyncio
import logging
import sys
from pathlib import Path

# Adiciona o diretório raiz do projeto ao sys.path para importações corretas
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

# Importando o cliente Supabase centralizado
from core.database_client import get_db_client

# Configuração de Logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

async def run_sql_from_file(file_path: Path):
    """
    Lê um arquivo .sql e o executa no Supabase usando a função RPC 'execute_sql'.
    """
    logger = logging.getLogger(__name__)
    
    if not file_path.exists():
        logger.error(f"ERRO CRÍTICO: Arquivo SQL não encontrado em '{file_path}'")
        sys.exit(1)

    with open(file_path, 'r', encoding='utf-8') as f:
        sql_query = f.read()

    if not sql_query.strip():
        logger.warning(f"O arquivo SQL '{file_path.name}' está vazio. Nada a fazer.")
        return

    logger.info(f"Conectando ao banco de dados para executar o script '{file_path.name}'...")
    db_client = get_db_client()
    
    try:
        # Divide o script em comandos individuais (ignorando linhas vazias)
        # O TRUNCATE e o INSERT são executados em transações separadas.
        sql_commands = [cmd.strip() for cmd in sql_query.split(';') if cmd.strip()]
        
        logger.info(f"Encontrados {len(sql_commands)} comandos no arquivo SQL.")

        for i, command in enumerate(sql_commands):
            logger.info(f"Executando comando {i+1}/{len(sql_commands)}...")
            # A função 'execute_sql' deve ter sido criada no Supabase SQL Editor.
            # create or replace function execute_sql(sql text) returns void as $$ begin execute sql; end; $$ language plpgsql;
            await asyncio.to_thread(
                db_client.rpc('execute_sql', {'sql': command}).execute
            )
            logger.info(f"Comando {i+1} concluído com sucesso.")

        logger.info(f"Script '{file_path.name}' executado com sucesso.")

    except Exception as e:
        logger.error(f"Ocorreu um erro ao executar o script SQL '{file_path.name}': {e}", exc_info=True)
        sys.exit(1)


async def main():
    """
    Função principal que orquestra a geração do leaderboard.
    """
    logger = logging.getLogger(__name__)
    logger.info("--- Iniciando a Geração Completa do Histórico do Leaderboard ---")

    # O caminho é relativo à localização deste script.
    sql_file = project_root / 'generate_retroactive_leaderboard.sql'
    await run_sql_from_file(sql_file)
    
    logger.info("--- Geração do Leaderboard Concluída ---")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    # Para garantir que o cliente Supabase funcione quando executado diretamente
    # Precisamos do `core.database_client`, então a importação foi ajustada no topo.
    asyncio.run(main()) 