import asyncio
import logging
from pathlib import Path
import sys
from datetime import datetime

# --- Configuração de Caminho e Logging ---
# Garante que o script possa encontrar o 'core.database_client'
try:
    project_root = Path(__file__).resolve().parent
    sys.path.append(str(project_root))
    from core.database_client import get_db_client
except ImportError:
    # Fallback para o caso de a estrutura de pastas ter mudado
    project_root = Path.cwd()
    sys.path.append(str(project_root.parent))
    from YELLOW_PROJECT_FINAL.core.database_client import get_db_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def inspect_leaderboard():
    """
    Conecta-se ao Supabase, busca o snapshot mais recente do leaderboard,
    e exibe os resultados ordenados por pontuação para análise.
    """
    logging.info("Iniciando a inspeção do leaderboard atual...")
    
    try:
        db_client = get_db_client()
        logging.info("Cliente Supabase obtido com sucesso.")

        # 1. Encontrar a data do snapshot mais recente
        logging.info("Buscando a data do snapshot mais recente na tabela 'leaderboard_history'...")
        latest_snapshot_response = await asyncio.to_thread(
            db_client.table('leaderboard_history')
            .select('snapshot_timestamp')
            .order('snapshot_timestamp', desc=True)
            .limit(1)
            .execute
        )

        if not latest_snapshot_response.data:
            logging.warning("Nenhum snapshot encontrado na tabela 'leaderboard_history'. Não há o que analisar.")
            return

        latest_snapshot_date = latest_snapshot_response.data[0]['snapshot_timestamp']
        # Converte para um objeto de data para a consulta
        latest_date_obj = datetime.fromisoformat(latest_snapshot_date).date()
        
        logging.info(f"Snapshot mais recente encontrado em: {latest_date_obj.isoformat()}")

        # 2. Buscar todos os dados do leaderboard para essa data
        logging.info(f"Buscando todos os registros do leaderboard para a data {latest_date_obj.isoformat()}...")
        leaderboard_data_response = await asyncio.to_thread(
            db_client.table('leaderboard_history')
            .select('rank, telegram_name, grand_total_score')
            .eq('snapshot_timestamp', latest_date_obj.isoformat())
            .order('grand_total_score', desc=True)
            .execute
        )

        if not leaderboard_data_response.data:
            logging.error(f"Inconsistência de dados: Nenhum registro encontrado para a data do snapshot mais recente ({latest_date_obj.isoformat()}).")
            return

        leaderboard_data = leaderboard_data_response.data
        logging.info(f"Encontrados {len(leaderboard_data)} registros de usuários para análise.")

        # 3. Exibir os resultados de forma formatada
        print("\\n" + "="*60)
        print("          LEADERBOARD ATUAL - DADOS DO SUPABASE")
        print("="*60)
        print(f"{'Rank':<5} | {'Nome (Telegram)':<35} | {'Pontuação Total':>15}")
        print("-"*60)
        
        for record in leaderboard_data:
            rank = record.get('rank', 'N/A')
            name = record.get('telegram_name', 'N/A')
            score = record.get('grand_total_score', 0)
            print(f"{rank:<5} | {name:<35} | {score:>15.2f}")
            
        print("="*60)
        print("\\n")
        
        return leaderboard_data

    except Exception as e:
        logging.error(f"Ocorreu um erro durante a inspeção: {e}", exc_info=True)
        return None

async def main():
    from dotenv import load_dotenv
    load_dotenv()
    await inspect_leaderboard()

if __name__ == '__main__':
    asyncio.run(main()) 