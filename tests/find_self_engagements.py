import asyncio
import os
import logging
from dotenv import load_dotenv

from bot.author_manager import get_supabase_client

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

async def find_and_report_self_engagements():
    """
    Script de diagnóstico para encontrar e contar registros de auto-engajamento
    na tabela 'ambassador_engagements'.
    Este script é somente leitura.
    """
    logger.info("--- Iniciando verificação de auto-engajamentos na base de dados ---")
    
    supabase = await get_supabase_client()
    if not supabase:
        logger.critical("Falha na conexão com o Supabase. Abortando.")
        return

    # A API do Supabase não suporta filtros 'coluna A = coluna B' diretamente.
    # Precisamos usar uma função RPC para esta lógica específica.
    rpc_function_name = 'find_self_engagements_rpc'
    
    # SQL para criar a função RPC que o script precisa
    sql_to_create_function = f"""
CREATE OR REPLACE FUNCTION {rpc_function_name}()
RETURNS TABLE (
    id BIGINT,
    tweet_id TEXT,
    tweet_author_id TEXT,
    interacting_user_id TEXT,
    action_type TEXT,
    created_at TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        ae.id,
        ae.tweet_id,
        ae.tweet_author_id,
        ae.interacting_user_id,
        ae.action_type,
        ae.created_at
    FROM
        ambassador_engagements AS ae
    WHERE
        ae.tweet_author_id = ae.interacting_user_id;
END;
$$ LANGUAGE plpgsql;
    """

    logger.info("Este script precisa de uma função auxiliar no banco de dados para funcionar.")
    print("-" * 80)
    print("Por favor, execute o seguinte código no seu Supabase SQL Editor uma vez, e depois rode este script novamente:")
    print(sql_to_create_function)
    print("-" * 80)

    try:
        response = await asyncio.to_thread(
            supabase.rpc(rpc_function_name).execute
        )
        self_engagements = response.data if response.data else []
    except Exception as e:
        logger.error(f"ERRO: Não foi possível executar a busca. Verifique se a função '{rpc_function_name}' foi criada no Supabase SQL Editor como instruído acima. Detalhes do erro: {e}")
        return

    # Apresentar o relatório
    print("\n" + "="*80)
    print("        Relatório de Diagnóstico de Auto-Engajamentos")
    print("="*80)
    
    total_found = len(self_engagements)
    
    if total_found == 0:
        print("\n[ SUCESSO ]")
        print("      - Nenhum registro de auto-engajamento foi encontrado na base de dados.")
    else:
        print(f"\n[ ALERTA ] Foram encontrados {total_found} registros de auto-engajamento.")
        print("      - Estes são os registros onde um usuário interagiu com o próprio tweet.")
        print("\n      - Amostra de até 5 registros encontrados:")
        for eng in self_engagements[:5]:
            print(f"        - ID do Registro: {eng['id']}, Usuário: {eng['interacting_user_id']}, Tweet: {eng['tweet_id']}, Ação: {eng['action_type']}")

    print("\n" + "="*80)
    logger.info("Análise concluída. Este script não removeu nenhum dado.")


async def main():
    load_dotenv()
    await find_and_report_self_engagements()

if __name__ == "__main__":
    asyncio.run(main()) 