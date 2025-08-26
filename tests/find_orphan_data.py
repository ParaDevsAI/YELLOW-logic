import asyncio
import os
import logging
from dotenv import load_dotenv

from bot.author_manager import get_supabase_client

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

async def find_orphan_engagements():
    """
    Verifica a tabela 'ambassador_engagements' em busca de registros "órfãos",
    ou seja, engajamentos cujo 'interacting_user_id' não corresponde a nenhum
    'twitter_id' na tabela 'authors'.
    """
    logger.info("--- Iniciando verificação de engajamentos órfãos ---")
    
    supabase = await get_supabase_client()
    if not supabase:
        logger.critical("Falha na conexão com o Supabase. Abortando.")
        return

    # 1. Obter todos os twitter_ids válidos da tabela 'authors'
    try:
        authors_response = await asyncio.to_thread(
            supabase.table('authors').select('twitter_id').execute
        )
        if not authors_response.data:
            logger.error("Nenhum autor encontrado na tabela 'authors'. Impossível verificar órfãos.")
            return
        valid_author_ids = {author['twitter_id'] for author in authors_response.data}
        logger.info(f"Encontrados {len(valid_author_ids)} autores válidos.")
    except Exception as e:
        logger.error(f"Erro ao buscar autores válidos: {e}")
        return

    # 2. Obter todos os 'interacting_user_id' distintos da tabela de engajamentos
    try:
        # A função rpc é uma forma de executar consultas mais complexas que a API padrão não suporta
        engagers_response = await asyncio.to_thread(
            supabase.rpc('get_distinct_engagers').execute
        )
        if not engagers_response.data:
            logger.warning("Nenhum engajamento encontrado para verificar.")
            return
        engager_ids = {item['interacting_user_id'] for item in engagers_response.data}
        logger.info(f"Encontrados {len(engager_ids)} usuários únicos que realizaram engajamentos.")
    except Exception as e:
        logger.error(f"Erro ao buscar usuários de engajamento. Você precisa criar a função 'get_distinct_engagers' primeiro. Detalhes: {e}")
        print("\nPOR FAVOR, execute o seguinte comando no seu Supabase SQL Editor e tente novamente:")
        print("CREATE OR REPLACE FUNCTION get_distinct_engagers() RETURNS TABLE(interacting_user_id TEXT) AS $$ BEGIN RETURN QUERY SELECT DISTINCT ae.interacting_user_id FROM ambassador_engagements ae; END; $$ LANGUAGE plpgsql;")
        return

    # 3. Comparar as duas listas para encontrar os órfãos
    orphan_ids = engager_ids - valid_author_ids

    # 4. Apresentar o relatório
    print("\n" + "="*80)
    print("        Relatório de Diagnóstico de Engajamentos Órfãos")
    print("="*80)
    
    if not orphan_ids:
        print("\n[ SUCESSO ]")
        print("      - Nenhuma inconsistência encontrada.")
        print("      - Todos os usuários que fizeram engajamentos estão devidamente registrados na tabela 'authors'.")
    else:
        print("\n[ ALERTA ] Foram encontrados engajamentos órfãos!")
        print(f"      - {len(orphan_ids)} ID(s) de usuário existem na tabela de engajamentos, mas não na tabela de autores.")
        print("      - Estes registros estão 'quebrados' e precisam ser removidos para que o leaderboard funcione.")
        print("\n      - IDs Órfãos Encontrados:")
        for orphan_id in orphan_ids:
            print(f"        - {orphan_id}")

    print("\n" + "="*80)

async def main():
    load_dotenv()
    await find_orphan_engagements()

if __name__ == "__main__":
    asyncio.run(main()) 