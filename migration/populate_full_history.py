import asyncio
import os
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

# Importando o cliente Supabase e a inicialização síncrona para o script
from bot.author_manager import get_supabase_client

# --- Configuração de Logging ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

async def populate_full_leaderboard_history():
    """
    Popula a tabela leaderboard_history com dados retroativos, dia a dia.
    USA AS REGRAS DE PONTUAÇÃO ATUAIS.
    """
    supabase = await get_supabase_client()
    if not supabase:
        logger.critical("Não foi possível conectar ao Supabase. Abortando.")
        return

    # --- AVISO DE SEGURANÇA ---
    print("\n" + "="*80)
    print("ATENÇÃO: Este script irá APAGAR TODOS OS DADOS da tabela 'leaderboard_history'")
    print("e irá repopulá-la do zero com dados retroativos.")
    print("Este processo pode levar vários minutos, dependendo do volume de dados.")
    print("="*80)
    
    proceed = input("Você tem certeza que quer continuar? (s/n): ")
    if proceed.lower() != 's':
        logger.info("Operação cancelada pelo usuário.")
        return

    # Passo 1: Limpar a tabela de histórico existente
    logger.info("Limpando a tabela 'leaderboard_history'...")
    try:
        # A maneira mais segura de apagar tudo é usar um filtro que corresponda a tudo.
        await asyncio.to_thread(
            supabase.table('leaderboard_history').delete().neq('id', -1).execute
        )
        logger.info("Tabela 'leaderboard_history' limpa com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao limpar a tabela 'leaderboard_history': {e}")
        return

    # Passo 2: Determinar o período da simulação
    logger.info("Encontrando a data da primeira interação para iniciar o histórico...")
    try:
        response = await asyncio.to_thread(
            supabase.table('tweets') # Usamos a tabela de tweets para a data mais antiga
            .select('createdat')
            .order('createdat', desc=False)
            .limit(1)
            .single()
            .execute
        )
        first_date = datetime.fromisoformat(response.data['createdat']).date()
        today = datetime.now(timezone.utc).date()
    except Exception as e:
        logger.error(f"Não foi possível determinar a data de início. Verifique se existem tweets na base. Erro: {e}")
        return

    logger.info(f"Período de geração do histórico: de {first_date} até {today}")

    # Passo 3: Loop diário para popular o histórico
    current_date = first_date
    while current_date <= today:
        snapshot_ts = datetime(current_date.year, current_date.month, current_date.day, 23, 59, 59, tzinfo=timezone.utc)
        date_str_for_rpc = snapshot_ts.isoformat()
        
        logger.info(f"--- Gerando histórico para a data: {current_date.strftime('%Y-%m-%d')} ---")
        
        try:
            # a) Calcular o leaderboard para a data
            calc_response = await asyncio.to_thread(
                supabase.rpc('calculate_leaderboard_for_date', {'target_date': date_str_for_rpc}).execute
            )
            
            if calc_response.data:
                leaderboard_data = calc_response.data
                
                # Adicionar o timestamp do snapshot a cada registro
                for row in leaderboard_data:
                    row['snapshot_timestamp'] = date_str_for_rpc
                    # O RPC retorna 'telegram_id', a tabela espera 'user_id'. Renomeamos.
                    row['user_id'] = row.pop('telegram_id')

                # b) Inserir o "snapshot" do dia na tabela de histórico
                await asyncio.to_thread(
                    supabase.table('leaderboard_history').insert(leaderboard_data).execute
                )
                logger.info(f"Snapshot de {current_date.strftime('%Y-%m-%d')} salvo com {len(leaderboard_data)} registros.")

                # c) Calcular e atualizar os rankings para o snapshot recém-criado
                await asyncio.to_thread(
                    supabase.rpc('update_leaderboard_history_ranks', {'snapshot_ts': date_str_for_rpc}).execute
                )
                logger.info(f"Ranking para {current_date.strftime('%Y-%m-%d')} calculado com sucesso.")

            else:
                logger.warning(f"Nenhum dado de leaderboard retornado para {current_date.strftime('%Y-%m-%d')}. Pulando.")

        except Exception as e:
            logger.error(f"Falha ao processar a data {current_date.strftime('%Y-%m-%d')}: {e}")

        current_date += timedelta(days=1)
        
    logger.info("\n" + "="*80)
    logger.info("População do histórico do leaderboard concluída com sucesso!")
    logger.info("="*80)


async def main():
    load_dotenv()
    # A função de teste SQL precisa existir no banco de dados.
    # Lembre o usuário de executá-la se ainda não o fez.
    logger.info("Verifique se a função 'calculate_leaderboard_for_date' existe no seu Supabase SQL Editor.")
    logger.info("Ela está definida no arquivo 'test_leaderboard_history.sql'.")
    
    await populate_full_leaderboard_history()

if __name__ == "__main__":
    asyncio.run(main()) 