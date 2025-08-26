"""
generate_leaderboard.py (Supabase Refactor - Daily Smart Generation)

Este script calcula as pontuações abrangentes de todos os embaixadores e
popula tanto o leaderboard ao vivo quanto o histórico, mas apenas UMA VEZ POR DIA.

Lógica Inteligente:
1. Verifica se já existe um leaderboard para a data atual no `leaderboard_history`.
2. Se já existe, pula a geração e informa que já foi processado.
3. Se não existe, executa toda a lógica de cálculo.

Fluxo de Execução (quando precisa gerar):
1. Conecta-se ao Supabase.
2. Chama a função RPC `calculate_leaderboard` que executa todos os cálculos complexos no servidor.
3. Recebe os dados calculados da RPC.
4. Processa os dados em Python para preparar para inserção:
   a) Adiciona `snapshot_timestamp` para a tabela de histórico.
   b) Adiciona `last_updated` timestamp para a tabela ao vivo.
   c) Mapeia as colunas do resultado RPC para as colunas da tabela.
5. Salva os dados em lotes:
   a) Insere no `leaderboard_history`.
   b) Faz upsert no `leaderboard`.
6. Chama duas outras funções RPC (`update_leaderboard_ranks` e 
   `update_leaderboard_history_ranks`) para calcular e definir o ranking final.
"""
import asyncio
import logging
import sys
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

# Importando o cliente Supabase centralizado (padrão do projeto)
from bot.author_manager import get_supabase_client

# Configuração de Logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

async def check_if_leaderboard_already_generated_today() -> bool:
    """
    Verifica se já existe um leaderboard gerado para hoje na tabela leaderboard_history.
    Retorna True se já foi gerado, False caso contrário.
    """
    supabase = await get_supabase_client()
    if not supabase:
        logger.error("Falha ao obter cliente Supabase para verificar histórico.")
        return False
    
    # Pega a data atual em UTC no formato YYYY-MM-DD
    today_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    try:
        # Busca qualquer registro do leaderboard_history para hoje
        response = await asyncio.to_thread(
            supabase.table('leaderboard_history')
            .select('id')
            .gte('snapshot_timestamp', f'{today_date} 00:00:00+00')
            .lt('snapshot_timestamp', f'{today_date} 23:59:59+00')
            .limit(1)
            .execute
        )
        
        if response.data:
            logger.info(f"✅ Leaderboard para {today_date} já foi gerado anteriormente.")
            logger.info(f"📊 Encontrado registro de histórico com timestamp: {response.data[0].get('snapshot_timestamp', 'N/A')}")
            return True
        else:
            logger.info(f"🆕 Nenhum leaderboard encontrado para {today_date}. Geração necessária.")
            return False
            
    except Exception as e:
        logger.error(f"Erro ao verificar histórico do leaderboard: {e}")
        return False

async def run_sql_from_file(file_path: Path):
    """
    Lê um arquivo .sql e o executa no Supabase usando a função RPC 'execute_sql'.
    NOTA: Esta função assume que a RPC 'execute_sql' foi criada no Supabase.
    """
    if not file_path.exists():
        logger.error(f"ERRO CRÍTICO: Arquivo SQL não encontrado em '{file_path}'")
        sys.exit(1)

    with open(file_path, 'r', encoding='utf-8') as f:
        sql_query = f.read()

    if not sql_query.strip():
        logger.warning(f"O arquivo SQL '{file_path.name}' está vazio. Nada a fazer.")
        return

    logger.info(f"📁 Executando script SQL: '{file_path.name}'...")
    supabase = await get_supabase_client()
    if not supabase:
        logger.error("Falha ao obter cliente Supabase para executar SQL.")
        sys.exit(1)
    
    try:
        # Divide o script em comandos individuais (ignorando linhas vazias)
        sql_commands = [cmd.strip() for cmd in sql_query.split(';') if cmd.strip()]
        
        logger.info(f"📝 Encontrados {len(sql_commands)} comandos no arquivo SQL.")

        for i, command in enumerate(sql_commands):
            logger.info(f"⚡ Executando comando {i+1}/{len(sql_commands)}...")
            # A função 'execute_sql' deve ter sido criada no Supabase SQL Editor:
            # CREATE OR REPLACE FUNCTION execute_sql(sql text) 
            # RETURNS void AS $$ BEGIN EXECUTE sql; END; $$ LANGUAGE plpgsql;
            await asyncio.to_thread(
                supabase.rpc('execute_sql', {'sql': command}).execute
            )
            logger.info(f"✅ Comando {i+1} concluído com sucesso.")

        logger.info(f"🎉 Script '{file_path.name}' executado com sucesso!")

    except Exception as e:
        logger.error(f"❌ Erro ao executar script SQL '{file_path.name}': {e}", exc_info=True)
        sys.exit(1)

async def clear_existing_data():
    """
    Limpa as tabelas de leaderboard existentes conforme solicitado pelo usuário.
    """
    supabase = await get_supabase_client()
    if not supabase:
        logger.error("Falha ao obter cliente Supabase para limpeza.")
        return False
    
    try:
        logger.info("🧹 Limpando dados existentes do leaderboard...")
        
        # Limpa leaderboard_history
        await asyncio.to_thread(
            supabase.table('leaderboard_history').delete().neq('id', 0).execute
        )
        logger.info("✅ Tabela leaderboard_history limpa.")
        
        # Limpa leaderboard
        await asyncio.to_thread(
            supabase.table('leaderboard').delete().neq('user_id', 0).execute
        )
        logger.info("✅ Tabela leaderboard limpa.")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao limpar dados existentes: {e}")
        return False

async def update_current_leaderboard():
    """
    Atualiza a tabela leaderboard (atual) com os dados mais recentes do leaderboard_history.
    Pega o snapshot mais recente e copia para a tabela ao vivo.
    """
    supabase = await get_supabase_client()
    if not supabase:
        logger.error("Falha ao obter cliente Supabase para atualizar leaderboard atual.")
        return False
    
    try:
        logger.info("📊 Atualizando tabela leaderboard atual com dados mais recentes...")
        
        # Primeiro, buscar o timestamp mais recente
        latest_snapshot_response = await asyncio.to_thread(
            supabase.table('leaderboard_history')
            .select('snapshot_timestamp')
            .order('snapshot_timestamp', desc=True)
            .limit(1)
            .execute
        )
        
        if not latest_snapshot_response.data:
            logger.warning("⚠️ Nenhum snapshot encontrado no leaderboard_history.")
            return False
        
        latest_timestamp = latest_snapshot_response.data[0]['snapshot_timestamp']
        
        # Buscar todos os dados do snapshot mais recente específico
        response = await asyncio.to_thread(
            supabase.table('leaderboard_history')
            .select('*')
            .eq('snapshot_timestamp', latest_timestamp)
            .order('rank')  # Ordenar por rank
            .execute
        )
        
        if not response.data:
            logger.warning("⚠️ Nenhum dado encontrado no snapshot mais recente.")
            return False
        
        # Limpar tabela leaderboard atual antes de inserir novos dados
        await asyncio.to_thread(
            supabase.table('leaderboard').delete().neq('user_id', 0).execute
        )
        
        # Preparar dados para inserção na tabela leaderboard
        current_timestamp = datetime.now(timezone.utc).isoformat()
        leaderboard_records = []
        
        for record in response.data:
            leaderboard_record = {
                'user_id': record['user_id'],
                'last_updated': current_timestamp,
                'rank': record['rank'],
                'telegram_name': record['telegram_name'],
                'twitter_username': record['twitter_username'],
                'count_tweets_text_only': record['count_tweets_text_only'],
                'count_tweets_image': record['count_tweets_image'],
                'count_tweets_thread': record['count_tweets_thread'],
                'count_tweets_video': record['count_tweets_video'],
                'total_score_from_tweets': record['total_score_from_tweets'],
                'count_retweets_made': record['count_retweets_made'],
                'count_comments_made': record['count_comments_made'],
                'total_score_from_engagements': record['total_score_from_engagements'],
                'total_score_from_telegram': record['total_score_from_telegram'],
                'count_partner_introduction': record['count_partner_introduction'],
                'count_hosting_ama': record['count_hosting_ama'],
                'count_recruitment_ambassador': record['count_recruitment_ambassador'],
                'count_product_feedback': record['count_product_feedback'],
                'count_recruitment_investor': record['count_recruitment_investor'],
                'total_score_from_contributions': record['total_score_from_contributions'],
                'grand_total_score': record['grand_total_score']
            }
            leaderboard_records.append(leaderboard_record)
        
        # Fazer upsert na tabela leaderboard
        await asyncio.to_thread(
            supabase.table('leaderboard').upsert(leaderboard_records).execute
        )
        
        logger.info(f"✅ Tabela leaderboard atual atualizada com {len(leaderboard_records)} registros.")
        logger.info(f"🏆 Líder atual: {leaderboard_records[0]['telegram_name']} (@{leaderboard_records[0]['twitter_username']}) com {leaderboard_records[0]['grand_total_score']:.2f} pontos")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar leaderboard atual: {e}")
        return False

async def main():
    """
    Função principal que orquestra a geração inteligente do leaderboard.
    """
    logger.info("🚀 --- Iniciando Geração Inteligente do Leaderboard ---")
    
    # Verificar se já foi gerado hoje
    already_generated = await check_if_leaderboard_already_generated_today()
    
    if already_generated:
        logger.info("⏭️  Leaderboard já foi gerado hoje. Nada a fazer.")
        logger.info("💡 Para forçar uma nova geração, limpe a tabela leaderboard_history.")
        return
    
    # Como você vai apagar tudo e começar do zero, vamos limpar primeiro
    logger.info("🔄 Modo de reset ativado - limpando dados existentes...")
    cleanup_success = await clear_existing_data()
    if not cleanup_success:
        logger.error("❌ Falha na limpeza. Abortando geração.")
        return
    
    # Agora gerar o leaderboard
    project_root = Path(__file__).resolve().parent
    sql_file = project_root / 'generate_retroactive_leaderboard.sql'
    
    if not sql_file.exists():
        logger.error(f"❌ Arquivo SQL não encontrado: {sql_file}")
        logger.error("💡 Certifique-se de que 'generate_retroactive_leaderboard.sql' existe no mesmo diretório.")
        return
    
    await run_sql_from_file(sql_file)
    
    # NOVA ETAPA: Atualizar a tabela leaderboard atual com os dados mais recentes
    logger.info("🔄 Atualizando tabela leaderboard atual...")
    update_success = await update_current_leaderboard()
    if not update_success:
        logger.warning("⚠️ Falha ao atualizar leaderboard atual, mas histórico foi gerado com sucesso.")
    
    logger.info("🎯 --- Geração do Leaderboard Concluída com Sucesso ---")
    logger.info(f"📅 Próxima geração será executada amanhã (se houver novos dados).")

if __name__ == "__main__":
    load_dotenv()
    asyncio.run(main()) 