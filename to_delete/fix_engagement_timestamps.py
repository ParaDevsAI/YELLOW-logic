import logging
from author_manager import initialize_supabase_client

# --- Configuração de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fix_timestamps.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_fix():
    """
    Busca todos os engajamentos, encontra a data de criação do tweet original
    e atualiza o timestamp do engajamento para corresponder.
    """
    supabase = initialize_supabase_client()
    if not supabase:
        logger.critical("Falha ao inicializar o cliente Supabase.")
        return

    try:
        # 1. Buscar todos os engajamentos com paginação
        logger.info("Buscando todos os registros de `ambassador_engagements` com paginação...")
        all_engagements = []
        page = 0
        page_size = 1000  # O limite padrão do Supabase por requisição

        while True:
            start_index = page * page_size
            end_index = start_index + page_size - 1
            logger.info(f"Buscando página {page + 1} (registros {start_index} a {end_index})...")
            
            response = supabase.table("ambassador_engagements").select("id, tweet_id").range(start_index, end_index).execute()
            
            if response.data:
                all_engagements.extend(response.data)
                if len(response.data) < page_size:
                    break  # É a última página
                page += 1
            else:
                break  # Não há mais dados

        if not all_engagements:
            logger.info("Nenhum registro de engajamento encontrado para corrigir.")
            return

        total_engagements = len(all_engagements)
        logger.info(f"Encontrados {total_engagements} registros de engajamento no total para processar.")

        # 2. Criar um mapa de tweet_id para createdat para eficiência
        tweet_ids = {eng['tweet_id'] for eng in all_engagements}
        logger.info(f"Buscando as datas de criação para {len(tweet_ids)} tweets únicos...")
        
        tweets_response = supabase.table("tweets").select("tweet_id, createdat").in_("tweet_id", list(tweet_ids)).execute()
        
        if not tweets_response.data:
            logger.error("Não foi possível buscar os tweets correspondentes. Abortando.")
            return
            
        tweet_date_map = {tweet['tweet_id']: tweet['createdat'] for tweet in tweets_response.data}
        logger.info("Mapa de datas de tweets criado com sucesso.")

        # 3. Iterar e preparar as atualizações
        updates_prepared = 0
        for eng in all_engagements:
            engagement_id = eng['id']
            tweet_id = eng['tweet_id']
            
            correct_timestamp = tweet_date_map.get(tweet_id)
            
            if correct_timestamp:
                logger.info(f"Atualizando engajamento ID {engagement_id} para o timestamp {correct_timestamp}...")
                try:
                    supabase.table("ambassador_engagements").update({
                        'created_at': correct_timestamp
                    }).eq('id', engagement_id).execute()
                    updates_prepared += 1
                except Exception as e:
                    logger.error(f"Falha ao atualizar o engajamento ID {engagement_id}: {e}")
            else:
                logger.warning(f"Não foi encontrado um timestamp para o tweet_id {tweet_id} (engajamento ID: {engagement_id}). Pulando.")
        
        logger.info(f"Processo de correção concluído. {updates_prepared}/{total_engagements} registros foram atualizados.")

    except Exception as e:
        logger.critical(f"Ocorreu um erro crítico durante o processo de correção: {e}", exc_info=True)


if __name__ == "__main__":
    logger.info("--- Iniciando Script de Correção de Timestamps de Engajamento ---")
    run_fix()
    logger.info("--- Script de Correção Finalizado ---") 