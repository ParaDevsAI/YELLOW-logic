"""
cross_engagement_tracker.py

Este script foi refatorado para usar o Supabase como banco de dados.
Ele é projetado para ser executado periodicamente (e.g., a cada 24 horas)
para analisar o engajamento cruzado entre embaixadores.

Lógica:
1. Conecta-se ao Supabase.
2. Busca todos os tweets postados por embaixadores do banco de dados.
3. Para cada tweet, ele faz duas chamadas de API para buscar:
   a) Respostas (Replies) e Citações (Quotes).
   b) Retweets.
4. Compara a lista de usuários que interagiram com a lista de embaixadores registrados.
5. Se um embaixador interagiu com a postagem de outro, ele registra a pontuação
   na tabela `ambassador_engagements` usando um 'upsert' para evitar duplicatas.
"""
import asyncio
import os
import httpx
import json
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Importando o cliente Supabase centralizado
from author_manager import get_supabase_client

# --- Configuração de Logging ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Funções de Acesso a Dados (Refatoradas para Supabase) ---

async def get_all_ambassador_twitter_ids() -> set:
    """Busca todos os IDs do Twitter dos embaixadores no Supabase."""
    supabase = await get_supabase_client()
    if not supabase:
        return set()
    try:
        response = await asyncio.to_thread(
            supabase.table('authors').select('twitter_id').execute
        )
        if response.data:
            return {item['twitter_id'] for item in response.data}
        return set()
    except Exception as e:
        logger.error(f"Erro no Supabase ao buscar IDs de embaixadores: {e}")
        return set()

async def get_all_tweets_from_db() -> list:
    """Busca todos os tweets registrados no Supabase que foram criados nos últimos 3 dias."""
    supabase = await get_supabase_client()
    if not supabase:
        return []
    
    # Calcula o ponto de corte: 3 dias atrás a partir de agora (UTC)
    cutoff_date = datetime.utcnow() - timedelta(days=3)
    cutoff_date_iso = cutoff_date.isoformat()

    logger.info(f"Buscando tweets criados desde: {cutoff_date_iso}")
    
    try:
        # Adiciona o filtro .gte (maior ou igual a) na coluna 'createdat'
        response = await asyncio.to_thread(
            supabase.table('tweets')
            .select('tweet_id, author_id')
            .gte('createdat', cutoff_date_iso)
            .execute
        )
        return response.data if response.data else []
    except Exception as e:
        logger.error(f"Erro no Supabase ao buscar tweets recentes: {e}")
        return []

# --- Funções de Chamada de API (Inalteradas) ---

async def fetch_with_pagination(session: httpx.AsyncClient, url: str, params: dict, headers: dict, data_key: str) -> list:
    """Função genérica para lidar com requisições paginadas."""
    all_data = []
    while True:
        try:
            response = await session.get(url, params=params, headers=headers, timeout=40.0)
            if response.status_code != 200:
                logger.error(f"API retornou erro {response.status_code} para {url} com params {params}. Resposta: {response.text}")
                break
            
            data = response.json()
            all_data.extend(data.get(data_key, []))

            if data.get('has_next_page') and data.get('next_cursor'):
                params['cursor'] = data.get('next_cursor')
                logger.info(f"Paginando para {url}... Encontrados {len(data.get(data_key, []))} itens.")
                await asyncio.sleep(1) # Cortesia para o rate-limiting
            else:
                break
        except (httpx.RequestError, json.JSONDecodeError) as e:
            logger.error(f"Erro na requisição ou no parse JSON para {url}: {e}")
            break
    return all_data

async def fetch_replies_and_quotes(session: httpx.AsyncClient, api_key: str, tweet_id: str) -> list:
    """Busca respostas e citações para um determinado ID de tweet."""
    logger.info(f"Buscando respostas/citações para o tweet {tweet_id}...")
    url = "https://api.twitterapi.io/twitter/tweet/advanced_search"
    query = f"(conversation_id:{tweet_id}) OR (quoted_tweet_id:{tweet_id})"
    params = {"query": query, "queryType": "Latest", "cursor": ""}
    headers = {"X-API-Key": api_key}
    return await fetch_with_pagination(session, url, params, headers, 'tweets')

async def fetch_retweeters(session: httpx.AsyncClient, api_key: str, tweet_id: str) -> list:
    """Busca usuários que deram retweet em um determinado ID de tweet."""
    logger.info(f"Buscando retweeters para o tweet {tweet_id}...")
    url = "https://api.twitterapi.io/twitter/tweet/retweeters"
    params = {"tweetId": tweet_id, "cursor": ""}
    headers = {"X-API-Key": api_key}
    return await fetch_with_pagination(session, url, params, headers, 'users')

# --- Função de Processamento e Armazenamento (Refatorada para Supabase) ---

async def process_and_save_engagements(engagements: list):
    """
    Salva uma lista de interações válidas no banco de dados Supabase.
    Usa 'upsert' para replicar o comportamento 'INSERT OR IGNORE',
    prevenindo duplicatas com base na restrição UNIQUE da tabela.
    """
    if not engagements:
        return

    supabase = await get_supabase_client()
    if not supabase:
        logger.error("Falha ao obter cliente Supabase para salvar engajamentos.")
        return

    now_iso = datetime.utcnow().isoformat()
    
    # Prepara os dados para o upsert em lote.
    data_to_insert = [
        {
            'tweet_id': eng['tweet_id'],
            'tweet_author_id': eng['tweet_author_id'],
            'interacting_user_id': eng['interacting_user_id'],
            'action_type': eng['action_type'],
            'points_awarded': eng['points_awarded'],
            'created_at': now_iso
        }
        for eng in engagements
    ]

    try:
        # O método 'upsert' do Supabase lida com conflitos de forma elegante.
        # Se um registro com a mesma (tweet_id, interacting_user_id, action_type) já existir,
        # a operação para aquele registro específico é ignorada, sem causar erro.
        response = await asyncio.to_thread(
            supabase.table('ambassador_engagements').upsert(data_to_insert).execute
        )
        
        # O upsert bem-sucedido não necessariamente retorna o número de linhas afetadas como o aiosqlite.
        # Nós apenas logamos que a operação foi enviada.
        logger.info(f"Lote de {len(data_to_insert)} engajamentos enviado para o Supabase via upsert.")

    except Exception as e:
        logger.error(f"FALHA CRÍTICA ao salvar lote de engajamentos no Supabase: {e}")

# --- Função Orquestradora Principal (Adaptada) ---

async def main():
    """Função principal que orquestra todo o processo."""
    logger.info("--- Iniciando Script de Rastreamento de Engajamento Cruzado (Versão Supabase) ---")
    load_dotenv()
    api_key = os.getenv("TWITTER_API_KEY")
    if not api_key:
        logger.critical("TWITTER_API_KEY não encontrada. Abortando.")
        return

    # A conexão com o DB agora é gerenciada pelo get_supabase_client()
    
    ambassador_ids = await get_all_ambassador_twitter_ids()
    tweets_to_check = await get_all_tweets_from_db()
    
    if not ambassador_ids:
        logger.warning("Nenhum embaixador encontrado no banco de dados. Abortando.")
        return
    if not tweets_to_check:
        logger.warning("Nenhum tweet encontrado para verificar. Abortando.")
        return

    logger.info(f"Encontrados {len(ambassador_ids)} embaixadores e {len(tweets_to_check)} tweets para analisar.")
    
    all_new_engagements = []

    async with httpx.AsyncClient() as session:
        for tweet in tweets_to_check:
            tweet_id = tweet['tweet_id']
            tweet_author_id = tweet['author_id']
            
            logger.info(f"--- Processando tweet: {tweet_id} (Autor: {tweet_author_id}) ---")

            # Executando as duas coletas de API em paralelo para o mesmo tweet
            replies_quotes_task = fetch_replies_and_quotes(session, api_key, tweet_id)
            retweeters_task = fetch_retweeters(session, api_key, tweet_id)
            
            results = await asyncio.gather(replies_quotes_task, retweeters_task, return_exceptions=True)
            
            # Desempacotando resultados
            reply_quote_data = results[0] if not isinstance(results[0], Exception) else []
            retweeter_data = results[1] if not isinstance(results[1], Exception) else []

            # Processa Respostas e Citações
            for item in reply_quote_data:
                interaction_author_id = item.get('author', {}).get('id')
                
                # Validação: é de um embaixador? não é auto-engajamento?
                if interaction_author_id in ambassador_ids and str(interaction_author_id) != str(tweet_author_id):
                    action = 'reply' if item.get('isReply') else 'retweet_or_quote'
                    all_new_engagements.append({
                        'tweet_id': tweet_id,
                        'tweet_author_id': tweet_author_id,
                        'interacting_user_id': str(interaction_author_id),
                        'action_type': action,
                        'points_awarded': 2
                    })
            
            # Processa Retweeters
            for user in retweeter_data:
                interaction_author_id = user.get('id')
                
                # Validação
                if interaction_author_id in ambassador_ids and str(interaction_author_id) != str(tweet_author_id):
                    all_new_engagements.append({
                        'tweet_id': tweet_id,
                        'tweet_author_id': tweet_author_id,
                        'interacting_user_id': str(interaction_author_id),
                        'action_type': 'retweet_or_quote',
                        'points_awarded': 2
                    })

    # Salva todos os engajamentos encontrados em um único lote
    await process_and_save_engagements(all_new_engagements)

    # O cliente Supabase não precisa ser fechado manualmente como uma conexão aiosqlite.

    logger.info("--- Script de Rastreamento de Engajamento Cruzado Concluído ---")


if __name__ == '__main__':
    asyncio.run(main()) 