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
from bot.author_manager import get_supabase_client

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
    """
    Busca tweets dos últimos 3 dias no Supabase para análise.
    Isso otimiza o script para execuções diárias, focando apenas em conteúdo recente.
    """
    supabase = await get_supabase_client()
    if not supabase:
        return []
    
    logger.info("Buscando tweets dos últimos 3 dias para análise...")
    
    three_days_ago = datetime.utcnow() - timedelta(days=3)
    
    all_tweets = []
    page = 0
    page_size = 1000 # O limite padrão do Supabase é 1000

    while True:
        try:
            start_index = page * page_size
            end_index = start_index + page_size - 1

            response = await asyncio.to_thread(
                supabase.table('tweets')
                .select('tweet_id, author_id, createdat')
                .gte('createdat', three_days_ago.isoformat())
                .order('createdat', desc=True)
                .range(start_index, end_index)
                .execute
            )

            if response.data:
                all_tweets.extend(response.data)
                # Se o número de resultados for menor que o tamanho da página, esta é a última página.
                if len(response.data) < page_size:
                    break
                page += 1
            else:
                break # Sai do loop se não houver mais dados
        except Exception as e:
            logger.error(f"Erro no Supabase ao buscar a página {page} de tweets recentes: {e}")
            break # Interrompe em caso de erro para evitar loops infinitos
            
    return all_tweets

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
        logger.info("Nenhum novo engajamento único encontrado para salvar.")
        return

    supabase = await get_supabase_client()
    if not supabase:
        logger.error("Falha ao obter cliente Supabase para salvar engajamentos.")
        return
    
    # Prepara os dados para o upsert em lote.
    data_to_insert = [
        {
            'tweet_id': eng['tweet_id'],
            'tweet_author_id': eng['tweet_author_id'],
            'interacting_user_id': eng['interacting_user_id'],
            'action_type': eng['action_type'],
            'points_awarded': eng['points_awarded'],
            'created_at': eng['created_at'] # Usa o timestamp do tweet original
        }
        for eng in engagements
    ]

    try:
        # O método 'upsert' do Supabase lida com conflitos.
        # Ao especificar as colunas do 'constraint', garantimos que ele
        # ignore a inserção se um registro com a mesma combinação já existir.
        response = await asyncio.to_thread(
            supabase.table('ambassador_engagements').upsert(
                data_to_insert,
                on_conflict='tweet_id,interacting_user_id,action_type'
            ).execute
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
    
    # Usar um 'set' para automaticamente remover duplicatas de engajamento
    unique_engagements = set()
    tweets_processed_in_batch = 0

    async with httpx.AsyncClient() as session:
        for i, tweet in enumerate(tweets_to_check):
            tweet_id = tweet['tweet_id']
            tweet_author_id = tweet['author_id']
            tweet_creation_date = tweet['createdat']
            
            logger.info(f"--- Processando tweet {i + 1}/{len(tweets_to_check)}: {tweet_id} (Autor: {tweet_author_id}) ---")

            # Mapa para rastrear engajamento por usuário e evitar pontos duplicados
            user_engagement_tracker = {} # Key: user_id, Value: set of actions ('reply', 'retweet_or_quote')

            # 1. Coletar todos os dados de API primeiro
            replies_quotes_task = fetch_replies_and_quotes(session, api_key, tweet_id)
            retweeters_task = fetch_retweeters(session, api_key, tweet_id)
            
            results = await asyncio.gather(replies_quotes_task, retweeters_task, return_exceptions=True)
            
            reply_quote_data = results[0] if not isinstance(results[0], Exception) else []
            retweeter_data = results[1] if not isinstance(results[1], Exception) else []

            # 2. Processar Retweets primeiro (eles têm prioridade na regra "OU")
            for user in retweeter_data:
                retweeter_id = str(user.get('id'))
                # Validação explícita de auto-engajamento
                if retweeter_id == str(tweet_author_id):
                    logger.info(f"[AUDITORIA] Ignorando auto-engajamento (Retweet) do usuário {retweeter_id} no próprio tweet {tweet_id}.")
                    continue # Pula para a próxima iteração

                if retweeter_id in ambassador_ids:
                    if retweeter_id not in user_engagement_tracker:
                        user_engagement_tracker[retweeter_id] = set()
                    
                    user_engagement_tracker[retweeter_id].add('retweet_or_quote')

            # 3. Processar Respostas e Citações
            for item in reply_quote_data:
                interaction_author_id = str(item.get('author', {}).get('id'))
                
                # Validação explícita de auto-engajamento
                if interaction_author_id == str(tweet_author_id):
                    logger.info(f"[AUDITORIA] Ignorando auto-engajamento (Reply/Quote) do usuário {interaction_author_id} no próprio tweet {tweet_id}.")
                    continue # Pula para a próxima iteração

                if interaction_author_id in ambassador_ids:
                    if interaction_author_id not in user_engagement_tracker:
                        user_engagement_tracker[interaction_author_id] = set()

                    # É uma resposta? Pode ser acumulada com um retweet/quote.
                    if item.get('isReply'):
                        user_engagement_tracker[interaction_author_id].add('reply')
                    # É uma citação E o usuário ainda NÃO foi creditado por um retweet?
                    elif 'retweet_or_quote' not in user_engagement_tracker[interaction_author_id]:
                        user_engagement_tracker[interaction_author_id].add('retweet_or_quote')

            # 4. Construir o lote de engajamentos únicos com base na nova lógica
            for user_id, actions in user_engagement_tracker.items():
                for action in actions:
                    points = 2 # Cada ação individual (reply ou retweet/quote) vale 2 pontos
                    unique_engagements.add((
                        tweet_id,
                        tweet_author_id,
                        user_id,
                        action,
                        points,
                        tweet_creation_date
                    ))

            tweets_processed_in_batch += 1
            
            # Lógica de lote para salvar no banco de dados a cada 10 tweets
            if tweets_processed_in_batch >= 10:
                # O set garante que, mesmo que a lógica adicione algo duas vezes,
                # apenas uma entrada será salva no final.
                # Convertemos para lista para passar para a função.
                engagements_to_save = [
                    {
                        'tweet_id': eng[0], 'tweet_author_id': eng[1], 'interacting_user_id': eng[2],
                        'action_type': eng[3], 'points_awarded': eng[4], 'created_at': eng[5]
                    }
                    for eng in unique_engagements
                ]
                await process_and_save_engagements(engagements_to_save)
                unique_engagements.clear()
                tweets_processed_in_batch = 0
                logger.info("Lote de engajamentos salvo. Continuando...")

    # Garante que o último lote de engajamentos seja salvo ao final do loop
    if unique_engagements:
        logger.info("Salvando lote final de engajamentos...")
        engagements_to_save = [
            {
                'tweet_id': eng[0], 'tweet_author_id': eng[1], 'interacting_user_id': eng[2],
                'action_type': eng[3], 'points_awarded': eng[4], 'created_at': eng[5]
            }
            for eng in unique_engagements
        ]
        await process_and_save_engagements(engagements_to_save)

    logger.info("--- Rastreamento de Engajamento Cruzado Concluído ---")


if __name__ == '__main__':
    asyncio.run(main()) 