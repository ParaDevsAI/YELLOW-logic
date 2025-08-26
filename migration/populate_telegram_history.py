"""
populate_telegram_history.py

Este script é uma ferramenta de uso único para popular as tabelas 'user_activity' e 'tweets'
com dados históricos de mensagens do Telegram. Ele não é um bot em tempo real.

Lógica:
1. Conecta-se à API do Telegram como uma conta de usuário (não um bot) usando Telethon.
   - Isso é necessário para ler o histórico de mensagens de grupos.
2. Define um período de tempo (data de início até data fim).
3. Busca todos os IDs de Telegram dos embaixadores registrados no Supabase.
4. Para cada embaixador, ele itera por cada dia do período.
5. SCORING_GROUP_ID: Busca mensagens para calcular activity scores (sessões de 3h, bônus)
6. TELEGRAM_GROUP_ID: Busca mensagens para extrair links de tweets dos embaixadores
7. Agrega os dados por dia e salva os resultados no Supabase usando 'upsert'.
"""
import asyncio
import os
import logging
import re
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from telethon import TelegramClient
import json

# Importando o cliente Supabase centralizado e outras funções
from bot.author_manager import get_supabase_client, get_all_ambassador_telegram_ids

# --- Configuração de Logging ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Carregar Variáveis de Ambiente ---
load_dotenv()
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "my_user_session")
SCORING_GROUP_ID = int(os.getenv("SCORING_GROUP_ID", 0))
TELEGRAM_GROUP_ID = -1002330680602  # Grupo dos embaixadores para tweets
SCORING_BONUS_MULTIPLIER = float(os.getenv("SCORING_BONUS_MULTIPLIER", 1.25))

# --- Configurações do Script ---
START_DATE = datetime(2025, 7, 12, tzinfo=timezone.utc)  # 12 JULHO
END_DATE = datetime(2025, 7, 17, tzinfo=timezone.utc)    # até 16 JULHO

# --- MODO DRY RUN ---
DRY_RUN = True  # True = apenas analisa, False = salva no DB

# --- Contadores para estatísticas ---
stats = {
    'activity_records': 0,
    'tweets_found': 0,
    'days_processed': 0,
    'ambassadors_processed': 0
}

# --- Regex para detectar tweets ---
TWITTER_URL_PATTERN = re.compile(r'https?://(?:www\.)?(?:x|twitter)\.com/(\w+)/status/(\d+)')

# --- Funções Auxiliares ---

def get_authors_twitter_usernames_sync() -> list[str]:
    """Busca todos os twitter_usernames da tabela authors de forma síncrona."""
    try:
        # Usar cliente síncrono para esta função
        from bot.author_manager import initialize_supabase_client
        supabase = initialize_supabase_client()
        if not supabase:
            return []
        
        response = supabase.table('authors').select('twitter_username').execute()
        if response.data:
            usernames = [item['twitter_username'] for item in response.data if item.get('twitter_username')]
            logger.info(f"Buscados {len(usernames)} Twitter usernames da base de dados.")
            return usernames
        logger.warning("Nenhum autor encontrado na base de dados.")
        return []
    except Exception as e:
        logger.error(f"Erro ao buscar autores do Supabase: {e}")
        return []

def extract_twitter_info_from_url(text: str) -> dict | None:
    """Extrai informações do tweet de uma URL do Twitter/X."""
    if not text:
        return None
    match = TWITTER_URL_PATTERN.search(text)
    if match:
        return {"username": match.group(1), "tweet_id": match.group(2)}
    return None

async def save_tweet_to_database(tweet_info: dict, message_date: datetime, shared_by_telegram_id: int):
    """Salva um tweet na base de dados (ou apenas simula se DRY_RUN=True)."""
    global stats
    
    # Verificar se já existe para evitar duplicatas
    if not DRY_RUN:
        supabase = await get_supabase_client()
        if supabase:
            try:
                existing = await asyncio.to_thread(
                    supabase.table('tweets').select('tweet_id').eq('tweet_id', tweet_info['tweet_id']).execute
                )
                if existing.data:
                    logger.warning(f"⚠️ DUPLICATA DETECTADA - Tweet {tweet_info['tweet_id']} já existe. Pulando...")
                    return
            except Exception as e:
                logger.error(f"Erro ao verificar duplicata de tweet: {e}")
    
    stats['tweets_found'] += 1
    
    if DRY_RUN:
        logger.info(f"🔍 [DRY RUN] TWEET que seria salvo:")
        logger.info(f"    👤 Author: {tweet_info['username']}")
        logger.info(f"    🐦 Tweet ID: {tweet_info['tweet_id']}")
        logger.info(f"    🔗 URL: https://twitter.com/{tweet_info['username']}/status/{tweet_info['tweet_id']}")
        logger.info(f"    📅 Data: {message_date.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"    👥 Compartilhado por: {shared_by_telegram_id}")
        return
    
    supabase = await get_supabase_client()
    if not supabase:
        logger.error("Falha ao obter cliente Supabase para salvar tweet.")
        return

    # Buscar author_id baseado no twitter_username
    try:
        author_response = await asyncio.to_thread(
            supabase.table('authors').select('author_id').eq('twitter_username', tweet_info['username']).limit(1).single().execute
        )
        
        if not author_response.data:
            logger.warning(f"Autor com twitter_username '{tweet_info['username']}' não encontrado na base de dados.")
            return
            
        author_id = author_response.data['author_id']
        
        # Preparar registro do tweet
        tweet_record = {
            'tweet_id': tweet_info['tweet_id'],
            'author_id': author_id,
            'shared_by_telegram_id': shared_by_telegram_id,
            'twitter_url': f"https://twitter.com/{tweet_info['username']}/status/{tweet_info['tweet_id']}",
            'createdat': message_date.isoformat()
        }
        
        # Upsert do tweet
        await asyncio.to_thread(
            supabase.table('tweets').upsert(tweet_record).execute
        )
        
        logger.info(f"✅ Tweet salvo: {tweet_info['username']}/status/{tweet_info['tweet_id']}")
        
    except Exception as e:
        logger.error(f"Erro ao salvar tweet {tweet_info['tweet_id']}: {e}")

def get_session_from_date(dt: datetime) -> str:
    """Determina a sessão de 3 horas para uma data/hora específica."""
    hour = dt.hour
    session_start = (hour // 3) * 3
    session_end = session_start + 3
    return f"{session_start:02d}-{session_end:02d}"

def print_final_statistics():
    """Exibe estatísticas finais da execução."""
    mode_text = "🔍 MODO DRY RUN (APENAS ANÁLISE)" if DRY_RUN else "✅ MODO PRODUÇÃO (DADOS SALVOS)"
    
    print("\n" + "="*60)
    print(f"📊 ESTATÍSTICAS FINAIS - {mode_text}")
    print("="*60)
    print(f"📅 Período processado: {START_DATE.strftime('%d/%m/%Y')} até {END_DATE.strftime('%d/%m/%Y')}")
    print(f"👥 Embaixadores processados: {stats['ambassadors_processed']}")
    print(f"📆 Dias analisados: {stats['days_processed']}")
    print(f"🎯 Registros de atividade encontrados: {stats['activity_records']}")
    print(f"🐦 Tweets de embaixadores encontrados: {stats['tweets_found']}")
    print("="*60)
    
    if DRY_RUN:
        print("💡 PRÓXIMOS PASSOS:")
        print("   1. Revise os logs acima para verificar os dados")
        print("   2. Se estiver OK, altere DRY_RUN = False")
        print("   3. Execute novamente para salvar na base de dados")
    else:
        print("✅ DADOS SALVOS COM SUCESSO NA BASE DE DADOS!")
    print("="*60 + "\n")

async def save_activity_to_supabase(user_id: int, activity_date: str, total_score: float, details_json: dict):
    """Salva os dados de atividade diária de um usuário no Supabase (ou apenas simula se DRY_RUN=True)."""
    global stats
    
    # Verificar se já existe para evitar duplicatas
    if not DRY_RUN:
        supabase = await get_supabase_client()
        if supabase:
            try:
                existing = await asyncio.to_thread(
                    supabase.table('user_activity').select('user_id').eq('user_id', user_id).eq('activity_date', activity_date).execute
                )
                if existing.data:
                    logger.warning(f"⚠️ DUPLICATA DETECTADA - Activity já existe para user {user_id} em {activity_date}. Pulando...")
                    return
            except Exception as e:
                logger.error(f"Erro ao verificar duplicata de activity: {e}")
    
    stats['activity_records'] += 1
    
    if DRY_RUN:
        logger.info(f"🔍 [DRY RUN] ACTIVITY que seria salva:")
        logger.info(f"    👤 User ID: {user_id}")
        logger.info(f"    📅 Data: {activity_date}")
        logger.info(f"    🎯 Score Total: {total_score:.2f}")
        logger.info(f"    📊 Sessões: {len(details_json)} intervalos")
        for session, data in details_json.items():
            logger.info(f"        {session}h: {data['messages']} msgs = {data['score']:.2f} pontos")
        return
    
    supabase = await get_supabase_client()
    if not supabase:
        logger.error("Falha ao obter cliente Supabase para salvar atividade.")
        return

    record_to_save = {
        'user_id': user_id,
        'activity_date': activity_date,
        'total_day_score': total_score,
        'intervals_details_json': details_json
    }

    try:
        await asyncio.to_thread(
            supabase.table('user_activity').upsert(record_to_save).execute
        )
        logger.info(f"✅ Salvo com sucesso a atividade para o usuário {user_id} na data {activity_date} com score {total_score:.2f}")
    except Exception as e:
        logger.error(f"FALHA CRÍTICA ao salvar atividade para o usuário {user_id} na data {activity_date}. Erro: {e}")

async def process_scoring_group(client, target_group, ambassador_ids, start_date, end_date):
    """Processa mensagens do grupo de scoring para calcular activity scores."""
    logger.info(f"🎯 Processando SCORING GROUP: {target_group.title}")
    global stats
    
    for i, user_id in enumerate(list(ambassador_ids)):
        logger.info(f"--- Processando Embaixador {i+1}/{len(ambassador_ids)} (ID: {user_id}) para ACTIVITY SCORES ---")
        stats['ambassadors_processed'] += 1

        current_date = start_date
        while current_date.date() < end_date.date():
            
            day_str = current_date.strftime('%Y-%m-%d')
            logger.info(f"Verificando ACTIVITY para data: {day_str} usuário {user_id}...")

            day_start = current_date
            day_end = current_date + timedelta(days=1)
            
            daily_activity = {}

            messages_in_day = []
            try:
                async for message in client.iter_messages(
                    target_group,
                    from_user=user_id,
                    offset_date=day_end,
                    reverse=True
                ):
                    if message.date < day_start:
                        break
                    messages_in_day.append(message)
            
            except Exception as e:
                logger.error(f"Erro ao buscar mensagens de activity para o usuário {user_id} no dia {day_str}: {e}")
                current_date += timedelta(days=1)
                continue

            if not messages_in_day:
                current_date += timedelta(days=1)
                continue

            logger.info(f"-> Encontradas {len(messages_in_day)} mensagens de ACTIVITY para {user_id} em {day_str}")

            # Processar as mensagens do dia para activity score
            for message in messages_in_day:
                session_id = get_session_from_date(message.date)

                if session_id not in daily_activity:
                    daily_activity[session_id] = {'messages': 0, 'score': 0.0, 'last_score': 0.0}

                session_state = daily_activity[session_id]

                if session_state['messages'] >= 10:
                    continue
                
                current_msg_score = 0.0
                if session_state['messages'] == 0:
                    current_msg_score = 1.0
                else:
                    current_msg_score = session_state['last_score'] * SCORING_BONUS_MULTIPLIER
                
                session_state['messages'] += 1
                session_state['score'] += current_msg_score
                session_state['last_score'] = current_msg_score

            # Salvar activity score no DB
            total_day_score = sum(details.get('score', 0.0) for details in daily_activity.values())
            
            intervals_details_json = {
                session: {'messages': data['messages'], 'score': data['score']}
                for session, data in daily_activity.items()
            }

            await save_activity_to_supabase(
                user_id=user_id,
                activity_date=day_str,
                total_score=total_day_score,
                details_json=intervals_details_json
            )

            current_date += timedelta(days=1)

async def process_tweets_group(client, tweets_group, start_date, end_date):
    """Processa mensagens do grupo de embaixadores para extrair tweets."""
    logger.info(f"🐦 Processando TWEETS GROUP: {tweets_group.title}")
    
    # Buscar usernames dos embaixadores
    ambassador_usernames = get_authors_twitter_usernames_sync()
    ambassador_usernames_lower = [u.lower() for u in ambassador_usernames]
    
    if not ambassador_usernames:
        logger.warning("Nenhum username de embaixador encontrado. Pulando processamento de tweets.")
        return
    
    logger.info(f"Procurando tweets de {len(ambassador_usernames)} embaixadores: {ambassador_usernames}")
    
    current_date = start_date
    while current_date.date() < end_date.date():
        
        day_str = current_date.strftime('%Y-%m-%d')
        logger.info(f"Verificando TWEETS para data: {day_str}...")

        day_start = current_date
        day_end = current_date + timedelta(days=1)
        
        messages_in_day = []
        try:
            async for message in client.iter_messages(
                tweets_group,
                offset_date=day_end,
                reverse=True
            ):
                if message.date < day_start:
                    break
                messages_in_day.append(message)
        
        except Exception as e:
            logger.error(f"Erro ao buscar mensagens de tweets no dia {day_str}: {e}")
            current_date += timedelta(days=1)
            continue

        if not messages_in_day:
            current_date += timedelta(days=1)
            continue

        logger.info(f"-> Encontradas {len(messages_in_day)} mensagens de TWEETS em {day_str}")

        # Processar mensagens procurando por tweets
        for message in messages_in_day:
            if not message.text:
                continue
                
            tweet_info = extract_twitter_info_from_url(message.text)
            if tweet_info:
                # Verificar se é tweet de embaixador
                if tweet_info['username'].lower() in ambassador_usernames_lower:
                    logger.info(f"Tweet de embaixador encontrado: {tweet_info['username']}/status/{tweet_info['tweet_id']}")
                    await save_tweet_to_database(tweet_info, message.date, message.from_user.id)

        current_date += timedelta(days=1)

async def main():
    """Função principal que orquestra a busca e o cálculo do histórico."""
    mode_text = "🔍 MODO DRY RUN (APENAS ANÁLISE)" if DRY_RUN else "✅ MODO PRODUÇÃO"
    logger.info(f"--- Iniciando Script de População de Histórico do Telegram (ACTIVITY + TWEETS) - {mode_text} ---")

    if not all([API_ID, API_HASH, SCORING_GROUP_ID]):
        logger.critical("Variáveis de ambiente TELEGRAM_API_ID, TELEGRAM_API_HASH ou SCORING_GROUP_ID não encontradas. Abortando.")
        return

    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)

    async with client:
        logger.info("Cliente Telegram conectado.")

        # 1. Processar SCORING GROUP para activity scores
        try:
            scoring_group = await client.get_entity(SCORING_GROUP_ID)
            logger.info(f"Grupo de scoring encontrado: '{scoring_group.title}'")
            
            ambassador_ids = await get_all_ambassador_telegram_ids()
            if not ambassador_ids:
                logger.warning("Nenhum ID de embaixador encontrado no banco de dados.")
            else:
                await process_scoring_group(client, scoring_group, ambassador_ids, START_DATE, END_DATE)
                
        except Exception as e:
            logger.error(f"Erro ao processar grupo de scoring {SCORING_GROUP_ID}: {e}")

        # 2. Processar TELEGRAM GROUP para tweets
        try:
            tweets_group = await client.get_entity(TELEGRAM_GROUP_ID)
            logger.info(f"Grupo de tweets encontrado: '{tweets_group.title}'")
            
            await process_tweets_group(client, tweets_group, START_DATE, END_DATE)
                
        except Exception as e:
            logger.error(f"Erro ao processar grupo de tweets {TELEGRAM_GROUP_ID}: {e}")

    # Exibir estatísticas finais
    print_final_statistics()
    logger.info("--- Script de População de Histórico Concluído ---")


if __name__ == "__main__":
    # Garante que o loop de eventos asyncio seja gerenciado corretamente
    # no Windows para evitar RuntimeError.
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main()) 