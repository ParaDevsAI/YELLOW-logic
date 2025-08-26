import asyncio
import os
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
import httpx

import sys
sys.path.append('..')
from author_manager import get_supabase_client, initialize_supabase_client

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()
SCORING_BONUS_MULTIPLIER = float(os.getenv("SCORING_BONUS_MULTIPLIER", 1.25))

DRY_RUN = True

BASE_DIR = Path("data")
SCORING_DIR = BASE_DIR / "scoring_group"
TWEETS_DIR = BASE_DIR / "tweets_group"

TWITTER_URL_PATTERN = re.compile(r'https?://(?:www\.)?(?:x|twitter)\.com/(\w+)/status/(\d+)')

stats = {
    'activity_records': 0,
    'tweets_found': 0,
    'days_processed': 0,
    'messages_processed': 0,
    'ambassadors_with_activity': set(),
    'tweet_authors_found': set()
}

ambassadors_cache = {
    'telegram_ids': set(),
    'twitter_usernames': set(),
    'twitter_username_to_id': {}
}

def load_ambassadors_data():
    logger.info("Carregando dados dos embaixadores...")
    
    supabase = initialize_supabase_client()
    if not supabase:
        logger.error("Falha ao conectar com Supabase")
        return False
    
    try:
        response = supabase.table('authors').select('telegram_id, twitter_username, twitter_id').execute()
        
        if not response.data:
            logger.warning("Nenhum embaixador encontrado na base de dados")
            return False
        
        for author in response.data:
            telegram_id = author.get('telegram_id')
            twitter_username = author.get('twitter_username')
            twitter_id = author.get('twitter_id')
            
            if telegram_id and telegram_id > 0:
                ambassadors_cache['telegram_ids'].add(telegram_id)
            
            if twitter_username:
                ambassadors_cache['twitter_usernames'].add(twitter_username.lower())
                if twitter_id:
                    ambassadors_cache['twitter_username_to_id'][twitter_username.lower()] = twitter_id
        
        logger.info(f"Carregados {len(ambassadors_cache['telegram_ids'])} embaixadores (Telegram)")
        logger.info(f"Carregados {len(ambassadors_cache['twitter_usernames'])} embaixadores (Twitter)")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao carregar embaixadores: {e}")
        return False

def get_session_from_datetime(dt: datetime) -> str:
    hour = dt.hour
    session_start = (hour // 3) * 3
    session_end = session_start + 3
    return f"{session_start:02d}-{session_end:02d}"

def process_activity_scores(date_str: str, messages: list):
    user_sessions = {}
    
    logger.info(f"Processando {len(messages)} mensagens para {date_str}")
    logger.info("Aplicando lógica de pontuação com sessões de 3 horas...")
    
    for message in messages:
        sender_id = message.get('sender_id')
        if not sender_id or sender_id not in ambassadors_cache['telegram_ids']:
            continue
        
        message_date = datetime.fromisoformat(message['date'].replace('Z', '+00:00'))
        session_id = get_session_from_datetime(message_date)
        
        if sender_id not in user_sessions:
            user_sessions[sender_id] = {}
        
        if session_id not in user_sessions[sender_id]:
            user_sessions[sender_id][session_id] = {
                'messages': 0,
                'score': 0.0,
                'last_score': 0.0
            }
        
        session_state = user_sessions[sender_id][session_id]
        
        if session_state['messages'] >= 10:
            continue
        
        if session_state['messages'] == 0:
            current_msg_score = 1.0
        else:
            current_msg_score = session_state['last_score'] * SCORING_BONUS_MULTIPLIER
        
        session_state['messages'] += 1
        session_state['score'] += current_msg_score
        session_state['last_score'] = current_msg_score
    
    logger.info(f"Calculando scores finais para {len(user_sessions)} usuários...")
    
    for user_id, sessions in user_sessions.items():
        total_day_score = sum(session_data['score'] for session_data in sessions.values())
        
        intervals_details_json = {
            session: {'messages': data['messages'], 'score': data['score']}
            for session, data in sessions.items()
        }
        
        save_activity_to_supabase(user_id, date_str, total_day_score, intervals_details_json)
    
    logger.info(f"{date_str}: {len(user_sessions)} usuários com atividade processados")

def save_activity_to_supabase(user_id: int, activity_date: str, total_score: float, details_json: dict):
    if DRY_RUN:
        logger.info(f"[DRY RUN] Activity seria salva: user {user_id} em {activity_date} com score {total_score:.2f}")
        return
    
    supabase = initialize_supabase_client()
    if not supabase:
        logger.error("Falha ao obter cliente Supabase")
        return
    
    try:
        existing = supabase.table('user_activity').select('user_id').eq('user_id', user_id).eq('activity_date', activity_date).execute()
        if existing.data:
            logger.warning(f"DUPLICATA - Activity já existe para user {user_id} em {activity_date}")
            return
    except Exception as e:
        logger.error(f"Erro ao verificar duplicata: {e}")
        return
    
    record_to_save = {
        'user_id': user_id,
        'activity_date': activity_date,
        'total_day_score': total_score,
        'intervals_details_json': details_json
    }
    
    try:
        supabase.table('user_activity').upsert(record_to_save).execute()
        stats['activity_records'] += 1
        logger.info(f"Activity salva: user {user_id} em {activity_date} com score {total_score:.2f}")
    except Exception as e:
        logger.error(f"Erro ao salvar activity: {e}")

def process_tweet_links(date_str: str, messages: list):
    logger.info(f"Processando {len(messages)} mensagens para tweets em {date_str}")
    logger.info("Buscando links de tweets de embaixadores...")
    
    for message in messages:
        text = message.get('text', '')
        match = TWITTER_URL_PATTERN.search(text)
        
        if match:
            username = match.group(1)
            tweet_id = match.group(2)
            
            if username.lower() in ambassadors_cache['twitter_usernames']:
                logger.info(f"Tweet de embaixador encontrado: {username}/status/{tweet_id}")
                
                save_tweet_to_supabase(username, tweet_id, message['date'])
    
    logger.info(f"{date_str}: {stats['tweets_found']} tweets processados")

def save_tweet_to_supabase(username: str, tweet_id: str, message_date: str):
    author_twitter_id = ambassadors_cache['twitter_username_to_id'].get(username.lower())
    if not author_twitter_id:
        logger.warning(f"Author ID não encontrado para {username}")
        return
    
    if DRY_RUN:
        logger.info(f"[DRY RUN] Tweet seria salvo: {username}/status/{tweet_id}")
        return
    
    supabase = initialize_supabase_client()
    if not supabase:
        logger.error("Falha ao obter cliente Supabase")
        return
    
    try:
        response = supabase.table('tweets').select('tweet_id').eq('tweet_id', tweet_id).execute()
        if response.data:
            logger.warning(f"DUPLICATA - Tweet {tweet_id} já existe")
            return
    except Exception as e:
        logger.error(f"Erro ao verificar duplicata: {e}")
        return
    
    api_key = os.getenv("TWITTER_API_KEY")
    if not api_key:
        logger.error("TWITTER_API_KEY não encontrada")
        return
    
    try:
        url = "https://api.twitterapi.io/twitter/tweets"
        headers = {"X-API-Key": api_key}
        params = {"tweet_ids": tweet_id}
        
        logger.info(f"Buscando dados do tweet {tweet_id} via API...")
        with httpx.Client(timeout=30.0) as client:
            api_response = client.get(url, headers=headers, params=params)
        
        if api_response.status_code == 200:
            api_data = api_response.json()
            tweets = api_data.get('tweets', [])
            if tweets:
                tweet_data = tweets[0]
                
                media_url = None
                if tweet_data.get('extendedEntities', {}).get('media'):
                    media_url = tweet_data['extendedEntities']['media'][0].get('media_url_https')
                
                tweet_record = {
                    'tweet_id': tweet_data.get('id'),
                    'author_id': author_twitter_id,
                    'twitter_url': tweet_data.get('twitterUrl'),
                    'text': tweet_data.get('text'),
                    'createdat': tweet_data.get('createdAt'),
                    'views': tweet_data.get('viewCount'),
                    'likes': tweet_data.get('likeCount'),
                    'retweets': tweet_data.get('retweetCount'),
                    'replies': tweet_data.get('replyCount'),
                    'quotes': tweet_data.get('quoteCount'),
                    'bookmarks': tweet_data.get('bookmarkCount'),
                    'content_type': tweet_data.get('extendedEntities', {}).get('media', [{}])[0].get('type'),
                    'media_url': media_url
                }
                
                supabase.table('tweets').upsert(tweet_record).execute()
                
                entities = tweet_data.get('entities', {})
                if entities:
                    supabase.table('tweet_entities').delete().eq('tweet_id', tweet_data.get('id')).execute()
                    
                    entities_to_insert = []
                    if entities.get('user_mentions'):
                        for mention in entities['user_mentions']:
                            entities_to_insert.append({
                                'tweet_id': tweet_data.get('id'),
                                'entity_type': 'user_mention',
                                'text_in_tweet': mention.get('screen_name'),
                                'mentioned_user_id': mention.get('id_str')
                            })
                    
                    if entities.get('hashtags'):
                        for hashtag in entities['hashtags']:
                            entities_to_insert.append({
                                'tweet_id': tweet_data.get('id'),
                                'entity_type': 'hashtag',
                                'text_in_tweet': hashtag.get('text')
                            })
                    
                    if entities.get('urls'):
                        for url_entity in entities['urls']:
                            entities_to_insert.append({
                                'tweet_id': tweet_data.get('id'),
                                'entity_type': 'url',
                                'text_in_tweet': url_entity.get('url'),
                                'expanded_url': url_entity.get('expanded_url')
                            })
                    
                    if entities_to_insert:
                        supabase.table('tweet_entities').insert(entities_to_insert).execute()
                
                stats['tweets_found'] += 1
                logger.info(f"Tweet salvo: {username}/status/{tweet_id}")
            else:
                logger.warning(f"API não retornou dados para o tweet {tweet_id}")
        else:
            logger.error(f"Erro na API: {api_response.status_code}")
            
    except Exception as e:
        logger.error(f"Erro ao salvar tweet: {e}")

def process_scoring_group():
    if not SCORING_DIR.exists():
        logger.warning(f"Diretório de scoring não encontrado: {SCORING_DIR}")
        return
    
    logger.info(f"Processando arquivos de scoring em: {SCORING_DIR}")
    
    json_files = list(SCORING_DIR.glob("*.json"))
    if not json_files:
        logger.info("Nenhum arquivo JSON de scoring encontrado")
        return
    
    logger.info(f"Encontrados {len(json_files)} arquivos de scoring para processar")
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            date_str = data.get('date', 'unknown')
            messages = data.get('messages', [])
            
            if messages:
                logger.info(f"Processando {len(messages)} mensagens de {date_str}")
                process_activity_scores(date_str, messages)
                stats['days_processed'] += 1
                stats['messages_processed'] += len(messages)
            else:
                logger.warning(f"Arquivo {json_file.name} não contém mensagens válidas")
                
        except Exception as e:
            logger.error(f"Erro ao processar arquivo {json_file.name}: {e}")

def process_tweets_group():
    if not TWEETS_DIR.exists():
        logger.warning(f"Diretório de tweets não encontrado: {TWEETS_DIR}")
        return
    
    logger.info(f"Processando arquivos de tweets em: {TWEETS_DIR}")
    
    json_files = list(TWEETS_DIR.glob("*.json"))
    if not json_files:
        logger.info("Nenhum arquivo JSON de tweets encontrado")
        return
    
    logger.info(f"Encontrados {len(json_files)} arquivos de tweets para processar")
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            date_str = data.get('date', 'unknown')
            messages = data.get('messages', [])
            
            if messages:
                logger.info(f"Processando {len(messages)} mensagens de tweets de {date_str}")
                process_tweet_links(date_str, messages)
                stats['days_processed'] += 1
                stats['messages_processed'] += len(messages)
            else:
                logger.warning(f"Arquivo {json_file.name} não contém mensagens válidas")
                
        except Exception as e:
            logger.error(f"Erro ao processar arquivo {json_file.name}: {e}")

def print_final_summary():
    logger.info("\n" + "=" * 60)
    logger.info("RESUMO FINAL DO PROCESSAMENTO")
    logger.info("=" * 60)
    logger.info(f"Dias processados: {stats['days_processed']}")
    logger.info(f"Mensagens processadas: {stats['messages_processed']}")
    logger.info(f"Activities salvas: {stats['activity_records']}")
    logger.info(f"Tweets encontrados: {stats['tweets_found']}")
    logger.info(f"Embaixadores com atividade: {len(stats['ambassadors_with_activity'])}")
    logger.info(f"Autores de tweets encontrados: {len(stats['tweet_authors_found'])}")
    logger.info("=" * 60)

def main():
    logger.info("--- Script de Processamento de Mensagens Baixadas Iniciado ---")
    
    if not load_ambassadors_data():
        logger.error("Falha ao carregar dados dos embaixadores. Abortando.")
        return
    
    logger.info("Iniciando processamento de mensagens...")
    
    process_scoring_group()
    process_tweets_group()
    
    print_final_summary()
    
    logger.info("--- Script de Processamento Concluído ---")

if __name__ == "__main__":
    main() 