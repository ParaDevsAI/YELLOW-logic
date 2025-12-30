#!/usr/bin/env python3

import asyncio
import os
import json
import logging
import re
import time
import httpx
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, List, Set, Any

from author_manager import get_supabase_client, initialize_supabase_client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

class YellowPipeline:
    def __init__(self):
        self.cache = {
            'scoring_messages': {},
            'tweets_messages': {},
            'ambassadors_cache': {
                'telegram_ids': set(),
                'twitter_usernames': set(),
                'twitter_username_to_id': {}
            }
        }
        self.stats = {
            'messages_downloaded': 0,
            'activity_records': 0,
            'tweets_found': 0,
            'threads_identified': 0,
            'cross_engagements_captured': 0,
            'leaderboard_generated': False
        }
        
        self.SCORING_GROUP_ID = int(os.getenv("SCORING_GROUP_ID", "-1001581599914"))
        self.TWEETS_GROUP_ID = int(os.getenv("TWEETS_GROUP_ID", "-1002330680602"))
        # Process today's data up to the moment the script is run
        self.START_DATE = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        self.END_DATE = datetime.now(timezone.utc)
        
    async def run_pipeline(self):
        logger.info("INICIANDO YELLOW PIPELINE COMPLETO")
        logger.info("=" * 60)
        
        try:
            # Check if Telegram session exists
            session_name = os.getenv("TELEGRAM_SESSION_NAME", "new_one")
            session_path = Path(f"telegram_tools/{session_name}.session")
            telegram_available = session_path.exists()
            
            if telegram_available:
                logger.info("INICIANDO ETAPA 1: DOWNLOAD TELEGRAM MESSAGES")
                await self.download_telegram_messages()
                logger.info("ETAPA 1 CONCLUÍDA: DOWNLOAD TELEGRAM MESSAGES")
                
                logger.info("INICIANDO ETAPA 2: PROCESS DOWNLOADED MESSAGES")
                await self.process_downloaded_messages()
                logger.info("ETAPA 2 CONCLUÍDA: PROCESS DOWNLOADED MESSAGES")
            else:
                logger.warning("⏭️ ETAPA 1-2 PULADAS: Sessão Telegram não encontrada")
                logger.warning(f"   Arquivo esperado: {session_path}")
                logger.info("   Carregando embaixadores para próximas etapas...")
                await self.load_ambassadors_data()
            
            logger.info("INICIANDO ETAPA 2.5: SCAN TWITTER TIMELINES")
            await self.scan_twitter_timelines()
            logger.info("ETAPA 2.5 CONCLUÍDA: SCAN TWITTER TIMELINES")
            
            logger.info("INICIANDO ETAPA 3: CROSS ENGAGEMENT TRACKER")
            await self.cross_engagement_tracker()
            logger.info("ETAPA 3 CONCLUÍDA: CROSS ENGAGEMENT TRACKER")
            
            logger.info("INICIANDO ETAPA 4: THREAD IDENTIFIER")
            await self.thread_identifier()
            logger.info("ETAPA 4 CONCLUÍDA: THREAD IDENTIFIER")
            
            logger.info("INICIANDO ETAPA 5: GENERATE LEADERBOARD")
            await self.generate_leaderboard()
            logger.info("ETAPA 5 CONCLUÍDA: GENERATE LEADERBOARD")
            
            self.print_final_summary()
            
        except Exception as e:
            logger.error(f"ERRO CRÍTICO NO PIPELINE: {e}")
            raise
    
    async def download_telegram_messages(self):
        logger.info("ETAPA 1: DOWNLOAD TELEGRAM MESSAGES")
        logger.info(f"PERÍODO: {self.START_DATE.strftime('%d/%m/%Y')} até {self.END_DATE.strftime('%d/%m/%Y')}")
        logger.info("Iniciando download de mensagens do Telegram...")
        
        try:
            from telethon import TelegramClient
            from telethon.tl.types import PeerChannel
            
            api_id = os.getenv("TELEGRAM_API_ID")
            api_hash = os.getenv("TELEGRAM_API_HASH")
            session_name = os.getenv("TELEGRAM_SESSION_NAME", "new_one.session")
            
            logger.info(f"Configurações carregadas - API ID: {api_id}, Session: {session_name}")
            
            if not api_id or not api_hash:
                logger.error("ERRO: TELEGRAM_API_ID ou TELEGRAM_API_HASH não configurados")
                return
            
            session_path = Path(f"telegram_tools/{session_name}.session")
            if not session_path.exists():
                logger.error(f"ERRO: Sessão não encontrada: telegram_tools/{session_name}.session")
                logger.error("Execute primeiro: python telegram_tools/reauthenticate.py")
                return
            
            logger.info(f"Sessão encontrada em: {session_path}")
            
            session_full_path = f"telegram_tools/{session_name}"
            client = TelegramClient(session_full_path, api_id, api_hash)
            
            try:
                logger.info("Conectando ao Telegram...")
                await client.connect()
                if not await client.is_user_authorized():
                    logger.error("ERRO: Sessão expirada! Execute reauthenticate.py primeiro")
                    await client.disconnect()
                    return
                logger.info("Cliente Telegram conectado com sessão existente")
            except Exception as e:
                logger.error(f"ERRO ao conectar: {e}")
                return
            
            logger.info("Cliente Telegram conectado com sucesso")
            
            logger.info("BAIXANDO MENSAGENS DO GRUPO DE SCORING")
            scoring_group = PeerChannel(self.SCORING_GROUP_ID)
            
            date_str = self.START_DATE.strftime('%Y-%m-%d')
            logger.info(f"Baixando mensagens de scoring para {date_str} (00:00 até 23:59)")
            
            logger.info("Buscando mensagens do grupo de scoring...")
            messages = await client.get_messages(scoring_group, limit=1000)
            logger.info(f"Total de mensagens encontradas no grupo: {len(messages)}")
            
            day_messages = []
            for message in messages:
                if message.date and message.sender_id:
                    message_date = message.date.replace(tzinfo=timezone.utc)
                    if self.START_DATE <= message_date <= self.END_DATE:
                        day_messages.append({
                            'sender_id': message.sender_id,
                            'text': message.text or '',
                            'date': message_date.isoformat()
                        })
            
            self.cache['scoring_messages'][date_str] = day_messages
            logger.info(f"Processadas {len(day_messages)} mensagens de scoring para o dia {date_str}")
            self.stats['messages_downloaded'] += len(day_messages)
            
            logger.info("BAIXANDO MENSAGENS DO GRUPO DE TWEETS")
            tweets_group = PeerChannel(self.TWEETS_GROUP_ID)
            
            logger.info(f"Baixando mensagens de tweets para {date_str} (00:00 até 23:59)")
            
            logger.info("Buscando mensagens do grupo de tweets...")
            messages = await client.get_messages(tweets_group, limit=1000)
            logger.info(f"Total de mensagens encontradas no grupo: {len(messages)}")
            
            day_messages = []
            for message in messages:
                if message.date and message.sender_id:
                    message_date = message.date.replace(tzinfo=timezone.utc)
                    if self.START_DATE <= message_date <= self.END_DATE:
                        day_messages.append({
                            'sender_id': message.sender_id,
                            'text': message.text or '',
                            'date': message_date.isoformat()
                        })
            
            self.cache['tweets_messages'][date_str] = day_messages
            logger.info(f"Processadas {len(day_messages)} mensagens de tweets para o dia {date_str}")
            self.stats['messages_downloaded'] += len(day_messages)
            
            await client.disconnect()
            logger.info("Cliente Telegram desconectado")
            logger.info(f"Download concluído: {self.stats['messages_downloaded']} mensagens em cache")
            
        except ImportError:
            logger.error("ERRO: Telethon não instalado. Execute: pip install telethon")
        except Exception as e:
            logger.error(f"ERRO no download: {e}")
    
    async def process_downloaded_messages(self):
        logger.info("ETAPA 2: PROCESS DOWNLOADED MESSAGES")
        logger.info("Iniciando processamento de mensagens baixadas...")
        
        logger.info("Carregando dados dos embaixadores...")
        await self.load_ambassadors_data()
        
        logger.info("Processando activity scores...")
        for date_str, messages in self.cache['scoring_messages'].items():
            await self.process_activity_scores(date_str, messages)
        
        logger.info("Processando tweet links...")
        for date_str, messages in self.cache['tweets_messages'].items():
            await self.process_tweet_links(date_str, messages)
        
        logger.info(f"Processamento concluído: {self.stats['activity_records']} activities, {self.stats['tweets_found']} tweets")
    
    async def load_ambassadors_data(self):
        logger.info("Carregando dados dos embaixadores...")
        
        supabase = await get_supabase_client()
        if not supabase:
            logger.error("ERRO: Falha ao conectar com Supabase")
            return False
        
        try:
            logger.info("Executando query para buscar embaixadores...")
            response = await asyncio.to_thread(
                supabase.table('authors').select('telegram_id, twitter_username, twitter_id').execute
            )
            
            if response.data:
                logger.info(f"Encontrados {len(response.data)} embaixadores na base")
                for author in response.data:
                    telegram_id = author.get('telegram_id')
                    twitter_username = author.get('twitter_username')
                    twitter_id = author.get('twitter_id')
                    
                    if telegram_id and telegram_id > 0:
                        self.cache['ambassadors_cache']['telegram_ids'].add(telegram_id)
                    
                    if twitter_username:
                        self.cache['ambassadors_cache']['twitter_usernames'].add(twitter_username.lower())
                        if twitter_id:
                            self.cache['ambassadors_cache']['twitter_username_to_id'][twitter_username.lower()] = twitter_id
                
                logger.info(f"Total de {len(self.cache['ambassadors_cache']['telegram_ids'])} embaixadores carregados com sucesso")
                return True
                
        except Exception as e:
            logger.error(f"ERRO ao carregar embaixadores: {e}")
            return False
    
    async def process_activity_scores(self, date_str: str, messages: List[Dict]):
        user_sessions = {}
        
        logger.info(f"Processando {len(messages)} mensagens para {date_str}")
        logger.info("Aplicando lógica de pontuação com sessões de 3 horas...")
        
        for message in messages:
            sender_id = message.get('sender_id')
            if not sender_id or sender_id not in self.cache['ambassadors_cache']['telegram_ids']:
                continue
            
            message_date = datetime.fromisoformat(message['date'].replace('Z', '+00:00'))
            session_id = self.get_session_from_datetime(message_date)
            
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
                current_msg_score = session_state['last_score'] * 1.25
            
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
            
            await self.save_activity_to_supabase(user_id, date_str, total_day_score, intervals_details_json)
        
        logger.info(f"{date_str}: {len(user_sessions)} usuários com atividade processados")
    
    def get_session_from_datetime(self, dt: datetime) -> str:
        hour = dt.hour
        session_start = (hour // 3) * 3
        session_end = session_start + 3
        return f"{session_start:02d}-{session_end:02d}"
    
    async def save_activity_to_supabase(self, user_id: int, activity_date: str, total_score: float, details_json: dict):
        supabase = await get_supabase_client()
        if not supabase:
            logger.error("ERRO: Falha ao obter cliente Supabase")
            return
        
        try:
            existing = await asyncio.to_thread(
                supabase.table('user_activity').select('user_id').eq('user_id', user_id).eq('activity_date', activity_date).execute
            )
            if existing.data:
                logger.warning(f"DUPLICATA - Activity já existe para user {user_id} em {activity_date}")
                return
        except Exception as e:
            logger.error(f"ERRO ao verificar duplicata: {e}")
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
            self.stats['activity_records'] += 1
            logger.info(f"Activity salva: user {user_id} em {activity_date} com score {total_score:.2f}")
        except Exception as e:
            logger.error(f"ERRO ao salvar activity: {e}")
    
    async def process_tweet_links(self, date_str: str, messages: List[Dict]):
        tweet_pattern = re.compile(r'https?://(?:www\.)?(?:x|twitter)\.com/(\w+)/status/(\d+)')
        
        logger.info(f"Processando {len(messages)} mensagens para tweets em {date_str}")
        logger.info("Buscando links de tweets de embaixadores...")
        
        for message in messages:
            text = message.get('text', '')
            match = tweet_pattern.search(text)
            
            if match:
                username = match.group(1)
                tweet_id = match.group(2)
                
                if username.lower() in self.cache['ambassadors_cache']['twitter_usernames']:
                    logger.info(f"Tweet de embaixador encontrado: {username}/status/{tweet_id}")
                    
                    await self.save_tweet_to_supabase(username, tweet_id, message['date'])
        
        logger.info(f"{date_str}: {self.stats['tweets_found']} tweets processados")
    
    async def save_tweet_to_supabase(self, username: str, tweet_id: str, message_date: str):
        author_twitter_id = self.cache['ambassadors_cache']['twitter_username_to_id'].get(username.lower())
        if not author_twitter_id:
            logger.warning(f"Author ID não encontrado para {username}")
            return
        
        api_key = os.getenv("TWITTER_API_KEY")
        if not api_key:
            logger.error("ERRO: TWITTER_API_KEY não encontrada")
            return
        
        try:
            url = "https://api.twitterapi.io/twitter/tweets"
            headers = {"X-API-Key": api_key}
            params = {"tweet_ids": tweet_id}
            
            logger.info(f"Buscando dados do tweet {tweet_id} via API...")
            async with httpx.AsyncClient(timeout=30.0) as client:
                api_response = await client.get(url, headers=headers, params=params)
            
            if api_response.status_code == 200:
                api_data = api_response.json()
                tweets = api_data.get('tweets', [])
                if tweets:
                    await self.save_tweet_json_to_db(tweets[0], author_twitter_id)
                    self.stats['tweets_found'] += 1
                    logger.info(f"Tweet salvo: {username}/status/{tweet_id}")
                else:
                    logger.warning(f"API não retornou dados para o tweet {tweet_id}")
            else:
                logger.error(f"ERRO na API: {api_response.status_code}")
                
        except Exception as e:
            logger.error(f"ERRO ao salvar tweet: {e}")

    async def scan_twitter_timelines(self):
        logger.info("ETAPA 2.5: SCAN TWITTER TIMELINES (DIRECT FROM X)")
        
        if not self.cache['ambassadors_cache']['twitter_usernames']:
            await self.load_ambassadors_data()
            
        api_key = os.getenv("TWITTER_API_KEY")
        if not api_key:
            logger.error("ERRO: TWITTER_API_KEY não encontrada")
            return

        # Let's look back 2 days to catch any missed tweets
        start_date = datetime.now(timezone.utc) - timedelta(days=2)
        end_date = self.END_DATE # from __init__, it is datetime.now(timezone.utc)

        logger.info(f"Scanning Twitter timelines from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

        yellow_keywords = [
            "yellow", "$yellow", "@yellow", "yellowmedia", 
            "alexisyellow", "clearport", "cleargate"
        ]
        
        usernames = list(self.cache['ambassadors_cache']['twitter_username_to_id'].keys())
        logger.info(f"Iniciando scan de {len(usernames)} perfis no X...")
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            for i, username in enumerate(usernames):
                try:
                    author_id = self.cache['ambassadors_cache']['twitter_username_to_id'].get(username)
                    if not author_id:
                        logger.warning(f"Could not find author_id for {username}")
                        continue
                    
                    logger.info(f"[{i+1}/{len(usernames)}] Scanning timeline for @{username}...")

                    cursor = None
                    tweets_found_for_user = 0
                    
                    while True:
                        url = "https://api.twitterapi.io/twitter/user/last_tweets"
                        headers = {"X-API-Key": api_key}
                        params = {"userName": username}
                        if cursor:
                            params['cursor'] = cursor

                        response = await client.get(url, headers=headers, params=params)
                        
                        if response.status_code == 429:
                            logger.warning(f"Rate limited on @{username}. Waiting 60 seconds...")
                            await asyncio.sleep(60)
                            continue

                        if response.status_code != 200:
                            logger.warning(f"Falha ao buscar timeline de {username}: {response.status_code}")
                            break
                        
                        data = response.json()
                        tweets_data = data.get('data', {})
                        if not tweets_data:
                            logger.info(f"No data object in response for @{username}")
                            break

                        tweets = tweets_data.get('tweets', [])
                        
                        if not tweets:
                            logger.info(f"No more tweets for @{username}")
                            break

                        stop_processing = False
                        for tweet in tweets:
                            created_at_str = tweet.get('createdAt', '')
                            if not created_at_str:
                                continue

                            try:
                                tweet_date = datetime.strptime(created_at_str, '%a %b %d %H:%M:%S %z %Y')
                            except ValueError:
                                logger.warning(f"Could not parse date: {created_at_str}")
                                continue

                            if tweet_date < start_date:
                                stop_processing = True
                                break

                            if start_date <= tweet_date <= end_date:
                                text = tweet.get('text', '').lower()
                                if any(k in text for k in yellow_keywords):
                                    saved = await self.save_tweet_json_to_db(tweet, author_id)
                                    if saved:
                                        tweets_found_for_user += 1
                        
                        if stop_processing:
                            logger.info(f"Reached tweets older than {start_date.strftime('%Y-%m-%d')} for @{username}")
                            break

                        next_cursor = data.get('next_cursor')
                        if data.get('has_next_page') and next_cursor:
                            cursor = next_cursor
                            logger.info(f"Paginating for @{username}...")
                            await asyncio.sleep(1)
                        else:
                            break
                    
                    if tweets_found_for_user > 0:
                        self.stats['tweets_found'] += tweets_found_for_user
                        logger.info(f"Found and saved {tweets_found_for_user} new Yellow-related tweets for @{username}")

                except Exception as e:
                    logger.error(f"Erro ao processar timeline de {username}: {e}")

    async def save_tweet_json_to_db(self, tweet_data: dict, author_id: str) -> bool:
        supabase = await get_supabase_client()
        if not supabase:
            return False

        tweet_id = tweet_data.get('id')
        if not tweet_id:
            return False
        
        # Check duplicate
        try:
            response = await asyncio.to_thread(
                supabase.table('tweets').select('tweet_id').eq('tweet_id', tweet_id).execute
            )
            if response.data:
                return False # Already exists
        except Exception:
            pass # Continue to upsert

        created_at_str = tweet_data.get('createdAt', '')
        created_at_iso = None
        if created_at_str:
            try:
                # Parse Twitter date format
                created_at = datetime.strptime(created_at_str, '%a %b %d %H:%M:%S %z %Y')
                created_at_iso = created_at.isoformat()
            except (ValueError, TypeError):
                logger.warning(f"Could not parse date '{created_at_str}' for tweet {tweet_id}. Using current time.")
                created_at_iso = datetime.now(timezone.utc).isoformat()
        else:
            created_at_iso = datetime.now(timezone.utc).isoformat()

        media_url = None
        if tweet_data.get('extendedEntities', {}).get('media'):
            media_url = tweet_data['extendedEntities']['media'][0].get('media_url_https')

        media_list = tweet_data.get('extendedEntities', {}).get('media', []) or []
        has_media = bool(media_list)
        has_video = any(m.get('type') == 'video' for m in media_list)
        
        content_type = 'text_only'
        if has_video:
            content_type = 'video'
        elif has_media:
            content_type = 'image'

        tweet_record = {
            'tweet_id': tweet_id,
            'author_id': author_id,
            'twitter_url': f"https://twitter.com/{tweet_data.get('author', {}).get('userName', 'unknown')}/status/{tweet_id}",
            'text': tweet_data.get('text'),
            'createdat': created_at_iso,
            'views': tweet_data.get('viewCount', 0) or 0,
            'likes': tweet_data.get('likeCount', 0) or 0,
            'retweets': tweet_data.get('retweetCount', 0) or 0,
            'replies': tweet_data.get('replyCount', 0) or 0,
            'quotes': tweet_data.get('quoteCount', 0) or 0,
            'bookmarks': tweet_data.get('bookmarkCount', 0) or 0,
            'content_type': content_type,
            'media_url': media_url,
            'is_thread_checked': False, # Will be checked by thread identifier
        }
        
        try:
            await asyncio.to_thread(
                supabase.table('tweets').upsert(tweet_record).execute
            )
            
            entities = tweet_data.get('entities', {})
            if entities:
                # Clean old entities if any (though we checked for duplicate tweet, upsert might update)
                await asyncio.to_thread(
                    supabase.table('tweet_entities').delete().eq('tweet_id', tweet_id).execute
                )
                
                entities_to_insert = []
                if entities.get('user_mentions'):
                    for mention in entities['user_mentions']:
                        entities_to_insert.append({
                            'tweet_id': tweet_id,
                            'entity_type': 'user_mention',
                            'text_in_tweet': mention.get('screen_name'),
                            'mentioned_user_id': mention.get('id_str')
                        })
                
                if entities.get('hashtags'):
                    for hashtag in entities['hashtags']:
                        entities_to_insert.append({
                            'tweet_id': tweet_id,
                            'entity_type': 'hashtag',
                            'text_in_tweet': hashtag.get('text')
                        })
                
                if entities.get('urls'):
                    for url_entity in entities['urls']:
                        entities_to_insert.append({
                            'tweet_id': tweet_id,
                            'entity_type': 'url',
                            'text_in_tweet': url_entity.get('url'),
                            'expanded_url': url_entity.get('expanded_url')
                        })
                
                if entities_to_insert:
                    await asyncio.to_thread(
                        supabase.table('tweet_entities').insert(entities_to_insert).execute
                    )
            
            logger.info(f"Tweet salvo/atualizado: {tweet_id}")
            return True
            
        except Exception as e:
            logger.error(f"ERRO ao salvar tweet {tweet_id}: {e}")
            return False
    
    async def cross_engagement_tracker(self):
        logger.info("ETAPA 3: CROSS ENGAGEMENT TRACKER")
        logger.info("Executando cross engagement tracker real...")
        
        try:
            from cross_engagement_tracker import main as cross_engagement_main
            
            logger.info("Iniciando cross engagement tracker...")
            result = await cross_engagement_main()
            
            if result and isinstance(result, dict):
                self.stats['cross_engagements_captured'] = result.get('engagements_captured', 0)
            else:
                await self.count_recent_cross_engagements()
            
            logger.info(f"Cross engagement tracker concluído: {self.stats['cross_engagements_captured']} engajamentos capturados")
            
        except Exception as e:
            logger.error(f"Erro no cross engagement tracker: {e}")
            logger.info("Continuando pipeline...")
            await self.count_recent_cross_engagements()
    
    async def count_recent_cross_engagements(self):
        try:
            supabase = await get_supabase_client()
            if not supabase:
                logger.warning("Não foi possível conectar ao Supabase para contar engajamentos")
                return
            
            yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
            today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            
            response = await asyncio.to_thread(
                supabase.table('ambassador_engagements')
                .select('id')
                .gte('created_at', yesterday)
                .lte('created_at', today)
                .execute
            )
            
            if response.data:
                self.stats['cross_engagements_captured'] = len(response.data)
                logger.info(f"Contados {len(response.data)} engajamentos das últimas 24h na base de dados")
            else:
                self.stats['cross_engagements_captured'] = 0
                logger.info("Nenhum engajamento encontrado nas últimas 24h")
                
        except Exception as e:
            logger.error(f"Erro ao contar engajamentos: {e}")
            self.stats['cross_engagements_captured'] = 0
    
    async def thread_identifier(self):
        logger.info("ETAPA 4: THREAD IDENTIFIER")
        logger.info("Executando thread identifier real...")
        
        try:
            import sys
            sys.path.append('telegram_tools')
            from thread_identifier import main as thread_identifier_main
            
            logger.info("Iniciando thread identifier...")
            result = await thread_identifier_main()
            
            if result and isinstance(result, dict):
                self.stats['threads_identified'] = result.get('threads_identified', 0)
            else:
                self.stats['threads_identified'] = 0
            
            logger.info(f"Thread identifier concluído: {self.stats['threads_identified']} threads identificadas")
            
        except Exception as e:
            logger.error(f"Erro no thread identifier: {e}")
            logger.info("Continuando pipeline...")
            self.stats['threads_identified'] = 0
    
    async def generate_leaderboard(self):
        logger.info("ETAPA 5: GENERATE LEADERBOARD")
        logger.info("Executando generate leaderboard real...")
        
        try:
            from generate_leaderboard import main as generate_leaderboard_main
            
            logger.info("Iniciando generate leaderboard...")
            await generate_leaderboard_main()
            
            self.stats['leaderboard_generated'] = True
            logger.info("Leaderboard gerado com sucesso")
            
        except Exception as e:
            logger.error(f"Erro no generate leaderboard: {e}")
            logger.info("Continuando pipeline...")
            self.stats['leaderboard_generated'] = False
    
    def print_final_summary(self):
        logger.info("\n" + "=" * 60)
        logger.info("RESUMO FINAL DO PIPELINE")
        logger.info("=" * 60)
        logger.info(f"Mensagens baixadas: {self.stats['messages_downloaded']}")
        logger.info(f"Activities processadas: {self.stats['activity_records']}")
        logger.info(f"Tweets encontrados: {self.stats['tweets_found']}")
        logger.info(f"Cross engagements capturados: {self.stats['cross_engagements_captured']}")
        logger.info(f"Threads identificadas: {self.stats['threads_identified']}")
        logger.info(f"Leaderboard gerado: {'SIM' if self.stats['leaderboard_generated'] else 'NAO'}")
        logger.info("=" * 60)
        logger.info("PIPELINE CONCLUÍDO COM SUCESSO!")
        logger.info("=" * 60)

async def main():
    pipeline = YellowPipeline()
    await pipeline.run_pipeline()

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())
