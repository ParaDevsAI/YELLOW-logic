"""
telegram_data_processor.py - Processador Inteligente de Dados do Telegram

Este script processa dados do Telegram de forma incremental a cada 6 horas:
1. Baixa mensagens dos grupos do Telegram desde a última execução
2. Processa scores de atividade (SCORING_GROUP_ID)
3. Extrai e valida tweets (TELEGRAM_GROUP_ID)
4. Salva dados incrementalmente em JSONs organizados por data/período
5. Atualiza banco de dados com novos dados

LÓGICA INTELIGENTE:
- Mantém histórico de última execução para processar apenas novos dados
- Organiza JSONs por data: data/YYYY-MM-DD/periodo_HH-HH.json
- Detecta início de novo dia e cria estrutura nova
- Rate limiting respeitoso com APIs
- Logs detalhados e estatísticas

FREQUÊNCIA RECOMENDADA: A cada 6 horas (00:00, 06:00, 12:00, 18:00 UTC)
"""

import asyncio
import os
import json
import logging
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set
from dotenv import load_dotenv

# Imports do Telegram
from telethon import TelegramClient
from telethon.errors import FloodWaitError, PeerIdInvalidError

# Imports do projeto
from bot.author_manager import get_supabase_client

# Configuração de logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Constantes
SCORING_GROUP_ID = -1002712090014  # Grupo de pontuação
TWEETS_GROUP_ID = -1002330680602   # Grupo de tweets
DATA_DIR = Path("telegram_data")
STATE_FILE = "telegram_processor_state.json"

# Configuração de sessão de 3 horas para scoring
SCORING_SESSIONS = [
    "00-03", "03-06", "06-09", "09-12",
    "12-15", "15-18", "18-21", "21-24"
]

class TelegramDataProcessor:
    """Classe principal para processamento inteligente de dados do Telegram."""
    
    def __init__(self):
        self.client = None
        self.supabase = None
        self.api_key = None
        self.state = {
            'last_run': None,
            'last_message_id_scoring': 0,
            'last_message_id_tweets': 0,
            'current_date': None
        }
        self.stats = {
            'messages_processed_scoring': 0,
            'messages_processed_tweets': 0,
            'activity_records_saved': 0,
            'tweets_extracted': 0,
            'tweets_validated': 0,
            'start_time': None,
            'errors': 0
        }
        self.ambassadors_cache = {
            'telegram_ids': set(),
            'twitter_username_to_id': {},
            'twitter_usernames': set()
        }
        
    async def initialize(self):
        """Inicializa conexões e carrega estado anterior."""
        logger.info("🚀 --- Iniciando Processador de Dados do Telegram ---")
        
        load_dotenv()
        
        # Configuração do Telegram
        api_id = os.getenv("TELEGRAM_API_ID")
        api_hash = os.getenv("TELEGRAM_API_HASH")
        self.api_key = os.getenv("TWITTER_API_KEY")
        
        if not all([api_id, api_hash, self.api_key]):
            logger.critical("❌ Variáveis de ambiente necessárias não encontradas.")
            return False
        
        # Inicializa cliente Telegram
        self.client = TelegramClient('telegram_processor_session', api_id, api_hash)
        await self.client.start()
        logger.info("✅ Cliente Telegram inicializado.")
        
        # Inicializa Supabase
        self.supabase = await get_supabase_client()
        if not self.supabase:
            logger.critical("❌ Falha ao conectar com Supabase.")
            return False
        logger.info("✅ Cliente Supabase inicializado.")
        
        # Carrega estado anterior
        await self.load_state()
        
        # Carrega cache de embaixadores
        await self.load_ambassadors_cache()
        
        # Cria estrutura de diretórios
        self.ensure_directory_structure()
        
        self.stats['start_time'] = datetime.now(timezone.utc)
        logger.info("✅ Inicialização concluída com sucesso.")
        return True
    
    async def load_state(self):
        """Carrega estado da última execução."""
        try:
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE, 'r', encoding='utf-8') as f:
                    self.state = json.load(f)
                    logger.info(f"📁 Estado carregado: última execução em {self.state.get('last_run', 'N/A')}")
            else:
                logger.info("🆕 Primeira execução - nenhum estado anterior encontrado.")
                # Se é primeira execução, pega mensagens das últimas 6 horas apenas
                six_hours_ago = datetime.now(timezone.utc) - timedelta(hours=6)
                self.state['last_run'] = six_hours_ago.isoformat()
        except Exception as e:
            logger.error(f"❌ Erro ao carregar estado: {e}")
            
    async def save_state(self):
        """Salva estado atual."""
        try:
            self.state['last_run'] = datetime.now(timezone.utc).isoformat()
            with open(STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=2, ensure_ascii=False)
            logger.info("💾 Estado salvo com sucesso.")
        except Exception as e:
            logger.error(f"❌ Erro ao salvar estado: {e}")
    
    async def load_ambassadors_cache(self):
        """Carrega cache de embaixadores para validação rápida."""
        try:
            response = await asyncio.to_thread(
                self.supabase.table('authors')
                .select('telegram_id, twitter_username, twitter_id')
                .execute
            )
            
            if response.data:
                for author in response.data:
                    # Filtra IDs inválidos
                    telegram_id = author.get('telegram_id')
                    if telegram_id and telegram_id not in [-1, -2]:
                        self.ambassadors_cache['telegram_ids'].add(telegram_id)
                    
                    twitter_username = author.get('twitter_username')
                    twitter_id = author.get('twitter_id')
                    if twitter_username and twitter_id:
                        self.ambassadors_cache['twitter_username_to_id'][twitter_username.lower()] = twitter_id
                        self.ambassadors_cache['twitter_usernames'].add(twitter_username.lower())
                
                logger.info(f"👥 Cache carregado: {len(self.ambassadors_cache['telegram_ids'])} embaixadores Telegram, {len(self.ambassadors_cache['twitter_usernames'])} Twitter.")
            
        except Exception as e:
            logger.error(f"❌ Erro ao carregar cache de embaixadores: {e}")
            
    def ensure_directory_structure(self):
        """Cria estrutura de diretórios para dados."""
        DATA_DIR.mkdir(exist_ok=True)
        logger.info(f"📁 Estrutura de diretórios verificada: {DATA_DIR}")
        
    def get_current_period(self) -> str:
        """Retorna o período atual baseado na hora (6 em 6 horas)."""
        hour = datetime.now(timezone.utc).hour
        if 0 <= hour < 6:
            return "00-06"
        elif 6 <= hour < 12:
            return "06-12"
        elif 12 <= hour < 18:
            return "12-18"
        else:
            return "18-24"
            
    def get_session_from_hour(self, hour: int) -> str:
        """Converte hora para sessão de 3 horas."""
        session_start = (hour // 3) * 3
        session_end = session_start + 3
        return f"{session_start:02d}-{session_end:02d}"
        
    async def download_messages_from_group(self, group_id: int, group_name: str) -> List[Dict]:
        """Baixa mensagens de um grupo desde a última execução."""
        logger.info(f"📥 Baixando mensagens do grupo {group_name}...")
        
        messages = []
        since_date = None
        
        if self.state.get('last_run'):
            since_date = datetime.fromisoformat(self.state['last_run'].replace('Z', '+00:00'))
            logger.info(f"📅 Buscando mensagens desde: {since_date}")
        
        try:
            async for message in self.client.iter_messages(
                group_id,
                offset_date=since_date,
                reverse=True  # Do mais antigo para o mais novo
            ):
                if message.text and message.sender_id:
                    message_data = {
                        'id': message.id,
                        'sender_id': message.sender_id,
                        'text': message.text,
                        'date': message.date.isoformat(),
                        'timestamp': message.date.timestamp()
                    }
                    messages.append(message_data)
                    
                    # Rate limiting
                    if len(messages) % 100 == 0:
                        await asyncio.sleep(1)
                        
            logger.info(f"✅ {len(messages)} mensagens baixadas do grupo {group_name}.")
            return messages
            
        except FloodWaitError as e:
            logger.warning(f"⏳ Rate limit atingido. Aguardando {e.seconds} segundos...")
            await asyncio.sleep(e.seconds)
            return []
        except PeerIdInvalidError:
            logger.error(f"❌ ID de grupo inválido: {group_id}")
            return []
        except Exception as e:
            logger.error(f"❌ Erro ao baixar mensagens do grupo {group_name}: {e}")
            return []
    
    def organize_messages_by_date_and_period(self, messages: List[Dict], group_type: str):
        """Organiza mensagens por data e período, salvando em JSONs."""
        organized = {}
        
        for message in messages:
            message_date = datetime.fromisoformat(message['date'])
            date_str = message_date.strftime('%Y-%m-%d')
            
            if group_type == 'scoring':
                # Para scoring, usa sessões de 3 horas
                period = self.get_session_from_hour(message_date.hour)
            else:
                # Para tweets, usa períodos de 6 horas
                period = self.get_current_period()
            
            key = f"{date_str}_{period}"
            if key not in organized:
                organized[key] = []
            organized[key].append(message)
        
        # Salva cada período em arquivo JSON separado
        for key, msgs in organized.items():
            date_str, period = key.split('_')
            
            # Cria diretório da data se não existir
            date_dir = DATA_DIR / date_str
            date_dir.mkdir(exist_ok=True)
            
            # Nome do arquivo baseado no tipo e período
            filename = f"{group_type}_messages_{period}.json"
            filepath = date_dir / filename
            
            # Salva dados
            data_to_save = {
                'group_type': group_type,
                'date': date_str,
                'period': period,
                'message_count': len(msgs),
                'generated_at': datetime.now(timezone.utc).isoformat(),
                'messages': msgs
            }
            
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(data_to_save, f, indent=2, ensure_ascii=False)
                logger.info(f"💾 Salvo: {filepath} ({len(msgs)} mensagens)")
            except Exception as e:
                logger.error(f"❌ Erro ao salvar {filepath}: {e}")
        
        return organized
    
    async def process_scoring_messages(self, messages: List[Dict]):
        """Processa mensagens de scoring e calcula pontuações."""
        logger.info("🎯 Processando mensagens de scoring...")
        
        # Organiza mensagens por usuário e sessão
        user_sessions = {}
        
        for message in messages:
            sender_id = message['sender_id']
            
            # Verifica se é embaixador
            if sender_id not in self.ambassadors_cache['telegram_ids']:
                continue
                
            message_date = datetime.fromisoformat(message['date'])
            date_str = message_date.strftime('%Y-%m-%d')
            session = self.get_session_from_hour(message_date.hour)
            
            key = f"{sender_id}_{date_str}_{session}"
            if key not in user_sessions:
                user_sessions[key] = {
                    'user_id': sender_id,
                    'date': date_str,
                    'session': session,
                    'messages': []
                }
            user_sessions[key]['messages'].append(message)
        
        # Calcula pontuações para cada sessão
        activity_records = []
        
        for session_data in user_sessions.values():
            messages_in_session = session_data['messages']
            if len(messages_in_session) > 10:
                messages_in_session = messages_in_session[:10]  # Limite de 10
            
            # Calcula score com multiplicador
            total_score = 0.0
            bonus_multiplier = 1.25
            
            for i, msg in enumerate(messages_in_session):
                if i == 0:
                    score = 1.0
                else:
                    score = total_score * (bonus_multiplier - 1) + 1.0
                total_score += score
            
            if total_score > 0:
                # Agrupa por usuário e data
                user_date_key = f"{session_data['user_id']}_{session_data['date']}"
                
                # Busca se já existe registro para este usuário/data
                existing_record = None
                for record in activity_records:
                    if record['user_id'] == session_data['user_id'] and record['activity_date'] == session_data['date']:
                        existing_record = record
                        break
                
                if existing_record:
                    # Atualiza registro existente
                    existing_record['total_day_score'] += total_score
                    existing_record['intervals_details_json'][session_data['session']] = {
                        'message_count': len(messages_in_session),
                        'score': total_score,
                        'messages': [{'id': m['id'], 'text': m['text'][:100]} for m in messages_in_session]
                    }
                else:
                    # Cria novo registro
                    activity_records.append({
                        'user_id': session_data['user_id'],
                        'activity_date': session_data['date'],
                        'total_day_score': total_score,
                        'intervals_details_json': {
                            session_data['session']: {
                                'message_count': len(messages_in_session),
                                'score': total_score,
                                'messages': [{'id': m['id'], 'text': m['text'][:100]} for m in messages_in_session]
                            }
                        }
                    })
        
        # Salva no banco de dados
        if activity_records:
            await self.save_activity_records(activity_records)
        
        self.stats['messages_processed_scoring'] = len(messages)
        logger.info(f"✅ Processadas {len(messages)} mensagens de scoring → {len(activity_records)} registros de atividade.")
    
    async def save_activity_records(self, records: List[Dict]):
        """Salva registros de atividade no banco de dados."""
        logger.info(f"💾 Salvando {len(records)} registros de atividade...")
        
        for record in records:
            try:
                # Verifica se já existe
                response = await asyncio.to_thread(
                    self.supabase.table('user_activity')
                    .select('user_id')
                    .eq('user_id', record['user_id'])
                    .eq('activity_date', record['activity_date'])
                    .execute
                )
                
                if response.data:
                    logger.info(f"⚠️ Registro já existe para user {record['user_id']} em {record['activity_date']}")
                    continue
                
                # Insere novo registro
                await asyncio.to_thread(
                    self.supabase.table('user_activity').insert(record).execute
                )
                
                self.stats['activity_records_saved'] += 1
                logger.info(f"✅ Atividade salva: user {record['user_id']} em {record['activity_date']} com score {record['total_day_score']:.2f}")
                
            except Exception as e:
                logger.error(f"❌ Erro ao salvar atividade para user {record['user_id']}: {e}")
                self.stats['errors'] += 1
    
    def extract_tweets_from_messages(self, messages: List[Dict]) -> List[Dict]:
        """Extrai tweets das mensagens usando regex."""
        logger.info("🐦 Extraindo tweets das mensagens...")
        
        # Padrões para detectar URLs do Twitter/X
        twitter_patterns = [
            r'https?://(?:www\.)?twitter\.com/\w+/status/(\d+)',
            r'https?://(?:www\.)?x\.com/\w+/status/(\d+)',
            r'https?://t\.co/\w+'
        ]
        
        tweets_found = []
        
        for message in messages:
            text = message.get('text', '')
            
            for pattern in twitter_patterns:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    if 'status/' in match.group():
                        # Extrai informações do tweet
                        url_parts = match.group().split('/')
                        username = url_parts[-3] if len(url_parts) >= 3 else None
                        tweet_id = url_parts[-1].split('?')[0] if len(url_parts) >= 1 else None
                        
                        if username and tweet_id and tweet_id.isdigit():
                            tweet_info = {
                                'tweet_id': tweet_id,
                                'username': username.lower(),
                                'url': match.group(),
                                'shared_by': message['sender_id'],
                                'shared_at': message['date'],
                                'message_id': message['id']
                            }
                            tweets_found.append(tweet_info)
                            self.stats['tweets_extracted'] += 1
        
        logger.info(f"✅ Extraídos {len(tweets_found)} tweets das mensagens.")
        return tweets_found
    
    async def validate_and_save_tweets(self, tweets: List[Dict]):
        """Valida tweets contra embaixadores registrados e salva."""
        logger.info(f"🔍 Validando {len(tweets)} tweets...")
        
        for tweet in tweets:
            username = tweet['username']
            
            # Verifica se o username é de um embaixador
            if username not in self.ambassadors_cache['twitter_usernames']:
                continue
            
            # Busca author_id
            author_id = self.ambassadors_cache['twitter_username_to_id'].get(username)
            if not author_id:
                continue
            
            # Verifica se tweet já existe
            try:
                response = await asyncio.to_thread(
                    self.supabase.table('tweets')
                    .select('tweet_id')
                    .eq('tweet_id', tweet['tweet_id'])
                    .execute
                )
                
                if response.data:
                    continue  # Tweet já existe
                
                # Busca dados completos do tweet via API
                tweet_data = await self.fetch_tweet_data(tweet['tweet_id'])
                if tweet_data:
                    await self.save_tweet_to_db(tweet_data, author_id, tweet['shared_at'])
                    self.stats['tweets_validated'] += 1
                    logger.info(f"✅ Tweet salvo: {username}/{tweet['tweet_id']}")
                
            except Exception as e:
                logger.error(f"❌ Erro ao processar tweet {tweet['tweet_id']}: {e}")
                self.stats['errors'] += 1
                
        logger.info(f"✅ Validação concluída: {self.stats['tweets_validated']} tweets salvos.")
    
    async def fetch_tweet_data(self, tweet_id: str) -> Optional[Dict]:
        """Busca dados completos do tweet via API."""
        url = f"https://api.twitterapi.io/twitter/tweets"
        headers = {"X-API-Key": self.api_key}
        params = {"tweet_ids": tweet_id}
        
        try:
            import httpx
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, headers=headers, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    tweets = data.get('data', [])
                    return tweets[0] if tweets else None
                else:
                    logger.warning(f"⚠️ Erro na API para tweet {tweet_id}: {response.status_code}")
                    return None
                    
        except Exception as e:
            logger.error(f"❌ Erro ao buscar dados do tweet {tweet_id}: {e}")
            return None
    
    async def save_tweet_to_db(self, tweet_data: Dict, author_id: str, shared_at: str):
        """Salva tweet completo no banco de dados."""
        try:
            # Determina tipo de conteúdo
            content_type = 'text_only'
            media_url = None
            
            if tweet_data.get('mediaUrls'):
                media_urls = tweet_data['mediaUrls']
                if any('video' in url or 'mp4' in url for url in media_urls):
                    content_type = 'video'
                else:
                    content_type = 'image'
                media_url = media_urls[0] if media_urls else None
            
            # Prepara dados do tweet
            tweet_record = {
                'tweet_id': tweet_data['id'],
                'author_id': author_id,
                'twitter_url': f"https://twitter.com/{tweet_data.get('author', {}).get('username', '')}/status/{tweet_data['id']}",
                'text': tweet_data.get('text', ''),
                'createdat': tweet_data.get('createdAt'),
                'views': tweet_data.get('viewCount', 0),
                'likes': tweet_data.get('likeCount', 0),
                'retweets': tweet_data.get('retweetCount', 0),
                'replies': tweet_data.get('replyCount', 0),
                'quotes': tweet_data.get('quoteCount', 0),
                'bookmarks': tweet_data.get('bookmarkCount', 0),
                'content_type': content_type,
                'media_url': media_url,
                'is_thread': False,
                'is_thread_checked': False
            }
            
            # Salva tweet
            await asyncio.to_thread(
                self.supabase.table('tweets').insert(tweet_record).execute
            )
            
        except Exception as e:
            logger.error(f"❌ Erro ao salvar tweet no DB: {e}")
            raise
    
    async def run_processing_cycle(self):
        """Executa um ciclo completo de processamento."""
        try:
            logger.info("🔄 Iniciando ciclo de processamento...")
            
            # 1. Baixa mensagens do grupo de scoring
            scoring_messages = await self.download_messages_from_group(
                SCORING_GROUP_ID, "SCORING_GROUP"
            )
            
            # 2. Baixa mensagens do grupo de tweets
            tweets_messages = await self.download_messages_from_group(
                TWEETS_GROUP_ID, "TWEETS_GROUP"
            )
            
            # 3. Organiza e salva em JSONs
            if scoring_messages:
                self.organize_messages_by_date_and_period(scoring_messages, 'scoring')
                await self.process_scoring_messages(scoring_messages)
            
            if tweets_messages:
                self.organize_messages_by_date_and_period(tweets_messages, 'tweets')
                tweets_extracted = self.extract_tweets_from_messages(tweets_messages)
                if tweets_extracted:
                    await self.validate_and_save_tweets(tweets_extracted)
            
            # 4. Atualiza estado
            await self.save_state()
            
            # 5. Relatório final
            await self.print_processing_report()
            
        except Exception as e:
            logger.error(f"❌ Erro crítico no processamento: {e}")
            self.stats['errors'] += 1
    
    async def print_processing_report(self):
        """Imprime relatório do processamento."""
        end_time = datetime.now(timezone.utc)
        duration = end_time - self.stats['start_time']
        
        logger.info("="*60)
        logger.info("📊 --- RELATÓRIO DE PROCESSAMENTO TELEGRAM ---")
        logger.info("="*60)
        logger.info(f"⏱️  Duração: {duration}")
        logger.info(f"💬 Mensagens scoring processadas: {self.stats['messages_processed_scoring']}")
        logger.info(f"💬 Mensagens tweets processadas: {self.stats['messages_processed_tweets']}")
        logger.info(f"🎯 Registros de atividade salvos: {self.stats['activity_records_saved']}")
        logger.info(f"🐦 Tweets extraídos: {self.stats['tweets_extracted']}")
        logger.info(f"✅ Tweets validados e salvos: {self.stats['tweets_validated']}")
        logger.info(f"❌ Erros: {self.stats['errors']}")
        logger.info("="*60)
        logger.info("✅ Processamento concluído!")
    
    async def cleanup(self):
        """Limpeza de recursos."""
        if self.client:
            await self.client.disconnect()
            logger.info("🔌 Cliente Telegram desconectado.")


async def main():
    """Função principal."""
    processor = TelegramDataProcessor()
    
    try:
        if await processor.initialize():
            await processor.run_processing_cycle()
        else:
            logger.error("❌ Falha na inicialização.")
    finally:
        await processor.cleanup()


if __name__ == "__main__":
    asyncio.run(main()) 