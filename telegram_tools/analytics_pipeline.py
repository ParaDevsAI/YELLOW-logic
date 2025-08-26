"""
analytics_pipeline.py - Script Combinado de Análise Inteligente

Este script combina 4 operações importantes em um pipeline único e eficiente:
1. metrics_snapshot.py - Atualização de métricas de tweets
2. cross_engagement_tracker.py - Rastreamento de engajamentos cruzados  
3. thread_identifier.py - Identificação de threads
4. generate_leaderboard.py - Geração de leaderboard

LÓGICA INTELIGENTE:
- Executa APENAS 1x por dia (verifica se já rodou hoje)
- Processa tweets de forma otimizada por prioridade/idade
- Usa processamento paralelo quando possível
- Logs detalhados e estatísticas completas

FREQUÊNCIA RECOMENDADA: Diário (00:00 UTC)
"""

import asyncio
import os
import httpx
import logging
import time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from typing import Dict, List, Set, Tuple, Optional

# Imports do projeto
from bot.author_manager import get_supabase_client

# Configuração de logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Constantes
BATCH_SIZE = 100
API_DELAY = 1.5  # Delay entre chamadas para ser respeitoso com a API
TWEETS_PRIORITY_DAYS = 3  # Tweets com menos de 3 dias têm prioridade

class AnalyticsPipeline:
    """Classe principal que orquestra todo o pipeline de análise."""
    
    def __init__(self):
        self.api_key = None
        self.supabase = None
        self.stats = {
            'tweets_processed': 0,
            'metrics_updated': 0,
            'threads_identified': 0,
            'engagements_found': 0,
            'leaderboard_generated': False,
            'start_time': None,
            'errors': 0
        }
        
    async def initialize(self):
        """Inicializa conexões e verifica configuração."""
        logger.info("🚀 --- Iniciando Pipeline de Análise Inteligente ---")
        
        load_dotenv()
        self.api_key = os.getenv("TWITTER_API_KEY")
        if not self.api_key:
            logger.critical("❌ TWITTER_API_KEY não encontrada. Abortando.")
            return False
            
        self.supabase = await get_supabase_client()
        if not self.supabase:
            logger.critical("❌ Falha ao conectar com Supabase. Abortando.")
            return False
            
        self.stats['start_time'] = datetime.now(timezone.utc)
        logger.info("✅ Inicialização concluída com sucesso.")
        return True
        
    async def check_if_already_ran_today(self) -> bool:
        """Verifica se o pipeline já foi executado hoje."""
        today_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        
        try:
            # Verifica na tabela leaderboard_history se já temos dados de hoje
            response = await asyncio.to_thread(
                self.supabase.table('leaderboard_history')
                .select('id')
                .gte('snapshot_timestamp', f'{today_date} 00:00:00+00')
                .lt('snapshot_timestamp', f'{today_date} 23:59:59+00')
                .limit(1)
                .execute
            )
            
            if response.data:
                logger.info(f"✅ Pipeline já executado hoje ({today_date}). Última execução encontrada.")
                return True
                
            logger.info(f"🆕 Pipeline não executado hoje ({today_date}). Execução necessária.")
            return False
            
        except Exception as e:
            logger.error(f"❌ Erro ao verificar execução diária: {e}")
            return False
    
    async def get_tweets_to_analyze(self) -> List[Dict]:
        """
        Busca tweets que precisam de análise, priorizando os mais recentes.
        """
        logger.info("📊 Buscando tweets para análise...")
        
        try:
            # Busca tweets dos últimos 7 dias para análise completa
            seven_days_ago = datetime.utcnow() - timedelta(days=7)
            
            response = await asyncio.to_thread(
                self.supabase.table('tweets')
                .select('tweet_id, author_id, createdat, is_thread_checked, views, likes, retweets')
                .gte('createdat', seven_days_ago.isoformat())
                .order('createdat', desc=True)
                .execute
            )
            
            if response.data:
                logger.info(f"📈 Encontrados {len(response.data)} tweets para análise.")
                return response.data
            else:
                logger.warning("⚠️ Nenhum tweet encontrado para análise.")
                return []
                
        except Exception as e:
            logger.error(f"❌ Erro ao buscar tweets: {e}")
            return []
    
    async def update_tweet_metrics(self, tweets_batch: List[str]) -> int:
        """
        Atualiza métricas de um lote de tweets usando a API.
        Baseado em metrics_snapshot.py
        """
        if not tweets_batch:
            return 0
            
        url = "https://api.twitterapi.io/twitter/tweets"
        headers = {"X-API-Key": self.api_key}
        params = {"tweet_ids": ",".join(tweets_batch)}
        
        try:
            async with httpx.AsyncClient(timeout=40.0) as client:
                response = await client.get(url, headers=headers, params=params)
                
                if response.status_code != 200:
                    logger.error(f"❌ Erro na API de métricas: {response.status_code}")
                    return 0
                    
                data = response.json()
                tweets_data = data.get('data', [])
                
                if not tweets_data:
                    return 0
                
                # Prepara dados para atualização
                history_records = []
                tweet_update_records = []
                snapshot_time = datetime.utcnow().isoformat()
                
                for tweet in tweets_data:
                    tweet_id = tweet.get('id')
                    author_id = tweet.get('author', {}).get('id')
                    
                    if not tweet_id or not author_id:
                        continue
                    
                    # Record para histórico
                    history_records.append({
                        'tweet_id': tweet_id,
                        'snapshot_at': snapshot_time,
                        'views': tweet.get('viewCount', 0),
                        'likes': tweet.get('likeCount', 0),
                        'retweets': tweet.get('retweetCount', 0),
                        'replies': tweet.get('replyCount', 0),
                        'quotes': tweet.get('quoteCount', 0),
                        'bookmarks': tweet.get('bookmarkCount', 0)
                    })
                    
                    # Record para atualização principal
                    tweet_update_records.append({
                        'tweet_id': tweet_id,
                        'author_id': author_id,
                        'views': tweet.get('viewCount', 0),
                        'likes': tweet.get('likeCount', 0),
                        'retweets': tweet.get('retweetCount', 0),
                        'replies': tweet.get('replyCount', 0),
                        'quotes': tweet.get('quoteCount', 0),
                        'bookmarks': tweet.get('bookmarkCount', 0),
                    })
                
                # Salva em lote
                if history_records:
                    await asyncio.to_thread(
                        self.supabase.table('tweet_metrics_history').insert(history_records).execute
                    )
                    
                if tweet_update_records:
                    await asyncio.to_thread(
                        self.supabase.table('tweets').upsert(tweet_update_records, on_conflict='tweet_id').execute
                    )
                
                logger.info(f"✅ Métricas atualizadas para {len(tweets_data)} tweets.")
                return len(tweets_data)
                
        except Exception as e:
            logger.error(f"❌ Erro ao atualizar métricas: {e}")
            self.stats['errors'] += 1
            return 0
    
    async def check_thread_status(self, tweet_id: str) -> Optional[bool]:
        """
        Verifica se um tweet é uma thread usando a API.
        Baseado em thread_identifier.py
        """
        url = "https://api.twitterapi.io/twitter/tweet/thread_context"
        headers = {"X-API-Key": self.api_key}
        params = {"tweetId": tweet_id}
        
        try:
            async with httpx.AsyncClient(timeout=40.0) as client:
                response = await client.get(url, headers=headers, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    thread_tweets = data.get('tweets', [])
                    is_thread = len(thread_tweets) >= 3
                    
                    # Atualiza no banco
                    await asyncio.to_thread(
                        self.supabase.table('tweets').update({
                            'is_thread': is_thread,
                            'is_thread_checked': True
                        }).eq('tweet_id', tweet_id).execute
                    )
                    
                    if is_thread:
                        self.stats['threads_identified'] += 1
                        
                    return is_thread
                else:
                    logger.warning(f"⚠️ Erro na API de thread para {tweet_id}: {response.status_code}")
                    return None
                    
        except Exception as e:
            logger.error(f"❌ Erro ao verificar thread {tweet_id}: {e}")
            self.stats['errors'] += 1
            return None
    
    async def analyze_cross_engagements(self, tweets_to_check: List[Dict]) -> int:
        """
        Analisa engajamentos cruzados entre embaixadores.
        Baseado em cross_engagement_tracker.py
        """
        logger.info("🔗 Analisando engajamentos cruzados...")
        
        # Busca todos os IDs de embaixadores
        try:
            response = await asyncio.to_thread(
                self.supabase.table('authors').select('twitter_id').execute
            )
            ambassador_ids = {item['twitter_id'] for item in response.data} if response.data else set()
            
            if not ambassador_ids:
                logger.warning("⚠️ Nenhum embaixador encontrado.")
                return 0
                
        except Exception as e:
            logger.error(f"❌ Erro ao buscar embaixadores: {e}")
            return 0
        
        engagements_found = 0
        
        async with httpx.AsyncClient(timeout=40.0) as client:
            for tweet in tweets_to_check[:50]:  # Limita para não sobrecarregar
                tweet_id = tweet['tweet_id']
                tweet_author_id = tweet['author_id']
                
                try:
                    # Busca retweets e replies em paralelo
                    retweets_task = self.fetch_retweeters(client, tweet_id)
                    replies_task = self.fetch_replies_and_quotes(client, tweet_id)
                    
                    results = await asyncio.gather(retweets_task, replies_task, return_exceptions=True)
                    retweeters = results[0] if not isinstance(results[0], Exception) else []
                    replies = results[1] if not isinstance(results[1], Exception) else []
                    
                    # Processa engajamentos
                    unique_engagements = set()
                    
                    # Processa retweets
                    for user in retweeters:
                        user_id = str(user.get('id'))
                        if user_id in ambassador_ids and user_id != tweet_author_id:
                            unique_engagements.add((tweet_id, tweet_author_id, user_id, 'retweet_or_quote', 2))
                    
                    # Processa replies
                    for reply_tweet in replies:
                        reply_author_id = str(reply_tweet.get('author', {}).get('id', ''))
                        if reply_author_id in ambassador_ids and reply_author_id != tweet_author_id:
                            unique_engagements.add((tweet_id, tweet_author_id, reply_author_id, 'reply', 2))
                    
                    # Salva engajamentos
                    if unique_engagements:
                        engagement_records = []
                        for eng in unique_engagements:
                            engagement_records.append({
                                'tweet_id': eng[0],
                                'tweet_author_id': eng[1],
                                'interacting_user_id': eng[2],
                                'action_type': eng[3],
                                'points_awarded': eng[4],
                                'created_at': tweet['createdat']
                            })
                        
                        try:
                            await asyncio.to_thread(
                                self.supabase.table('ambassador_engagements').upsert(
                                    engagement_records,
                                    on_conflict='tweet_id,interacting_user_id,action_type'
                                ).execute
                            )
                            engagements_found += len(engagement_records)
                        except Exception as e:
                            logger.error(f"❌ Erro ao salvar engajamentos: {e}")
                    
                    await asyncio.sleep(API_DELAY)  # Rate limiting respeitoso
                    
                except Exception as e:
                    logger.error(f"❌ Erro ao processar engajamentos do tweet {tweet_id}: {e}")
                    continue
        
        logger.info(f"✅ Encontrados {engagements_found} novos engajamentos.")
        self.stats['engagements_found'] = engagements_found
        return engagements_found
    
    async def fetch_retweeters(self, client: httpx.AsyncClient, tweet_id: str) -> List[Dict]:
        """Busca usuários que retweetaram."""
        url = "https://api.twitterapi.io/twitter/tweet/retweeters"
        headers = {"X-API-Key": self.api_key}
        params = {"tweetId": tweet_id}
        
        try:
            response = await client.get(url, headers=headers, params=params)
            if response.status_code == 200:
                return response.json().get('users', [])
        except Exception as e:
            logger.error(f"❌ Erro ao buscar retweeters: {e}")
        return []
    
    async def fetch_replies_and_quotes(self, client: httpx.AsyncClient, tweet_id: str) -> List[Dict]:
        """Busca replies e quotes."""
        url = "https://api.twitterapi.io/twitter/tweet/advanced_search"
        headers = {"X-API-Key": self.api_key}
        query = f"(conversation_id:{tweet_id}) OR (quoted_tweet_id:{tweet_id})"
        params = {"query": query, "queryType": "Latest"}
        
        try:
            response = await client.get(url, headers=headers, params=params)
            if response.status_code == 200:
                return response.json().get('tweets', [])
        except Exception as e:
            logger.error(f"❌ Erro ao buscar replies/quotes: {e}")
        return []
    
    async def generate_leaderboard(self):
        """
        Gera o leaderboard usando as funções RPC corretas do schema.
        Baseado em generate_leaderboard.py
        """
        try:
            # Executa a geração do leaderboard
            logger.info("🔄 Gerando leaderboard...")
            
            # Usa a função RPC calculate_leaderboard() do schema
            logger.info("⚡ Calculando dados do leaderboard...")
            leaderboard_data = await asyncio.to_thread(
                self.supabase.rpc('calculate_leaderboard').execute
            )
            
            if not leaderboard_data.data:
                logger.warning("⚠️ Nenhum dado retornado do calculate_leaderboard")
                return False
            
            # Limpa e atualiza a tabela leaderboard
            logger.info("🔄 Atualizando tabela leaderboard...")
            
            # Limpa tabela atual
            await asyncio.to_thread(
                self.supabase.table('leaderboard').delete().neq('user_id', 0).execute
            )
            
            # Prepara dados para inserção com rank calculado
            current_time = datetime.now(timezone.utc).isoformat()
            insert_data = []
            
            for i, record in enumerate(leaderboard_data.data):
                insert_data.append({
                    'user_id': record['telegram_id'],
                    'rank': i + 1,  # Rank baseado na ordem dos resultados
                    'last_updated': current_time,
                    'telegram_name': record['telegram_name'],
                    'twitter_username': record['twitter_username'],
                    'count_tweets_text_only': record['count_tweets_text_only'],
                    'count_tweets_image': record['count_tweets_image'],
                    'count_tweets_thread': record['count_tweets_thread'],
                    'count_tweets_video': record['count_tweets_video'],
                    'total_score_from_tweets': float(record['total_score_from_tweets']),
                    'count_retweets_made': record['count_retweets_made'],
                    'count_comments_made': record['count_comments_made'],
                    'total_score_from_engagements': float(record['total_score_from_engagements']),
                    'total_score_from_telegram': float(record['total_score_from_telegram']),
                    'count_partner_introduction': record['count_partner_introduction'],
                    'count_hosting_ama': record['count_hosting_ama'],
                    'count_recruitment_ambassador': record['count_recruitment_ambassador'],
                    'count_product_feedback': record['count_product_feedback'],
                    'count_recruitment_investor': record['count_recruitment_investor'],
                    'total_score_from_contributions': float(record['total_score_from_contributions']),
                    'grand_total_score': float(record['grand_total_score'])
                })
            
            # Insere os dados atualizados
            await asyncio.to_thread(
                self.supabase.table('leaderboard').insert(insert_data).execute
            )
            
            # Salva snapshot no histórico
            logger.info("💾 Salvando snapshot no histórico...")
            history_data = []
            for record in insert_data:
                history_record = record.copy()
                history_record['snapshot_timestamp'] = current_time
                history_data.append(history_record)
            
            await asyncio.to_thread(
                self.supabase.table('leaderboard_history').insert(history_data).execute
            )
            
            logger.info(f"🎉 Leaderboard gerado com sucesso! {len(insert_data)} usuários processados.")
            self.stats['leaderboard_generated'] = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao gerar leaderboard: {e}")
            self.stats['errors'] += 1
            return False
    
    async def run_pipeline(self):
        """Executa o pipeline completo de análise."""
        try:
            # 1. Verifica se já executou hoje
            if await self.check_if_already_ran_today():
                logger.info("📊 Pipeline já executado hoje. Finalizando.")
                return
            
            # 2. Busca tweets para analisar
            tweets_to_analyze = await self.get_tweets_to_analyze()
            if not tweets_to_analyze:
                logger.warning("⚠️ Nenhum tweet para analisar. Finalizando.")
                return
            
            logger.info(f"📈 Iniciando análise de {len(tweets_to_analyze)} tweets...")
            
            # 3. Processa tweets em lotes
            tweet_ids = [tweet['tweet_id'] for tweet in tweets_to_analyze]
            
            # 3a. Atualiza métricas em lotes
            logger.info("📊 Atualizando métricas dos tweets...")
            for i in range(0, len(tweet_ids), BATCH_SIZE):
                batch = tweet_ids[i:i + BATCH_SIZE]
                updated = await self.update_tweet_metrics(batch)
                self.stats['metrics_updated'] += updated
                await asyncio.sleep(API_DELAY)
            
            # 3b. Verifica threads para tweets não verificados
            logger.info("🧵 Verificando status de threads...")
            tweets_to_check_threads = [t for t in tweets_to_analyze if not t.get('is_thread_checked', True)]
            
            for tweet in tweets_to_check_threads[:50]:  # Limita para não sobrecarregar
                thread_result = await self.check_thread_status(tweet['tweet_id'])
                if thread_result is not None:
                    self.stats['tweets_processed'] += 1
                await asyncio.sleep(API_DELAY)
            
            # 3c. Analisa engajamentos cruzados
            await self.analyze_cross_engagements(tweets_to_analyze)
            
            # 4. Gera leaderboard se necessário
            await self.generate_leaderboard()
            
            # 5. Relatório final
            await self.print_final_report()
            
        except Exception as e:
            logger.error(f"❌ Erro crítico no pipeline: {e}")
            self.stats['errors'] += 1
    
    async def print_final_report(self):
        """Imprime relatório final das estatísticas."""
        end_time = datetime.now(timezone.utc)
        duration = end_time - self.stats['start_time']
        
        logger.info("="*60)
        logger.info("🎯 --- RELATÓRIO FINAL DO PIPELINE DE ANÁLISE ---")
        logger.info("="*60)
        logger.info(f"⏱️  Duração total: {duration}")
        logger.info(f"📊 Tweets processados: {self.stats['tweets_processed']}")
        logger.info(f"📈 Métricas atualizadas: {self.stats['metrics_updated']}")
        logger.info(f"🧵 Threads identificadas: {self.stats['threads_identified']}")
        logger.info(f"🔗 Engajamentos encontrados: {self.stats['engagements_found']}")
        logger.info(f"🏆 Leaderboard gerado: {'✅ Sim' if self.stats['leaderboard_generated'] else '❌ Não'}")
        logger.info(f"❌ Erros encontrados: {self.stats['errors']}")
        logger.info("="*60)
        logger.info("✅ Pipeline de análise concluído com sucesso!")


async def main():
    """Função principal do pipeline."""
    pipeline = AnalyticsPipeline()
    
    if await pipeline.initialize():
        await pipeline.run_pipeline()
    else:
        logger.error("❌ Falha na inicialização. Pipeline abortado.")


if __name__ == "__main__":
    asyncio.run(main()) 