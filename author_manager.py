import os
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime
import asyncio

load_dotenv()

logger = logging.getLogger(__name__)

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

_supabase_client: Client = None
_client_lock = asyncio.Lock()

async def get_supabase_client() -> Client:
    global _supabase_client
    if _supabase_client is None:
        async with _client_lock:
            if _supabase_client is None:
                try:
                    logger.info("Cliente Supabase não inicializado. Criando agora...")
                    _supabase_client = create_client(url, key)
                    logger.info("Cliente Supabase inicializado com sucesso.")
                except Exception as e:
                    logger.critical(f"Falha ao inicializar o cliente Supabase: {e}")
                    return None
    return _supabase_client

def initialize_supabase_client() -> Client:
    global _supabase_client
    if _supabase_client is None:
        try:
            logger.info("Cliente Supabase (síncrono) não inicializado. Criando agora...")
            _supabase_client = create_client(url, key)
            logger.info("Cliente Supabase (síncrono) inicializado com sucesso.")
        except Exception as e:
            logger.critical(f"Falha ao inicializar o cliente Supabase (síncrono): {e}")
            return None
    return _supabase_client

async def is_author_registered(telegram_id: int) -> bool:
    supabase = await get_supabase_client()
    if not supabase:
        return False
    
    try:
        response = await asyncio.to_thread(
            supabase.table('authors').select('telegram_id', count='exact').eq('telegram_id', telegram_id).execute
        )
        logger.info(f"Verificação de registro para telegram_id {telegram_id}: {response.count} encontrado(s).")
        return response.count > 0
    except Exception as e:
        logger.error(f"Erro de API ao verificar autor {telegram_id}: {e}")
        return False

async def get_author_twitter_username_from_db(telegram_id: int) -> str | None:
    supabase = await get_supabase_client()
    if not supabase:
        return None
        
    try:
        response = await asyncio.to_thread(
            supabase.table('authors').select('twitter_username').eq('telegram_id', telegram_id).limit(1).single().execute
        )
        if response.data:
            return response.data.get('twitter_username')
        return None
    except Exception as e:
        logger.error(f"Erro de API ao buscar twitter_userName para {telegram_id}: {e}")
        return None

async def get_twitter_id_from_telegram_id(telegram_id: int) -> str | None:
    supabase = await get_supabase_client()
    if not supabase:
        return None

    try:
        response = await asyncio.to_thread(
            supabase.table('authors').select('twitter_id').eq('telegram_id', telegram_id).limit(1).single().execute
        )
        if response.data:
            return response.data.get('twitter_id')
        return None
    except Exception as e:
        logger.error(f"Erro de API ao buscar twitter_id para {telegram_id}: {e}")
        return None

async def get_author_telegram_id_from_twitter_username(twitter_username: str) -> int | None:
    supabase = await get_supabase_client()
    if not supabase:
        return None

    try:
        response = await asyncio.to_thread(
            supabase.table('authors').select('telegram_id').eq('twitter_username', twitter_username).limit(1).single().execute
        )
        if response.data:
            return response.data.get('telegram_id')
        return None
    except Exception as e:
        logger.error(f"Erro de API ao buscar telegram_id para {twitter_username}: {e}")
        return None

async def register_new_author(telegram_id: int, telegram_username: str, twitter_username: str, twitter_id: str) -> bool:
    supabase = await get_supabase_client()
    if not supabase:
        return False

    try:
        author_data = {
            'telegram_id': telegram_id,
            'telegram_username': telegram_username,
            'twitter_username': twitter_username,
            'twitter_id': twitter_id,
            'created_at': datetime.utcnow().isoformat()
        }
        
        response = await asyncio.to_thread(
            supabase.table('authors').insert(author_data).execute
        )
        
        if response.data:
            logger.info(f"Novo autor registrado com sucesso: {telegram_username} (@{twitter_username})")
            return True
        else:
            logger.error("Falha ao registrar novo autor: resposta vazia da API")
            return False
            
    except Exception as e:
        logger.error(f"Erro ao registrar novo autor: {e}")
        return False

async def update_author_twitter_info(telegram_id: int, twitter_username: str, twitter_id: str) -> bool:
    supabase = await get_supabase_client()
    if not supabase:
        return False

    try:
        update_data = {
            'twitter_username': twitter_username,
            'twitter_id': twitter_id,
            'updated_at': datetime.utcnow().isoformat()
        }
        
        response = await asyncio.to_thread(
            supabase.table('authors').update(update_data).eq('telegram_id', telegram_id).execute
        )
        
        if response.data:
            logger.info(f"Informações do Twitter atualizadas para {telegram_id}: @{twitter_username}")
            return True
        else:
            logger.error(f"Falha ao atualizar informações do Twitter para {telegram_id}")
            return False
            
    except Exception as e:
        logger.error(f"Erro ao atualizar informações do Twitter: {e}")
        return False

async def get_all_authors() -> list:
    supabase = await get_supabase_client()
    if not supabase:
        return []

    try:
        response = await asyncio.to_thread(
            supabase.table('authors').select('*').execute
        )
        
        if response.data:
            logger.info(f"Recuperados {len(response.data)} autores da base de dados")
            return response.data
        else:
            logger.warning("Nenhum autor encontrado na base de dados")
            return []
            
    except Exception as e:
        logger.error(f"Erro ao recuperar autores: {e}")
        return []

async def delete_author(telegram_id: int) -> bool:
    supabase = await get_supabase_client()
    if not supabase:
        return False

    try:
        response = await asyncio.to_thread(
            supabase.table('authors').delete().eq('telegram_id', telegram_id).execute
        )
        
        if response.data:
            logger.info(f"Autor {telegram_id} removido com sucesso")
            return True
        else:
            logger.warning(f"Nenhum autor encontrado para remoção: {telegram_id}")
            return False
            
    except Exception as e:
        logger.error(f"Erro ao remover autor: {e}")
        return False

def get_supabase_url() -> str:
    return url

def get_supabase_key() -> str:
    return key 