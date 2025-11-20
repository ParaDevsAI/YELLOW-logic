import os
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime
import asyncio

# Carregar variáveis de ambiente
load_dotenv()

logger = logging.getLogger(__name__)

# --- Configuração do Cliente Supabase ---
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

_supabase_client: Client = None
_client_lock = asyncio.Lock()

async def get_supabase_client() -> Client:
    """
    Retorna uma instância única do cliente Supabase.
    A biblioteca 'supabase-py' gerencia internamente as chamadas assíncronas
    quando usada em um loop de eventos asyncio.
    """
    global _supabase_client
    if _supabase_client is None:
        async with _client_lock:
            if _supabase_client is None:
                try:
                    logger.info("Cliente Supabase não inicializado. Criando agora...")
                    # A criação é síncrona, mas as chamadas serão assíncronas
                    _supabase_client = create_client(url, key)
                    logger.info("Cliente Supabase inicializado com sucesso.")
                except Exception as e:
                    logger.critical(f"Falha ao inicializar o cliente Supabase: {e}")
                    return None
    return _supabase_client

# --- Funções Refatoradas para serem Assíncronas ---

async def is_author_registered(telegram_id: int) -> bool:
    """Verifica de forma assíncrona se um usuário já está registrado."""
    supabase = await get_supabase_client()
    if not supabase:
        return False
    
    try:
        # A chamada à API é um I/O, então usamos await aqui.
        response = await asyncio.to_thread(
            supabase.table('authors').select('telegram_id', count='exact').eq('telegram_id', telegram_id).execute
        )
        logger.info(f"Verificação de registro para telegram_id {telegram_id}: {response.count} encontrado(s).")
        return response.count > 0
    except Exception as e:
        logger.error(f"Erro de API ao verificar autor {telegram_id}: {e}")
        return False

async def get_author_twitter_username_from_db(telegram_id: int) -> str | None:
    """Busca de forma assíncrona o nome de usuário do Twitter."""
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
    """Busca de forma assíncrona o Twitter ID do autor."""
    supabase = await get_supabase_client()
    if not supabase:
        return None

    try:
        response = await asyncio.to_thread(
            supabase.table('authors').select('twitter_id').eq('telegram_id', telegram_id).limit(1).single().execute
        )
        if response.data:
            return response.data.get('twitter_id')
        logger.warning(f"Não foi possível encontrar um twitter_id para o telegram_id {telegram_id}.")
        return None
    except Exception as e:
        logger.error(f"Erro de API ao buscar twitter_id para {telegram_id}: {e}")
        return None

async def register_author(telegram_user, twitter_data: dict) -> bool:
    """Salva de forma assíncrona um novo autor no Supabase."""
    supabase = await get_supabase_client()
    if not supabase:
        return False

    telegram_name = telegram_user.full_name or telegram_user.username

    # Defensive check: ensure twitter_data contains an id before attempting insert.
    twitter_id = twitter_data.get('id') if twitter_data else None
    if not twitter_id:
        logger.error(f"register_author aborted: missing twitter_id for telegram {telegram_user.id}")
        return False

    author_record = {
        'telegram_id': telegram_user.id,
        'telegram_name': telegram_name,
        'twitter_id': twitter_id,
        'twitter_username': twitter_data.get('userName'),
        'twitter_name': twitter_data.get('name'),
        'twitter_description': twitter_data.get('description'),
        'twitter_followers': twitter_data.get('followers'),
        'twitter_following': twitter_data.get('following'),
        'twitter_statusescount': twitter_data.get('statusesCount'),
        'twitter_mediacount': twitter_data.get('mediaCount'),
        'twitter_createdat': twitter_data.get('createdAt'),
        'twitter_isblueverified': twitter_data.get('isBlueVerified', False),
        'twitter_profilepicture': twitter_data.get('profilePicture'),
        'sync_timestamp': datetime.utcnow().isoformat()
    }
    
    try:
        logger.info(f"Tentando registrar autor no Supabase. Telegram ID: {telegram_user.id}, Twitter User: {twitter_data.get('userName')}")
        response = await asyncio.to_thread(
            supabase.table('authors').insert(author_record).execute
        )
        
        # A resposta de insert é uma lista, verificamos se ela não está vazia.
        if response.data and len(response.data) > 0:
            logger.info(f"Autor {telegram_name} (@{twitter_data.get('userName')}) registrado com sucesso no Supabase.")
            return True
        else:
            # Captura a resposta completa em caso de falha silenciosa para diagnóstico.
            logger.error(f"O registro do autor {telegram_user.id} falhou sem um erro explícito. Resposta da API: {response}")
            return False
            
    except Exception as e:
        logger.error(f"Erro de API ao registrar autor {telegram_user.id}: {e}")
        return False 