"""
database_client.py

Este módulo centraliza a lógica de conexão com o banco de dados PostgreSQL no Supabase.
Ele cria um pool de conexões assíncronas que pode ser reutilizado em toda a aplicação,
melhorando a performance e o gerenciamento de recursos.
"""

import os
import asyncpg
import logging
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

logger = logging.getLogger(__name__)

# Variável global para armazenar o pool de conexões
db_pool = None

async def init_db_pool():
    """
    Inicializa o pool de conexões com o banco de dados.
    Esta função deve ser chamada uma única vez quando a aplicação inicia.
    """
    global db_pool
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        logger.critical("DATABASE_URL não foi encontrada nas variáveis de ambiente. A aplicação não pode se conectar ao banco de dados.")
        raise ValueError("DATABASE_URL não configurada.")

    try:
        # asyncpg.create_pool() é a forma recomendada para gerenciar múltiplas conexões
        db_pool = await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=10)
        logger.info("Pool de conexões com o banco de dados PostgreSQL inicializado com sucesso.")
    except Exception as e:
        logger.critical(f"Falha ao criar o pool de conexões com o PostgreSQL: {e}")
        db_pool = None # Garante que o pool esteja nulo em caso de falha

async def get_db_connection():
    """
    Adquire uma conexão do pool.

    Esta função será usada por todos os outros módulos para interagir com o banco de dados.
    O `acquire()` pega uma conexão disponível do pool ou espera por uma se todas estiverem em uso.
    """
    if db_pool is None:
        logger.error("O pool de conexões não foi inicializado. Chame init_db_pool() primeiro.")
        await init_db_pool() # Tenta inicializar como fallback

    if db_pool:
        # Retorna uma conexão do pool
        return db_pool.acquire()
    
    return None

async def close_db_pool():
    """Fecha o pool de conexões. Chamado ao encerrar a aplicação."""
    global db_pool
    if db_pool:
        await db_pool.close()
        logger.info("Pool de conexões com o banco de dados PostgreSQL fechado.")
        db_pool = None 