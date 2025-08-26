import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

def get_db_client():
    """Cria e retorna um cliente Supabase."""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("Supabase URL and Key must be set in .env file")
    return create_client(url, key) 