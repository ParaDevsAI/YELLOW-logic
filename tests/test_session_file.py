"""
test_session_file.py
Teste para verificar o arquivo de sessão
"""
import os
import sqlite3
from pathlib import Path

def test_session_file():
    """Testa o arquivo de sessão."""
    print("🔍 TESTE DO ARQUIVO DE SESSÃO")
    print("="*50)
    
    session_file = Path("my_user_session.session")
    
    print(f"1️⃣ Verificando arquivo: {session_file}")
    
    if session_file.exists():
        print(f"✅ Arquivo existe")
        print(f"📁 Tamanho: {session_file.stat().st_size} bytes")
        print(f"📅 Modificado: {datetime.fromtimestamp(session_file.stat().st_mtime)}")
        
        # Tentar abrir como SQLite
        try:
            conn = sqlite3.connect(session_file)
            cursor = conn.cursor()
            
            # Listar tabelas
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            print(f"📊 Tabelas no arquivo: {[table[0] for table in tables]}")
            
            # Verificar se há dados
            for table in tables:
                table_name = table[0]
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                print(f"   📋 {table_name}: {count} registros")
            
            conn.close()
            print("✅ Arquivo de sessão válido")
            
        except Exception as e:
            print(f"❌ Erro ao ler arquivo: {e}")
            print("💡 Arquivo pode estar corrompido")
            
    else:
        print("❌ Arquivo não existe")
        print("💡 Execute autenticação primeiro")

if __name__ == "__main__":
    from datetime import datetime
    test_session_file() 