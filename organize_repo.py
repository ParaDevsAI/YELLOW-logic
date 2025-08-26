#!/usr/bin/env python3
"""
organize_repo.py - Script para organizar automaticamente o repositório YELLOW
"""
import os
import shutil
from pathlib import Path
import re
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_folder_structure():
    """Cria a estrutura de pastas organizadas"""
    folders = [
        'bot',
        'automation', 
        'telegram_tools',
        'migration',
        'tests',
        'config',
        'data/csv',
        'data/logs', 
        'data/sessions',
        'to_delete'
    ]
    
    for folder in folders:
        try:
            Path(folder).mkdir(parents=True, exist_ok=True)
            logger.info(f"✅ Pasta criada/verificada: {folder}")
        except Exception as e:
            logger.warning(f"⚠️ Pasta {folder} já existe ou erro: {e}")

def move_files():
    """Move arquivos para suas respectivas pastas"""
    
    # Mapeamento: arquivo -> pasta_destino
    file_moves = {
        # Bot files
        'main_bot.py': 'bot',
        'registration_handler.py': 'bot', 
        'tweet_link_tracker.py': 'bot',
        'message_tracker.py': 'bot',
        'author_manager.py': 'bot',
        'twitter_client.py': 'bot',
        
        # Automation files
        'metrics_snapshot.py': 'automation',
        'cross_engagement_tracker.py': 'automation',
        'generate_leaderboard.py': 'automation',
        'generate_retroactive_leaderboard.sql': 'automation',
        'update_current_leaderboard.py': 'automation',
        'update_leaderboard.py': 'automation',
        'realtime_activity_updater.py': 'automation',
        
        # Telegram tools
        'download_telegram_messages_final.py': 'telegram_tools',
        'download_telegram_messages_fixed.py': 'telegram_tools', 
        'process_downloaded_messages.py': 'telegram_tools',
        'telegram_history_parser.py': 'telegram_tools',
        'telegram_data_processor.py': 'telegram_tools',
        'thread_identifier.py': 'telegram_tools',
        'analytics_pipeline.py': 'telegram_tools',
        'list_my_chats.py': 'telegram_tools',
        
        # Migration files
        'populate_entities.py': 'migration',
        'populate_from_csv.py': 'migration',
        'populate_from_json.py': 'migration', 
        'populate_from_legacy.py': 'migration',
        'populate_full_history.py': 'migration',
        'populate_leaderboard_history.py': 'migration',
        'populate_metrics_history.py': 'migration',
        'populate_missing.py': 'migration',
        'populate_telegram_history.py': 'migration',
        'historical_importer.py': 'migration',
        'insert_missing_tweets.py': 'migration',
        'compare_db_and_legacy_csv.py': 'migration',
        'process_and_compare_tweets.py': 'migration',
        
        # Test files
        'test_leaderboard_history.py': 'tests',
        'test_new_scoring_rule.py': 'tests',
        'test_telegram_session.py': 'tests', 
        'test_telegram.py': 'tests',
        'test_tweet_fetching.py': 'tests',
        'test_connection_no_context.py': 'tests',
        'test_connection_no_updates.py': 'tests',
        'test_connection_raw.py': 'tests',
        'test_connection_simple.py': 'tests',
        'test_connection_timeout.py': 'tests',
        'test_download_debug.py': 'tests',
        'test_new_session.py': 'tests',
        'test_session_file.py': 'tests',
        'test_step_by_step.py': 'tests',
        'test_system_time.py': 'tests',
        'debug_complete.py': 'tests',
        'debug_simple.py': 'tests',
        'inspect_current_leaderboard.py': 'tests',
        'inspect_schema.py': 'tests',
        'inspect_user_activity.py': 'tests',
        'diagnose_orphan_data.py': 'tests',
        'find_orphan_data.py': 'tests',
        'find_self_engagements.py': 'tests',
        'verify_user_engagement.py': 'tests',
        'get_user_score_details.py': 'tests',
        'analyze_leaderboard_history.py': 'tests',
        'analyze_tweet_dates.py': 'tests',
        
        # Config files
        'create_schema.sql': 'config',
        'requirements.txt': 'config',
        'CONTEXT.MD': 'config',
        'DATABASE_DOCUMENTATION.MD': 'config',
        'TASK.md': 'config',
        'test_leaderboard_history.sql': 'config',
        'test_retroactive_leaderboard.sql': 'config',
        
        # CSV files to data/csv
        'authors_final.csv': 'data/csv',
        'authors_rows.csv': 'data/csv',
        'found_tweets.csv': 'data/csv',
        'group_members.csv': 'data/csv',
        'tweet_entities_rows.csv': 'data/csv',
        'tweet_metrics_history_rows.csv': 'data/csv',
        'tweets_rows.csv': 'data/csv',
        
        # JSON files to data
        'result_yellow.json': 'data',
        'result.json': 'data',
        'telegram_processor_state.json': 'data',
        
        # Log files to data/logs
        'fix_timestamps.log': 'data/logs',
        'leaderboard_update.log': 'data/logs',
        'population_entities.log': 'data/logs',
        'population_legacy.log': 'data/logs',
        'population_metrics.log': 'data/logs',
        'population_missing.log': 'data/logs',
        'population.log': 'data/logs',
        'thread_identifier.log': 'data/logs',
        
        # Session files to data/sessions
        'my_user_session.session': 'data/sessions',
        'new_one.session': 'data/sessions',
        'telegram_processor_session.session': 'data/sessions',
        'test_session_fresh.session': 'data/sessions',
        
        # Files to delete
        'database_client.py': 'to_delete',
        'organize_project.py': 'to_delete',
        'reauthenticate.py': 'to_delete',
        'manual_contributions_manager.py': 'to_delete',
        'fix_engagement_timestamps.py': 'to_delete',
    }
    
    # Move files
    for filename, destination in file_moves.items():
        if Path(filename).exists():
            try:
                # Se o arquivo já existe no destino, remove primeiro
                dest_path = Path(destination) / filename
                if dest_path.exists():
                    dest_path.unlink()
                
                shutil.move(filename, destination)
                logger.info(f"✅ Movido: {filename} -> {destination}/")
            except Exception as e:
                logger.error(f"❌ Erro ao mover {filename}: {e}")
        else:
            logger.warning(f"⚠️ Arquivo não encontrado: {filename}")

def move_directories():
    """Move diretórios especiais"""
    dirs_to_move = {
        'core': 'to_delete',
        'scripts': 'to_delete',
        'LEGACY': 'to_delete',
        'telegram_data': 'data'
    }
    
    for dirname, destination in dirs_to_move.items():
        if Path(dirname).exists():
            try:
                dest_path = Path(destination) / dirname
                if dest_path.exists():
                    shutil.rmtree(dest_path)
                shutil.move(dirname, destination)
                logger.info(f"✅ Pasta movida: {dirname} -> {destination}/")
            except Exception as e:
                logger.error(f"❌ Erro ao mover pasta {dirname}: {e}")

def fix_imports():
    """Corrige imports nos arquivos movidos"""
    import_fixes = {
        'bot': {
            'from author_manager import': 'from bot.author_manager import',
            'from twitter_client import': 'from bot.twitter_client import',
            'from registration_handler import': 'from bot.registration_handler import',
            'from tweet_link_tracker import': 'from bot.tweet_link_tracker import', 
            'from message_tracker import': 'from bot.message_tracker import',
        },
        'automation': {
            'from author_manager import': 'from bot.author_manager import',
            'from twitter_client import': 'from bot.twitter_client import',
        },
        'telegram_tools': {
            'from author_manager import': 'from bot.author_manager import',
            'from twitter_client import': 'from bot.twitter_client import',
        },
        'migration': {
            'from author_manager import': 'from bot.author_manager import',
            'from twitter_client import': 'from bot.twitter_client import',
        },
        'tests': {
            'from author_manager import': 'from bot.author_manager import',
            'from twitter_client import': 'from bot.twitter_client import',
        }
    }
    
    for folder, fixes in import_fixes.items():
        folder_path = Path(folder)
        if folder_path.exists():
            for py_file in folder_path.glob('*.py'):
                try:
                    content = py_file.read_text(encoding='utf-8')
                    original_content = content
                    
                    for old_import, new_import in fixes.items():
                        content = content.replace(old_import, new_import)
                    
                    if content != original_content:
                        py_file.write_text(content, encoding='utf-8')
                        logger.info(f"✅ Imports corrigidos: {py_file}")
                        
                except Exception as e:
                    logger.error(f"❌ Erro ao corrigir imports em {py_file}: {e}")

def create_init_files():
    """Cria arquivos __init__.py para tornar as pastas pacotes Python"""
    folders = ['bot', 'automation', 'telegram_tools', 'migration', 'tests']
    
    for folder in folders:
        init_file = Path(folder) / '__init__.py'
        if not init_file.exists():
            init_file.write_text('# -*- coding: utf-8 -*-\n"""Package initialization"""')
            logger.info(f"✅ Criado: {init_file}")

def create_readme():
    """Cria README.md com a nova estrutura"""
    readme_content = """# 🟡 YELLOW Ambassador Engagement Tracker

Sistema automatizado para monitorar, pontuar e classificar o engajamento dos embaixadores.

## 📁 Estrutura do Projeto

```
YELLOW/
├── 🤖 bot/                 # Sistema principal do bot Telegram
├── ⚙️ automation/          # Scripts de automação (cron jobs)
├── 📥 telegram_tools/      # Ferramentas de download/processamento
├── 🔄 migration/           # Scripts de migração (temporários)
├── 🧪 tests/              # Testes e debug
├── 📊 data/               # Dados, logs e sessões
├── 📋 config/             # Configurações e documentação
└── 🗑️ to_delete/          # Arquivos obsoletos
```

## 🚀 Scripts Principais

### Bot do Telegram
- `bot/main_bot.py` - Bot principal
- `bot/author_manager.py` - Cliente Supabase (DAL)

### Automação (Cron Jobs)
- `automation/metrics_snapshot.py` - A cada 6 horas
- `automation/cross_engagement_tracker.py` - A cada 24 horas  
- `automation/generate_leaderboard.py` - A cada 30 minutos

### Ferramentas Telegram
- `telegram_tools/download_telegram_messages_final.py` - Download de mensagens
- `telegram_tools/process_downloaded_messages.py` - Processamento de dados

## ⚙️ Configuração

1. Copie `.env.example` para `.env` e configure as variáveis
2. Execute `pip install -r config/requirements.txt`
3. Configure o banco com `config/create_schema.sql`

## 📚 Documentação

- `config/CONTEXT.MD` - Documentação técnica completa
- `config/DATABASE_DOCUMENTATION.MD` - Documentação do banco
"""
    
    Path('README.md').write_text(readme_content, encoding='utf-8')
    logger.info("✅ README.md criado")

def main():
    """Função principal"""
    logger.info("🚀 Iniciando organização do repositório YELLOW...")
    
    # 1. Criar estrutura de pastas
    logger.info("📁 Criando estrutura de pastas...")
    create_folder_structure()
    
    # 2. Mover arquivos
    logger.info("📦 Movendo arquivos...")
    move_files()
    
    # 3. Mover diretórios
    logger.info("📂 Movendo diretórios...")
    move_directories()
    
    # 4. Corrigir imports
    logger.info("🔧 Corrigindo imports...")
    fix_imports()
    
    # 5. Criar arquivos __init__.py
    logger.info("📝 Criando arquivos __init__.py...")
    create_init_files()
    
    # 6. Criar README
    logger.info("📋 Criando README.md...")
    create_readme()
    
    logger.info("✅ Organização concluída com sucesso!")
    logger.info("🗑️ Arquivos obsoletos movidos para 'to_delete/' - podem ser removidos")
    logger.info("🔧 Verifique os imports nos arquivos se necessário")

if __name__ == "__main__":
    main()
