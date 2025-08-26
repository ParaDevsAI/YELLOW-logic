# Yellow Engagement Tracker

Pipeline automatizado para rastreamento de engajamento de embaixadores.

## Estrutura

```
YELLOW/
├── yellow_pipeline.py          # Pipeline principal
├── author_manager.py           # Cliente Supabase
├── cross_engagement_tracker.py # Rastreador de engajamentos
├── generate_leaderboard.py     # Gerador de leaderboard
├── telegram_tools/
│   ├── thread_identifier.py   # Identificador de threads
│   └── process_downloaded_messages.py
├── config/
│   └── requirements.txt        # Dependências
└── .github/workflows/
    └── scheduled_jobs.yml     # GitHub Actions
```

## Configuração

1. Configure as variáveis de ambiente no arquivo `.env`:
```
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
SCORING_GROUP_ID=-1001581599914
TWEETS_GROUP_ID=-1002330680602
TELEGRAM_SESSION_NAME=new_one
TWITTER_API_KEY=your_twitter_api_key
```

2. Instale as dependências:
```bash
pip install -r config/requirements.txt
```

3. Execute o pipeline:
```bash
python yellow_pipeline.py
```

## GitHub Actions

O pipeline executa automaticamente todos os dias às 02:00 UTC via GitHub Actions.

Configure os secrets necessários no repositório para execução automática.
