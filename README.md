# YELLOW Dashboard - Engagement Tracker

Sistema de rastreamento de engajamento para embaixadores YELLOW com pipeline automatizado.

## 🏗️ Estrutura do Projeto

### **GitHub Actions (Pipeline de Dados)**
- `yellow_pipeline.py` - Pipeline principal que executa diariamente
- `author_manager.py` - Gerenciamento de clientes Supabase
- `cross_engagement_tracker.py` - Rastreamento de engajamentos cruzados
- `generate_leaderboard.py` - Geração de leaderboards
- `telegram_tools/` - Ferramentas para processamento de mensagens

### **VPS (Bot Telegram)**
- `bot_vps.py` - Bot principal para VPS
- `registration_handler.py` - Handler de registro de usuários
- `twitter_client.py` - Cliente da API Twitter
- `message_tracker.py` - Rastreamento de mensagens
- `tweet_link_tracker.py` - Rastreamento de links de tweets

## 🚀 Configuração

### GitHub Actions
1. Configure as secrets no repositório:
   - `SUPABASE_URL`
   - `SUPABASE_KEY`
   - `TELEGRAM_API_ID`
   - `TELEGRAM_API_HASH`
   - `TWITTER_API_KEY`

2. O pipeline executa automaticamente às 02:00 UTC diariamente

### VPS
1. Instale as dependências: `pip install -r config/requirements.txt`
2. Configure o arquivo `.env` com as variáveis necessárias
3. Execute: `python bot_vps.py`

## 📊 Funcionalidades

- **Pipeline Diário**: Download de mensagens Telegram, processamento de atividades, rastreamento de engajamentos
- **Bot de Registro**: Sistema de cadastro de embaixadores via Telegram
- **Leaderboard**: Cálculo automático de pontuações e rankings
- **Cross Engagement**: Monitoramento de interações entre embaixadores

## 🔧 Dependências

Ver `config/requirements.txt` para lista completa de pacotes Python.
