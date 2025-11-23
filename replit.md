# Yellow Ambassador Engagement Tracker

## Project Overview
This is a Python-based Telegram bot and data pipeline application designed to track and score engagement for Yellow ambassadors. The system monitors Telegram group activity, tracks tweets, identifies cross-engagement between ambassadors, and generates leaderboards.

## Architecture
- **Type**: Backend application (Telegram bot + scheduled data pipeline)
- **Language**: Python 3.11
- **Database**: Supabase (PostgreSQL)
- **Key Services**: 
  - Telegram API (for bot and message downloading)
  - Twitter API (for tweet data)
  - Supabase (for data storage)

## Project Structure
- `yellow_pipeline.py` - Main pipeline script that runs the complete data processing workflow
- `author_manager.py` - Supabase client and author management
- `cross_engagement_tracker.py` - Tracks cross-engagement between ambassadors
- `generate_leaderboard.py` - Generates and updates leaderboards
- `automation/` - Automated scripts for metrics, engagement tracking, and leaderboard updates
- `telegram_tools/` - Tools for downloading and processing Telegram messages
- `migration/` - Database migration and historical data import scripts
- `config/` - Configuration files, SQL schemas, and documentation

## Setup Instructions

### 1. Environment Variables
Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

Required variables:
- `SUPABASE_URL` - Your Supabase project URL
- `SUPABASE_KEY` - Your Supabase API key
- `TELEGRAM_API_ID` - Telegram API ID (from my.telegram.org)
- `TELEGRAM_API_HASH` - Telegram API hash
- `TELEGRAM_BOT_TOKEN` - Your Telegram bot token (optional, for bot mode)
- `TWITTER_API_KEY` - Twitter API key
- `SCORING_GROUP_ID` - Telegram group ID for scoring
- `TWEETS_GROUP_ID` - Telegram group ID for tweets

### 2. Dependencies
All Python dependencies are managed via `uv` and installed automatically:
- telethon==1.34.0
- supabase==2.0.2
- python-dotenv==1.0.0
- httpx==0.24.1

### 3. Database Setup
Run the SQL schema found in `config/create_schema.sql` in your Supabase database to create the required tables.

### 4. Telegram Session
Before running the pipeline, you need to authenticate with Telegram:
```bash
python telegram_tools/reauthenticate.py
```
This creates a session file that allows the pipeline to download messages.

### 5. Running the Pipeline
Once configured:
```bash
python yellow_pipeline.py
```

The pipeline will:
1. Download Telegram messages from configured groups
2. Process activity scores with session-based multipliers
3. Track cross-engagement between ambassadors
4. Identify tweet threads
5. Generate and update leaderboards

## Usage Notes
- This is designed to run as a scheduled task (e.g., GitHub Actions, cron job)
- The bot components in `Yellow-main/` are legacy and have been cleaned up
- All data is stored in Supabase for persistence and rollback support

## Recent Changes (Nov 23, 2025)
- Cleaned up duplicate folders (`Yellow-main/`, `to_delete/`)
- Installed Python 3.11 and all dependencies
- Created `.env.example` template
- Set up workflow for dependency verification
- Updated `.gitignore` for Python and Replit environment

## User Preferences
- Language: Portuguese (PT-BR) for documentation and logs
