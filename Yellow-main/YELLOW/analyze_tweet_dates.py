import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
import re
import sys
import pandas as pd
from collections import Counter

# --- Configuration ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set console to utf-8 on windows, to handle special characters
if os.name == 'nt':
    sys.stdout.reconfigure(encoding='utf-8')

# --- Supabase Client ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logging.error("Supabase URL and Key must be set in the .env file.")
    sys.exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    logging.error(f"Failed to create Supabase client: {e}")
    sys.exit(1)

# --- Constants ---
JSON_HISTORY_PATH = 'result.json'
FOUND_TWEETS_CSV_PATH = 'found_tweets.csv'
TWITTER_ID_PATTERN = re.compile(r'status/(\d+)')
TWITTER_URL_PATTERN_GENERIC = re.compile(r'https?://(?:www\.)?(?:x|twitter)\.com/(\w+)/status/(\d+)')


def get_tweet_to_date_map_from_json() -> dict[str, datetime]:
    """
    Parses the full Telegram JSON history to create a mapping from each unique
    tweet ID to its original message timestamp.
    """
    logging.info(f"Parsing '{JSON_HISTORY_PATH}' to map tweet IDs to dates...")
    tweet_id_to_date = {}
    
    try:
        with open(JSON_HISTORY_PATH, 'r', encoding='utf-8') as f:
            history = json.load(f)
    except FileNotFoundError:
        logging.error(f"Required history file '{JSON_HISTORY_PATH}' not found.")
        return {}
    except json.JSONDecodeError:
        logging.error(f"Could not decode JSON from '{JSON_HISTORY_PATH}'.")
        return {}

    for message in history.get('messages', []):
        message_date_str = message.get('date')
        if not message_date_str:
            continue
        
        try:
            message_date = datetime.fromisoformat(message_date_str)
        except (ValueError, TypeError):
            continue

        text_content = message.get('text', [])
        entities = text_content if isinstance(text_content, list) else [text_content]

        for entity in entities:
            url = None
            if isinstance(entity, dict) and entity.get('type') == 'link':
                url = entity.get('text', '')
            elif isinstance(entity, str):
                url = entity

            if url:
                match = TWITTER_URL_PATTERN_GENERIC.search(url)
                if match:
                    tweet_id = match.group(2)
                    # Keep the earliest date if a tweet is shared multiple times
                    if tweet_id not in tweet_id_to_date:
                        tweet_id_to_date[tweet_id] = message_date
    
    logging.info(f"Finished parsing JSON. Found {len(tweet_id_to_date)} unique tweets in total.")
    return tweet_id_to_date


def get_unique_tweet_ids_from_csv() -> set[str]:
    """Reads the found_tweets.csv and returns a set of unique tweet IDs."""
    try:
        df = pd.read_csv(FOUND_TWEETS_CSV_PATH)
    except FileNotFoundError:
        logging.error(f"Error: The file '{FOUND_TWEETS_CSV_PATH}' was not found.")
        return set()

    df['clean_url'] = df['tweet_url'].apply(lambda url: url.split('?')[0])
    df['tweet_id'] = df['clean_url'].str.extract(TWITTER_ID_PATTERN)
    df.dropna(subset=['tweet_id'], inplace=True)
    return set(df['tweet_id'].unique())


def get_existing_tweets_from_supabase() -> dict[str, datetime]:
    """Fetches all tweets from Supabase and returns a map of {tweet_id: createdat}."""
    logging.info("Fetching existing tweets from Supabase...")
    existing_tweets = {}
    try:
        page = 0
        while True:
            response = supabase.table('tweets').select('tweet_id, createdat').range(page * 1000, (page + 1) * 1000 - 1).execute()
            if not response.data:
                break
            
            for item in response.data:
                if item.get('tweet_id') and item.get('createdat'):
                    tweet_id = str(item['tweet_id'])
                    created_at = datetime.fromisoformat(item['createdat'].replace('Z', '+00:00'))
                    existing_tweets[tweet_id] = created_at
            
            if len(response.data) < 1000:
                break
            page += 1
    except Exception as e:
        logging.error(f"Error fetching tweets from Supabase: {e}")
    
    logging.info(f"Found {len(existing_tweets)} existing tweets in Supabase.")
    return existing_tweets


def main():
    """Main analysis function."""
    # 1. Get all unique tweet IDs from our registered authors' shares
    csv_tweet_ids = get_unique_tweet_ids_from_csv()
    if not csv_tweet_ids:
        logging.error(f"No tweets found in '{FOUND_TWEETS_CSV_PATH}'. Aborting.")
        return

    # 2. Get all existing tweets from Supabase with their creation dates
    supabase_tweets_map = get_existing_tweets_from_supabase()
    
    # 3. Determine which tweets are missing from the CSV list
    supabase_ids = set(supabase_tweets_map.keys())
    missing_ids = csv_tweet_ids - supabase_ids

    # 4. Get the date mapping for ALL tweets from the full history,
    #    so we can find the dates for our missing tweets.
    json_tweet_to_date_map = get_tweet_to_date_map_from_json()
    if not json_tweet_to_date_map:
        return

    # 5. Aggregate counts by month
    # For missing tweets, use the original post date from the JSON history
    missing_monthly_counts = Counter(
        json_tweet_to_date_map[tid].strftime('%Y-%m') 
        for tid in missing_ids if tid in json_tweet_to_date_map
    )
    # For existing tweets, use the date they were created in our DB
    existing_monthly_counts = Counter(date.strftime('%Y-%m') for date in supabase_tweets_map.values())

    # 6. Print the report
    print("\n" + "="*60)
    print("        Relatório de Análise de Datas dos Tweets (Corrigido)")
    print("="*60 + "\n")

    print(f"--- 1. Tweets Faltando ({len(missing_ids)} no total) ---")
    print("(Agrupados pela data em que foram postados no Telegram)\n")
    if not missing_monthly_counts:
        print("Nenhum tweet novo encontrado para adicionar.")
    else:
        for month in sorted(missing_monthly_counts.keys()):
            print(f"  - {month}: {missing_monthly_counts[month]} tweets")
    
    print("\n" + "-" * 60 + "\n")

    print(f"--- 2. Tweets Existentes ({len(supabase_ids)} no total) ---")
    print("(Agrupados pela data em que foram inseridos no banco de dados)\n")
    if not existing_monthly_counts:
        print("Nenhum tweet existente no Supabase.")
    else:
        for month in sorted(existing_monthly_counts.keys()):
            print(f"  - {month}: {existing_monthly_counts[month]} tweets")
    
    print("\n" + "="*60)


if __name__ == "__main__":
    main() 