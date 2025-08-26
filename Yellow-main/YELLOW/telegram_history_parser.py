import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
import re
import sys
import csv

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
    exit()

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    logging.error(f"Failed to create Supabase client: {e}")
    exit()

# --- Constants ---
JSON_FILE_PATH = 'result.json' # Assuming the file is in the same directory
OUTPUT_CSV_PATH = 'found_tweets.csv' # Output file name
START_DATE = datetime.fromisoformat("2025-04-23T00:00:00")
# This regex captures the username (group 1) and tweet ID (group 2) from Twitter/X URLs
TWITTER_URL_PATTERN = re.compile(r'https?://(?:www\.)?(?:x|twitter)\.com/(\w+)/status/(\d+)')

def get_authors_twitter_usernames() -> list[str]:
    """Fetches all twitter_username from the authors table."""
    try:
        response = supabase.table('authors').select('twitter_username').execute()
        if response.data:
            usernames = [item['twitter_username'] for item in response.data if item.get('twitter_username')]
            logging.info(f"Successfully fetched {len(usernames)} Twitter usernames from the database.")
            return usernames
        logging.warning("No authors found in the database.")
        return []
    except Exception as e:
        logging.error(f"Error fetching authors from Supabase: {e}")
        return []

def extract_tweet_url_from_text_entity(entity):
    """Extracts a URL from a message text entity if it's a Twitter/X link."""
    url = None
    if isinstance(entity, dict) and entity.get('type') == 'link':
        url = entity.get('text', '')
    elif isinstance(entity, str):
        url = entity
    
    if url and ('twitter.com/' in url or 'x.com/' in url):
        return url
        
    return None

def find_tweets_by_author_in_history(author_usernames: list[str]):
    """
    Parses the Telegram history JSON, finds all tweet links by known authors,
    and saves them to a CSV file.
    """
    try:
        with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
            history_data = json.load(f)
    except FileNotFoundError:
        logging.error(f"Error: The file '{JSON_FILE_PATH}' was not found. Please make sure it's in the same directory as the script.")
        return
    except json.JSONDecodeError:
        logging.error(f"Error: Could not decode JSON from the file '{JSON_FILE_PATH}'. Check if the file is a valid JSON.")
        return

    messages = history_data.get('messages', [])
    if not messages:
        logging.warning("No messages found in the JSON file.")
        return

    found_tweets = {username: set() for username in author_usernames}
    author_usernames_lower = {user.lower() for user in author_usernames}

    logging.info(f"Starting to parse {len(messages)} messages from {START_DATE.isoformat()}...")

    for message in messages:
        message_date_str = message.get('date')
        if not message_date_str:
            continue
        
        try:
            message_date = datetime.fromisoformat(message_date_str)
        except (ValueError, TypeError):
            continue

        if message_date < START_DATE:
            continue

        text_content = message.get('text', [])
        
        # The 'text' field can be a string or a list of entities.
        # We normalize it into a list to handle both cases uniformly.
        if isinstance(text_content, str):
             text_entities = [text_content]
        elif isinstance(text_content, list):
            text_entities = text_content
        else:
            continue

        for entity in text_entities:
            url = extract_tweet_url_from_text_entity(entity)
            if not url:
                continue

            match = TWITTER_URL_PATTERN.search(url)
            if match:
                tweet_author_username = match.group(1)
                # Compare in a case-insensitive way
                if tweet_author_username.lower() in author_usernames_lower:
                    # Find the original cased username to use as a key
                    original_username = next((u for u in author_usernames if u.lower() == tweet_author_username.lower()), None)
                    if original_username:
                         found_tweets[original_username].add(url)

    logging.info("Finished parsing messages. Now writing to CSV...")
    
    total_found = 0
    try:
        with open(OUTPUT_CSV_PATH, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            # Write header
            writer.writerow(['author_twitter_username', 'tweet_url'])
            
            # Write data
            for username, tweets in sorted(found_tweets.items()):
                if tweets:
                    total_found += len(tweets)
                    for tweet_url in sorted(list(tweets)):
                        writer.writerow([username, tweet_url])

        if total_found == 0:
            logging.warning("\nNo matching tweet links were found to save.")
        else:
            logging.info(f"\n--- Successfully saved {total_found} unique tweets to '{OUTPUT_CSV_PATH}' ---")

    except IOError as e:
        logging.error(f"Error writing to CSV file '{OUTPUT_CSV_PATH}': {e}")


if __name__ == "__main__":
    author_twitter_usernames = get_authors_twitter_usernames()
    if author_twitter_usernames:
        find_tweets_by_author_in_history(author_twitter_usernames)
    else:
        logging.warning("No author Twitter usernames found in the database. Cannot proceed.") 