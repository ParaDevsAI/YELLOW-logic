import os
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
import re
import sys
import pandas as pd

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
INPUT_CSV_PATH = 'found_tweets.csv'
TWITTER_ID_PATTERN = re.compile(r'status/(\d+)')

def get_unique_tweet_ids_from_csv() -> set[str]:
    """
    Reads the CSV, cleans the URLs, extracts tweet IDs, and returns a unique set of IDs.
    """
    try:
        df = pd.read_csv(INPUT_CSV_PATH)
        logging.info(f"Successfully read {len(df)} rows from '{INPUT_CSV_PATH}'.")
    except FileNotFoundError:
        logging.error(f"Error: The file '{INPUT_CSV_PATH}' was not found.")
        return set()

    # Clean URLs by removing query parameters
    df['clean_url'] = df['tweet_url'].apply(lambda url: url.split('?')[0])

    # Extract tweet IDs using regex
    df['tweet_id'] = df['clean_url'].str.extract(TWITTER_ID_PATTERN)

    # Drop rows where no tweet_id could be extracted
    df.dropna(subset=['tweet_id'], inplace=True)
    
    unique_ids = set(df['tweet_id'].unique())
    logging.info(f"Found {len(unique_ids)} unique tweet IDs in the CSV file.")
    
    return unique_ids

def get_all_tweet_ids_from_supabase() -> set[str]:
    """
    Fetches all tweet_id from the tweets table in Supabase.
    Handles pagination to retrieve all records.
    """
    all_tweet_ids = set()
    try:
        page = 0
        while True:
            # Supabase Python client's range is inclusive, so we fetch 1000 at a time.
            response = supabase.table('tweets').select('tweet_id').range(page * 1000, (page + 1) * 1000 - 1).execute()
            
            if not response.data:
                break
                
            for item in response.data:
                # Ensure tweet_id is stored as a string, matching the CSV extraction
                if item.get('tweet_id'):
                    all_tweet_ids.add(str(item['tweet_id']))
            
            if len(response.data) < 1000:
                break # Last page
            
            page += 1

        logging.info(f"Successfully fetched {len(all_tweet_ids)} tweet IDs from the Supabase 'tweets' table.")
        return all_tweet_ids
        
    except Exception as e:
        logging.error(f"Error fetching tweet IDs from Supabase: {e}")
        return set()

def main():
    """Main function to process and compare tweet IDs."""
    logging.info("Starting the tweet comparison process...")
    
    csv_tweet_ids = get_unique_tweet_ids_from_csv()
    if not csv_tweet_ids:
        logging.error("Could not retrieve tweet IDs from CSV. Aborting.")
        return

    supabase_tweet_ids = get_all_tweet_ids_from_supabase()
    # It's okay if supabase has no tweets yet, so we don't abort if it's empty.

    # Find the tweet IDs that are in the CSV but NOT in Supabase
    missing_tweet_ids = csv_tweet_ids - supabase_tweet_ids
    
    num_missing = len(missing_tweet_ids)

    print("\n" + "="*50)
    print("           Comparison Report")
    print("="*50 + "\n")
    print(f"Tweets found in CSV (unique): {len(csv_tweet_ids)}")
    print(f"Tweets already in Supabase:   {len(supabase_tweet_ids)}")
    print("-" * 50)
    print(f"Tweets to be added:           {num_missing}")
    print("\n" + "="*50)

if __name__ == "__main__":
    main() 