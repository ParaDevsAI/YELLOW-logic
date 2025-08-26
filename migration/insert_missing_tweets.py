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

def get_existing_tweet_ids_from_supabase() -> set[str]:
    """Fetches all tweet_id from the tweets table in Supabase."""
    logging.info("Fetching existing tweet IDs from Supabase...")
    all_tweet_ids = set()
    try:
        page = 0
        while True:
            response = supabase.table('tweets').select('tweet_id').range(page * 1000, (page + 1) * 1000 - 1).execute()
            if not response.data:
                break
            all_tweet_ids.update(str(item['tweet_id']) for item in response.data if item.get('tweet_id'))
            if len(response.data) < 1000:
                break
            page += 1
    except Exception as e:
        logging.error(f"Error fetching existing tweet IDs from Supabase: {e}")
    logging.info(f"Found {len(all_tweet_ids)} existing tweet IDs in Supabase.")
    return all_tweet_ids

def get_username_to_twitter_id_map() -> dict[str, str]:
    """
    Fetches authors from Supabase and creates a map from 
    twitter_username (lowercase) to twitter_id. This twitter_id is
    what's used as 'author_id' in the 'tweets' table.
    """
    logging.info("Fetching author username-to-ID map from Supabase...")
    author_map = {}
    try:
        # Fetch the two columns we need for mapping
        response = supabase.table('authors').select('twitter_id, twitter_username').execute()
        if response.data:
            author_map = {
                item['twitter_username'].lower(): item['twitter_id'] 
                for item in response.data 
                if item.get('twitter_username') and item.get('twitter_id')
            }
    except Exception as e:
        logging.error(f"Error fetching authors from Supabase: {e}")
    logging.info(f"Created a map for {len(author_map)} authors.")
    return author_map

def main():
    """Main function to insert missing tweets."""
    logging.info("Starting process to insert missing tweets...")

    # 1. Get data from Supabase
    existing_ids = get_existing_tweet_ids_from_supabase()
    username_to_id_map = get_username_to_twitter_id_map()
    if not username_to_id_map:
        logging.error("Could not fetch author map. Aborting.")
        return

    # 2. Read and process the CSV file
    try:
        df = pd.read_csv(INPUT_CSV_PATH, encoding='utf-8')
        df.columns = df.columns.str.strip() # Remove leading/trailing spaces from headers
        logging.info(f"Read {len(df)} rows from '{INPUT_CSV_PATH}'.")
    except FileNotFoundError:
        logging.error(f"Input file '{INPUT_CSV_PATH}' not found. Aborting.")
        return

    df['clean_url'] = df['tweet_url'].apply(lambda url: str(url).split('?')[0])
    df['tweet_id'] = df['clean_url'].str.extract(TWITTER_ID_PATTERN)
    df.dropna(subset=['tweet_id', 'author_twitter_username'], inplace=True)
    df.drop_duplicates(subset=['tweet_id'], inplace=True)

    # 3. Filter for tweets that are actually missing
    missing_df = df[~df['tweet_id'].isin(existing_ids)].copy()
    logging.info(f"Found {len(missing_df)} tweets to be inserted.")

    if missing_df.empty:
        logging.info("No new tweets to insert. The database is already up to date.")
        return

    # 4. Prepare the data for insertion
    missing_df['author_id'] = missing_df['author_twitter_username'].str.lower().map(username_to_id_map)
    
    # Check for any authors in the CSV that weren't found in the database
    rows_without_author_id = missing_df[missing_df['author_id'].isnull()]
    if not rows_without_author_id.empty:
        logging.warning(f"Could not find a twitter_id for {len(rows_without_author_id)} rows. These tweets will be skipped.")
        logging.warning("Skipped authors: " + ", ".join(rows_without_author_id['author_twitter_username'].unique()))
        missing_df.dropna(subset=['author_id'], inplace=True)

    # Ensure tweet_id is a numeric type that can be handled by the DB.
    missing_df['tweet_id'] = missing_df['tweet_id'].astype('int64')

    # Rename 'clean_url' to 'url' to match the database schema
    missing_df.rename(columns={'clean_url': 'url'}, inplace=True)

    records_to_insert = missing_df[['tweet_id', 'author_id', 'url']].to_dict('records')

    # 5. Insert the data in batches
    batch_size = 500
    total_inserted = 0
    logging.info(f"Beginning insertion of {len(records_to_insert)} records in batches of {batch_size}...")
    
    for i in range(0, len(records_to_insert), batch_size):
        batch = records_to_insert[i:i + batch_size]
        try:
            response = supabase.table('tweets').insert(batch).execute()
            # PostgREST does not return the count of inserted rows in the same way,
            # so we check for errors. If no error, we assume success for the batch.
            if response.data:
                inserted_count = len(response.data)
                total_inserted += inserted_count
                logging.info(f"Successfully inserted batch {i//batch_size + 1}, containing {inserted_count} records.")

        except Exception as e:
            logging.error(f"An error occurred during batch insertion: {e}")
            logging.error(f"Problematic batch starts with tweet_id: {batch[0]['tweet_id']}")
            # Decide if you want to stop or continue on error
            break
            
    print("\n" + "="*50)
    print("         Insertion Complete")
    print("="*50 + "\n")
    print(f"Successfully inserted {total_inserted} new tweets into the database.")
    print("\n" + "="*50)


if __name__ == "__main__":
    main() 