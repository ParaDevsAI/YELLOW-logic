"""
generate_leaderboard.py (Supabase Refactor)

This script calculates the comprehensive scores for all ambassadors and
populates both the live and historical leaderboards by leveraging
functions (RPCs) directly within the Supabase database.

Execution Flow:
1.  Connects to Supabase.
2.  Calls the `calculate_leaderboard` RPC function. This function performs
    all the complex calculations on the database server.
3.  Receives the calculated data from the RPC.
4.  Processes the data in Python to prepare it for insertion:
    a) Adds a `snapshot_timestamp` for the history table.
    b) Adds a `last_updated` timestamp for the live table.
    c) Maps the columns from the RPC result to the table columns.
5.  Saves the data in batches:
    a) Inserts into `leaderboard_history`.
    b) Upserts into `leaderboard`.
6.  Calls two more RPC functions (`update_leaderboard_ranks` and 
    `update_leaderboard_history_ranks`) to calculate and set the final
    rank for each user in both tables.
"""
import asyncio
import logging
from datetime import datetime
import os

# Import the centralized Supabase client
from author_manager import get_supabase_client

# --- Logging Configuration ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# All complex SQL logic has been moved to Supabase functions.
# The Python script is now a simple orchestrator.

async def main():
    """Main function to orchestrate the entire leaderboard generation using Supabase RPCs."""
    logger.info("--- Starting Leaderboard Generation Script (Supabase RPC Version) ---")
    
    supabase = await get_supabase_client()
    if not supabase:
        logger.critical("Failed to get Supabase client. Aborting.")
        return
    
    try:
        # 1. Call the RPC function to get the calculated leaderboard data
        logger.info("Calling RPC 'calculate_leaderboard' on Supabase...")
        response = await asyncio.to_thread(
            supabase.rpc('calculate_leaderboard').execute
        )
        
        calculated_data = response.data
        if not calculated_data:
            logger.warning("No data returned from 'calculate_leaderboard' function. Exiting.")
            return
            
        logger.info(f"Calculated scores for {len(calculated_data)} ambassadors.")
        
        snapshot_time = datetime.utcnow()
        snapshot_time_iso = snapshot_time.isoformat()

        # 2. Prepare data for batch operations
        history_records = []
        live_records = []

        for row in calculated_data:
            # Map the RPC result to the table schema.
            # The RPC returns 'telegram_id', but the table's column is 'user_id'.
            record = {
                'user_id': row.get('telegram_id'),
                'telegram_name': row.get('telegram_name'),
                'twitter_username': row.get('twitter_username'),
                'count_tweets_text_only': row.get('count_tweets_text_only'),
                'count_tweets_image': row.get('count_tweets_image'),
                'count_tweets_thread': row.get('count_tweets_thread'),
                'count_tweets_video': row.get('count_tweets_video'),
                'total_score_from_tweets': row.get('total_score_from_tweets'),
                'count_retweets_made': row.get('count_retweets_made'),
                'count_comments_made': row.get('count_comments_made'),
                'total_score_from_engagements': row.get('total_score_from_engagements'),
                'total_score_from_telegram': row.get('total_score_from_telegram'),
                'count_partner_introduction': row.get('count_partner_introduction'),
                'count_hosting_ama': row.get('count_hosting_ama'),
                'count_recruitment_ambassador': row.get('count_recruitment_ambassador'),
                'count_product_feedback': row.get('count_product_feedback'),
                'count_recruitment_investor': row.get('count_recruitment_investor'),
                'total_score_from_contributions': row.get('total_score_from_contributions'),
                'grand_total_score': row.get('grand_total_score')
            }
            
            # Prepare record for leaderboard_history
            history_records.append({
                'snapshot_timestamp': snapshot_time_iso,
                **record
            })
            
            # Prepare record for the main leaderboard
            live_records.append({
                'last_updated': snapshot_time_iso,
                **record
            })

        # 3. Save the data to the tables
        logger.info("Saving calculated data to leaderboard tables...")
        if live_records:
            await asyncio.to_thread(
                supabase.table('leaderboard').upsert(live_records, on_conflict='user_id').execute
            )
            logger.info(f"Upserted {len(live_records)} records into the live 'leaderboard' table.")
        
        if history_records:
            await asyncio.to_thread(
                supabase.table('leaderboard_history').insert(history_records).execute
            )
            logger.info(f"Inserted {len(history_records)} new records into 'leaderboard_history'.")

        # 4. Call the RPC functions to update ranks
        logger.info("Updating ranks by calling RPCs...")
        await asyncio.to_thread(
            supabase.rpc('update_leaderboard_ranks').execute
        )
        logger.info("Live leaderboard ranks updated.")

        await asyncio.to_thread(
            supabase.rpc('update_leaderboard_history_ranks', {'snapshot_ts': snapshot_time_iso}).execute
        )
        logger.info("Leaderboard history ranks updated for the current snapshot.")

    except Exception as e:
        logger.error(f"An error occurred during leaderboard generation: {e}", exc_info=True)
            
    logger.info("--- Leaderboard Generation Script Finished ---")

if __name__ == '__main__':
    # Make sure to load environment variables for the Supabase client
    from dotenv import load_dotenv
    load_dotenv()
    asyncio.run(main()) 