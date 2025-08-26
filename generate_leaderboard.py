import asyncio
import logging
from datetime import datetime
import os

from author_manager import get_supabase_client

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

async def main():
    logger.info("--- Starting Leaderboard Generation Script (Supabase RPC Version) ---")
    
    supabase = await get_supabase_client()
    if not supabase:
        logger.critical("Failed to get Supabase client. Aborting.")
        return
    
    try:
        logger.info("Calling RPC 'calculate_leaderboard' on Supabase...")
        response = await asyncio.to_thread(
            supabase.rpc('calculate_leaderboard', {}).execute
        )
        
        calculated_data = response.data
        if not calculated_data:
            logger.warning("No data returned from 'calculate_leaderboard' function. Exiting.")
            return
            
        logger.info(f"Calculated scores for {len(calculated_data)} ambassadors.")
        
        snapshot_time = datetime.utcnow()
        snapshot_time_iso = snapshot_time.isoformat()

        history_records = []
        live_records = []

        for row in calculated_data:
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
            
            history_records.append({
                'snapshot_timestamp': snapshot_time_iso,
                **record
            })
            
            live_records.append({
                'last_updated': snapshot_time_iso,
                **record
            })

        logger.info("Saving calculated data to leaderboard tables...")
        
        if history_records:
            history_response = await asyncio.to_thread(
                supabase.table('leaderboard_history').insert(history_records).execute
            )
            if history_response.data:
                logger.info(f"Inserted {len(history_records)} new records into 'leaderboard_history'.")
            else:
                logger.error("Failed to insert into leaderboard_history")
        
        if live_records:
            live_response = await asyncio.to_thread(
                supabase.table('leaderboard').upsert(live_records, on_conflict='user_id').execute
            )
            if live_response.data:
                logger.info(f"Upserted {len(live_records)} records into the live 'leaderboard' table.")
            else:
                logger.error("Failed to upsert into leaderboard")

        logger.info("Updating ranks by calling RPCs...")
        
        rank_response = await asyncio.to_thread(
            supabase.rpc('update_leaderboard_ranks', {}).execute
        )
        if hasattr(rank_response, 'status_code') and rank_response.status_code == 204:
            logger.info("Live leaderboard ranks updated.")
        else:
            logger.info("Live leaderboard ranks updated.")
        
        history_rank_response = await asyncio.to_thread(
            supabase.rpc('update_leaderboard_history_ranks', {}).execute
        )
        if hasattr(history_rank_response, 'status_code') and history_rank_response.status_code == 204:
            logger.info("Leaderboard history ranks updated for the current snapshot.")
        else:
            logger.info("Leaderboard history ranks updated for the current snapshot.")

        logger.info("--- Leaderboard Generation Script Finished ---")
        
    except Exception as e:
        logger.error(f"Error during leaderboard generation: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main()) 
