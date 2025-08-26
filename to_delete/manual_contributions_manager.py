"""
manual_contributions_manager.py (Supabase Refactor)

This module provides a set of functions to manage manual point contributions
for ambassadors, interacting directly with the Supabase database. It serves
as the backend logic that an admin dashboard or a bot command interface would call.
"""
import logging
from datetime import datetime
from typing import List, Dict, Any

# Import the centralized Supabase client
from author_manager import initialize_supabase_client

# --- Logging Configuration ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def add_contribution(
    user_id: int, 
    contribution_type: str, 
    points_awarded: int, 
    description: str, 
    recorded_by: str
) -> Dict[str, Any] | None:
    """
    Adds a new manual contribution record to the Supabase database.
    This version allows for variable points.

    Returns:
        The newly created record as a dictionary, or None on failure.
    """
    supabase = initialize_supabase_client()
    if not supabase:
        logger.error("Failed to get Supabase client.")
        return None

    record = {
        'user_id': user_id,
        'contribution_type': contribution_type,
        'points_awarded': points_awarded,
        'description': description,
        'recorded_by': recorded_by,
        'created_at': datetime.utcnow().isoformat()
    }

    try:
        response = supabase.table('manual_contributions').insert(record).execute()
        
        if response.data and len(response.data) > 0:
            new_record = response.data[0]
            logger.info(f"Successfully added contribution {new_record.get('id')} for user {user_id} with {points_awarded} points.")
            return new_record
        else:
            logger.error(f"Failed to add contribution for user {user_id}. Response: {response}")
            return None
    except Exception as e:
        logger.error(f"Database error while adding contribution for user {user_id}: {e}", exc_info=True)
        return None

def remove_contribution(contribution_id: int) -> bool:
    """
    Removes a manual contribution record from the Supabase database by its unique ID.
    """
    supabase = initialize_supabase_client()
    if not supabase: return False

    try:
        response = supabase.table('manual_contributions').delete().eq('id', contribution_id).execute()
        if response.data and len(response.data) > 0:
            logger.info(f"Successfully removed contribution ID {contribution_id}.")
            return True
        else:
            logger.warning(f"Attempted to remove contribution ID {contribution_id}, but it was not found.")
            return False
    except Exception as e:
        logger.error(f"Database error while removing contribution {contribution_id}: {e}", exc_info=True)
        return False

def edit_contribution(contribution_id: int, new_points: int, new_description: str) -> bool:
    """
    Edits the points and description of an existing manual contribution in Supabase.
    """
    supabase = initialize_supabase_client()
    if not supabase: return False

    update_data = {'points_awarded': new_points, 'description': new_description}
    
    try:
        response = supabase.table('manual_contributions').update(update_data).eq('id', contribution_id).execute()
        if response.data and len(response.data) > 0:
            logger.info(f"Successfully edited contribution ID {contribution_id}.")
            return True
        else:
            logger.warning(f"Attempted to edit contribution ID {contribution_id}, but it was not found.")
            return False
    except Exception as e:
        logger.error(f"Database error while editing contribution {contribution_id}: {e}", exc_info=True)
        return False

def get_contributions_for_user(user_id: int) -> List[Dict[str, Any]]:
    """
    Retrieves all manual contributions for a specific user from Supabase.
    """
    supabase = initialize_supabase_client()
    if not supabase: return []

    try:
        response = supabase.table('manual_contributions').select('*').eq('user_id', user_id).order('created_at', desc=True).execute()
        return response.data if response.data else []
    except Exception as e:
        logger.error(f"Database error retrieving contributions for user {user_id}: {e}", exc_info=True)
        return [] 