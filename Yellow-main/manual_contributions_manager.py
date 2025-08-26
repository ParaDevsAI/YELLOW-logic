"""
manual_contributions_manager.py

This module provides a set of functions to manage manual point contributions
for ambassadors. It serves as the database backend logic that an admin dashboard
or a bot command interface would call.

Each function handles a specific CRUD (Create, Read, Update, Delete) operation
on the `manual_contributions` table.
"""
import aiosqlite
import logging
from datetime import datetime
import os
from typing import List, Dict, Any

# --- Logging Configuration ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Constants and Configuration ---
DATABASE_DIR = os.path.dirname(__file__)
DATABASE_FILE = os.path.join(DATABASE_DIR, 'engagement_database.db')

# --- Standardized Contribution Types ---
# Using constants prevents typos and ensures data consistency.
CONTRIBUTION_TYPES = {
    "PARTNER_INTRODUCTION": "Introduce Yellow to potential partners and integrations",
    "HOSTING_EVENT": "Hosting Twitter Spaces, AMAs, or meetups",
    "RECRUIT_AMBASSADOR_HIGH_LEVEL": "Bringing in other high-level ambassadors",
    "PRODUCT_FEEDBACK": "Helping test Yellow products, reporting bugs, and giving feedback",
    "RECRUIT_INVESTOR": "Recruitment of investors",
    "RECRUIT_AMBASSADOR_REGULAR": "Recruitment of high-quality ambassadors",
    "PRIVATE_SALE_PARTICIPATION": "Bringing KOLs or friends to take part in the private sale"
}

async def get_db_connection() -> aiosqlite.Connection:
    """Establishes a connection to the SQLite database."""
    conn = await aiosqlite.connect(DATABASE_FILE)
    conn.row_factory = aiosqlite.Row
    return conn

async def add_contribution(
    user_id: int, 
    contribution_type: str, 
    points_awarded: int, 
    description: str, 
    recorded_by: str
) -> int | None:
    """
    Adds a new manual contribution record to the database.

    Returns:
        The ID of the newly created record, or None on failure.
    """
    if contribution_type not in CONTRIBUTION_TYPES:
        logger.error(f"Invalid contribution type: {contribution_type}")
        return None

    sql = """
        INSERT INTO manual_contributions 
        (user_id, contribution_type, points_awarded, description, recorded_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?);
    """
    conn = await get_db_connection()
    try:
        cursor = await conn.cursor()
        await cursor.execute(sql, (
            user_id,
            contribution_type,
            points_awarded,
            description,
            recorded_by,
            datetime.utcnow()
        ))
        await conn.commit()
        new_id = cursor.lastrowid
        logger.info(f"Successfully added contribution {new_id} for user {user_id} with {points_awarded} points.")
        return new_id
    except aiosqlite.Error as e:
        logger.error(f"Database error while adding contribution for user {user_id}: {e}")
        await conn.rollback()
        return None
    finally:
        if conn:
            await conn.close()

async def remove_contribution(contribution_id: int) -> bool:
    """
    Removes a manual contribution record from the database by its unique ID.

    Returns:
        True on success, False on failure.
    """
    sql = "DELETE FROM manual_contributions WHERE id = ?;"
    conn = await get_db_connection()
    try:
        cursor = await conn.cursor()
        await cursor.execute(sql, (contribution_id,))
        await conn.commit()
        if cursor.rowcount > 0:
            logger.info(f"Successfully removed contribution ID {contribution_id}.")
            return True
        else:
            logger.warning(f"Attempted to remove contribution ID {contribution_id}, but it was not found.")
            return False
    except aiosqlite.Error as e:
        logger.error(f"Database error while removing contribution {contribution_id}: {e}")
        await conn.rollback()
        return False
    finally:
        if conn:
            await conn.close()

async def edit_contribution(contribution_id: int, new_points: int, new_description: str) -> bool:
    """
    Edits the points and description of an existing manual contribution.

    Returns:
        True on success, False on failure.
    """
    sql = "UPDATE manual_contributions SET points_awarded = ?, description = ? WHERE id = ?;"
    conn = await get_db_connection()
    try:
        cursor = await conn.cursor()
        await cursor.execute(sql, (new_points, new_description, contribution_id))
        await conn.commit()
        if cursor.rowcount > 0:
            logger.info(f"Successfully edited contribution ID {contribution_id}.")
            return True
        else:
            logger.warning(f"Attempted to edit contribution ID {contribution_id}, but it was not found.")
            return False
    except aiosqlite.Error as e:
        logger.error(f"Database error while editing contribution {contribution_id}: {e}")
        await conn.rollback()
        return False
    finally:
        if conn:
            await conn.close()

async def get_contributions_for_user(user_id: int) -> List[Dict[str, Any]]:
    """
    Retrieves all manual contributions for a specific user.

    Returns:
        A list of dictionaries, each representing a contribution record.
    """
    sql = "SELECT * FROM manual_contributions WHERE user_id = ? ORDER BY created_at DESC;"
    conn = await get_db_connection()
    try:
        cursor = await conn.cursor()
        await cursor.execute(sql, (user_id,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    except aiosqlite.Error as e:
        logger.error(f"Database error retrieving contributions for user {user_id}: {e}")
        return []
    finally:
        if conn:
            await conn.close() 