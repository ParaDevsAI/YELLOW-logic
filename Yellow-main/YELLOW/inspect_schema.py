import sqlite3
import logging
import sys
import os

DATABASE_FILE = 'engagement_database.db'
logger = logging.getLogger(__name__)

def inspect_database_schema():
    """
    Connects to the SQLite database and prints the schema of all tables.
    """
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()

        # Query the sqlite_master table to get the schema for all user-defined tables
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        
        tables = cursor.fetchall()
        
        if not tables:
            print("No tables found in the database.")
            return

        print("=" * 50)
        print(f"DATABASE SCHEMA: {DATABASE_FILE}")
        print("=" * 50, "\n")

        for table in tables:
            table_name = table[0]
            schema_sql = table[1]
            
            print(f"-- Schema for table: {table_name} --")
            print(schema_sql.strip() + ";")
            print("-" * 50)

        print("\n" + "=" * 50)
        print("          TABLE DATA          ")
        print("=" * 50, "\n")

        for table in tables:
            table_name = table[0]
            print(f"-- Data for table: {table_name} --")
            
            try:
                cursor.execute(f"SELECT * FROM {table_name}")
                rows = cursor.fetchall()

                if not rows:
                    print("Table is empty.\n")
                else:
                    # Print header
                    column_names = [description[0] for description in cursor.description]
                    print(" | ".join(column_names))
                    print("-" * (sum(len(col) for col in column_names) + 3 * (len(column_names) - 1)))

                    # Print rows
                    for row in rows:
                        print(" | ".join(str(item) for item in row))
                    print("\n")

            except sqlite3.Error as e:
                print(f"Could not retrieve data from {table_name}. Error: {e}\n")
            
            print("-" * 50 + "\n")

    except sqlite3.Error as e:
        logger.error(f"Database error while inspecting schema: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    # Set console to utf-8 on windows, to handle special characters
    if os.name == 'nt':
        sys.stdout.reconfigure(encoding='utf-8')
    inspect_database_schema() 