#!/usr/bin/env python3
"""
Create missing SQL tables for the Deepgram analysis platform
"""
import os
import sys
import pymssql
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def connect_to_sql():
    """Connect to the Azure SQL database"""
    try:
        # Get connection details from environment variables
        db_server = os.getenv('PGHOST') or 'callcenter1.database.windows.net'
        db_name = os.getenv('PGDATABASE') or 'callcenter'
        db_user = os.getenv('PGUSER') or 'shahul'
        db_password = os.getenv('PGPASSWORD') or 'apple123!@#'
        
        # Connect to the database
        conn = pymssql.connect(
            server=db_server,
            database=db_name,
            user=db_user,
            password=db_password
        )
        
        logger.info(f"Connected to Azure SQL Server: {db_server}, database: {db_name}")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Azure SQL database: {str(e)}")
        return None

def check_table_exists(conn, table_name):
    """Check if a table exists in the database"""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM sys.tables WHERE name = '{table_name}'")
        result = cursor.fetchone()
        exists = result[0] > 0
        logger.info(f"Table {table_name} exists: {exists}")
        return exists
    except Exception as e:
        logger.error(f"Error checking if table {table_name} exists: {str(e)}")
        return False
    finally:
        cursor.close()

def create_forbidden_phrase_details_table(conn):
    """Create the rdt_forbidden_phrase_details table"""
    cursor = conn.cursor()
    try:
        # First check if the table already exists
        if check_table_exists(conn, 'rdt_forbidden_phrase_details'):
            logger.info("Table rdt_forbidden_phrase_details already exists")
            return True
        
        # Create the table
        sql = """
        CREATE TABLE rdt_forbidden_phrase_details (
            id INT IDENTITY(1,1) PRIMARY KEY,
            forbidden_phrase_id INT NOT NULL,
            category NVARCHAR(100) NOT NULL,
            phrase NVARCHAR(255) NOT NULL,
            start_time INT,
            end_time INT,
            confidence INT,
            snippet NVARCHAR(MAX),
            created_dt DATETIME DEFAULT GETDATE() NOT NULL
        )
        """
        cursor.execute(sql)
        
        # Add foreign key if needed
        sql = """
        IF EXISTS (SELECT * FROM sys.tables WHERE name = 'rdt_forbidden_phrases')
        BEGIN
            IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_forbidden_phrases_details_forbidden_phrases')
            BEGIN
                ALTER TABLE rdt_forbidden_phrase_details
                ADD CONSTRAINT FK_forbidden_phrases_details_forbidden_phrases
                FOREIGN KEY (forbidden_phrase_id) REFERENCES rdt_forbidden_phrases(id);
            END
        END
        """
        cursor.execute(sql)
        
        conn.commit()
        logger.info("Successfully created rdt_forbidden_phrase_details table")
        return True
    except Exception as e:
        logger.error(f"Error creating rdt_forbidden_phrase_details table: {str(e)}")
        conn.rollback()
        return False
    finally:
        cursor.close()

def main():
    """Main function to create missing SQL tables"""
    conn = connect_to_sql()
    if not conn:
        logger.error("Failed to connect to database. Exiting.")
        sys.exit(1)
    
    try:
        # Create the rdt_forbidden_phrase_details table
        success = create_forbidden_phrase_details_table(conn)
        if success:
            logger.info("SQL table creation completed successfully")
        else:
            logger.error("Failed to create SQL tables")
            sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()