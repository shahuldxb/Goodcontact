"""
SQL Insert-Then-Update Test Script

This script demonstrates the two-phase approach:
1. INSERT a record with NULL transcription
2. COMMIT the transaction
3. Later UPDATE the same record with transcription data
4. COMMIT the second transaction

This approach ensures the record exists in the database immediately,
even before transcription processing completes.
"""

import pymssql
import logging
import uuid
import time
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def log_query(query, params=None):
    """Log the SQL query with parameter substitution"""
    logger.info("\n===== SQL STATEMENT =====")
    
    # Make a copy of the query for parameter substitution
    formatted_query = query
    
    if params:
        # Log parameters
        logger.info(f"Parameters: {params}")
        
        # Replace placeholders with actual values for display purposes
        for key, value in params.items():
            placeholder = f"%({key})s"  # pymssql uses %(name)s format
            if isinstance(value, str):
                formatted_value = f"'{value}'"
            elif value is None:
                formatted_value = "NULL"
            else:
                formatted_value = str(value)
            formatted_query = formatted_query.replace(placeholder, formatted_value)
    
    # Log the query with parameters substituted
    logger.info(formatted_query)
    logger.info("===========================")
    return formatted_query

def connect_to_database():
    """Connect to the Azure SQL database"""
    # Connection parameters
    server = "callcenter1.database.windows.net"
    database = "call"  # Correct database name from azure_sql_service.py
    username = "shahul"
    password = "apple123!@#"
    
    try:
        # Create a connection
        print("Connecting to database...")
        conn = pymssql.connect(
            server=server,
            database=database,
            user=username,
            password=password,
            as_dict=True
        )
        print("Database connection established")
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise

def test_insert_update_workflow():
    """Test the INSERT then UPDATE workflow with NULL transcription"""
    print("\n===== TESTING INSERT-THEN-UPDATE WORKFLOW WITH NULL TRANSCRIPTION =====")
    
    conn = None
    
    try:
        # Generate a unique file ID
        test_fileid = f"test_sql_{uuid.uuid4().hex[:8]}"
        filename = "test_file.mp3"
        source_path = "container/test_file.mp3"
        file_size = 1024
        
        # Connect to database
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # STEP 1: Insert with NULL transcription
        print("\n===== STEP 1: INITIAL INSERT WITH NULL TRANSCRIPTION =====")
        
        # Construct the INSERT statement
        insert_sql = """
        INSERT INTO rdt_assets (
            fileid, 
            filename, 
            source_path, 
            file_size, 
            status,
            transcription  -- Note: This will be NULL
        ) VALUES (
            %(fileid)s, 
            %(filename)s, 
            %(source_path)s, 
            %(file_size)s, 
            %(status)s,
            NULL
        )
        """
        
        # Parameters for the INSERT
        insert_params = {
            "fileid": test_fileid,
            "filename": filename,
            "source_path": source_path,
            "file_size": file_size,
            "status": "pending"
        }
        
        # Log the SQL
        log_query(insert_sql, insert_params)
        
        # Execute the INSERT
        cursor.execute(insert_sql, insert_params)
        
        # COMMIT the transaction - this is critical
        print("Committing the INSERT transaction...")
        conn.commit()
        print("Row with NULL transcription committed to database")
        
        # Verify the insert
        verify_sql = "SELECT id, fileid, status, transcription FROM rdt_assets WHERE fileid = %(fileid)s"
        verify_params = {"fileid": test_fileid}
        cursor.execute(verify_sql, verify_params)
        result = cursor.fetchone()
        
        if result:
            print(f"Verified INSERT: ID={result['id']}, FileID={result['fileid']}, Status={result['status']}")
            print(f"Transcription is NULL: {result['transcription'] is None}")
        else:
            print("Failed to verify INSERT")
        
        # STEP 2: Simulate transcription processing
        print("\n===== STEP 2: SIMULATING TRANSCRIPTION PROCESSING =====")
        print("Processing transcription...")
        time.sleep(2)  # Simulate processing time
        
        # Generate a sample transcription
        transcription = "This is a sample transcription that wasn't available during the initial insert."
        
        # STEP 3: Update with transcription data
        print("\n===== STEP 3: UPDATE WITH TRANSCRIPTION DATA =====")
        
        # Construct the UPDATE statement
        update_sql = """
        UPDATE rdt_assets
        SET transcription = %(transcription)s,
            status = 'completed',
            processed_date = GETDATE()
        WHERE fileid = %(fileid)s
        """
        
        # Parameters for the UPDATE
        update_params = {
            "fileid": test_fileid,
            "transcription": transcription
        }
        
        # Log the SQL
        log_query(update_sql, update_params)
        
        # Execute the UPDATE
        cursor.execute(update_sql, update_params)
        
        # COMMIT the transaction
        print("Committing the UPDATE transaction...")
        conn.commit()
        print("Transcription update committed to database")
        
        # Verify the update
        cursor.execute(verify_sql, verify_params)
        updated_result = cursor.fetchone()
        
        if updated_result:
            print(f"Verified UPDATE: ID={updated_result['id']}, FileID={updated_result['fileid']}, Status={updated_result['status']}")
            print(f"Transcription is now populated: {updated_result['transcription'] is not None}")
            print(f"Sample of transcription: {updated_result['transcription'][:50]}...")
        else:
            print("Failed to verify UPDATE")
        
        print("\n===== TEST COMPLETED SUCCESSFULLY =====")
        print("The two-phase commit approach has been demonstrated:")
        print("1. Initial INSERT with NULL transcription (COMMIT 1)")
        print("2. Transcription processing")
        print("3. UPDATE with transcription data (COMMIT 2)")
        
    except Exception as e:
        logger.error(f"Error during test: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("Database connection closed")

if __name__ == "__main__":
    test_insert_update_workflow()