#!/usr/bin/env python3
"""
Test if paragraphs and sentences are being stored correctly.
This script:
1. Gets a completed transcription result from the database
2. Processes it to store paragraphs and sentences
3. Verifies the counts match expectations
"""

import os
import sys
import json
import logging
import pymssql
import asyncio
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add python_backend to path
current_dir = os.path.dirname(os.path.abspath(__file__))
python_backend_dir = os.path.join(current_dir, 'python_backend')
if os.path.exists(python_backend_dir):
    sys.path.append(python_backend_dir)
else:
    sys.path.append(current_dir)  # Already in python_backend

# Import the async update function
try:
    from async_update_sentence_tables import store_transcription_details_async
except ImportError:
    logger.error("Could not import store_transcription_details_async")
    sys.exit(1)

def connect_to_database():
    """Connect to the SQL database using environment variables"""
    try:
        # Get connection parameters from environment
        server = os.environ.get("PGHOST", "callcenter1.database.windows.net")
        database = os.environ.get("PGDATABASE", "call")
        username = os.environ.get("PGUSER", "shahul")
        password = os.environ.get("PGPASSWORD", "apple123!@#")
        
        # Connect to database
        conn = pymssql.connect(
            server=server,
            database=database,
            user=username,
            password=password
        )
        
        logger.info(f"Connected to database {database} on {server}")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        return None

def get_recent_transcription():
    """Get a recently completed transcription from the database"""
    try:
        conn = connect_to_database()
        if not conn:
            return None
            
        cursor = conn.cursor(as_dict=True)
        
        # Get a recently completed transcription
        query = """
        SELECT TOP 1
            fileid,
            transcription_json
        FROM
            rdt_assets
        WHERE
            status = 'completed'
            AND transcription_json IS NOT NULL
            AND transcription_json <> ''
        ORDER BY
            processed_date DESC
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if result and 'transcription_json' in result:
            # Convert JSON string to object
            try:
                json_data = json.loads(result['transcription_json'])
                return {
                    'fileid': result['fileid'],
                    'transcription_json': json_data
                }
            except json.JSONDecodeError:
                logger.error(f"Error decoding JSON for fileid {result['fileid']}")
                return None
        
        return None
    except Exception as e:
        logger.error(f"Error getting recent transcription: {str(e)}")
        return None

def get_paragraph_sentence_counts(fileid):
    """Get the current paragraph and sentence counts for a fileid"""
    try:
        conn = connect_to_database()
        if not conn:
            return None
            
        cursor = conn.cursor()
        
        # Get paragraph count
        cursor.execute("""
        SELECT COUNT(*) FROM rdt_paragraphs WHERE fileid = %s
        """, (fileid,))
        
        paragraph_count = cursor.fetchone()[0]
        
        # Get sentence count
        cursor.execute("""
        SELECT COUNT(*) FROM rdt_sentences 
        WHERE paragraph_id IN (SELECT id FROM rdt_paragraphs WHERE fileid = %s)
        """, (fileid,))
        
        sentence_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return {
            'paragraphs': paragraph_count,
            'sentences': sentence_count
        }
    except Exception as e:
        logger.error(f"Error getting paragraph/sentence counts: {str(e)}")
        return None

async def process_transcription(transcription_data):
    """Process the transcription data using the async update function"""
    if not transcription_data:
        logger.error("No transcription data to process")
        return False
        
    fileid = transcription_data['fileid']
    
    # Get current counts
    before_counts = get_paragraph_sentence_counts(fileid)
    if before_counts:
        logger.info(f"Before processing - Paragraphs: {before_counts['paragraphs']}, Sentences: {before_counts['sentences']}")
    
    # Process the transcription data
    logger.info(f"Processing transcription for fileid {fileid}")
    result = await store_transcription_details_async(fileid, transcription_data['transcription_json'])
    
    logger.info(f"Processing result: {result}")
    
    # Get updated counts
    after_counts = get_paragraph_sentence_counts(fileid)
    if after_counts:
        logger.info(f"After processing - Paragraphs: {after_counts['paragraphs']}, Sentences: {after_counts['sentences']}")
    
    return True

async def main():
    """Main function"""
    logger.info("Starting test of paragraphs and sentences processing")
    
    # Get a recent transcription
    transcription_data = get_recent_transcription()
    if not transcription_data:
        logger.error("No completed transcription found in database")
        return
    
    # Process the transcription
    success = await process_transcription(transcription_data)
    
    if success:
        logger.info("Test completed successfully")
    else:
        logger.error("Test failed")

if __name__ == "__main__":
    asyncio.run(main())