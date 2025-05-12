#!/usr/bin/env python3
"""
Test script for verifying transcription integration with paragraphs and sentences

This script:
1. Transcribes an audio file directly from Azure storage
2. Verifies the transcription result
3. Stores the result in the database with paragraphs and sentences
4. Checks that paragraphs and sentences were correctly stored
"""

import os
import sys
import json
import time
import logging
import pymssql
from datetime import datetime

# Add python_backend to path if needed
current_dir = os.path.dirname(os.path.abspath(__file__))
python_backend_dir = os.path.join(current_dir, 'python_backend')
if os.path.exists(python_backend_dir):
    sys.path.append(python_backend_dir)

# Import the necessary modules
try:
    from python_backend.direct_transcribe import DirectTranscribe
    from python_backend.direct_transcribe_db import DirectTranscribeDB
except ImportError:
    try:
        from direct_transcribe import DirectTranscribe
        from direct_transcribe_db import DirectTranscribeDB
    except ImportError:
        print("ERROR: Could not import DirectTranscribe or DirectTranscribeDB")
        sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO,
                  format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def connect_to_database():
    """Connect to the database using environment variables"""
    try:
        # Get connection parameters from environment
        server = os.environ.get("PGHOST", "callcenter1.database.windows.net")
        database = os.environ.get("PGDATABASE", "call")
        username = os.environ.get("PGUSER", "shahul")
        password = os.environ.get("PGPASSWORD", "apple123!@#")
        
        # Connect to the database
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

def get_paragraph_sentence_counts(fileid):
    """Get paragraph and sentence counts for the file"""
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

def main():
    """Main function"""
    # Get the Deepgram API key from environment
    deepgram_api_key = os.environ.get("DEEPGRAM_API_KEY", "ba94baf7840441c378c58ccd1d5202c38ddc42d8")
    
    # Initialize the transcriber
    transcriber = DirectTranscribe(deepgram_api_key)
    
    # Initialize the database handler
    db_handler = DirectTranscribeDB()
    
    # Setup test parameters
    test_file = "agricultural_leasing_(ijarah)_normal.mp3"
    test_fileid = f"test_{int(time.time())}"
    
    # Generate a SAS URL for the test file
    from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
    from datetime import timedelta
    
    # Azure Storage connection string
    azure_connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", 
                                            "DefaultEndpointsProtocol=https;AccountName=infolder;AccountKey=NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==;EndpointSuffix=core.windows.net")
    source_container = "shahulin"
    
    # Extract account info from connection string
    conn_parts = {p.split('=')[0]: p.split('=', 1)[1] for p in azure_connection_string.split(';') if '=' in p}
    account_name = conn_parts.get('AccountName')
    account_key = conn_parts.get('AccountKey')
    
    # Check if blob exists first
    blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
    container_client = blob_service_client.get_container_client(source_container)
    
    try:
        blob_client = container_client.get_blob_client(test_file)
        if not blob_client.exists():
            logger.error(f"Test file {test_file} does not exist in container {source_container}")
            return
        
        # Calculate expiry time (240 hours)
        expiry = datetime.now() + timedelta(hours=240)
        
        # Generate SAS token
        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=source_container,
            blob_name=test_file,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=expiry
        )
        
        # Construct the full URL
        sas_url = f"https://{account_name}.blob.core.windows.net/{source_container}/{test_file}?{sas_token}"
        logger.info(f"Generated SAS URL for {test_file}")
        
        # STEP 1: Transcribe the file
        logger.info("STEP 1: Transcribing the file...")
        result = transcriber.transcribe_audio(
            sas_url, 
            paragraphs=True,
            punctuate=True,
            smart_format=True,
            diarize=True
        )
        
        # Check if transcription was successful
        if not result["success"]:
            logger.error(f"Transcription failed: {result.get('error', {}).get('message', 'Unknown error')}")
            return
        
        logger.info(f"Transcription successful: {len(result['transcript'])} characters")
        
        # Check if any paragraphs are detected
        paragraphs_found = False
        sentences_found = False
        
        if "results" in result["result"]:
            if "paragraphs" in result["result"]["results"]:
                if "paragraphs" in result["result"]["results"]["paragraphs"]:
                    paragraphs = result["result"]["results"]["paragraphs"]["paragraphs"]
                    paragraphs_found = len(paragraphs) > 0
                    logger.info(f"Found {len(paragraphs)} paragraphs in transcription")
                    
                    # Check for sentences
                    for para in paragraphs:
                        if "sentences" in para:
                            sentences = para["sentences"]
                            sentences_found = True
                            logger.info(f"Found {len(sentences)} sentences in paragraph")
                            break
        
        if not paragraphs_found:
            logger.warning("No paragraphs found in transcription result")
        
        if not sentences_found:
            logger.warning("No sentences found in transcription result")
        
        # STEP 2: Store in database
        logger.info("\nSTEP 2: Storing in database...")
        
        # Prepare the processing result
        processing_result = {
            "blob_name": test_file,
            "source_container": source_container,
            "destination_container": "shahulout",
            "transcription": {
                "success": result["success"],
                "result": result["result"],
                "transcript": result["transcript"],
                "error": result.get("error")
            },
            "file_movement": {
                "success": True,
                "destination_url": f"https://infolder.blob.core.windows.net/shahulout/{test_file}"
            },
            "fileid": test_fileid,
            "processing_time": 0
        }
        
        # Store in database
        db_result = db_handler.store_transcription_result(processing_result)
        
        # Check if database storage was successful
        if db_result.get("status") == "success":
            logger.info(f"Database storage successful: {db_result}")
        else:
            logger.error(f"Database storage failed: {db_result}")
            return
        
        # STEP 3: Verify paragraphs and sentences
        logger.info("\nSTEP 3: Verifying paragraphs and sentences...")
        
        # Get counts
        counts = get_paragraph_sentence_counts(test_fileid)
        
        if counts:
            logger.info(f"Found {counts['paragraphs']} paragraphs and {counts['sentences']} sentences in database")
            
            if counts['paragraphs'] > 0:
                logger.info("✓ Paragraphs were successfully stored in database")
            else:
                logger.error("✗ No paragraphs were stored in database")
                
            if counts['sentences'] > 0:
                logger.info("✓ Sentences were successfully stored in database")
            else:
                logger.error("✗ No sentences were stored in database")
        else:
            logger.error("Failed to get paragraph and sentence counts")
        
        logger.info("\nTest completed.")
        
    except Exception as e:
        logger.error(f"Error during test: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()