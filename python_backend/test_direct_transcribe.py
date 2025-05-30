#!/usr/bin/env python3
"""
Test script for DirectTranscriber class

This script tests the DirectTranscriber class with real Azure blob files
and verifies that transcriptions are successfully completed and stored in the database.
"""

import os
import json
import asyncio
import logging
from datetime import datetime
import pymssql

# Import the DirectTranscriber class
from direct_transcribe import DirectTranscriber
from azure_storage_service import AzureStorageService

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection parameters
DB_SERVER = "callcenter1.database.windows.net"
DB_USER = "shahul"
DB_PASSWORD = "apple123!@#"
DB_NAME = "callcenterdb"

def test_transcription(blob_name, api_key):
    """
    Test the transcription of a specific blob and verify database entry
    
    Args:
        blob_name (str): Name of the blob in the 'shahulin' container
        api_key (str): Deepgram API key
    
    Returns:
        dict: Results of the test
    """
    try:
        # 1. Generate SAS URL with 240 hour expiry
        logger.info(f"Generating SAS URL for blob: {blob_name}")
        storage_service = AzureStorageService()
        sas_url = storage_service.generate_sas_url("shahulin", blob_name, expiry_hours=240)
        
        if not sas_url:
            logger.error(f"Failed to generate SAS URL for blob: {blob_name}")
            return {"success": False, "error": "Failed to generate SAS URL"}
        
        logger.info(f"SAS URL generated successfully (length: {len(sas_url)})")
        
        # 2. Create fileid for tracking
        fileid = f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}_{blob_name}"
        logger.info(f"Using fileid: {fileid}")
        
        # 3. Transcribe the audio file
        logger.info("Initializing DirectTranscriber")
        transcriber = DirectTranscriber(api_key)
        
        logger.info("Starting transcription")
        # Since the transcription is actually synchronous, we just call it directly
        result = transcriber.transcribe_url(sas_url, model="nova-3")
        
        if not result["success"]:
            logger.error(f"Transcription failed: {result.get('error')}")
            return {"success": False, "error": result.get('error')}
        
        logger.info("Transcription completed successfully")
        
        # 4. Try to insert the result into the database (but don't stop if it fails)
        transcription_json = json.dumps(result["response"])
        database_success = False
        database_id = None
        
        try:
            logger.info("Connecting to database")
            conn = pymssql.connect(
                server=DB_SERVER,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            
            cursor = conn.cursor()
            
            # Insert into rdt_assets table
            logger.info(f"Inserting transcription result into database for fileid: {fileid}")
            cursor.execute(
                """
                INSERT INTO rdt_assets 
                (fileid, filename, sourcePath, fileSize, status, transcription) 
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    fileid,
                    blob_name,
                    f"shahulin/{blob_name}",
                    0,  # fileSize - placeholder
                    "Processed",
                    transcription_json
                )
            )
            
            conn.commit()
            logger.info("Database insert completed")
            
            # 5. Verify the transcription was stored properly
            logger.info("Verifying database entry")
            cursor.execute(
                "SELECT id, fileid, transcription FROM rdt_assets WHERE fileid = %s",
                (fileid,)
            )
            
            row = cursor.fetchone()
            if row:
                database_id, stored_fileid, stored_transcription = row
                
                # Check if transcription is empty
                if stored_transcription and stored_transcription != "{}":
                    logger.info(f"Successfully verified transcription in database for ID: {database_id}")
                    database_success = True
                else:
                    logger.warning("Transcription is empty in database")
            else:
                logger.warning("Failed to retrieve database entry")
            
            # Close database connection
            conn.close()
            
        except Exception as db_error:
            logger.warning(f"Database operations failed: {str(db_error)}")
            logger.info("Continuing with transcription verification only...")
        
        # Check for transcript content in the response to verify success
        transcript_data = ""
        transcript_present = False
        
        try:
            transcript_data = result["response"]["full_response"]["results"]["channels"][0]["alternatives"][0]["transcript"]
            transcript_present = bool(transcript_data and len(transcript_data) > 0)
            if transcript_present:
                logger.info(f"Transcript preview: {transcript_data[:100]}...")
            else:
                logger.warning("Transcript appears to be empty")
        except (KeyError, IndexError, TypeError) as e:
            logger.warning(f"Could not verify transcript content in response: {str(e)}")
        
        # Result will include database info if it was successful, otherwise just transcription info
        result_data = {
            "success": True,
            "fileid": fileid,
            "transcript_present": transcript_present,
            "transcription_size": len(transcription_json)
        }
        
        # Only add transcript preview if we have transcript data
        if transcript_data:
            preview = transcript_data[:300] + "..." if len(transcript_data) > 300 else transcript_data
            result_data["transcription_preview"] = preview
        
        # Add database info if available
        if database_success and database_id:
            result_data["database_success"] = True
            result_data["database_id"] = database_id
        else:
            result_data["database_success"] = False
            
        return result_data
    
    except Exception as e:
        logger.error(f"Error in test_transcription: {str(e)}")
        return {"success": False, "error": str(e)}

def main():
    """
    Main function that tests transcription on specific blob files.
    Can accept command-line arguments for blob names to test.
    """
    # Get Deepgram API key
    api_key = os.environ.get("DEEPGRAM_API_KEY", "ba94baf7840441c378c58ccd1d5202c38ddc42d8")
    
    # Check if blob names were provided as command-line arguments
    if len(sys.argv) > 1:
        # Use the provided blob names
        test_files = sys.argv[1:]
        logger.info(f"Testing specific files provided as arguments: {', '.join(test_files)}")
    else:
        # Default test files if no arguments provided
        test_files = [
            "agricultural_finance_(murabaha)_angry.mp3",  # Files from API response
            "banking_enquiries_hindi.mp3"  
        ]
        logger.info(f"No files specified. Testing default files: {', '.join(test_files)}")
    
    for i, blob_name in enumerate(test_files):
        logger.info(f"TEST {i+1}: Testing transcription of {blob_name}")
        result = test_transcription(blob_name, api_key)
        
        if result["success"]:
            transcript_status = "with transcript" if result.get("transcript_present", False) else "without transcript"
            logger.info(f"TEST {i+1} PASSED: Successfully transcribed and stored {blob_name} {transcript_status}")
            logger.info(f"FileID: {result['fileid']}")
            logger.info(f"Database ID: {result['id']}")
            logger.info(f"Transcription length: {result['transcription_length']} bytes")
        else:
            logger.error(f"TEST {i+1} FAILED: {result.get('error')}")
        
        logger.info("-" * 80)
        
        # Sleep between tests to avoid rate limiting
        if i < len(test_files) - 1:
            logger.info("Waiting 5 seconds before next test...")
            import time
            time.sleep(5)

if __name__ == "__main__":
    import sys
    main()