#!/usr/bin/env python3
"""
Enhanced Transcription with Database Storage

This module provides a function to transcribe an audio file using Deepgram's REST API
and store the detailed results in the database, including paragraphs and sentences.
"""

import os
import json
import logging
import traceback
import time
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transcribe_and_store(file_path=None, file_url=None, fileid=None, model="nova-2", diarize=True, store_results=True):
    """
    Transcribe an audio file using REST API and store detailed results in the database.
    
    Args:
        file_path (str, optional): Path to local audio file
        file_url (str, optional): URL to audio file (e.g., Azure Storage SAS URL)
        fileid (str, optional): Unique ID for the file. If None, one will be generated.
        model (str): Deepgram model to use (default: nova-2)
        diarize (bool): Whether to enable speaker diarization (default: True)
        store_results (bool): Whether to store results in the database (default: True)
        
    Returns:
        dict: Result dictionary with transcription data and storage status
    """
    try:
        start_time = time.time()
        
        # Generate a file ID if none was provided
        if not fileid:
            fileid = f"transcription_{int(time.time())}"
            
        logger.info(f"Starting transcription for ID {fileid} using REST API")
        
        # Step 1: Transcribe the audio using our REST API implementation
        from dg_class_critical_transcribe_rest import DgClassCriticalTranscribeRest
        
        # Get API key from environment
        api_key = os.environ.get('DEEPGRAM_API_KEY')
        if not api_key:
            return {
                "success": False,
                "error": "DEEPGRAM_API_KEY environment variable not set",
                "fileid": fileid
            }
        
        # Initialize the transcriber
        transcriber = DgClassCriticalTranscribeRest(api_key)
        
        # Perform transcription
        if file_url:
            logger.info(f"Transcribing from URL: {file_url[:50]}...")
            result = transcriber.transcribe_shortcut(audio_url=file_url, model=model, diarize=diarize)
        elif file_path:
            logger.info(f"Transcribing from file: {file_path}")
            result = transcriber.transcribe_shortcut(file_path=file_path, model=model, diarize=diarize)
        else:
            return {
                "success": False,
                "error": "Either file_path or file_url must be provided",
                "fileid": fileid
            }
            
        # Check if transcription was successful
        if not result.get('success', False):
            error_message = result.get('error', 'Unknown error during transcription')
            logger.error(f"Transcription failed: {error_message}")
            return {
                "success": False,
                "error": error_message,
                "fileid": fileid
            }
        
        # Step 2: Store results in the database
        storage_result = {"stored": False, "message": "Storage skipped"}
        
        if store_results:
            logger.info(f"Storing detailed transcription results in database for {fileid}")
            try:
                # Import the storage function
                from update_sentence_tables import store_transcription_details
                
                # Store the results
                storage_result = store_transcription_details(fileid, result)
                
                if storage_result.get('status') != 'success':
                    logger.warning(f"Database storage warning: {storage_result.get('message')}")
            except Exception as e:
                storage_error = f"Error storing transcription details: {str(e)}"
                logger.error(storage_error)
                logger.error(traceback.format_exc())
                storage_result = {"stored": False, "error": storage_error}
        
        # Step 3: Prepare the final result
        processing_time = time.time() - start_time
        
        return {
            "success": True,
            "fileid": fileid,
            "transcript": result.get('transcript', ''),
            "confidence": result.get('confidence', 0.0),
            "language": result.get('language', None),
            "request_id": result.get('request_id', None),
            "sha256": result.get('sha256', None),
            "duration": result.get('duration', None),
            "speaker_count": result.get('speaker_count', 0),
            "paragraph_count": result.get('paragraph_count', 0),
            "sentence_count": result.get('sentence_count', 0),
            "storage_result": storage_result,
            "processing_time": processing_time,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        error_message = f"Error in transcribe_and_store: {str(e)}"
        logger.error(error_message)
        logger.error(traceback.format_exc())
        return {
            "success": False,
            "error": error_message,
            "fileid": fileid
        }

# Example usage when run directly
if __name__ == "__main__":
    # Test with a local file if available
    test_file = "test_audio.wav"
    if os.path.exists(test_file):
        result = transcribe_and_store(file_path=test_file)
        print(json.dumps(result, indent=2))
    else:
        print(f"Test file {test_file} not found. Please provide a valid file to test.")