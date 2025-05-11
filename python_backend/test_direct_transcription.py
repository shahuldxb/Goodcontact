#!/usr/bin/env python3
"""
Test the direct Deepgram transcription method with Azure Storage files
"""

import os
import sys
import json
import logging
from azure_deepgram_transcribe import transcribe_azure_audio, process_audio_file

# Set up logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Make sure we have the Deepgram API key
api_key = os.environ.get("DEEPGRAM_API_KEY")
if not api_key:
    logger.error("DEEPGRAM_API_KEY environment variable is not set")
    sys.exit(1)

def test_transcribe_specific_file(blob_name):
    """Test transcribe a specific file from Azure Storage"""
    logger.info(f"Testing transcription of {blob_name} using direct API implementation")
    
    # Test the transcribe_azure_audio function
    try:
        result = transcribe_azure_audio(blob_name, api_key=api_key)
        logger.info(f"Transcription result type: {type(result)}")
        logger.info(f"Transcription result keys: {result.keys() if isinstance(result, dict) else 'Not a dict'}")
        
        # Print the first part of the transcript
        if isinstance(result, dict) and "results" in result and "channels" in result.get("results", {}):
            transcript = result["results"]["channels"][0]["alternatives"][0]["transcript"]
            logger.info(f"Transcript excerpt: {transcript[:100]}...")
        else:
            logger.error(f"Unexpected result structure: {json.dumps(result, indent=2)}")
            
        return result
    except Exception as e:
        logger.error(f"Error transcribing audio: {str(e)}")
        return {"error": str(e)}

def test_process_specific_file(blob_name, fileid="test123"):
    """Test process a specific file from Azure Storage"""
    logger.info(f"Testing full processing of {blob_name} using direct API implementation")
    
    try:
        result = process_audio_file(blob_name, fileid)
        logger.info(f"Processing result type: {type(result)}")
        logger.info(f"Processing result keys: {result.keys() if isinstance(result, dict) else 'Not a dict'}")
        
        # Log the result details
        if isinstance(result, dict):
            logger.info(f"Processing result: {json.dumps(result, indent=2)}")
        
        return result
    except Exception as e:
        logger.error(f"Error processing audio: {str(e)}")
        return {"error": str(e)}

def list_available_files():
    """List available files in the Azure Storage container"""
    try:
        from azure_storage_service import AzureStorageService
        azure_storage = AzureStorageService()
        files = azure_storage.list_source_blobs()
        logger.info(f"Available files in Azure Storage: {len(files)} files")
        for file in files[:10]:  # Show first 10 files only
            logger.info(f"  - {file.name}")
        return files
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}")
        return []

if __name__ == "__main__":
    # List available files
    files = list_available_files()
    
    # Select a file to test
    test_file = None
    
    if len(sys.argv) > 1:
        test_file = sys.argv[1]
        logger.info(f"Using command line provided file: {test_file}")
    elif files:
        # Take the first file as a test if none specified
        test_file = files[0].name
        logger.info(f"Using first available file: {test_file}")
    
    if test_file:
        # Test the transcription
        transcription_result = test_transcribe_specific_file(test_file)
        
        # Test the full processing
        processing_result = test_process_specific_file(test_file)
    else:
        logger.error("No files available to test")