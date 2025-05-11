#!/usr/bin/env python3
"""
Test script for Azure blob transcription using the shortcut method

This script tests the shortcut transcription method on a real Azure blob file,
which is now set as the default method in the application.
"""
import os
import sys
import json
import logging
import requests
import time
from datetime import datetime, timedelta

# Local imports
try:
    from azure_storage_service import AzureStorageService
    from test_direct_transcription import test_direct_transcription
except ImportError:
    print("Could not import required modules. Make sure you're running from the correct directory.")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

OUTPUT_DIR = "direct_test_results"

def test_azure_file_with_shortcut():
    """
    Test the transcription of an Azure blob file using the shortcut method.
    """
    try:
        # Initialize Azure Storage Service
        azure_storage_service = AzureStorageService()
        logger.info("Azure Storage Service initialized")
        
        # Get a list of available files in the source container
        source_files = azure_storage_service.list_blobs(container_name=azure_storage_service.source_container)
        
        if not source_files:
            logger.error(f"No files found in container '{azure_storage_service.source_container}'")
            return False
            
        # Choose the first file for testing
        test_file = source_files[0]["name"]
        logger.info(f"Selected test file: {test_file}")
        
        # Generate a SAS URL for the file
        sas_url = azure_storage_service.generate_sas_url(
            azure_storage_service.source_container, 
            test_file,
            expiry_hours=2  # 2 hour expiry for testing
        )
        
        logger.info(f"Generated SAS URL: {sas_url[:100]}...")
        
        # Create a unique file ID for this test
        fileid = f"test_shortcut_{int(time.time())}"
        
        # Run the transcription using the shortcut method
        logger.info(f"Starting transcription with shortcut method for file {test_file}")
        start_time = time.time()
        
        # Call test_direct_transcription directly with the correct parameters
        result = test_direct_transcription(
            blob_name=test_file,
            container_name=azure_storage_service.source_container
        )
        
        elapsed_time = time.time() - start_time
        logger.info(f"Transcription completed in {elapsed_time:.2f} seconds")
        
        # Ensure the output directory exists
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
            
        # Save the results to a file
        output_file = os.path.join(OUTPUT_DIR, f"azure_shortcut_test_{fileid}.json")
        with open(output_file, 'w') as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "file": test_file,
                "fileid": fileid,
                "status": "success",
                "elapsed_seconds": elapsed_time,
                "transcription_result": result
            }, f, indent=2)
            
        logger.info(f"Test results saved to {output_file}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error during test: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False
        
if __name__ == "__main__":
    logger.info("=== Testing Azure Blob File with Shortcut Method ===")
    success = test_azure_file_with_shortcut()
    
    if success:
        logger.info("Test completed successfully")
    else:
        logger.error("Test failed")
        sys.exit(1)