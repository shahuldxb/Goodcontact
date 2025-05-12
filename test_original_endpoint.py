#!/usr/bin/env python3
"""
Test the original direct_transcribe endpoint to check file_size handling
"""
import requests
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def test_original_endpoint():
    """
    Test the original /direct/transcribe endpoint with a real file
    """
    url = "http://localhost:5001/direct/transcribe"
    
    # Use a file we know exists in the shahulin container
    test_data = {
        "filename": "business_investment_account_(mudarabah)_neutral.mp3",
        "fileid": "original_endpoint_test_1"
    }
    
    logger.info(f"Testing original endpoint with file {test_data['filename']}")
    
    try:
        # Send the request
        response = requests.post(url, json=test_data)
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"Transcription successful: {result['success']}")
            return True
        else:
            logger.error(f"Error: {response.status_code}, {response.text}")
            return False
    
    except Exception as e:
        logger.error(f"Exception: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_original_endpoint()
    print(f"Test result: {'SUCCESS' if success else 'FAILURE'}")