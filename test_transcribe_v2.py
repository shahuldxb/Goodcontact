#!/usr/bin/env python3
"""
Test for the new direct_transcribe_v2 endpoint
This test verifies that the new endpoint properly reads file_size from Azure blob
"""
import requests
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def test_transcribe_v2():
    """
    Test the direct_transcribe_v2 endpoint with a real file
    """
    url = "http://localhost:5001/direct/transcribe_v2"
    
    # Use a file we know exists in the shahulin container
    test_data = {
        "filename": "business_investment_account_(mudarabah)_neutral.mp3",
        "fileid": "file_size_test_1"
    }
    
    logger.info(f"Testing direct_transcribe_v2 with file {test_data['filename']}")
    
    try:
        # Send the request
        response = requests.post(url, json=test_data)
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"Transcription successful: {result['success']}")
            
            # Check if file_size is included and is non-zero
            if "file_size" in result and result["file_size"] > 0:
                logger.info(f"File size correctly retrieved: {result['file_size']} bytes")
                return True
            else:
                logger.error(f"File size is missing or zero: {result.get('file_size', 'Missing')}")
                return False
        else:
            logger.error(f"Error: {response.status_code}, {response.text}")
            return False
    
    except Exception as e:
        logger.error(f"Exception: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_transcribe_v2()
    print(f"Test result: {'SUCCESS' if success else 'FAILURE'}")