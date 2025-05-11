#!/usr/bin/env python3
"""
Test direct transcription with a specific file from shahulin container
"""

import os
import sys
import json
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set Azure Storage connection details from the main application
os.environ["AZURE_STORAGE_ACCOUNT_URL"] = "https://infolder.blob.core.windows.net"
os.environ["AZURE_STORAGE_ACCOUNT_KEY"] = "NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw=="
# Construct connection string for Azure Storage
os.environ["AZURE_STORAGE_CONNECTION_STRING"] = f"DefaultEndpointsProtocol=https;AccountName=infolder;AccountKey={os.environ['AZURE_STORAGE_ACCOUNT_KEY']};EndpointSuffix=core.windows.net"

# Import the direct transcription module
from azure_deepgram_transcribe import transcribe_azure_audio

def main():
    """Test directly transcribing a file from the shahulin container"""
    
    # Use a specific audio file name from the shahulin container
    # This file is known to exist in the container based on previous logs
    file_name = "agricultural_finance_(murabaha)_impatient.mp3"
    
    logger.info(f"Testing direct transcription of file: {file_name}")
    
    # Check if we have the API key
    api_key = os.environ.get("DEEPGRAM_API_KEY")
    if not api_key:
        logger.error("DEEPGRAM_API_KEY environment variable is not set")
        sys.exit(1)
    
    # First test just the transcription function
    result = transcribe_azure_audio(file_name, api_key=api_key)
    
    # Also test the full processing function that includes file movement
    from azure_deepgram_transcribe import process_audio_file
    logger.info("Now testing the full process_audio_file function...")
    process_result = process_audio_file(file_name, fileid="direct_test_" + str(int(time.time())))
    
    # Log the result
    logger.info(f"Transcription result type: {type(result)}")
    if isinstance(result, dict):
        logger.info(f"Result keys: {', '.join(result.keys()) if result.keys() else 'Empty dict'}")
        
        # Check for error
        if "error" in result:
            logger.error(f"Transcription error: {json.dumps(result['error'], indent=2)}")
        else:
            # Extract transcript if available
            if "results" in result and "channels" in result.get("results", {}):
                channels = result["results"]["channels"]
                if channels and len(channels) > 0:
                    alternatives = channels[0].get("alternatives", [])
                    if alternatives and len(alternatives) > 0:
                        transcript = alternatives[0].get("transcript", "")
                        logger.info(f"Transcript excerpt: {transcript[:200]}...")
                    else:
                        logger.warning("No alternatives found in channels")
                else:
                    logger.warning("No channels found in results")
            else:
                logger.warning(f"Unexpected result structure: {json.dumps(result, indent=2)}")
    
    # Print full results for inspection
    print("\nFull transcription result:")
    print(json.dumps(result, indent=2))
    
    # Print process result
    print("\nFull process result (including file move):")
    print(json.dumps(process_result, indent=2))

if __name__ == "__main__":
    main()