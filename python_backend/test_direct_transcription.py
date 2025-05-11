#!/usr/bin/env python3
"""
Test Direct Transcription of Azure Blob

This script tests the direct transcription method by downloading an audio file from 
Azure Blob Storage and sending it to Deepgram for transcription.
"""

import os
import sys
import json
import logging
from datetime import datetime
from azure_deepgram_transcribe import transcribe_azure_audio

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_direct_transcription(blob_name="call_center_sample.mp3", container_name="shahulin"):
    """
    Test the direct transcription of a blob from Azure Storage
    
    Args:
        blob_name: Name of the blob (audio file) to transcribe
        container_name: Azure container name where the file is stored
    """
    logger.info(f"Testing direct transcription of blob: {blob_name} from container: {container_name}")
    
    # Set up Azure Storage credentials using the provided connection string
    azure_connection_string = "DefaultEndpointsProtocol=https;AccountName=infolder;AccountKey=NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==;EndpointSuffix=core.windows.net"
    
    # Set environment variables
    os.environ['AZURE_STORAGE_CONNECTION_STRING'] = azure_connection_string
    
    logger.info("Set Azure Storage environment variables with connection string")
    
    # Get API key from environment
    api_key = os.environ.get('DEEPGRAM_API_KEY')
    if not api_key:
        # Fallback to the one in the provided file if environment variable is not set
        api_key = "d6290865c35bddd50928c5d26983769682fca987"  
        logger.warning("DEEPGRAM_API_KEY environment variable not set, using fallback value")
        os.environ['DEEPGRAM_API_KEY'] = api_key
    
    # Set the API key for debugging
    logger.info(f"Using Deepgram API key: {api_key[:4]}...{api_key[-4:]} (length: {len(api_key)})")
    
    # Test with different diarization and model settings
    try:
        # Test 1: Default settings (nova-2 model, diarization enabled)
        logger.info("Test 1: Default settings (nova-2 model, diarization enabled)")
        result1 = transcribe_azure_audio(
            blob_name=blob_name,
            api_key=api_key,
            model="nova-2",
            diarize=True,
            container_name=container_name
        )
        
        # Log the results
        if 'error' in result1:
            logger.error(f"Test 1 failed: {json.dumps(result1, indent=2)}")
        else:
            logger.info(f"Test 1 succeeded: {json.dumps(result1, indent=2)}")
        
        return result1
    
    except Exception as e:
        logger.error(f"Exception during transcription test: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {"error": str(e)}

def main():
    """Command-line interface"""
    # Get blob name from command line if provided
    # Default to a file we know exists in the shahulin container
    blob_name = sys.argv[1] if len(sys.argv) > 1 else "agricultural_finance_(murabaha)_normal.mp3"
    
    # Print a header with timestamp
    print(f"\n===== DIRECT TRANSCRIPTION TEST - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
    print(f"Testing blob: {blob_name}")
    
    # Run the test
    result = test_direct_transcription(blob_name)
    
    # Print a summary of the result
    if 'error' in result:
        print(f"\n❌ Test failed: {result['error']}")
    else:
        print(f"\n✅ Test succeeded!")
        
        # Print key information about the transcription
        if 'results' in result and 'channels' in result['results']:
            transcript = result['results']['channels'][0]['alternatives'][0]['transcript']
            print(f"\nTranscript: {transcript[:100]}...")
        elif 'result' in result and 'channels' in result['result']:
            transcript = result['result']['channels'][0]['alternatives'][0]['transcript']
            print(f"\nTranscript: {transcript[:100]}...")
        else:
            print(f"\nTranscript structure unknown. Full result:")
            print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()