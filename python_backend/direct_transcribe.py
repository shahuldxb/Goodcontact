#!/usr/bin/env python3
"""
Direct transcription module using DgClassCriticalTranscribeRest
"""

import os
import logging
from dg_class_critical_transcribe_rest import DgClassCriticalTranscribeRest

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get API key from environment variables
DEEPGRAM_API_KEY = os.environ.get("DEEPGRAM_API_KEY", "ba94baf7840441c378c58ccd1d5202c38ddc42d8")

def transcribe_url(audio_url, model="nova-3", diarize=True):
    """
    Transcribe an audio file from a URL using Deepgram's API via DgClassCriticalTranscribeRest

    Args:
        audio_url (str): URL to the audio file
        model (str): Model to use for transcription
        diarize (bool): Whether to enable speaker diarization

    Returns:
        dict: The transcription response
    """
    try:
        logger.info(f"Initializing DgClassCriticalTranscribeRest with API key")
        transcriber = DgClassCriticalTranscribeRest(DEEPGRAM_API_KEY)

        logger.info(f"Transcribing audio from URL with model {model}")
        result = transcriber.transcribe_with_url(
            audio_url=audio_url,
            model=model,
            diarize=diarize,
            debug_mode=True
        )
        
        if result['success']:
            logger.info("Transcription completed successfully")
            return {
                "success": True,
                "response": result['full_response']
            }
        else:
            logger.error(f"Transcription failed: {result.get('error', 'Unknown error')}")
            return {
                "success": False,
                "error": result.get('error', 'Unknown error')
            }
    except Exception as e:
        logger.error(f"Error in transcription: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

if __name__ == "__main__":
    # Get URL from command line argument or environment
    import sys
    from azure_storage_service import AzureStorageService
    
    if len(sys.argv) > 1:
        # If blob name is provided as argument
        blob_name = sys.argv[1]
        print(f"Using blob name from command line: {blob_name}")
        
        # Generate SAS URL
        storage_service = AzureStorageService()
        test_url = storage_service.generate_sas_url("shahulin", blob_name, expiry_hours=24)
        
        if not test_url:
            print(f"Error: Could not generate SAS URL for blob {blob_name}")
            sys.exit(1)
    else:
        print("Error: Please provide a blob name as command line argument")
        print("Usage: python direct_transcribe.py <blob_name>")
        print("Example: python direct_transcribe.py agricultural_finance_(murabaha)_angry.mp3")
        sys.exit(1)
    
    print(f"Transcribing audio from SAS URL (length: {len(test_url)})")
    result = transcribe_url(test_url)
    
    if result["success"]:
        print("Transcription successful!")
        print(f"Results preview: {str(result['response'])[:500]}...")
    else:
        print(f"Transcription failed: {result['error']}")