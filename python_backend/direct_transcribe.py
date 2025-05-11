#!/usr/bin/env python3
"""
Direct transcription module using DgClassCriticalTranscribeRest

This module provides a minimal implementation for transcribing audio files 
using Deepgram's API directly from a SAS URL.

Usage:
    python direct_transcribe.py <deepgram_api_key> <blob_sas_url>
"""

import os
import sys
import logging
from dg_class_critical_transcribe_rest import DgClassCriticalTranscribeRest

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transcribe_url(api_key, audio_url, model="nova-3", diarize=True):
    """
    Transcribe an audio file from a URL using Deepgram's API via DgClassCriticalTranscribeRest

    Args:
        api_key (str): Deepgram API key
        audio_url (str): SAS URL to the audio file
        model (str): Model to use for transcription
        diarize (bool): Whether to enable speaker diarization

    Returns:
        dict: The transcription response
    """
    try:
        logger.info(f"Initializing DgClassCriticalTranscribeRest with provided API key")
        transcriber = DgClassCriticalTranscribeRest(api_key)

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
    # Get API key and SAS URL from command line arguments
    if len(sys.argv) < 3:
        print("Error: Missing required parameters")
        print("Usage: python direct_transcribe.py <deepgram_api_key> <blob_sas_url>")
        print("Example: python direct_transcribe.py ba94baf7840441c378c58ccd1d5202c38ddc42d8 https://infolder.blob.core.windows.net/shahulin/example.mp3?sv=...")
        sys.exit(1)
    
    # Get parameters from command line
    api_key = sys.argv[1]
    sas_url = sys.argv[2]
    
    # Optional model parameter
    model = sys.argv[3] if len(sys.argv) > 3 else "nova-3"
    
    print(f"API Key: {api_key[:5]}...{api_key[-5:]}")
    print(f"SAS URL length: {len(sas_url)} characters")
    print(f"Using model: {model}")
    
    # Call the transcription function
    result = transcribe_url(api_key, sas_url, model=model)
    
    if result["success"]:
        print("Transcription successful!")
        print(f"Results preview: {str(result['response'])[:500]}...")
    else:
        print(f"Transcription failed: {result['error']}")