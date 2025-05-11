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

DEEPGRAM_API_KEY = "ba94baf7840441c378c58ccd1d5202c38ddc42d8"

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
    # Test the function with a sample URL
    test_url = "https://infolder.blob.core.windows.net/shahulin/agricultural_finance_(murabaha)_angry.mp3?sp=r&st=2025-05-11T14:30:26Z&se=2025-11-12T22:30:26Z&spr=https&sv=2024-11-04&sr=b&sig=q2gumh51pXiVFgidPda5JQJXvGWwF4z%2BhE2tI9Ahkm0%3D"
    result = transcribe_url(test_url)
    
    if result["success"]:
        print("Transcription successful!")
        print(f"Results: {result['response']}")
    else:
        print(f"Transcription failed: {result['error']}")