#!/usr/bin/env python3
"""
Direct transcription module for Deepgram API

This module provides a class for transcribing audio files directly from a SAS URL
using Deepgram's API.
"""

import os
import sys
import logging
from deepgram import DeepgramClient, PrerecordedOptions

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DirectTranscriber:
    """Class for direct transcription using Deepgram API"""
    
    def __init__(self, api_key):
        """
        Initialize the DirectTranscriber with a Deepgram API key
        
        Args:
            api_key (str): Deepgram API key
        """
        self.api_key = api_key
        self.client = DeepgramClient(api_key)
    
    async def transcribe_url(self, audio_url, model="nova-3", diarize=True):
        """
        Transcribe an audio file from a URL using Deepgram's API
        
        Args:
            audio_url (str): SAS URL to the audio file
            model (str): Model to use for transcription (default: nova-3)
            diarize (bool): Whether to enable speaker diarization (default: True)
            
        Returns:
            dict: The transcription response
        """
        try:
            logger.info(f"Transcribing audio from URL with model {model}")
            
            # Prepare the audio URL in the format expected by Deepgram
            audio_source = {"url": audio_url}
            
            # Configure transcription options
            options = PrerecordedOptions(
                model=model,
                smart_format=True,
                diarize=diarize
            )
            
            # Call the Deepgram API
            response = await self.client.listen.asyncio.v("1").transcribe_url(audio_source, options)
            
            logger.info("Transcription completed successfully")
            return {
                "success": True,
                "response": response
            }
        except Exception as e:
            logger.error(f"Error in transcription: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }

# Main function for demonstration and testing
async def main(api_key, audio_url, model="nova-3"):
    """
    Main function to demonstrate the usage of the DirectTranscriber class
    
    Args:
        api_key (str): Deepgram API key
        audio_url (str): SAS URL to the audio file
        model (str): Model to use for transcription (default: nova-3)
        
    Returns:
        dict: The transcription response
    """
    transcriber = DirectTranscriber(api_key)
    result = await transcriber.transcribe_url(audio_url, model=model)
    return result

if __name__ == "__main__":
    # Execute only when run directly
    import asyncio
    
    # Get API key and SAS URL from command line arguments
    if len(sys.argv) < 3:
        print("Error: Missing required parameters")
        print("Usage: python direct_transcribe.py <deepgram_api_key> <blob_sas_url>")
        print("Example: python direct_transcribe.py YOUR_API_KEY https://infolder.blob.core.windows.net/shahulin/example.mp3?sv=...")
        sys.exit(1)
    
    # Get parameters from command line
    api_key = sys.argv[1]
    sas_url = sys.argv[2]
    
    # Optional model parameter
    model = sys.argv[3] if len(sys.argv) > 3 else "nova-3"
    
    print(f"API Key: {api_key[:5]}...{api_key[-5:]}")
    print(f"SAS URL length: {len(sas_url)} characters")
    print(f"Using model: {model}")
    
    # Call the main function with asyncio
    result = asyncio.run(main(api_key, sas_url, model))
    
    if result["success"]:
        print("Transcription successful!")
        print(f"Results preview: {str(result['response'])[:500]}...")
    else:
        print(f"Transcription failed: {result['error']}")