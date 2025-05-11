#!/usr/bin/env python3
"""
Transcribe Azure Storage Audio Files using Deepgram's Listen REST API

This implementation uses the latest Deepgram SDK with the listen.rest API,
which is a more modern approach compared to the older Deepgram client.
"""
import os
import logging
from typing import Dict, Any, Optional

# Import the new Deepgram SDK
from deepgram import (
    DeepgramClient,
    PrerecordedOptions,
    DeepgramClientOptions
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get Deepgram API key from environment variable
DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY", "")

def transcribe_with_listen_rest(audio_url: str, options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Transcribe audio using Deepgram's listen.rest API with a Blob SAS URL.
    
    Args:
        audio_url: The SAS URL to the audio file in Azure Blob Storage
        options: Optional dictionary of transcription options
        
    Returns:
        dict: The complete Deepgram response
    """
    try:
        logger.info(f"Transcribing using listen.rest API: {audio_url[:60]}...")
        
        # Initialize the Deepgram client
        client_options = DeepgramClientOptions(
            verbose=True  # Enable verbose logging for debugging
        )
        deepgram = DeepgramClient(DEEPGRAM_API_KEY, options=client_options)
        
        # Set up transcription options
        transcription_options = PrerecordedOptions(
            model="nova-3",  # Using the latest model
            smart_format=True,
            diarize=True,
            detect_language=True,
            punctuate=True,
            utterances=True,
            summarize=True
        )
        
        # Apply any additional options if provided
        if options:
            for key, value in options.items():
                if hasattr(transcription_options, key):
                    setattr(transcription_options, key, value)
        
        # Prepare the URL in the format expected by the API
        url_data = {
            "url": audio_url
        }
        
        # Make the transcription request
        logger.info("Sending request to Deepgram listen.rest API...")
        response = deepgram.listen.rest.v("1").transcribe_url(url_data, transcription_options)
        
        logger.info("Transcription completed successfully")
        return response
        
    except Exception as e:
        logger.error(f"Error in listen.rest transcription: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise

def generate_sas_url(blob_name: str, container_name: str = "shahulin") -> str:
    """
    Generate a SAS URL for a blob in Azure Storage.
    This is a placeholder - in a real implementation, this would call Azure Storage SDK.
    
    In our actual implementation, this function would be imported from azure_storage_service.py
    """
    from python_backend.azure_storage_service import AzureStorageService
    storage_service = AzureStorageService()
    return storage_service.generate_sas_url(container_name, blob_name)

def main():
    """Test function to demonstrate usage"""
    # Example blob name
    blob_name = "agricultural_finance_(murabaha)_angry.mp3"
    
    # Generate a SAS URL for the blob
    audio_url = generate_sas_url(blob_name)
    
    # Transcribe using the listen.rest API
    result = transcribe_with_listen_rest(audio_url)
    
    # Print the transcript
    if "results" in result and "channels" in result["results"]:
        transcript = result["results"]["channels"][0]["alternatives"][0]["transcript"]
        print(f"Transcript: {transcript[:200]}...")
    else:
        print("No transcript found in response")

if __name__ == "__main__":
    main()