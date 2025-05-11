#!/usr/bin/env python3
"""
Test script for SDK transcription method only
"""
import os
import logging
import json
import asyncio
from deepgram import Deepgram

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get API key from environment or use a default for testing (replace in production)
DEEPGRAM_API_KEY = os.environ.get("DEEPGRAM_API_KEY", "")


async def transcribe_with_sdk(audio_file_path):
    """
    Transcribe audio using the official Deepgram SDK.
    """
    try:
        # Ensure file exists
        if not os.path.exists(audio_file_path):
            raise FileNotFoundError(f"Audio file not found: {audio_file_path}")
            
        # Initialize Deepgram client
        deepgram = Deepgram(DEEPGRAM_API_KEY)
        
        # Determine mime type based on file extension
        file_extension = os.path.splitext(audio_file_path)[1].lower()
        mimetype = "audio/wav"  # Default
        if file_extension == ".mp3":
            mimetype = "audio/mp3"
        elif file_extension == ".ogg":
            mimetype = "audio/ogg"
        elif file_extension == ".flac":
            mimetype = "audio/flac"
        
        # Log file info
        logger.info(f"Transcribing file {audio_file_path} ({mimetype}) using Deepgram SDK")
        file_size = os.path.getsize(audio_file_path)
        logger.info(f"File size: {file_size} bytes")
        
        # Configure transcription options
        options = {
            "smart_format": True,
            "model": "nova",
            "diarize": True,
            "punctuate": True,
            "utterances": True,
            "detect_language": True
        }
        
        # Open audio file and send to Deepgram
        with open(audio_file_path, "rb") as audio:
            source = {'buffer': audio, 'mimetype': mimetype}
            response = await deepgram.transcription.prerecorded(source, options)
            
            # Extract relevant information for debugging
            if response and 'results' in response:
                try:
                    detected_language = "unknown"
                    if ('channels' in response['results'] and 
                        len(response['results']['channels']) > 0 and 
                        'detected_language' in response['results']['channels'][0]):
                        detected_language = response['results']['channels'][0]['detected_language']
                        
                    logger.info(f"Detected language: {detected_language}")
                    
                    # Check for language confidence
                    if ('channels' in response['results'] and 
                        len(response['results']['channels']) > 0 and 
                        'language_confidence' in response['results']['channels'][0]):
                        confidence = response['results']['channels'][0]['language_confidence']
                        logger.info(f"Language confidence: {confidence}")
                    
                    transcript = ""
                    if ('channels' in response['results'] and 
                        len(response['results']['channels']) > 0 and 
                        'alternatives' in response['results']['channels'][0] and
                        len(response['results']['channels'][0]['alternatives']) > 0 and
                        'transcript' in response['results']['channels'][0]['alternatives'][0]):
                        transcript = response['results']['channels'][0]['alternatives'][0]['transcript']
                        
                    logger.info(f"Transcript (first 100 chars): {transcript[:100]}...")
                except Exception as extract_err:
                    logger.error(f"Error extracting response data: {str(extract_err)}")
            
            return response
    except Exception as e:
        logger.error(f"Error in SDK transcription: {str(e)}")
        raise


async def main():
    """Test the SDK transcription method"""
    test_file = "test_speech_fr.wav"
    
    if not os.path.exists(test_file):
        logger.error(f"Test file {test_file} not found in {os.getcwd()}")
        return
    
    try:
        result = await transcribe_with_sdk(test_file)
        
        # Print a pretty version of the result
        print("\nTranscription Result:")
        print(json.dumps(result, indent=2))
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())