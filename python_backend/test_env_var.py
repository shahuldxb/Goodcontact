"""
Test script that sets environment variable for testing both SDK and REST API methods
"""

import os
import asyncio
import json
import logging
from deepgram_service import DeepgramService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_with_env_var(audio_file_path, method="sdk"):
    """Test transcription with specified method via environment variable"""
    
    # Set environment variable
    os.environ["DEEPGRAM_TRANSCRIPTION_METHOD"] = method
    logger.info(f"Set DEEPGRAM_TRANSCRIPTION_METHOD to {method}")
    
    # Initialize service and process file
    service = DeepgramService()
    
    # Get file ID from filename
    file_id = f"test_{os.path.basename(audio_file_path).split('.')[0]}"
    
    # Process the file
    logger.info(f"Processing file {audio_file_path} with ID {file_id} using {method} method")
    result = await service.transcribe_audio(audio_file_path)
    
    # Check for error
    if result['error'] is not None:
        logger.error(f"Error: {result['error']}")
        return
    
    # Extract and log detected language
    try:
        channels = result['result']['results']['channels']
        if channels and len(channels) > 0:
            detected_language = channels[0].get('detected_language', 'unknown')
            language_confidence = channels[0].get('language_confidence', 0.0)
            logger.info(f"Detected language: {detected_language}")
            logger.info(f"Language confidence: {language_confidence}")
    except (KeyError, IndexError) as e:
        logger.error(f"Error extracting language: {e}")
    
    # Print abbreviated response
    logger.info(f"Transcription Result:\n{json.dumps(result['result'], indent=2)[:500]}...")
    
    return result

async def main():
    """Test both transcription methods on the same file"""
    audio_file = "test_speech_sdk.wav"
    
    # First, test with SDK method
    logger.info("TESTING WITH SDK METHOD")
    await test_with_env_var(audio_file, "sdk")
    
    # Then, test with REST API method
    logger.info("\n\nTESTING WITH REST API METHOD")
    await test_with_env_var(audio_file, "rest_api")

if __name__ == "__main__":
    asyncio.run(main())