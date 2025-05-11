#!/usr/bin/env python3
"""
Test script for the REST API transcription method
"""
import os
import logging
from transcription_methods import transcribe_with_rest_api

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Test the REST API transcription method with a sample audio file"""
    # Use the test file we created earlier
    test_file = "test_speech_fr.wav"
    
    if not os.path.exists(test_file):
        logger.error(f"Test file {test_file} not found in {os.getcwd()}")
        return
    
    try:
        logger.info(f"Testing REST API transcription with file: {test_file}")
        result = transcribe_with_rest_api(test_file)
        
        # Show response summary
        if result:
            logger.info("Transcription successful!")
            
            # Display some results
            if "results" in result and "channels" in result["results"]:
                channel = result["results"]["channels"][0]
                if "detected_language" in channel:
                    logger.info(f"Detected language: {channel['detected_language']}")
                    if "language_confidence" in channel:
                        logger.info(f"Language confidence: {channel['language_confidence']}")
                
                if "alternatives" in channel and len(channel["alternatives"]) > 0:
                    transcript = channel["alternatives"][0].get("transcript", "")
                    logger.info(f"Transcript: {transcript[:100]}...")
                    
                    if "confidence" in channel["alternatives"][0]:
                        logger.info(f"Transcript confidence: {channel['alternatives'][0]['confidence']}")
        else:
            logger.warning("Transcription completed but returned empty result")
            
    except Exception as e:
        logger.error(f"Error during REST API transcription test: {str(e)}")

if __name__ == "__main__":
    main()