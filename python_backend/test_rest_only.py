#!/usr/bin/env python3
"""
Test script for REST API transcription method only
"""
import os
import logging
import requests
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get API key from environment or use a default for testing (replace in production)
DEEPGRAM_API_KEY = os.environ.get("DEEPGRAM_API_KEY", "")

def transcribe_with_rest_api(audio_file_path):
    """
    Transcribe audio using direct REST API calls to Deepgram.
    """
    try:
        # Ensure file exists
        if not os.path.exists(audio_file_path):
            raise FileNotFoundError(f"Audio file not found: {audio_file_path}")
            
        # Determine content type based on file extension
        file_extension = os.path.splitext(audio_file_path)[1].lower()
        content_type = "audio/wav"  # Default
        if file_extension == ".mp3":
            content_type = "audio/mp3"
        elif file_extension == ".ogg":
            content_type = "audio/ogg"
        elif file_extension == ".flac":
            content_type = "audio/flac"
        
        # Log file info
        logger.info(f"Transcribing file {audio_file_path} ({content_type}) using REST API")
        file_size = os.path.getsize(audio_file_path)
        logger.info(f"File size: {file_size} bytes")
        
        # Set up the request 
        url = "https://api.deepgram.com/v1/listen"
        
        # Prepare headers with API key
        headers = {
            "Authorization": f"Token {DEEPGRAM_API_KEY}",
            "Content-Type": content_type
        }
        
        # Prepare query parameters for transcription options
        params = {
            "smart_format": "true",
            "model": "nova",
            "detect_language": "true",
            "punctuate": "true",
            "diarize": "true",
            "utterances": "true"
        }
        
        # Open and read audio file
        with open(audio_file_path, "rb") as audio:
            # Send POST request to Deepgram API
            response = requests.post(
                url, 
                headers=headers,
                params=params,
                data=audio
            )
            
            # Check if request was successful
            if response.status_code == 200:
                result = response.json()
                
                # Extract relevant information for debugging
                try:
                    detected_language = "unknown"
                    if ("results" in result and 
                        "channels" in result["results"] and 
                        len(result["results"]["channels"]) > 0 and
                        "detected_language" in result["results"]["channels"][0]):
                        detected_language = result["results"]["channels"][0]["detected_language"]
                    
                    logger.info(f"Detected language: {detected_language}")
                    
                    transcript = ""
                    if ("results" in result and 
                        "channels" in result["results"] and 
                        len(result["results"]["channels"]) > 0 and
                        "alternatives" in result["results"]["channels"][0] and
                        len(result["results"]["channels"][0]["alternatives"]) > 0 and
                        "transcript" in result["results"]["channels"][0]["alternatives"][0]):
                        transcript = result["results"]["channels"][0]["alternatives"][0]["transcript"]
                    
                    logger.info(f"Transcript (first 100 chars): {transcript[:100]}...")
                except Exception as extract_err:
                    logger.error(f"Error extracting response data: {str(extract_err)}")
                
                return result
            else:
                error = f"Deepgram API request failed: {response.status_code} - {response.text}"
                logger.error(error)
                raise Exception(error)
    except Exception as e:
        logger.error(f"Error in REST API transcription: {str(e)}")
        raise


def main():
    """Test the REST API transcription method"""
    test_file = "test_speech_fr.wav"
    
    if not os.path.exists(test_file):
        logger.error(f"Test file {test_file} not found in {os.getcwd()}")
        return
    
    try:
        result = transcribe_with_rest_api(test_file)
        
        # Print a pretty version of the result
        print("\nTranscription Result:")
        print(json.dumps(result, indent=2))
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")


if __name__ == "__main__":
    main()