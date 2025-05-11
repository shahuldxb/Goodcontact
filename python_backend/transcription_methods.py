#!/usr/bin/env python3
"""
This module provides methods for transcribing audio using Deepgram:
1. Using the official Deepgram SDK (old method)
2. Using direct REST API calls with requests library
3. Using a shortcut method that calls the test_direct_transcription script
4. Using the modern Deepgram listen.rest API (recommended method)
"""
import os
import json
import logging
import requests
import asyncio
import time
from datetime import datetime
from deepgram import Deepgram

# Import modern Deepgram SDK for listen.rest method
from deepgram import (
    DeepgramClient,
    PrerecordedOptions,
    DeepgramClientOptions
)

from test_direct_transcription import test_direct_transcription

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get API key from environment or use a default for testing (replace in production)
DEEPGRAM_API_KEY = os.environ.get("DEEPGRAM_API_KEY", "ba94baf7840441c378c58ccd1d5202c38ddc42d8")


async def transcribe_with_sdk(audio_file_path):
    """
    Transcribe audio using the official Deepgram SDK.
    
    Args:
        audio_file_path (str): Path to the local audio file to transcribe.
        
    Returns:
        dict: The complete Deepgram response including transcript and metadata.
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


def transcribe_with_rest_api(audio_file_path):
    """
    Transcribe audio using direct REST API calls to Deepgram.
    
    Args:
        audio_file_path (str): Path to the local audio file to transcribe.
        
    Returns:
        dict: The complete Deepgram response including transcript and metadata.
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
            "utterances": "true",
            "summarize": "true"
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


def transcribe_with_listen_rest(audio_url):
    """
    Transcribe audio using Deepgram's listen.rest API with a Blob SAS URL.
    This is the modern, recommended approach for using Deepgram.
    
    Args:
        audio_url: The SAS URL to the audio file in Azure Blob Storage
        
    Returns:
        dict: The complete Deepgram response
    """
    try:
        logger.info(f"Transcribing using listen.rest API: {audio_url[:60]}...")
        
        # Initialize the Deepgram client with our API key
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

async def main():
    """
    Example of how to use all transcription methods.
    """
    # Path to audio file for testing
    test_file = "test_speech_fr.wav"
    
    if not os.path.exists(test_file):
        logger.error(f"Test file {test_file} not found!")
        return
    
    # Method 1: Using the SDK
    try:
        logger.info("=== Transcribing with SDK ===")
        sdk_result = await transcribe_with_sdk(test_file)
        logger.info("SDK transcription completed successfully")
    except Exception as sdk_err:
        logger.error(f"SDK transcription failed: {str(sdk_err)}")
    
    # Method 2: Using the REST API
    try:
        logger.info("=== Transcribing with REST API ===")
        rest_result = transcribe_with_rest_api(test_file)
        logger.info("REST API transcription completed successfully")
    except Exception as rest_err:
        logger.error(f"REST API transcription failed: {str(rest_err)}")
    
    # Method 4: Using the Listen REST API (would need a URL)
    # Example SAS URL (replace with a real one for testing)
    example_url = "https://infolder.blob.core.windows.net/shahulin/agricultural_finance_(murabaha)_angry.mp3?sp=r&st=2025-05-11T14:30:26Z&se=2025-11-12T22:30:26Z&spr=https&sv=2024-11-04&sr=b&sig=q2gumh51pXiVFgidPda5JQJXvGWwF4z%2BhE2tI9Ahkm0%3D"
    try:
        logger.info("=== Transcribing with Listen REST API ===")
        listen_rest_result = transcribe_with_listen_rest(example_url)
        logger.info("Listen REST API transcription completed successfully")
    except Exception as listen_err:
        logger.error(f"Listen REST API transcription failed: {str(listen_err)}")


# Memory store for test_direct_transcription results
direct_transcription_results = {}

def transcribe_audio_directly(audio_file_path):
    """
    Transcribe a local audio file directly using Deepgram API,
    similar to the direct REST API implementation used for Azure blobs.
    
    Args:
        audio_file_path (str): Path to the local audio file to transcribe.
        
    Returns:
        dict: The complete Deepgram response including transcript and metadata.
    """
    try:
        logger.info(f"Directly transcribing local file: {audio_file_path}")
        
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
        logger.info(f"File type: {content_type}")
        file_size = os.path.getsize(audio_file_path)
        logger.info(f"File size: {file_size} bytes")
        
        # Set up the request to Deepgram API
        url = "https://api.deepgram.com/v1/listen"
        
        # Prepare headers with API key
        headers = {
            "Authorization": f"Token {DEEPGRAM_API_KEY}",
            "Content-Type": content_type
        }
        
        # Prepare query parameters for transcription options
        params = {
            "smart_format": "true",
            "model": "nova-2",
            "detect_language": "true",
            "punctuate": "true",
            "diarize": "true",
            "utterances": "true",
            "summarize": "true"
        }
        
        # Open and read audio file
        with open(audio_file_path, "rb") as audio:
            # Track start time for performance monitoring
            start_time = time.time()
            
            # Send POST request to Deepgram API
            response = requests.post(
                url, 
                headers=headers,
                params=params,
                data=audio
            )
            
            # Calculate elapsed time
            elapsed_time = time.time() - start_time
            logger.info(f"API request completed in {elapsed_time:.2f} seconds")
            
            # Check if request was successful
            if response.status_code == 200:
                result = response.json()
                
                # Process result for easier transcript extraction
                transcript_text = ""
                detected_language = "unknown"
                
                try:
                    # Extract language
                    if ("results" in result and 
                        "channels" in result["results"] and 
                        len(result["results"]["channels"]) > 0 and
                        "detected_language" in result["results"]["channels"][0]):
                        detected_language = result["results"]["channels"][0]["detected_language"]
                    
                    logger.info(f"Detected language: {detected_language}")
                    
                    # Extract transcript
                    if ("results" in result and 
                        "channels" in result["results"] and 
                        len(result["results"]["channels"]) > 0 and
                        "alternatives" in result["results"]["channels"][0] and
                        len(result["results"]["channels"][0]["alternatives"]) > 0 and
                        "transcript" in result["results"]["channels"][0]["alternatives"][0]):
                        transcript_text = result["results"]["channels"][0]["alternatives"][0]["transcript"]
                    
                    if transcript_text:
                        logger.info(f"Transcript (first 100 chars): {transcript_text[:100]}...")
                    else:
                        logger.warning("No transcript text was extracted")
                except Exception as extract_err:
                    logger.error(f"Error extracting response data: {str(extract_err)}")
                
                # Format response similar to other transcription methods
                formatted_response = {
                    "result": result,
                    "error": None,
                    "transcript": transcript_text
                }
                
                return formatted_response
            else:
                error = f"Deepgram API request failed: {response.status_code} - {response.text}"
                logger.error(error)
                return {"result": None, "error": {"name": "DeepgramApiError", "message": error, "status": response.status_code}}
    except Exception as e:
        logger.error(f"Error in direct local file transcription: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {"result": None, "error": {"name": "TranscriptionException", "message": str(e), "status": 500}}

def transcribe_audio_shortcut(audio_file_path):
    """
    Transcribe audio using the shortcut method that directly calls test_direct_transcription.
    This method is a fourth transcription option that bypasses all other methods and
    uses the working test script directly.
    
    Args:
        audio_file_path (str): Path to the local audio file to transcribe.
        
    Returns:
        dict: The complete Deepgram response including transcript and metadata.
    """
    try:
        # Simply extract the blob name from the file path
        blob_name = os.path.basename(audio_file_path)
        logger.info(f"SHORTCUT method: Processing blob: {blob_name}")
        
        # Log start of transcription
        start_time = time.time()
        
        # Just call the test function directly with the blob name
        result = test_direct_transcription(blob_name=blob_name)
        
        # Store the raw result in memory for inspection
        direct_transcription_results[blob_name] = {
            'timestamp': datetime.now().isoformat(),
            'result': result
        }
        
        # Log completion
        elapsed_time = time.time() - start_time
        logger.info(f"SHORTCUT transcription completed in {elapsed_time:.2f} seconds")
        
        # Extract the transcript if possible
        transcript_text = ""
        if "results" in result and "channels" in result["results"] and len(result["results"]["channels"]) > 0:
            # Extract transcript from first channel's first alternative
            if ("alternatives" in result["results"]["channels"][0] and
                len(result["results"]["channels"][0]["alternatives"]) > 0 and
                "transcript" in result["results"]["channels"][0]["alternatives"][0]):
                transcript_text = result["results"]["channels"][0]["alternatives"][0]["transcript"]
                logger.info(f"Extracted transcript: {transcript_text[:50]}...")
        
        # Format the result to match the extraction logic expectations
        # Add transcript at the root level to match Method 0 in the extraction logic
        response = {
            "result": result,
            "error": None,
            "transcript": transcript_text  # This matches the first extraction path in the service
        }
        
        logger.info(f"Returning response with keys: {response.keys()}")
        return response
        
    except Exception as e:
        logger.error(f"Error in SHORTCUT transcription: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {"error": {"name": "ShortcutException", "message": str(e), "status": 500}}

if __name__ == "__main__":
    # Run the main async function
    asyncio.run(main())