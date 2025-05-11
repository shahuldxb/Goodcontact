"""
Transcribe Azure Storage Audio Files using Deepgram's Listen REST API

This implementation uses the direct REST API approach to handle audio transcription
with Deepgram, supporting both local files and Azure Storage blobs via SAS URLs.
"""

import os
import requests
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transcribe_with_listen_rest(audio_url: str, options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Transcribe audio using Deepgram's listen.rest API with a Blob SAS URL.
    
    Args:
        audio_url: The SAS URL to the audio file in Azure Blob Storage
        options: Optional dictionary of transcription options
        
    Returns:
        dict: The complete Deepgram response
    """
    # Get Deepgram API key from environment
    api_key = os.environ.get("DEEPGRAM_API_KEY")
    if not api_key:
        raise ValueError("DEEPGRAM_API_KEY environment variable not set")
        
    # Set up default options if none provided
    if options is None:
        options = {
            "model": "nova-2", 
            "smart_format": True,
            "diarize": True,
            "detect_language": True,
            "punctuate": True,
            "utterances": True
        }
        
    # Prepare API request
    url = "https://api.deepgram.com/v1/listen"
    
    # Set up headers with API key
    headers = {
        "Authorization": f"Token {api_key}",
        "Content-Type": "application/json"
    }
    
    # Prepare the request body with URL and options
    payload = {
        "url": audio_url,
        **options
    }
    
    logger.info(f"Transcribing audio from URL: {audio_url[:60]}... with options: {options}")
    
    # Make the request
    response = requests.post(url, headers=headers, json=payload)
    
    # Check if the request was successful
    if response.status_code == 200:
        logger.info("Transcription successful")
        return response.json()
    else:
        error_message = f"Deepgram API request failed: {response.status_code} - {response.text}"
        logger.error(error_message)
        return {
            "success": False,
            "error": error_message
        }

def transcribe_local_file(file_path: str, options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Transcribe a local audio file using Deepgram's listen.rest API.
    This is useful for debugging and testing.
    
    Args:
        file_path: Path to the local audio file
        options: Optional dictionary of transcription options
        
    Returns:
        dict: The complete Deepgram response
    """
    # Get Deepgram API key from environment
    api_key = os.environ.get("DEEPGRAM_API_KEY")
    if not api_key:
        raise ValueError("DEEPGRAM_API_KEY environment variable not set")
        
    # Set up default options if none provided
    if options is None:
        options = {
            "model": "nova-2", 
            "smart_format": True,
            "diarize": True,
            "detect_language": True,
            "punctuate": True,
            "utterances": True
        }
        
    # Determine file type from extension
    file_type = file_path.split('.')[-1].lower()
    
    # Configure API parameters as query string
    params = options
    
    # Set up API endpoint
    url = "https://api.deepgram.com/v1/listen"
    
    # Set up headers with API key
    headers = {
        "Authorization": f"Token {api_key}",
        "Content-Type": f"audio/{file_type}"
    }
    
    logger.info(f"Transcribing local file: {file_path} with options: {options}")
    
    try:
        # Read the audio file
        with open(file_path, 'rb') as audio_file:
            audio_data = audio_file.read()
        
        # Send the request to Deepgram
        response = requests.post(url, params=params, headers=headers, data=audio_data)
        
        # Check if the request was successful
        if response.status_code == 200:
            logger.info("Transcription successful")
            return response.json()
        else:
            error_message = f"Deepgram API request failed: {response.status_code} - {response.text}"
            logger.error(error_message)
            return {
                "success": False,
                "error": error_message
            }
            
    except FileNotFoundError:
        error_message = f"File not found: {file_path}"
        logger.error(error_message)
        return {
            "success": False,
            "error": error_message
        }
    except Exception as e:
        error_message = f"Error: {str(e)}"
        logger.error(error_message)
        return {
            "success": False,
            "error": error_message
        }

def save_transcription_result(result: Dict[str, Any], output_path: str) -> None:
    """
    Save transcription result to a file.
    
    Args:
        result: The transcription result dictionary
        output_path: Path to save the result
    """
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2)
    
    logger.info(f"Transcription result saved to: {output_path}")

def test_transcription_pipeline(audio_file_name: str) -> Dict[str, Any]:
    """
    Test the complete transcription pipeline using a file from Azure Storage.
    
    Args:
        audio_file_name: The name of the audio file in the Azure Storage container
        
    Returns:
        dict: The transcription result
    """
    from azure_storage_service import AzureStorageService
    
    # Initialize Azure Storage Service
    storage = AzureStorageService()
    
    # Generate SAS URL for the audio file
    container_name = "shahulin"
    sas_url = storage.generate_sas_url(container_name, audio_file_name)
    
    if not sas_url:
        return {
            "success": False,
            "error": f"Failed to generate SAS URL for {audio_file_name}"
        }
    
    # Transcribe using URL
    transcription_options = {
        "model": "nova-2",
        "smart_format": True,
        "diarize": True,
        "detect_language": True,
        "punctuate": True,
        "utterances": True
    }
    
    result = transcribe_with_listen_rest(sas_url, transcription_options)
    
    # Save result to a file
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_file = f"direct_test_results/transcription_{audio_file_name}_{timestamp}.json"
    save_transcription_result(result, output_file)
    
    return result

def main():
    """Test function to demonstrate usage"""
    # Test with an Azure blob
    test_blob = "call_audio_1.mp3"  # Replace with an actual blob name
    result = test_transcription_pipeline(test_blob)
    
    if result.get("success", True):  # Default to True if no "success" key (Deepgram response doesn't include it)
        logger.info("Test completed successfully!")
    else:
        logger.error(f"Test failed: {result.get('error', 'Unknown error')}")

if __name__ == "__main__":
    # Create output directory if it doesn't exist
    os.makedirs("direct_test_results", exist_ok=True)
    main()