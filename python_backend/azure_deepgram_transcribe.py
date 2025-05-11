#!/usr/bin/env python3
"""
Transcribe Azure Storage Audio Files using Deepgram's API

An adapted script to transcribe audio files from Azure Blob Storage using Deepgram's REST API directly.
This script demonstrates the speaker diarization functionality to identify
different speakers in an audio recording.

This is built as a module that can be imported and used by the main application.
"""

import os
import json
import time
import uuid
import logging
import tempfile
import requests
from datetime import datetime
import shutil
import sys
import json
import mimetypes
import uuid
from io import BytesIO
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Azure Storage imports
try:
    from azure.storage.blob import BlobServiceClient, BlobClient
    from azure.identity import DefaultAzureCredential
except ImportError:
    logger.warning("Azure Storage libraries not installed. Install with: pip install azure-storage-blob azure-identity")

def transcribe_azure_audio(blob_name, api_key=None, model="nova-2", diarize=True, container_name="shahulin"):
    """
    Transcribe an audio file from Azure Blob Storage using Deepgram API
    
    Args:
        blob_name: Name of the blob in Azure Storage
        api_key: Deepgram API key (if None, will use environment variable)
        model: Deepgram model to use
        diarize: Whether to enable speaker diarization
        container_name: Azure Storage container name
        
    Returns:
        dict: Result of the transcription
    """
    # Get API key from environment if not provided
    if api_key is None:
        api_key = os.environ.get("DEEPGRAM_API_KEY")
        if not api_key:
            logger.error("DEEPGRAM_API_KEY environment variable not set")
            return {"error": "No API key provided"}

    # Azure Storage connection
    try:
        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        if not connect_str:
            logger.error("AZURE_STORAGE_CONNECTION_STRING environment variable not set")
            return {"error": "Azure Storage connection string not provided"}
        
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        
        # Get a blob client for the specified file
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        # Create temp directory for processing
        temp_dir = Path(tempfile.gettempdir()) / "deepgram-processing"
        temp_dir.mkdir(exist_ok=True)
        local_path = temp_dir / blob_name
        
        # Download the audio file to a temporary location
        with open(local_path, "wb") as file:
            file.write(blob_client.download_blob().readall())
        
        logger.info(f"Downloaded {blob_name} to {local_path}")
        
        # Process the file
        try:
            # Prepare the Deepgram API URL with parameters
            url = "https://api.deepgram.com/v1/listen"
            params = {
                "model": model,
                "diarize": "true" if diarize else "false",
                "punctuate": "true",
                "utterances": "true"
            }
            
            # Set the headers with API key
            headers = {
                "Authorization": f"Token {api_key}"
            }
            
            # Open the file for sending to Deepgram
            with open(local_path, "rb") as file:
                # Get the file's MIME type
                mime_type, _ = mimetypes.guess_type(local_path)
                if not mime_type:
                    # Default to audio/mpeg if MIME type can't be determined
                    mime_type = "audio/mpeg"
                
                logger.info(f"Sending {blob_name} with mimetype {mime_type} to Deepgram for transcription...")
                
                # Make the API request
                response = requests.post(
                    url,
                    params=params,
                    headers=headers,
                    data=file,
                    timeout=60  # 60-second timeout
                )
                
                # Log the response status
                logger.info(f"Deepgram API response status: {response.status_code}")
                
                # Check if the request was successful
                if response.status_code == 200:
                    # Parse and return the JSON response
                    result = response.json()
                    logger.info(f"DEEPGRAM RAW RESPONSE: {json.dumps(result)}")
                    
                    # Print debug info about the response
                    if isinstance(result, dict):
                        logger.info(f"Response keys: {', '.join(result.keys())}")
                                
                    return result
                else:
                    # If the request failed, build an error response
                    error_message = f"Deepgram API error: {response.status_code} - {response.text}"
                    logger.error(error_message)
                    
                    # Try to parse error as JSON if possible
                    try:
                        error_json = response.json()
                        return {"error": error_json}
                    except:
                        return {"error": {"status": response.status_code, "message": response.text}}
                    
        except Exception as e:
            # Log and return any exceptions
            error_message = f"Exception in Deepgram transcription: {str(e)}"
            logger.error(error_message)
            return {"error": {"message": str(e)}}
        
        finally:
            # Clean up the temporary file
            try:
                if local_path.exists():
                    local_path.unlink()
                    logger.debug(f"Removed temporary file {local_path}")
            except Exception as cleanup_error:
                logger.warning(f"Error cleaning up temporary file: {str(cleanup_error)}")
    
    except Exception as e:
        # Handle Azure Storage exceptions
        error_message = f"Azure Storage error: {str(e)}"
        logger.error(error_message)
        return {"error": {"message": str(e)}}

def process_audio_file(blob_name, fileid=None, output_container="shahulout"):
    """
    Process an audio file from Azure Blob Storage:
    1. Transcribe it using Deepgram
    2. Copy it to the output container
    3. Return the transcription results
    
    Args:
        blob_name: Name of the blob in Azure Storage
        fileid: Optional ID for the file (for database tracking)
        output_container: Container to move the processed file to
        
    Returns:
        dict: Processing results including transcription
    """
    # Generate a file ID if not provided
    if fileid is None:
        fileid = str(uuid.uuid4())
    
    # Record processing start time
    start_time = time.time()
    
    # Transcribe the audio
    transcription_result = transcribe_azure_audio(blob_name)
    
    # Calculate transcription time
    transcription_time = time.time() - start_time
    
    # Process the transcription result to extract useful information
    result = {
        "fileid": fileid,
        "transcription": transcription_result,
        "processing_time": transcription_time,
        "original_filename": blob_name
    }
    
    # Move the file to the output container
    try:
        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        if connect_str:
            # Create a BlobServiceClient
            blob_service_client = BlobServiceClient.from_connection_string(connect_str)
            
            # Get the source blob client
            source_blob_client = blob_service_client.get_blob_client(container="shahulin", blob=blob_name)
            
            # Get the destination blob client
            dest_blob_client = blob_service_client.get_blob_client(container=output_container, blob=blob_name)
            
            # Start copy operation
            dest_blob_client.start_copy_from_url(source_blob_client.url)
            
            # Add destination URL to the result
            result["destination_url"] = dest_blob_client.url
            
            logger.info(f"Moved {blob_name} to {output_container} container")
        else:
            logger.warning("Could not move file: Azure Storage connection string not set")
    except Exception as e:
        logger.error(f"Error moving file to output container: {str(e)}")
        result["move_error"] = str(e)
    
    return result

def main():
    """Command-line test function"""
    import sys
    
    # Check if a file name is provided as argument
    if len(sys.argv) < 2:
        print("Usage: python azure_deepgram_transcribe.py <blob_name>")
        sys.exit(1)
    
    # Get the file name from arguments
    blob_name = sys.argv[1]
    
    # Process the file
    result = process_audio_file(blob_name)
    
    # Print the result
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()