"""
Test Blob SAS URL Transcription with Deepgram

This script tests transcribing an audio file from Azure Blob Storage using a proper Blob SAS URL.
It demonstrates the correct way to generate a Blob SAS URL for use with Deepgram's API.
"""
import os
import sys
import logging
import json
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Azure Storage settings
STORAGE_ACCOUNT_NAME = os.environ.get("AZURE_STORAGE_ACCOUNT", "infolder")
STORAGE_ACCOUNT_KEY = os.environ.get("AZURE_STORAGE_KEY", "NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==")
STORAGE_CONNECTION_STRING = os.environ.get(
    "AZURE_STORAGE_CONNECTION_STRING", 
    f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={STORAGE_ACCOUNT_KEY};EndpointSuffix=core.windows.net"
)
CONTAINER_NAME = os.environ.get("AZURE_CONTAINER_NAME", "shahulin")

# Deepgram API key
DEEPGRAM_API_KEY = os.environ.get("DEEPGRAM_API_KEY", "ba94baf7840441c378c58ccd1d5202c38ddc42d8")

def generate_blob_sas_url(blob_name, container_name=CONTAINER_NAME, expiry_hours=240):
    """
    Generate a Blob SAS URL for a specific blob in Azure Storage
    
    Args:
        blob_name: Name of the blob
        container_name: Name of the container
        expiry_hours: Number of hours until the SAS token expires
        
    Returns:
        str: SAS URL for the specific blob
    """
    try:
        # Generate SAS token
        sas_token = generate_blob_sas(
            account_name=STORAGE_ACCOUNT_NAME,
            container_name=container_name,
            blob_name=blob_name,
            account_key=STORAGE_ACCOUNT_KEY,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=expiry_hours)
        )
        
        # Build the full SAS URL specifically for this blob
        blob_sas_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
        
        logger.info(f"Generated Blob SAS URL for {blob_name}")
        return blob_sas_url
    except Exception as e:
        logger.error(f"Error generating Blob SAS URL: {str(e)}")
        return None

def list_audio_blobs(container_name=CONTAINER_NAME, limit=5):
    """
    List audio blobs in the container
    
    Args:
        container_name: Name of the container
        limit: Maximum number of blobs to return
        
    Returns:
        list: List of audio blob names
    """
    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(container_name)
        
        # List all blobs in the container
        all_blobs = list(container_client.list_blobs())
        
        # Filter for audio files
        audio_extensions = ('.mp3', '.wav', '.m4a', '.ogg', '.aac', '.flac')
        audio_blobs = [blob.name for blob in all_blobs if blob.name.lower().endswith(audio_extensions)]
        
        return audio_blobs[:limit]
    except Exception as e:
        logger.error(f"Error listing audio blobs: {str(e)}")
        return []

def transcribe_with_deepgram_api(blob_sas_url, model="nova-3", diarize=True):
    """
    Transcribe audio using Deepgram API directly
    
    Args:
        blob_sas_url: SAS URL to the blob
        model: Deepgram model to use
        diarize: Whether to enable speaker diarization
        
    Returns:
        dict: Transcription response
    """
    try:
        # Configure API endpoint and parameters
        api_url = "https://api.deepgram.com/v1/listen"
        params = {
            "model": model,
            "detect_language": "true",
            "punctuate": "true",
            "smart_format": "true"
        }
        
        # Add diarization if requested
        if diarize:
            params["diarize"] = "true"
        
        # Set up headers with API key
        headers = {
            "Authorization": f"Token {DEEPGRAM_API_KEY}",
            "Content-Type": "application/json"
        }
        
        # Prepare request body with URL
        body = {
            "url": blob_sas_url
        }
        
        logger.info(f"Sending request to Deepgram API with URL param")
        logger.info(f"Model: {model}, Diarization: {diarize}")
        
        # Send request to Deepgram
        response = requests.post(api_url, json=body, params=params, headers=headers)
        
        # Check if request was successful
        if response.status_code == 200:
            logger.info("Transcription successful")
            return response.json()
        else:
            logger.error(f"Transcription failed: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Error transcribing with Deepgram: {str(e)}")
        return None

def main():
    """
    Main function to test blob SAS URL transcription
    """
    # Step 1: List audio blobs
    logger.info("Listing audio blobs in container...")
    audio_blobs = list_audio_blobs()
    
    if not audio_blobs:
        logger.error("No audio blobs found")
        return False
    
    logger.info(f"Found {len(audio_blobs)} audio blobs:")
    for i, blob_name in enumerate(audio_blobs):
        logger.info(f"  {i+1}. {blob_name}")
    
    # Step 2: Select first blob for testing
    blob_name = audio_blobs[0]
    logger.info(f"Selected blob: {blob_name}")
    
    # Step 3: Generate SAS URL for the blob
    blob_sas_url = generate_blob_sas_url(blob_name)
    if not blob_sas_url:
        logger.error("Failed to generate blob SAS URL")
        return False
    
    # Print the SAS URL (but truncate the middle for security)
    url_start = blob_sas_url[:60]
    url_end = blob_sas_url[-20:]
    logger.info(f"Generated URL: {url_start}...{url_end}")
    
    # Step 4: Transcribe audio with Deepgram
    logger.info("Transcribing audio with Deepgram...")
    result = transcribe_with_deepgram_api(blob_sas_url)
    
    if not result:
        logger.error("Transcription failed")
        return False
    
    # Step 5: Process and display results
    logger.info("Extracting transcript...")
    
    # Try to find transcript in the response
    transcript = None
    if 'results' in result and 'channels' in result['results']:
        channels = result['results']['channels']
        if channels and 'alternatives' in channels[0]:
            alternatives = channels[0]['alternatives']
            if alternatives and 'transcript' in alternatives[0]:
                transcript = alternatives[0]['transcript']
    
    if transcript:
        logger.info("Transcript excerpt:")
        logger.info(f"{transcript[:200]}...")
        
        # Save full result to file
        output_file = f"transcript_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)
        logger.info(f"Full transcription saved to {output_file}")
        
        return True
    else:
        logger.error("No transcript found in response")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)