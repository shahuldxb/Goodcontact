"""
Test audio transcription with Deepgram API using a proper SAS URL
"""
import os
import logging
import sys
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta
from direct_transcribe import DirectTranscriber

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Azure Storage settings
STORAGE_CONNECTION_STRING = os.environ.get(
    "AZURE_STORAGE_CONNECTION_STRING",
    "DefaultEndpointsProtocol=https;AccountName=infolder;AccountKey=NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==;EndpointSuffix=core.windows.net"
)
SOURCE_CONTAINER = os.environ.get("AZURE_SOURCE_CONTAINER", "shahulin")

# Deepgram API key
DEEPGRAM_API_KEY = os.environ.get("DEEPGRAM_API_KEY", "ba94baf7840441c378c58ccd1d5202c38ddc42d8")

def generate_sas_url(blob_name, container_name=SOURCE_CONTAINER, expiry_hours=240):
    """
    Generate a SAS URL for the specified blob.
    
    Args:
        blob_name (str): Name of the blob
        container_name (str): Name of the container
        expiry_hours (int): Number of hours until the SAS URL expires
        
    Returns:
        str: SAS URL for the blob
    """
    try:
        # Extract account information from connection string
        account_name = None
        account_key = None
        
        parts = STORAGE_CONNECTION_STRING.split(';')
        for part in parts:
            if part.startswith('AccountName='):
                account_name = part.split('=', 1)[1]
            elif part.startswith('AccountKey='):
                account_key = part.split('=', 1)[1]
        
        if not account_name or not account_key:
            logger.error("Could not extract account name or key from connection string")
            return None
        
        # Generate SAS token
        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=expiry_hours)
        )
        
        # Build the full SAS URL
        sas_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
        return sas_url
    except Exception as e:
        logger.error(f"Error generating SAS URL: {str(e)}")
        return None

def list_container_blobs(container_name=SOURCE_CONTAINER, limit=5):
    """
    List blobs in the container
    
    Args:
        container_name (str): Name of the container
        limit (int): Maximum number of blobs to list
        
    Returns:
        list: List of blob names
    """
    try:
        # Connect to Azure Storage
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(container_name)
        
        # List blobs in the container
        all_blobs = list(container_client.list_blobs())
        blobs = all_blobs[:limit]
        
        return [blob.name for blob in blobs]
    except Exception as e:
        logger.error(f"Error listing blobs: {str(e)}")
        return []

def test_transcription_with_sas():
    """Test transcription with Deepgram API using a generated SAS URL"""
    
    # List available blobs
    blob_names = list_container_blobs()
    if not blob_names:
        logger.error("No blobs found in container")
        return False
    
    # Find a suitable audio file
    audio_blobs = [blob for blob in blob_names if blob.endswith('.mp3') or blob.endswith('.wav') or blob.endswith('.m4a')]
    if not audio_blobs:
        logger.error("No audio blobs found")
        return False
    
    # Use the first audio file
    blob_name = audio_blobs[0]
    logger.info(f"Found audio blob: {blob_name}")
    
    # Generate SAS URL
    sas_url = generate_sas_url(blob_name)
    if not sas_url:
        logger.error("Failed to generate SAS URL")
        return False
    
    logger.info(f"Generated SAS URL: {sas_url[:60]}...")
    
    try:
        # Create a DirectTranscriber instance
        transcriber = DirectTranscriber(DEEPGRAM_API_KEY)
        
        # Transcribe the audio
        result = transcriber.transcribe_url(sas_url, model="nova-3", diarize=True)
        
        if result and "results" in result:
            # Extract transcript if available
            logger.info("Transcription successful")
            
            # Print response structure
            logger.info(f"Response structure: {list(result.keys())}")
            
            # Extract some basic information from the transcription result
            if "channels" in result["results"]:
                channels = result["results"]["channels"]
                if channels and "alternatives" in channels[0]:
                    alternatives = channels[0]["alternatives"]
                    if alternatives:
                        transcript = alternatives[0].get("transcript", "")
                        confidence = alternatives[0].get("confidence", 0)
                        logger.info(f"Transcript confidence: {confidence:.4f}")
                        logger.info(f"Transcript preview: {transcript[:100]}...")
            
            return True
        else:
            logger.error(f"Transcription failed - response: {result}")
            return False
    except Exception as e:
        logger.error(f"Transcription failed with error: {str(e)}")
        return False

if __name__ == "__main__":
    test_transcription_with_sas()