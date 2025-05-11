#!/usr/bin/env python3
"""
Script to upload a test audio file to Azure Blob Storage
"""
import os
import sys
import logging
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def upload_test_file():
    """Create and upload a test audio file to Azure Storage"""
    # Azure Storage account configuration
    storage_account_url = os.environ.get("AZURE_STORAGE_ACCOUNT_URL", "https://infolder.blob.core.windows.net")
    storage_account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY", "NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==")
    source_container_name = os.environ.get("AZURE_SOURCE_CONTAINER_NAME", "shahulin")
    
    # Initialize BlobServiceClient
    try:
        blob_service_client = BlobServiceClient(
            account_url=storage_account_url,
            credential=storage_account_key
        )
        container_client = blob_service_client.get_container_client(source_container_name)
        
        # Create a test file - valid WAV file with 1 second of silence
        test_file_path = "test_audio.wav"
        
        # Generate a basic WAV header (44 bytes) + 1 second of silence
        with open(test_file_path, "wb") as f:
            # RIFF header
            f.write(b'RIFF')
            f.write((36 + 1 * 16000).to_bytes(4, 'little'))  # File size
            f.write(b'WAVE')
            
            # Format chunk
            f.write(b'fmt ')
            f.write((16).to_bytes(4, 'little'))  # Format chunk size
            f.write((1).to_bytes(2, 'little'))   # PCM format
            f.write((1).to_bytes(2, 'little'))   # Mono
            f.write((16000).to_bytes(4, 'little'))  # Sample rate
            f.write((32000).to_bytes(4, 'little'))  # Byte rate
            f.write((2).to_bytes(2, 'little'))   # Block align
            f.write((16).to_bytes(2, 'little'))  # Bits per sample
            
            # Data chunk
            f.write(b'data')
            f.write((1 * 16000).to_bytes(4, 'little'))  # Data size
            
            # One second of silence (16000 samples at 16 bit)
            f.write(b'\x00\x00' * 16000)
        
        # Upload file to Azure Storage
        logger.info(f"Uploading test file {test_file_path} to container {source_container_name}...")
        with open(test_file_path, "rb") as data:
            container_client.upload_blob(name="test_audio.wav", data=data, overwrite=True)
        
        logger.info(f"Successfully uploaded test file to Azure Storage")
        return True
    except Exception as e:
        logger.error(f"Error uploading test file: {str(e)}")
        return False

if __name__ == "__main__":
    upload_test_file()