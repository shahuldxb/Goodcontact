#!/usr/bin/env python3
"""
Script to upload an MP3 test audio file to Azure Blob Storage with better format validation
"""
import os
import sys
import logging
import tempfile
import requests
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure_storage_service import AzureStorageService

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_valid_mp3():
    """Create a valid MP3 test file using WAV with explicit content type"""
    try:
        # Directory to store the file
        temp_dir = tempfile.mkdtemp()
        local_path = os.path.join(temp_dir, "test_audio.wav")
        
        # Create a valid WAV file first since it's easier to generate programmatically
        logger.info(f"Creating valid WAV file at {local_path}...")
        
        # Generate a basic WAV header (44 bytes) + 1 second of sine wave tone
        with open(local_path, "wb") as f:
            # RIFF header
            f.write(b'RIFF')
            f.write((36 + 1 * 16000 * 2).to_bytes(4, 'little'))  # File size
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
            f.write((1 * 16000 * 2).to_bytes(4, 'little'))  # Data size
            
            # Generate a simple sine wave instead of silence - gives better test results
            import math
            for i in range(16000):  # 1 second at 16kHz
                # Generate sine wave values (amplitude of 16000, frequency of 440Hz)
                value = int(16000 * math.sin(2 * math.pi * 440 * i / 16000))
                f.write(value.to_bytes(2, 'little', signed=True))
        
        file_size = os.path.getsize(local_path)
        logger.info(f"Created WAV file of size {file_size} bytes at {local_path}")
        
        return local_path
    except Exception as e:
        logger.error(f"Error creating valid test audio file: {str(e)}")
        return None

def upload_test_audio():
    """Upload a valid test audio file to Azure Storage"""
    try:
        # Get a valid audio file
        audio_path = create_valid_mp3()
        if not audio_path:
            logger.error("Failed to create a valid audio file")
            return False
            
        # Initialize Azure storage service
        azure_service = AzureStorageService()
        
        # Upload file name - we'll keep the WAV extension for clarity
        dest_file_name = "test_voice_sample.wav"
        
        # Upload to Azure
        storage_account_url = os.environ.get("AZURE_STORAGE_ACCOUNT_URL", "https://infolder.blob.core.windows.net")
        storage_account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY", "NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==")
        source_container_name = os.environ.get("AZURE_SOURCE_CONTAINER_NAME", "shahulin")
        
        blob_service_client = BlobServiceClient(
            account_url=storage_account_url,
            credential=storage_account_key
        )
        container_client = blob_service_client.get_container_client(source_container_name)
        
        # Upload with correct content type
        logger.info(f"Uploading WAV file to container {source_container_name} as {dest_file_name}...")
        with open(audio_path, "rb") as data:
            container_client.upload_blob(
                name=dest_file_name, 
                data=data, 
                overwrite=True,
                content_type="audio/wav"  # Explicitly set the content type for WAV
            )
        
        # Cleanup
        os.remove(audio_path)
        
        logger.info(f"Successfully uploaded WAV file to Azure Storage as {dest_file_name}")
        return True
    except Exception as e:
        logger.error(f"Error uploading audio file: {str(e)}")
        return False

if __name__ == "__main__":
    upload_test_audio()