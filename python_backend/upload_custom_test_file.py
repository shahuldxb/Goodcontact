#!/usr/bin/env python3
"""
Script to upload a custom test audio file to Azure Blob Storage
"""
import os
import sys
import logging
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def upload_custom_file(file_path):
    """Upload a custom audio file to Azure Storage"""
    if not os.path.exists(file_path):
        logger.error(f"File does not exist: {file_path}")
        return False
        
    # Azure Storage account configuration
    storage_account_url = os.environ.get("AZURE_STORAGE_ACCOUNT_URL", "https://infolder.blob.core.windows.net")
    storage_account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY", "NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==")
    source_container_name = os.environ.get("AZURE_SOURCE_CONTAINER_NAME", "shahulin")
    
    # Get filename only from path
    file_name = os.path.basename(file_path)
    
    # Initialize BlobServiceClient
    try:
        blob_service_client = BlobServiceClient(
            account_url=storage_account_url,
            credential=storage_account_key
        )
        container_client = blob_service_client.get_container_client(source_container_name)
        
        # Upload file to Azure Storage
        logger.info(f"Uploading file {file_path} to container {source_container_name}...")
        with open(file_path, "rb") as data:
            container_client.upload_blob(name=file_name, data=data, overwrite=True)
        
        logger.info(f"Successfully uploaded file {file_name} to Azure Storage")
        return True
    except Exception as e:
        logger.error(f"Error uploading file: {str(e)}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("No file specified. Usage: python upload_custom_test_file.py <file_path>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    success = upload_custom_file(file_path)
    if not success:
        sys.exit(1)