#!/usr/bin/env python3
"""
List available blobs in Azure Storage containers
"""

import os
import logging
from azure.storage.blob import BlobServiceClient

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Azure Storage account configuration
STORAGE_ACCOUNT_URL = "https://infolder.blob.core.windows.net"
STORAGE_ACCOUNT_KEY = "NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw=="
SOURCE_CONTAINER = "shahulin"

def list_blobs():
    """List available blobs in the container"""
    try:
        # Create the BlobServiceClient
        blob_service_client = BlobServiceClient(
            account_url=STORAGE_ACCOUNT_URL,
            credential=STORAGE_ACCOUNT_KEY
        )
        
        # Get the container client
        container_client = blob_service_client.get_container_client(SOURCE_CONTAINER)
        
        # List the blobs
        logger.info(f"Listing blobs in container: {SOURCE_CONTAINER}")
        
        blob_list = list(container_client.list_blobs())
        logger.info(f"Found {len(blob_list)} blobs")
        
        # Print first 10 blobs
        for i, blob in enumerate(blob_list[:10]):
            logger.info(f"{i+1}. {blob.name}")
            
        return blob_list
        
    except Exception as e:
        logger.error(f"Error listing blobs: {str(e)}")
        return []

if __name__ == "__main__":
    list_blobs()