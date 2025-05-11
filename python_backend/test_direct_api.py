#!/usr/bin/env python3
"""
Testing the direct REST API transcription with a specific file
"""

import os
import json
import requests
import logging
import asyncio
from direct_transcribe import DirectTranscribe
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
DEEPGRAM_API_KEY = "ba94baf7840441c378c58ccd1d5202c38ddc42d8"
AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=infolder;AccountKey=NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==;EndpointSuffix=core.windows.net"
SOURCE_CONTAINER = "shahulin"

async def test_direct_api():
    """Test the direct API with a specific file"""
    # 1. Get a file from Azure Storage
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(SOURCE_CONTAINER)
    
    # List files in container
    files = []
    for blob in container_client.list_blobs():
        if blob.name.lower().endswith(('.mp3', '.wav')):
            files.append(blob.name)
            if len(files) >= 5:
                break
    
    if not files:
        logger.error("No audio files found in container")
        return
    
    logger.info(f"Found {len(files)} audio files in container")
    for i, file in enumerate(files):
        logger.info(f"{i+1}. {file}")
    
    # Choose one file (first one)
    test_file = files[0]
    logger.info(f"Testing direct API with file: {test_file}")
    
    # 2. Generate SAS URL
    account_name = blob_service_client.account_name
    account_key = blob_service_client.credential.account_key
    expiry = datetime.utcnow() + timedelta(hours=240)
    
    sas_token = generate_blob_sas(
        account_name=account_name,
        container_name=SOURCE_CONTAINER,
        blob_name=test_file,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=expiry
    )
    
    sas_url = f"https://{account_name}.blob.core.windows.net/{SOURCE_CONTAINER}/{test_file}?{sas_token}"
    logger.info(f"Generated SAS URL for {test_file} (valid for 240 hours)")
    
    # 3. Create DirectTranscribe instance
    transcriber = DirectTranscribe(DEEPGRAM_API_KEY)
    
    # 4. Transcribe audio
    logger.info("Transcribing audio with direct_transcribe.py...")
    result = await transcriber.transcribe_audio(sas_url)
    
    # 5. Log results
    if result["success"]:
        logger.info("✅ DIRECT_TRANSCRIBE SUCCESS!")
        transcript = result["transcript"]
        logger.info(f"Transcript length: {len(transcript)} characters")
        logger.info(f"Transcript preview: {transcript[:100]}...")
        
        # Save result to file
        with open("direct_transcribe_result.json", "w") as f:
            json.dump(result["result"], f, indent=2)
        logger.info("Full result saved to direct_transcribe_result.json")
    else:
        logger.error(f"❌ DIRECT_TRANSCRIBE FAILED: {result['error']['message']}")

if __name__ == "__main__":
    asyncio.run(test_direct_api())