#!/usr/bin/env python3
"""
Test the DirectTranscribe class with a real Azure Storage blob
"""

import os
import json
import logging
from direct_transcribe import DirectTranscribe
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
DEEPGRAM_API_KEY = "ba94baf7840441c378c58ccd1d5202c38ddc42d8"
AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=infolder;AccountKey=NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==;EndpointSuffix=core.windows.net"

def list_blobs_in_container(container_name="shahulin", max_results=5):
    """List available blobs in a container"""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(container_name)
        
        blobs = []
        for blob in container_client.list_blobs():
            if len(blobs) >= max_results:
                break
            if blob.name.lower().endswith(('.mp3', '.wav')):
                blobs.append(blob.name)
        
        return blobs
    except Exception as e:
        logger.error(f"Error listing blobs: {str(e)}")
        return []

def generate_sas_url(blob_name, container_name="shahulin", expiry_hours=240):
    """Generate SAS URL with long expiry time"""
    try:
        # Extract account info from connection string
        conn_parts = {p.split('=')[0]: p.split('=', 1)[1] for p in AZURE_STORAGE_CONNECTION_STRING.split(';') if '=' in p}
        account_name = conn_parts.get('AccountName')
        account_key = conn_parts.get('AccountKey')
        
        # Calculate expiry time
        expiry = datetime.utcnow() + timedelta(hours=expiry_hours)
        
        # Generate SAS token
        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=expiry
        )
        
        # Construct full URL
        url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
        logger.info(f"Generated SAS URL for {blob_name} (valid for {expiry_hours} hours)")
        return url
    except Exception as e:
        logger.error(f"Error generating SAS URL: {str(e)}")
        return None

def main():
    # List available blobs
    logger.info("Listing available blobs in shahulin container:")
    blobs = list_blobs_in_container()
    
    if not blobs:
        logger.error("No audio files found in shahulin container")
        return
    
    # Display available blobs
    for i, blob in enumerate(blobs):
        logger.info(f"{i+1}. {blob}")
    
    # Select a blob for testing (first in the list)
    selected_blob = blobs[0]
    logger.info(f"Selected blob for testing: {selected_blob}")
    
    # Generate SAS URL
    sas_url = generate_sas_url(selected_blob)
    if not sas_url:
        logger.error("Failed to generate SAS URL")
        return
    
    # Create DirectTranscribe instance
    transcriber = DirectTranscribe(DEEPGRAM_API_KEY)
    
    # Transcribe audio
    logger.info("Transcribing audio with DirectTranscribe...")
    result = transcriber.transcribe_audio(sas_url)
    
    # Check result
    if result["success"]:
        logger.info("✅ Transcription successful!")
        transcript = result["transcript"]
        logger.info(f"Transcript length: {len(transcript)} characters")
        logger.info(f"Transcript preview: {transcript[:100]}...")
        
        # Save result to file
        output_file = f"{selected_blob}_transcription.json"
        with open(output_file, "w") as f:
            json.dump(result["result"], f, indent=2)
        logger.info(f"Full result saved to {output_file}")
        
        # Display the implementation for production
        logger.info("\n==== HOW TO INTEGRATE THIS IN PRODUCTION ====")
        logger.info("""
1. Use DirectTranscribe class for transcription in the production workflow
2. Generate SAS URLs with long expiry (240 hours) for audio files
3. Pass the SAS URL directly to DirectTranscribe.transcribe_audio()
4. Check the result["success"] flag to determine if transcription was successful
5. Use result["transcript"] for transcript text and result["result"] for full response
6. Handle errors using result["error"]["message"]
        """)
    else:
        logger.error(f"❌ Transcription failed: {result['error']['message']}")

if __name__ == "__main__":
    main()