#!/usr/bin/env python3
"""
Test script to demonstrate direct transcription from Azure Storage
using SAS URLs and our DirectTranscribe class.

This script will:
1. Connect to Azure blob storage
2. Select a file from the shahulin container
3. Generate a SAS URL with 240-hour expiry
4. Send the SAS URL directly to Deepgram (no download)
5. Process the Deepgram response
6. Log the results

No database interactions are performed in this test.
"""

import os
import json
import sys
import time
import asyncio
import logging
from datetime import datetime, timedelta

from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas

# Setup logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants - Azure Storage
AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=infolder;AccountKey=NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==;EndpointSuffix=core.windows.net"
SOURCE_CONTAINER = "shahulin"

# Constants - Deepgram
DEEPGRAM_API_KEY = "ba94baf7840441c378c58ccd1d5202c38ddc42d8"
DEEPGRAM_API_URL = "https://api.deepgram.com/v1/listen"

# Helper functions
def generate_sas_url(blob_name, expiry_hours=240):
    """Generate SAS URL with long expiry time for Deepgram processing"""
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
            container_name=SOURCE_CONTAINER,
            blob_name=blob_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=expiry
        )
        
        # Construct full URL
        blob_url = f"https://{account_name}.blob.core.windows.net/{SOURCE_CONTAINER}/{blob_name}?{sas_token}"
        logger.info(f"Generated SAS URL for {blob_name} with {expiry_hours} hour expiry")
        return blob_url
        
    except Exception as e:
        logger.error(f"Error generating SAS URL: {str(e)}")
        return None

async def transcribe_with_sas_url(sas_url):
    """Transcribe audio using Deepgram API with SAS URL"""
    import aiohttp
    
    headers = {
        "Authorization": f"Token {DEEPGRAM_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "url": sas_url,
        "model": "nova-2",
        "language": "en-US",
        "tier": "nova",
        "diarize": True,
        "punctuate": True,
        "paragraphs": True,
        "utterances": True,
        "keywords": [], 
        "detect_language": True
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            start_time = time.time()
            async with session.post(DEEPGRAM_API_URL, headers=headers, json=payload) as response:
                processing_time = time.time() - start_time
                response_text = await response.text()
                status_code = response.status
                
                logger.info(f"Deepgram API response status: {status_code}")
                logger.info(f"Processing time: {processing_time:.2f} seconds")
                
                if status_code != 200:
                    logger.error(f"Deepgram API error: {response_text}")
                    return {
                        "success": False,
                        "error": response_text,
                        "status_code": status_code
                    }
                
                try:
                    result = json.loads(response_text)
                    
                    # Extract transcript to verify content
                    transcript = ""
                    if "results" in result and "channels" in result["results"]:
                        channels = result["results"]["channels"]
                        if channels and "alternatives" in channels[0]:
                            alternatives = channels[0]["alternatives"]
                            if alternatives and "transcript" in alternatives[0]:
                                transcript = alternatives[0]["transcript"]
                    
                    logger.info(f"Transcript length: {len(transcript)} characters")
                    logger.info(f"Transcript preview: {transcript[:100]}...")
                    
                    # Include full response for debugging
                    return {
                        "success": True,
                        "result": result,
                        "transcript": transcript,
                        "processing_time": processing_time
                    }
                    
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse Deepgram response as JSON: {response_text}")
                    return {
                        "success": False,
                        "error": "Invalid JSON response",
                        "raw_response": response_text,
                        "status_code": status_code
                    }
                    
    except Exception as e:
        logger.error(f"Error during transcription: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

async def list_blobs_in_container(container_name, max_results=10):
    """List blobs in the specified container"""
    try:
        service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container_client = service_client.get_container_client(container_name)
        
        blobs = []
        for blob in container_client.list_blobs():
            if len(blobs) >= max_results:
                break
            blobs.append(blob.name)
        
        return blobs
    except Exception as e:
        logger.error(f"Error listing blobs: {str(e)}")
        return []

async def main():
    """Main function to run the test"""
    logger.info("Starting Azure storage file transcription test")
    
    # List files in container
    logger.info(f"Listing files in {SOURCE_CONTAINER} container")
    blob_names = await list_blobs_in_container(SOURCE_CONTAINER)
    
    if not blob_names:
        logger.error(f"No files found in {SOURCE_CONTAINER} container")
        return
    
    # Select a file (first mp3 or wav file)
    test_blob = None
    for blob in blob_names:
        if blob.lower().endswith(('.mp3', '.wav')):
            test_blob = blob
            break
    
    if not test_blob:
        test_blob = blob_names[0]  # Fallback to first file regardless of type
    
    logger.info(f"Selected file for testing: {test_blob}")
    
    # Generate SAS URL
    sas_url = generate_sas_url(test_blob)
    if not sas_url:
        logger.error("Failed to generate SAS URL")
        return
    
    # Transcribe with SAS URL
    logger.info(f"Transcribing file with SAS URL: {test_blob}")
    result = await transcribe_with_sas_url(sas_url)
    
    # Display results
    logger.info("\n--- TRANSCRIPTION RESULTS ---")
    if result.get("success"):
        logger.info(f"✅ Transcription successful!")
        logger.info(f"Processing time: {result['processing_time']:.2f} seconds")
        logger.info(f"Transcript length: {len(result['transcript'])} characters")
        logger.info(f"Transcript preview: {result['transcript'][:200]}...")
        
        # Save result to file for further analysis
        with open("successful_transcription_result.json", "w") as f:
            json.dump(result["result"], f, indent=2)
        logger.info("Full result saved to successful_transcription_result.json")
        
    else:
        logger.error(f"❌ Transcription failed: {result.get('error')}")
        if "raw_response" in result:
            logger.error(f"Raw response: {result['raw_response']}")

if __name__ == "__main__":
    asyncio.run(main())