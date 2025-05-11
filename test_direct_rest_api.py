#!/usr/bin/env python3
"""
Test direct REST API call to Deepgram using sample URL
This method does NOT use the Deepgram SDK - it uses direct REST API calls
"""

import os
import json
import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants - Exactly as provided
DEEPGRAM_API_KEY = "ba94baf7840441c378c58ccd1d5202c38ddc42d8"

# We'll generate a fresh SAS URL for a file we know exists
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime, timedelta

# Azure Storage connection string
AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=infolder;AccountKey=NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==;EndpointSuffix=core.windows.net"

def generate_fresh_sas_url():
    """Generate a fresh SAS URL for a file we know exists"""
    try:
        # Extract account info from connection string
        conn_parts = {p.split('=')[0]: p.split('=', 1)[1] for p in AZURE_STORAGE_CONNECTION_STRING.split(';') if '=' in p}
        account_name = conn_parts.get('AccountName')
        account_key = conn_parts.get('AccountKey')
        
        # Use a file we know exists
        container_name = "shahulin"
        blob_name = "agricultural_leasing_(ijarah)_normal.mp3"
        
        # Calculate expiry time (24 hours)
        expiry = datetime.utcnow() + timedelta(hours=24)
        
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
        logger.info(f"Generated fresh SAS URL for {blob_name}")
        return url
    except Exception as e:
        logger.error(f"Error generating SAS URL: {str(e)}")
        # Fallback to the original URL if generation fails
        return "https://infolder.blob.core.windows.net/shahulin/agricultural_finance_(murabaha)_angry.mp3?sp=r&st=2025-05-11T14:30:26Z&se=2025-11-12T22:30:26Z&spr=https&sv=2024-11-04&sr=b&sig=q2gumh51pXiVFgidPda5JQJXvGWwF4z%2BhE2tI9Ahkm0%3D"

# Generate a fresh SAS URL for a file we know exists
AUDIO_URL = generate_fresh_sas_url()
DEEPGRAM_API_ENDPOINT = "https://api.deepgram.com/v1/listen"

def transcribe_with_rest_api(audio_url):
    """
    Transcribe audio using Deepgram REST API (not SDK)
    This is the direct REST API implementation you requested
    """
    headers = {
        "Authorization": f"Token {DEEPGRAM_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "url": audio_url,
        "model": "nova-3",  # Using nova-3 as specified in your example
        "smart_format": True,
        "diarize": True,    # Enable speaker separation
        "punctuate": True,
        "utterances": True,
        "paragraphs": True,
    }
    
    logger.info("Sending direct REST API request to Deepgram")
    try:
        response = requests.post(DEEPGRAM_API_ENDPOINT, headers=headers, json=payload)
        
        logger.info(f"Response status code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            
            # Extract transcript for verification
            transcript = ""
            if "results" in result and "channels" in result["results"]:
                channels = result["results"]["channels"]
                if channels and "alternatives" in channels[0]:
                    alternatives = channels[0]["alternatives"]
                    if alternatives and "transcript" in alternatives[0]:
                        transcript = alternatives[0]["transcript"]
            
            logger.info(f"Transcript length: {len(transcript)}")
            if transcript:
                logger.info(f"Transcript preview: {transcript[:100]}...")
            
            # Save full response to file for examination
            with open("deepgram_response.json", "w") as f:
                json.dump(result, f, indent=2)
            logger.info("Full response saved to deepgram_response.json")
            
            return {
                "success": True,
                "transcript": transcript,
                "result": result
            }
        else:
            logger.error(f"Error from Deepgram API: {response.text}")
            return {
                "success": False,
                "error": response.text
            }
    except Exception as e:
        logger.error(f"Exception during transcription: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

def main():
    """Run the direct REST API test"""
    logger.info("Starting direct REST API test with provided URL")
    result = transcribe_with_rest_api(AUDIO_URL)
    
    if result["success"]:
        logger.info("✅ Direct REST API transcription successful!")
        
        # Extract key information for verification
        transcript = result["transcript"]
        logger.info(f"Transcript length: {len(transcript)} characters")
        
        # Define the function needed for production implementation
        logger.info("\n==== IMPLEMENTATION FOR PRODUCTION ====")
        implementation = """
def transcribe_audio_with_rest_api(audio_url):
    # REST API implementation that should be used in production
    headers = {
        "Authorization": f"Token {DEEPGRAM_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "url": audio_url,
        "model": "nova-3",
        "smart_format": True,
        "diarize": True,
        "punctuate": True,
        "utterances": True,
        "paragraphs": True
    }
    
    response = requests.post("https://api.deepgram.com/v1/listen", headers=headers, json=payload)
    
    if response.status_code == 200:
        return {"result": response.json(), "error": None}
    else:
        return {"result": None, "error": {"message": response.text, "status": response.status_code}}
        """
        logger.info(implementation)
    else:
        logger.error(f"❌ Direct REST API transcription failed: {result.get('error')}")

if __name__ == "__main__":
    main()