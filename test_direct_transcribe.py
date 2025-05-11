#!/usr/bin/env python3
"""
Test direct transcription with Deepgram using a SAS URL
"""
import os
import sys
import json
import time
import asyncio
import requests
from python_backend.azure_storage_service import AzureStorageService
from datetime import datetime, timedelta

# Fixed Deepgram API key - using the one that works
DEEPGRAM_API_KEY = "ba94baf7840441c378c58ccd1d5202c38ddc42d8"

def generate_sas_url(blob_name, expiry_hours=240):
    """Generate SAS URL with long expiry time"""
    service = AzureStorageService()
    
    # Get container client
    container_client = service.blob_service_client.get_container_client('shahulin')
    blob_client = container_client.get_blob_client(blob_name)
    
    # Calculate expiry time
    start_time = datetime.utcnow() - timedelta(minutes=5)  # Start 5 minutes ago to avoid clock skew
    expiry_time = datetime.utcnow() + timedelta(hours=expiry_hours)
    
    # Generate SAS token with read permission
    from azure.storage.blob import generate_blob_sas, BlobSasPermissions
    
    sas_token = generate_blob_sas(
        account_name=service.blob_service_client.account_name,
        container_name='shahulin',
        blob_name=blob_name,
        account_key=service.blob_service_client.credential.account_key,
        permission=BlobSasPermissions(read=True),
        start=start_time,
        expiry=expiry_time
    )
    
    # Construct full URL
    sas_url = f"{blob_client.url}?{sas_token}"
    return sas_url

async def transcribe_audio_url(audio_url):
    """
    Transcribe audio from URL using Deepgram API (REST API call)
    """
    print(f"Sending audio URL to Deepgram: {audio_url}")
    
    # Deepgram API endpoint for URL-based transcription
    url = "https://api.deepgram.com/v1/listen"
    
    # Configure headers with API key
    headers = {
        "Authorization": f"Token {DEEPGRAM_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # Configure parameters
    params = {
        "punctuate": "true",
        "diarize": "true",
        "paragraphs": "true", 
        "utterances": "true",
        "smart_format": "true",
        "detect_language": "true"
    }
    
    # Prepare payload with audio URL
    payload = {
        "url": audio_url
    }
    
    try:
        # Make the API request
        start_time = time.time()
        response = requests.post(url, headers=headers, params=params, json=payload, timeout=30)
        end_time = time.time()
        
        # Check for successful response
        if response.status_code == 200:
            print(f"Transcription successful in {end_time - start_time:.2f} seconds")
            return response.json()
        else:
            print(f"Transcription failed with status code: {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"Error during transcription: {str(e)}")
        return None

def save_response_to_file(response, output_file):
    """Save response to file"""
    with open(output_file, 'w') as f:
        json.dump(response, f, indent=2)
    print(f"Response saved to {output_file}")

async def main():
    if len(sys.argv) < 2:
        print("Usage: python test_direct_transcribe.py <blob_name>")
        return
        
    blob_name = sys.argv[1]
    print(f"Testing direct transcription for blob: {blob_name}")
    
    # Generate SAS URL
    sas_url = generate_sas_url(blob_name)
    print(f"Generated SAS URL: {sas_url[:100]}...")
    
    # Transcribe audio
    response = await transcribe_audio_url(sas_url)
    
    if response:
        # Save response to file
        output_dir = "direct_test_results"
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"{blob_name.replace('.mp3', '')}_result.json")
        save_response_to_file(response, output_file)
        
        # Extract and print transcript
        try:
            transcript = response['results']['channels'][0]['alternatives'][0]['transcript']
            print("\nTranscript:")
            print(transcript[:500] + "..." if len(transcript) > 500 else transcript)
        except (KeyError, IndexError) as e:
            print(f"Error extracting transcript: {e}")
            print("Response structure:")
            print(json.dumps(response, indent=2)[:500] + "...")
    
if __name__ == "__main__":
    asyncio.run(main())