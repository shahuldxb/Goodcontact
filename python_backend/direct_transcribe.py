#!/usr/bin/env python3
"""
Direct SAS URL Transcription Class

This class directly transcribes audio files from Azure Blob Storage using SAS URLs
without downloading them first, as per user requirements.
"""

import json
import logging
import requests
from datetime import datetime, timedelta
from azure.storage.blob import BlobSasPermissions, generate_blob_sas
from azure.storage.blob import BlobServiceClient
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DirectTranscribe:
    def __init__(self):
        """Initialize the DirectTranscribe class"""
        self.logger = logging.getLogger(__name__)
    
    def generate_blob_sas_url(self, connection_string, container_name, blob_name, expiry_hours=240):
        """
        Generate a SAS URL for a blob with specified expiry time
        
        Args:
            connection_string: Azure Storage connection string
            container_name: Container name
            blob_name: Blob name
            expiry_hours: Expiry time in hours (default: 240 hours = 10 days)
            
        Returns:
            str: SAS URL for the blob
        """
        try:
            # Create a BlobServiceClient
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            
            # Get account information from the connection string
            account_name = blob_service_client.account_name
            account_key = None
            
            # Extract account key from connection string
            parts = connection_string.split(';')
            for part in parts:
                if part.startswith('AccountKey='):
                    account_key = part.split('=', 1)[1]
                    break
            
            if not account_key:
                raise ValueError("Account key not found in connection string")
            
            # Generate SAS token
            sas_token = generate_blob_sas(
                account_name=account_name,
                container_name=container_name,
                blob_name=blob_name,
                account_key=account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(hours=expiry_hours)
            )
            
            # Construct the full SAS URL
            sas_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
            
            return sas_url
        except Exception as e:
            self.logger.error(f"Error generating blob SAS URL: {str(e)}")
            raise
    
    def transcribe_audio(self, blob_sas_url, api_key, model="nova-2"):
        """
        Transcribe audio directly from Azure Blob Storage SAS URL without downloading
        
        Args:
            blob_sas_url: SAS URL for the blob
            api_key: Deepgram API key
            model: Deepgram model to use (default: nova-2)
            
        Returns:
            dict: Raw Deepgram response
        """
        try:
            self.logger.info(f"Transcribing audio from SAS URL (URL not logged for security)")
            
            # Construct the request URL
            url = "https://api.deepgram.com/v1/listen"
            
            # Prepare parameters
            params = {
                "model": model,
                "diarize": "true",
                "punctuate": "true",
                "utterances": "true",
                "paragraphs": "true",
                "filler_words": "true",
                "detect_language": "true",
                "smart_format": "true"
            }
            
            # Prepare headers
            headers = {
                "Authorization": f"Token {api_key}",
                "Content-Type": "application/json"
            }
            
            # Prepare the JSON payload with the SAS URL
            payload = {
                "url": blob_sas_url
            }
            
            # Send the request with the SAS URL
            response = requests.post(url, params=params, headers=headers, json=payload)
            
            # Check if the request was successful
            response.raise_for_status()
            
            # Parse the response
            result = response.json()
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error transcribing audio from SAS URL: {str(e)}")
            raise

# Simple test if run directly
if __name__ == "__main__":
    # Test the class with sample data
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python direct_transcribe.py <blob_sas_url> [api_key]")
        sys.exit(1)
    
    blob_sas_url = sys.argv[1]
    api_key = sys.argv[2] if len(sys.argv) > 2 else os.environ.get('DEEPGRAM_API_KEY', 'your_default_api_key')
    
    transcriber = DirectTranscribe()
    try:
        result = transcriber.transcribe_audio(blob_sas_url, api_key)
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error: {str(e)}")