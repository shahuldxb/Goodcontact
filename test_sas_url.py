#!/usr/bin/env python3
"""
Test SAS URL generation and transcription
"""

from python_backend.direct_transcribe import DirectTranscribe
from python_backend.azure_storage_service import AzureStorageService
import requests
import json

def main():
    # Create the Azure storage service
    service = AzureStorageService()
    
    # Generate a SAS URL for the blob
    blob_name = "agricultural_finance_(murabaha)_neutral.mp3"
    sas_url = service.generate_sas_url('shahulin', blob_name)
    
    print(f'Generated SAS URL: {sas_url[:60]}...')
    
    # Try to access the URL directly first to check if it's accessible
    try:
        print(f"Testing SAS URL accessibility...")
        response = requests.head(sas_url)
        print(f"SAS URL accessible: {response.status_code == 200}")
        print(f"Status code: {response.status_code}")
        print(f"Headers: {dict(response.headers)}")
    except Exception as e:
        print(f"Error accessing SAS URL: {str(e)}")
    
    # Try to transcribe using the DirectTranscribe class
    try:
        print("\nAttempting transcription with DirectTranscribe...")
        transcriber = DirectTranscribe()
        api_key = 'ba94baf7840441c378c58ccd1d5202c38ddc42d8'
        result = transcriber.transcribe_audio(sas_url, api_key)
        print('Transcription success!')
        print(json.dumps(result, indent=2)[:200] + "...")
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error during transcription: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response text: {e.response.text}")
    except Exception as e:
        print(f"General error during transcription: {str(e)}")

if __name__ == "__main__":
    main()