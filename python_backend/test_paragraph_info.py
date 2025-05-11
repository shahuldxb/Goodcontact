#!/usr/bin/env python3
"""
Test to see if we're getting paragraph info from Deepgram
"""
import os
import json
import logging
from dg_class_critical_transcribe_rest import DgClassCriticalTranscribeRest
from azure_storage_service import AzureStorageService

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main function to test paragraph extraction"""
    try:
        # Get the API key
        api_key = os.environ.get('DEEPGRAM_API_KEY')
        if not api_key:
            print("DEEPGRAM_API_KEY environment variable not set")
            return
        
        # Get a test file from Azure
        storage = AzureStorageService()
        blob_name = 'agricultural_finance_(murabaha)_neutral.mp3'
        sas_url = storage.generate_sas_url('shahulin', blob_name, 240)
        
        # Initialize the transcriber
        transcriber = DgClassCriticalTranscribeRest(api_key)
        
        # Transcribe the file
        print(f"Transcribing {blob_name}...")
        result = transcriber.transcribe_shortcut(audio_url=sas_url, model="nova-2", diarize=True)
        
        # Check if successful
        if not result.get('success', False):
            print(f"Transcription failed: {result.get('error')}")
            return
        
        # Check if there are paragraphs
        paragraphs = result.get('paragraphs', [])
        print(f"Number of paragraphs: {len(paragraphs)}")
        
        if paragraphs:
            print("\nFirst paragraph:")
            print(json.dumps(paragraphs[0], indent=2))
            
            # Check if there are sentences in the first paragraph
            sentences = paragraphs[0].get('sentences', [])
            print(f"\nNumber of sentences in first paragraph: {len(sentences)}")
            
            if sentences:
                print("\nFirst sentence:")
                print(json.dumps(sentences[0], indent=2))
        
        # Save the full response for analysis
        with open('deepgram_response.json', 'w') as f:
            json.dump(result, f, indent=2)
        print("\nFull response saved to deepgram_response.json")
        
    except Exception as e:
        logger.error(f"Error in test: {str(e)}")
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()