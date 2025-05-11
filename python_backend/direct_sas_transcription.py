#!/usr/bin/env python3
"""
Direct SAS URL Transcription

This script directly transcribes audio files from Azure Blob Storage using SAS URLs
without downloading them first, as per user requirements.
"""

import os
import json
import logging
import requests
import uuid
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables
DEEPGRAM_API_KEY = os.environ.get('DEEPGRAM_API_KEY', 'ba94baf7840441c378c58ccd1d5202c38ddc42d8')

def transcribe_from_sas_url(blob_sas_url, api_key=DEEPGRAM_API_KEY, model="nova-2", diarize=True):
    """
    Transcribe audio directly from Azure Blob Storage SAS URL without downloading
    
    Args:
        blob_sas_url: SAS URL for the blob
        api_key: Deepgram API key
        model: Deepgram model to use
        diarize: Whether to enable speaker diarization
        
    Returns:
        dict: Result of the transcription
    """
    try:
        logger.info(f"Transcribing audio from SAS URL (URL not logged for security)")
        
        # Construct the request URL
        url = "https://api.deepgram.com/v1/listen"
        
        # Prepare parameters
        params = {
            "model": model,
            "diarize": "true" if diarize else "false",
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
        
        # Check if we have valid results
        if 'results' not in result:
            logger.error("No results in Deepgram response")
            return {
                'success': False,
                'error': 'No results in Deepgram response',
                'response_data': result
            }
        
        # Extract the basic transcript
        basic_transcript = ""
        if 'results' in result and 'channels' in result['results'] and len(result['results']['channels']) > 0:
            if 'alternatives' in result['results']['channels'][0] and len(result['results']['channels'][0]['alternatives']) > 0:
                basic_transcript = result['results']['channels'][0]['alternatives'][0].get('transcript', '')
        
        # Extract speaker transcript if diarization is enabled
        speaker_transcript = ""
        if diarize and 'results' in result and 'utterances' in result['results']:
            utterances = result['results']['utterances']
            speaker_segments = []
            
            for utterance in utterances:
                speaker = utterance.get('speaker', 'unknown')
                text = utterance.get('transcript', '')
                speaker_segments.append(f"Speaker {speaker}: {text}")
            
            speaker_transcript = "\n".join(speaker_segments)
        
        # Save the response to a file for debugging and reference
        filename = f"deepgram_response_{uuid.uuid4().hex[:16]}.json"
        with open(filename, 'w') as f:
            json.dump(result, f, indent=2)
        logger.info(f"Saved Deepgram response to {filename}")
        
        return {
            'success': True,
            'basic_transcript': basic_transcript,
            'speaker_transcript': speaker_transcript,
            'response_data': result,
            'response_file': filename
        }
    except Exception as e:
        logger.error(f"Error transcribing audio from SAS URL: {str(e)}")
        return {
            'success': False,
            'error': f"Error: {str(e)}"
        }


def main():
    """
    Example usage of the direct SAS URL transcription function
    """
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python direct_sas_transcription.py <blob_sas_url>")
        sys.exit(1)
    
    blob_sas_url = sys.argv[1]
    result = transcribe_from_sas_url(blob_sas_url)
    
    if result['success']:
        print("\n=== TRANSCRIPTION SUCCESSFUL ===")
        print("\nBasic Transcript:")
        print(result['basic_transcript'])
        
        print("\nSpeaker Transcript:")
        print(result['speaker_transcript'])
        
        print(f"\nFull response saved to: {result.get('response_file', 'Not saved')}")
    else:
        print("\n=== TRANSCRIPTION FAILED ===")
        print(f"Error: {result.get('error', 'Unknown error')}")

    
if __name__ == "__main__":
    main()