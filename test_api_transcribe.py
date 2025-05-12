#!/usr/bin/env python3
"""
Test the direct transcribe API endpoint
This script sends a request to the Flask API to transcribe a file
"""

import requests
import json
import time

# API endpoint
API_ENDPOINT = "http://localhost:5001/direct/transcribe"

# Test data
test_data = {
    "filename": "agricultural_leasing_(ijarah)_normal.mp3",
    "fileid": f"api_test_{int(time.time())}"
}

# Send the request
print(f"Sending transcription request for {test_data['filename']} with ID {test_data['fileid']}")
response = requests.post(API_ENDPOINT, json=test_data)

# Display the response
print(f"Status code: {response.status_code}")

try:
    result = response.json()
    print(f"Success: {result.get('success', False)}")
    
    if result.get('success', False):
        print(f"Transcript length: {result.get('transcript_length', 0)} characters")
        print(f"DB storage success: {result.get('db_storage', {}).get('success', False)}")
        print(f"Paragraphs processed: {result.get('db_storage', {}).get('paragraphs_processed', 0)}")
        
        # Extract sample of transcript
        transcript = result.get('transcript', '')
        print(f"\nTranscript sample: {transcript[:200]}...")
    else:
        print(f"Error: {result.get('error', 'Unknown error')}")
except Exception as e:
    print(f"Error parsing response: {str(e)}")
    print(f"Response text: {response.text[:500]}...")