#!/usr/bin/env python3
"""
Deepgram Response Structure Debug Script
This script will transcribe a test file and save the full response structure
to help us understand how paragraphs and sentences are formatted
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO,
                  format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add python_backend to path if needed
current_dir = os.path.dirname(os.path.abspath(__file__))
python_backend_dir = os.path.join(current_dir, 'python_backend')
if os.path.exists(python_backend_dir):
    sys.path.append(python_backend_dir)

try:
    from python_backend.direct_transcribe import DirectTranscribe
except ImportError:
    try:
        from direct_transcribe import DirectTranscribe
    except ImportError:
        print("ERROR: Could not import DirectTranscribe")
        sys.exit(1)

def generate_sas_url(blob_name, container_name="shahulin", expiry_hours=24):
    """Generate SAS URL for testing"""
    from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
    
    # Azure Storage connection string
    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", 
                                      "DefaultEndpointsProtocol=https;AccountName=infolder;AccountKey=NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==;EndpointSuffix=core.windows.net")
    
    # Extract account info from connection string
    conn_parts = {p.split('=')[0]: p.split('=', 1)[1] for p in connection_string.split(';') if '=' in p}
    account_name = conn_parts.get('AccountName')
    account_key = conn_parts.get('AccountKey')
    
    # Verify the blob exists
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    
    if not blob_client.exists():
        logger.error(f"Blob {blob_name} does not exist in container {container_name}")
        return None
    
    # Calculate expiry time
    expiry = datetime.now() + timedelta(hours=expiry_hours)
    
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
    sas_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
    logger.info(f"Generated SAS URL for {blob_name} with {expiry_hours} hour expiry")
    
    return sas_url

def save_response_to_file(response, filename):
    """Save Deepgram response to file with proper indentation"""
    with open(filename, 'w') as f:
        json.dump(response, f, indent=2)
    logger.info(f"Response saved to {filename}")

def analyze_response_structure(response):
    """Analyze the response structure to understand paragraphs and sentences"""
    logger.info("Analyzing response structure:")
    
    # Check for results key
    if "results" not in response:
        logger.warning("No 'results' key found in response")
        return
        
    results = response["results"]
    
    # Check for channels
    if "channels" in results:
        channels = results["channels"]
        logger.info(f"Found {len(channels)} channels")
        
        for i, channel in enumerate(channels):
            logger.info(f"Channel {i}:")
            
            if "alternatives" in channel:
                alternatives = channel["alternatives"]
                logger.info(f"Found {len(alternatives)} alternatives in channel {i}")
                
                for j, alt in enumerate(alternatives):
                    logger.info(f"Alternative {j}:")
                    
                    # Check for paragraphs
                    if "paragraphs" in alt:
                        paragraphs_container = alt["paragraphs"]
                        
                        if "paragraphs" in paragraphs_container:
                            paragraphs = paragraphs_container["paragraphs"]
                            logger.info(f"Found {len(paragraphs)} paragraphs in alternative {j}")
                            
                            # Look at first paragraph
                            if paragraphs:
                                first_para = paragraphs[0]
                                logger.info(f"First paragraph text: {first_para.get('text', '')[:100]}...")
                                
                                # Check for sentences
                                if "sentences" in first_para:
                                    sentences = first_para["sentences"]
                                    logger.info(f"Found {len(sentences)} sentences in first paragraph")
                                    
                                    # Look at first sentence
                                    if sentences:
                                        logger.info(f"First sentence text: {sentences[0].get('text', '')}")
                                else:
                                    logger.warning("No 'sentences' found in first paragraph")
                        else:
                            logger.warning("No 'paragraphs' array found in 'paragraphs' container")
                    else:
                        logger.warning("No 'paragraphs' found in alternative")
    
    # Check for paragraphs directly in results
    if "paragraphs" in results:
        paragraphs_container = results["paragraphs"]
        
        if "paragraphs" in paragraphs_container:
            paragraphs = paragraphs_container["paragraphs"]
            logger.info(f"Found {len(paragraphs)} paragraphs directly in results")
    
    # Check for utterances
    if "utterances" in results:
        utterances = results["utterances"]
        logger.info(f"Found {len(utterances)} utterances in results")

def main():
    """Main function"""
    # Get the Deepgram API key from environment
    deepgram_api_key = os.environ.get("DEEPGRAM_API_KEY", "ba94baf7840441c378c58ccd1d5202c38ddc42d8")
    
    # Initialize the transcriber
    transcriber = DirectTranscribe(deepgram_api_key)
    
    # Test file to transcribe
    test_file = "agricultural_leasing_(ijarah)_normal.mp3"
    
    # Generate SAS URL for the test file
    sas_url = generate_sas_url(test_file)
    
    if not sas_url:
        logger.error("Failed to generate SAS URL")
        return
    
    # Transcribe the audio with explicit parameters
    logger.info("Transcribing audio with paragraphs and sentences...")
    result = transcriber.transcribe_audio(
        sas_url, 
        model="nova-3",
        paragraphs=True,
        punctuate=True,
        smart_format=True,
        diarize=True,
        utterances=True,
        detect_language=True
    )
    
    if not result["success"]:
        logger.error(f"Transcription failed: {result.get('error', {}).get('message', 'Unknown error')}")
        return
    
    # Save the full response to a file
    output_file = "deepgram_response_full.json"
    save_response_to_file(result["result"], output_file)
    
    # Analyze the response structure
    analyze_response_structure(result["result"])
    
    logger.info("Done. Check the output file for the full response structure.")

if __name__ == "__main__":
    main()