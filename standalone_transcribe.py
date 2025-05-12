#!/usr/bin/env python3
"""
Standalone Transcription Script for Azure Storage Files

This script:
1. Lists files in the Azure Storage container
2. Generates SAS URLs for each file
3. Sends the SAS URL to Deepgram for transcription
4. Saves the transcription result to a JSON file
5. Moves the processed file to the destination container

No database connection is required.
"""

import os
import sys
import json
import time
import requests
import logging
import uuid
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables
DEEPGRAM_API_KEY = os.environ.get("DEEPGRAM_API_KEY", "ba94baf7840441c378c58ccd1d5202c38ddc42d8")
STORAGE_CONN_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", 
                                 "DefaultEndpointsProtocol=https;AccountName=infolder;AccountKey=NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==;EndpointSuffix=core.windows.net")
SOURCE_CONTAINER = os.environ.get("AZURE_SOURCE_CONTAINER", "shahulin")
DESTINATION_CONTAINER = os.environ.get("AZURE_DESTINATION_CONTAINER", "shahulout")
AZURE_STORAGE_ACCOUNT = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME", "infolder")
AZURE_STORAGE_KEY = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY", "NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==")

def generate_sas_url(blob_name, container_name=SOURCE_CONTAINER, expiry_hours=24):
    """Generate a SAS URL for the blob"""
    try:
        # Generate a SAS token
        sas_token = generate_blob_sas(
            account_name=AZURE_STORAGE_ACCOUNT,
            account_key=AZURE_STORAGE_KEY,
            container_name=container_name,
            blob_name=blob_name,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=expiry_hours)
        )
        
        # Construct the full URL
        url = f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
        logger.info(f"Generated SAS URL for {blob_name} with {expiry_hours} hour expiry")
        return url
    except Exception as e:
        logger.error(f"Error generating SAS URL: {str(e)}")
        return None

def list_blobs(container_name=SOURCE_CONTAINER, max_results=None):
    """List blobs in the specified container"""
    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONN_STRING)
        container_client = blob_service_client.get_container_client(container_name)
        
        # List blobs
        blobs = list(container_client.list_blobs())
        
        # Limit results if requested
        if max_results:
            blobs = blobs[:max_results]
        
        logger.info(f"Found {len(blobs)} blobs in container {container_name}")
        return [blob.name for blob in blobs]
    except Exception as e:
        logger.error(f"Error listing blobs: {str(e)}")
        return []

def get_blob_properties(blob_name, container_name=SOURCE_CONTAINER):
    """Get the properties of a blob"""
    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONN_STRING)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        # Get properties
        properties = blob_client.get_blob_properties()
        return properties
    except Exception as e:
        logger.error(f"Error getting blob properties: {str(e)}")
        return None

def transcribe_with_deepgram(audio_url):
    """Transcribe audio using Deepgram API"""
    try:
        headers = {
            "Authorization": f"Token {DEEPGRAM_API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "url": audio_url,
            "model": "nova-2",
            "diarize": True,
            "punctuate": True,
            "smart_format": True,
            "utterances": True,
            "detect_language": True
        }
        
        logger.info(f"Sending request to Deepgram API")
        response = requests.post(
            "https://api.deepgram.com/v1/listen", 
            json=payload,
            headers=headers
        )
        
        if response.status_code == 200:
            logger.info("Transcription successful")
            return response.json()
        else:
            logger.error(f"Error from Deepgram API: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Error transcribing with Deepgram: {str(e)}")
        return None

def extract_transcript(result):
    """Extract transcript from Deepgram API response"""
    try:
        if not result or "results" not in result:
            return ""
        
        # Extract simple transcript first
        transcript = result["results"]["channels"][0]["alternatives"][0]["transcript"]
        
        # Check for utterances (preferred format with speaker info)
        if "utterances" in result["results"]:
            utterances = result["results"]["utterances"]
            formatted_transcript = ""
            
            for utterance in utterances:
                speaker = utterance.get("speaker", 0)
                text = utterance.get("transcript", "")
                formatted_transcript += f"Speaker {speaker}: {text}\n\n"
            
            return formatted_transcript.strip() if formatted_transcript else transcript
        
        # If no utterances, check for paragraphs
        elif "paragraphs" in result["results"] and "paragraphs" in result["results"]["paragraphs"]:
            paragraphs = result["results"]["paragraphs"]["paragraphs"]
            formatted_transcript = ""
            current_speaker = None
            
            for paragraph in paragraphs:
                if "speaker" in paragraph:
                    speaker_num = paragraph.get("speaker", 0)
                    
                    if current_speaker != speaker_num:
                        current_speaker = speaker_num
                        if formatted_transcript:
                            formatted_transcript += "\n\n"
                        formatted_transcript += f"Speaker {speaker_num}: "
                    
                    if "text" in paragraph:
                        formatted_transcript += paragraph["text"] + " "
            
            return formatted_transcript.strip() if formatted_transcript else transcript
        
        return transcript
    except Exception as e:
        logger.error(f"Error extracting transcript: {str(e)}")
        return ""

def move_blob(blob_name, source_container=SOURCE_CONTAINER, destination_container=DESTINATION_CONTAINER):
    """Move a blob from source to destination container"""
    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONN_STRING)
        source_blob_client = blob_service_client.get_blob_client(container=source_container, blob=blob_name)
        
        # Create a destination blob client
        destination_blob_client = blob_service_client.get_blob_client(container=destination_container, blob=blob_name)
        
        # Start copy from source
        source_url = source_blob_client.url + "?" + generate_blob_sas(
            account_name=AZURE_STORAGE_ACCOUNT,
            account_key=AZURE_STORAGE_KEY,
            container_name=source_container,
            blob_name=blob_name,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=1)
        )
        
        destination_blob_client.start_copy_from_url(source_url)
        
        # Delete source blob after successful copy
        source_blob_client.delete_blob()
        
        logger.info(f"Successfully moved {blob_name} from {source_container} to {destination_container}")
        return True
    except Exception as e:
        logger.error(f"Error moving blob: {str(e)}")
        return False

def save_result_to_file(blob_name, result, transcript, output_dir="./transcription_results"):
    """Save the transcription result to a JSON file"""
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Create unique filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{os.path.splitext(blob_name)[0]}_{timestamp}.json"
        output_path = os.path.join(output_dir, filename)
        
        # Create summary data
        summary = {
            "blob_name": blob_name,
            "transcription_time": timestamp,
            "transcript": transcript,
            "full_result": result
        }
        
        # Write to file
        with open(output_path, "w") as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Saved transcription result to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Error saving result to file: {str(e)}")
        return None

def process_blob(blob_name):
    """Process a single blob"""
    try:
        # Generate a unique file ID
        fileid = f"standalone_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        
        # Start timing
        start_time = time.time()
        
        # Get blob properties
        properties = get_blob_properties(blob_name)
        file_size = properties.size if properties else 0
        
        logger.info(f"Processing blob: {blob_name} (size: {file_size} bytes)")
        
        # Generate SAS URL
        sas_url = generate_sas_url(blob_name)
        if not sas_url:
            return False, f"Failed to generate SAS URL for {blob_name}"
        
        # Transcribe audio
        transcription_result = transcribe_with_deepgram(sas_url)
        if not transcription_result:
            return False, f"Failed to transcribe {blob_name}"
        
        # Extract transcript
        transcript = extract_transcript(transcription_result)
        if not transcript:
            return False, f"Failed to extract transcript from {blob_name}"
        
        # Save result to file
        output_file = save_result_to_file(blob_name, transcription_result, transcript)
        if not output_file:
            return False, f"Failed to save transcription result for {blob_name}"
        
        # Move blob to destination container
        moved = move_blob(blob_name)
        if not moved:
            logger.warning(f"Failed to move {blob_name} to destination container")
        
        # Calculate processing time
        end_time = time.time()
        processing_time = end_time - start_time
        
        logger.info(f"Successfully processed {blob_name} in {processing_time:.2f} seconds")
        return True, {
            "fileid": fileid,
            "blob_name": blob_name,
            "processing_time": processing_time,
            "transcript_length": len(transcript),
            "output_file": output_file,
            "moved": moved
        }
    except Exception as e:
        logger.error(f"Error processing blob: {str(e)}")
        return False, f"Error processing {blob_name}: {str(e)}"

def main(max_files=None):
    """Main function"""
    try:
        # List blobs in source container
        blobs = list_blobs(max_results=max_files)
        if not blobs:
            logger.info("No blobs found in source container")
            return
        
        # Process each blob
        results = []
        for blob_name in blobs:
            success, result = process_blob(blob_name)
            if success:
                results.append(result)
            else:
                logger.error(f"Failed to process {blob_name}: {result}")
        
        # Print summary
        logger.info(f"Processed {len(results)} out of {len(blobs)} blobs")
        for result in results:
            if isinstance(result, dict):
                logger.info(f"Blob: {result['blob_name']}, Processing time: {result['processing_time']:.2f}s, Transcript length: {result['transcript_length']}")
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")

if __name__ == "__main__":
    # Parse command line arguments
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        max_files = int(sys.argv[1])
        logger.info(f"Processing up to {max_files} files")
        main(max_files=max_files)
    else:
        logger.info("Processing all files")
        main()