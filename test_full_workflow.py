#!/usr/bin/env python3
"""
Comprehensive Test Script for Call Center Workflow
This script tests the full workflow:
1. Download a real blob from Azure Storage
2. Use the shortcut REST API call for transcription (not SDK)
3. Call all analysis routines with the transcription output
"""

import os
import json
import time
import logging
import asyncio
import requests
from datetime import datetime, timedelta

# Azure Storage
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants - Azure Storage
AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=infolder;AccountKey=NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==;EndpointSuffix=core.windows.net"
SOURCE_CONTAINER = "shahulin"
DESTINATION_CONTAINER = "shahulout"

# Constants - Deepgram
DEEPGRAM_API_KEY = "ba94baf7840441c378c58ccd1d5202c38ddc42d8"
DEEPGRAM_API_URL = "https://api.deepgram.com/v1/listen"

# Create output directory
OUTPUT_DIR = "./test_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Helper functions
def connect_to_azure_storage():
    """Connect to Azure Blob Storage"""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        logger.info(f"Successfully connected to Azure Blob Storage: {blob_service_client.account_name}")
        return blob_service_client
    except Exception as e:
        logger.error(f"Error connecting to Azure Blob Storage: {str(e)}")
        return None

def list_blobs_in_container(blob_service_client, container_name, max_results=10):
    """List blobs in the specified container"""
    try:
        container_client = blob_service_client.get_container_client(container_name)
        
        blobs = []
        for blob in container_client.list_blobs():
            if len(blobs) >= max_results:
                break
            if blob.name.lower().endswith(('.mp3', '.wav')):
                blobs.append(blob.name)
        
        return blobs
    except Exception as e:
        logger.error(f"Error listing blobs: {str(e)}")
        return []

def download_blob(blob_service_client, container_name, blob_name, destination_path):
    """Download a blob from Azure Storage"""
    try:
        # Get container client
        container_client = blob_service_client.get_container_client(container_name)
        
        # Get blob client
        blob_client = container_client.get_blob_client(blob_name)
        
        # Check if blob exists
        if not blob_client.exists():
            logger.error(f"Blob {blob_name} does not exist in container {container_name}")
            return False
        
        # Download the blob
        logger.info(f"Downloading blob {blob_name} from container {container_name} to {destination_path}")
        with open(destination_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        
        # Verify download
        if os.path.exists(destination_path):
            file_size = os.path.getsize(destination_path)
            logger.info(f"Successfully downloaded {blob_name} ({file_size} bytes)")
            return True
        else:
            logger.error(f"Failed to download {blob_name}")
            return False
            
    except Exception as e:
        logger.error(f"Error downloading blob: {str(e)}")
        return False

def generate_sas_url(blob_service_client, container_name, blob_name, expiry_hours=240):
    """Generate SAS URL with long expiry time for Deepgram processing"""
    try:
        # Get account information
        account_name = blob_service_client.account_name
        account_key = blob_service_client.credential.account_key
        
        # Calculate expiry time
        expiry = datetime.utcnow() + timedelta(hours=expiry_hours)
        
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
        blob_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
        logger.info(f"Generated SAS URL for {blob_name} with {expiry_hours} hour expiry")
        return blob_url
        
    except Exception as e:
        logger.error(f"Error generating SAS URL: {str(e)}")
        return None

def transcribe_with_rest_api(sas_url):
    """Transcribe audio using Deepgram REST API with SAS URL (shortcut method)"""
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
        "detect_language": True
    }
    
    try:
        logger.info(f"Sending transcription request to Deepgram API with SAS URL")
        start_time = time.time()
        response = requests.post(DEEPGRAM_API_URL, headers=headers, json=payload, timeout=300)
        processing_time = time.time() - start_time
        
        logger.info(f"Deepgram API response status: {response.status_code}")
        logger.info(f"Processing time: {processing_time:.2f} seconds")
        
        if response.status_code == 200:
            result = response.json()
            
            # Extract transcript to verify content
            transcript = ""
            if "results" in result and "channels" in result["results"]:
                channels = result["results"]["channels"]
                if channels and "alternatives" in channels[0]:
                    alternatives = channels[0]["alternatives"]
                    if alternatives and "transcript" in alternatives[0]:
                        transcript = alternatives[0]["transcript"]
            
            logger.info(f"Transcript length: {len(transcript)} characters")
            if transcript:
                logger.info(f"Transcript preview: {transcript[:100]}...")
            else:
                logger.warning("No transcript content found in response")
            
            return {
                "success": True,
                "result": result,
                "transcript": transcript,
                "processing_time": processing_time
            }
        else:
            logger.error(f"Deepgram API error: {response.text}")
            return {
                "success": False,
                "error": response.text,
                "status_code": response.status_code
            }
            
    except Exception as e:
        logger.error(f"Error during transcription: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

def transcribe_with_local_file(file_path):
    """Transcribe audio using Deepgram REST API with local file upload"""
    headers = {
        "Authorization": f"Token {DEEPGRAM_API_KEY}"
    }
    
    try:
        logger.info(f"Reading audio file: {file_path}")
        with open(file_path, 'rb') as audio:
            # Get content type based on file extension
            if file_path.lower().endswith('.mp3'):
                content_type = 'audio/mpeg'
            elif file_path.lower().endswith('.wav'):
                content_type = 'audio/wav'
            else:
                content_type = 'application/octet-stream'
            
            # Set up multipart form data
            files = {
                'file': (os.path.basename(file_path), audio, content_type)
            }
            
            # Set up parameters
            data = {
                'model': 'nova-2',
                'language': 'en-US',
                'diarize': 'true',
                'punctuate': 'true',
                'paragraphs': 'true',
                'utterances': 'true',
                'detect_language': 'true'
            }
            
            logger.info(f"Sending transcription request to Deepgram API with local file")
            start_time = time.time()
            response = requests.post(DEEPGRAM_API_URL, headers=headers, data=data, files=files, timeout=300)
            processing_time = time.time() - start_time
            
            logger.info(f"Deepgram API response status: {response.status_code}")
            logger.info(f"Processing time: {processing_time:.2f} seconds")
            
            if response.status_code == 200:
                result = response.json()
                
                # Extract transcript to verify content
                transcript = ""
                if "results" in result and "channels" in result["results"]:
                    channels = result["results"]["channels"]
                    if channels and "alternatives" in channels[0]:
                        alternatives = channels[0]["alternatives"]
                        if alternatives and "transcript" in alternatives[0]:
                            transcript = alternatives[0]["transcript"]
                
                logger.info(f"Transcript length: {len(transcript)} characters")
                if transcript:
                    logger.info(f"Transcript preview: {transcript[:100]}...")
                else:
                    logger.warning("No transcript content found in response")
                
                return {
                    "success": True,
                    "result": result,
                    "transcript": transcript,
                    "processing_time": processing_time
                }
            else:
                logger.error(f"Deepgram API error: {response.text}")
                return {
                    "success": False,
                    "error": response.text,
                    "status_code": response.status_code
                }
                
    except Exception as e:
        logger.error(f"Error during transcription: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }

def perform_sentiment_analysis(transcription_data):
    """Perform sentiment analysis on transcription data"""
    try:
        logger.info("Performing sentiment analysis")
        
        # Extract transcript text
        transcript = ""
        if "results" in transcription_data and "channels" in transcription_data["results"]:
            channels = transcription_data["results"]["channels"]
            if channels and "alternatives" in channels[0]:
                alternatives = channels[0]["alternatives"]
                if alternatives and "transcript" in alternatives[0]:
                    transcript = alternatives[0]["transcript"]
        
        if not transcript:
            logger.warning("No transcript text available for sentiment analysis")
            return {
                "status": "error",
                "message": "No transcript text available for sentiment analysis"
            }
        
        # Simple sentiment analysis logic (in real system, would use a more sophisticated approach)
        # This is just a placeholder for testing the workflow
        positive_words = ["good", "great", "excellent", "happy", "awesome", "amazing", "love", "thank", "appreciate"]
        negative_words = ["bad", "terrible", "awful", "sad", "angry", "hate", "dislike", "problem", "issue", "error"]
        
        # Count occurrences
        positive_count = sum(transcript.lower().count(word) for word in positive_words)
        negative_count = sum(transcript.lower().count(word) for word in negative_words)
        
        # Determine sentiment
        total = positive_count + negative_count
        if total == 0:
            sentiment = "neutral"
            score = 0.5
        elif positive_count > negative_count:
            sentiment = "positive"
            score = 0.5 + (positive_count - negative_count) / (2 * total)
        else:
            sentiment = "negative"
            score = 0.5 - (negative_count - positive_count) / (2 * total)
        
        logger.info(f"Sentiment analysis complete: {sentiment} (score: {score:.2f})")
        return {
            "status": "success",
            "sentiment": sentiment,
            "score": score,
            "details": {
                "positive_count": positive_count,
                "negative_count": negative_count,
                "transcript_length": len(transcript)
            }
        }
        
    except Exception as e:
        logger.error(f"Error performing sentiment analysis: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

def perform_language_detection(transcription_data):
    """Detect language from transcription data"""
    try:
        logger.info("Performing language detection")
        
        # Extract language from Deepgram response
        detected_language = None
        
        # Check in metadata
        if "metadata" in transcription_data and "detected_language" in transcription_data["metadata"]:
            detected_language = transcription_data["metadata"]["detected_language"]
        
        # Check in results
        if not detected_language and "results" in transcription_data and "language" in transcription_data["results"]:
            detected_language = transcription_data["results"]["language"]
        
        if not detected_language:
            logger.warning("No language information found in transcription data")
            return {
                "status": "error",
                "message": "No language information found in transcription data"
            }
        
        # Map language code to language name (simplified for testing)
        language_map = {
            "en": "English",
            "es": "Spanish",
            "fr": "French",
            "de": "German",
            "it": "Italian",
            "ar": "Arabic"
        }
        
        language_name = language_map.get(detected_language, detected_language)
        
        logger.info(f"Language detection complete: {language_name} ({detected_language})")
        return {
            "status": "success",
            "language_code": detected_language,
            "language_name": language_name,
            "confidence": 0.95  # Placeholder, would be extracted from real service
        }
        
    except Exception as e:
        logger.error(f"Error performing language detection: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

# Main execution
def main():
    """Main execution function"""
    try:
        logger.info("Starting full workflow test")
        
        # Step 1: Connect to Azure Storage
        blob_service_client = connect_to_azure_storage()
        if not blob_service_client:
            logger.error("Failed to connect to Azure Storage, exiting")
            return
        
        # Step 2: List blobs in source container
        audio_files = list_blobs_in_container(blob_service_client, SOURCE_CONTAINER)
        if not audio_files:
            logger.error(f"No audio files found in container {SOURCE_CONTAINER}, exiting")
            return
        
        logger.info(f"Found {len(audio_files)} audio files in container {SOURCE_CONTAINER}")
        
        # Step 3: Select a test file (first in the list or specific file)
        test_file = next((f for f in audio_files if f.endswith('normal.mp3')), audio_files[0])
        logger.info(f"Selected test file: {test_file}")
        
        # Step 4: Download the blob
        local_file_path = os.path.join(OUTPUT_DIR, test_file)
        download_success = download_blob(blob_service_client, SOURCE_CONTAINER, test_file, local_file_path)
        
        if not download_success:
            logger.error(f"Failed to download {test_file}, exiting")
            return
        
        # Step 5: Generate SAS URL for the blob
        sas_url = generate_sas_url(blob_service_client, SOURCE_CONTAINER, test_file)
        if not sas_url:
            logger.error(f"Failed to generate SAS URL for {test_file}, exiting")
            return
        
        # Step 6: Transcribe using REST API with SAS URL (shortcut method)
        logger.info("===== TESTING TRANSCRIPTION WITH SAS URL (SHORTCUT METHOD) =====")
        sas_url_result = transcribe_with_rest_api(sas_url)
        
        if not sas_url_result.get("success"):
            logger.error(f"SAS URL transcription failed: {sas_url_result.get('error')}")
            
            # Try with local file as fallback
            logger.info("Trying transcription with local file as fallback")
            transcription_result = transcribe_with_local_file(local_file_path)
        else:
            transcription_result = sas_url_result
            
        # Step 7: Verify transcription result
        if not transcription_result.get("success"):
            logger.error(f"All transcription methods failed, exiting")
            return
            
        # Save transcription result to file
        transcription_output_path = os.path.join(OUTPUT_DIR, f"{test_file}_transcription.json")
        with open(transcription_output_path, "w") as f:
            json.dump(transcription_result.get("result"), f, indent=2)
        logger.info(f"Saved transcription result to {transcription_output_path}")
        
        # Step 8: Run sentiment analysis
        logger.info("===== TESTING SENTIMENT ANALYSIS =====")
        sentiment_result = perform_sentiment_analysis(transcription_result.get("result"))
        
        # Save sentiment result to file
        sentiment_output_path = os.path.join(OUTPUT_DIR, f"{test_file}_sentiment.json")
        with open(sentiment_output_path, "w") as f:
            json.dump(sentiment_result, f, indent=2)
        logger.info(f"Saved sentiment analysis result to {sentiment_output_path}")
        
        # Step 9: Run language detection
        logger.info("===== TESTING LANGUAGE DETECTION =====")
        language_result = perform_language_detection(transcription_result.get("result"))
        
        # Save language result to file
        language_output_path = os.path.join(OUTPUT_DIR, f"{test_file}_language.json")
        with open(language_output_path, "w") as f:
            json.dump(language_result, f, indent=2)
        logger.info(f"Saved language detection result to {language_output_path}")
        
        # Step 10: Summarize results
        logger.info("\n===== TEST RESULTS SUMMARY =====")
        logger.info(f"Test file: {test_file}")
        logger.info(f"Transcription: {'SUCCESS' if transcription_result.get('success') else 'FAILED'}")
        logger.info(f"Transcript length: {len(transcription_result.get('transcript', ''))}")
        logger.info(f"Sentiment analysis: {sentiment_result.get('status').upper()} - {sentiment_result.get('sentiment', 'N/A')}")
        logger.info(f"Language detection: {language_result.get('status').upper()} - {language_result.get('language_name', 'N/A')}")
        
        # Success message
        logger.info("\n===== TEST COMPLETED SUCCESSFULLY =====")
        logger.info(f"All test outputs saved to {OUTPUT_DIR}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")

if __name__ == "__main__":
    main()