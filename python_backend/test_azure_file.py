#!/usr/bin/env python3
"""
Test script to download a file from Azure and process it with Deepgram
"""
import os
import asyncio
import json
import logging
import tempfile
from azure_storage_service import AzureStorageService
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("azure_file_test")

async def test_process_azure_file(blob_name="m2.wav"):
    """Download and test a specific file from Azure blob storage"""
    try:
        # Initialize Azure Storage Service
        azure_service = AzureStorageService()
        
        # Create a temporary directory
        temp_dir = tempfile.mkdtemp()
        local_path = os.path.join(temp_dir, blob_name)
        
        # Download the blob
        logger.info(f"Downloading {blob_name} from Azure blob storage...")
        azure_service.download_blob(blob_name, local_path)
        
        # Verify the file
        if not os.path.exists(local_path):
            logger.error(f"Failed to download {blob_name}")
            return {"error": "File download failed"}
            
        file_size = os.path.getsize(local_path)
        logger.info(f"Downloaded {blob_name} to {local_path} ({file_size} bytes)")
        
        # Check file format
        with open(local_path, "rb") as f:
            header = f.read(12)  # Read first 12 bytes
            logger.info(f"File header: {header.hex()}")
            
            # Verify WAV header (should start with "RIFF" and contain "WAVE")
            if header[:4] != b'RIFF' or header[8:12] != b'WAVE':
                logger.warning(f"File does not appear to be a valid WAV file")
            
        # Get Deepgram API key
        deepgram_api_key = os.environ.get("DEEPGRAM_API_KEY")
        if not deepgram_api_key:
            logger.error("DEEPGRAM_API_KEY environment variable is not set!")
            return {"error": "Missing Deepgram API key"}
        
        # Set up Deepgram API request
        url = "https://api.deepgram.com/v1/listen"
        headers = {
            "Authorization": f"Token {deepgram_api_key}",
            "Content-Type": "audio/wav"
        }
        params = {
            "model": "nova-2",
            "smart_format": "true",
            "diarize": "true",
            "punctuate": "true", 
            "detect_language": "true",
            "summarize": "true"
        }
        
        # Read the file and send to Deepgram
        with open(local_path, "rb") as f:
            audio_data = f.read()
            logger.info(f"Sending {len(audio_data)} bytes to Deepgram API...")
            
            # Make the request
            response = requests.post(url, headers=headers, params=params, data=audio_data)
            
            # Check the response
            logger.info(f"Deepgram API response status: {response.status_code}")
            
            if response.status_code == 200:
                response_json = response.json()
                logger.info(f"Deepgram API response: {json.dumps(response_json, indent=2)}")
                
                # Test for transcript in the response
                transcript = ""
                if "results" in response_json and "channels" in response_json["results"]:
                    for channel in response_json["results"]["channels"]:
                        if "alternatives" in channel and len(channel["alternatives"]) > 0:
                            if "transcript" in channel["alternatives"][0]:
                                transcript += channel["alternatives"][0]["transcript"]
                
                logger.info(f"Extracted transcript ({len(transcript)} chars): {transcript[:100]}...")
                return {"success": True, "response": response_json, "transcript_length": len(transcript)}
            else:
                error_text = response.text
                logger.error(f"Deepgram API error: {response.status_code}, {error_text}")
                return {"error": error_text}
    
    except Exception as e:
        logger.error(f"Error processing Azure file: {str(e)}", exc_info=True)
        return {"error": str(e)}
    finally:
        # Clean up
        if os.path.exists(local_path):
            os.remove(local_path)
            logger.info(f"Removed temporary file {local_path}")

# Main function
async def main():
    """Main function"""
    logger.info("Testing processing of Azure file")
    result = await test_process_azure_file()
    logger.info("Test complete")
    return result

# Run the test script
if __name__ == "__main__":
    asyncio.run(main())