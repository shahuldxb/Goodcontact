#!/usr/bin/env python3
"""
Test the Deepgram SDK directly to diagnose audio processing issues
"""
import os
import logging
import asyncio
import json
import tempfile
import math
from deepgram import Deepgram

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_valid_wav():
    """Create a valid WAV file with a sine wave tone"""
    try:
        # Directory to store the file
        temp_dir = tempfile.mkdtemp()
        local_path = os.path.join(temp_dir, "test_audio.wav")
        
        logger.info(f"Creating valid WAV file at {local_path}...")
        
        # Generate a basic WAV header (44 bytes) + 1 second of sine wave tone
        with open(local_path, "wb") as f:
            # RIFF header
            f.write(b'RIFF')
            f.write((36 + 1 * 16000 * 2).to_bytes(4, 'little'))  # File size
            f.write(b'WAVE')
            
            # Format chunk
            f.write(b'fmt ')
            f.write((16).to_bytes(4, 'little'))  # Format chunk size
            f.write((1).to_bytes(2, 'little'))   # PCM format
            f.write((1).to_bytes(2, 'little'))   # Mono
            f.write((16000).to_bytes(4, 'little'))  # Sample rate
            f.write((32000).to_bytes(4, 'little'))  # Byte rate
            f.write((2).to_bytes(2, 'little'))   # Block align
            f.write((16).to_bytes(2, 'little'))  # Bits per sample
            
            # Data chunk
            f.write(b'data')
            f.write((1 * 16000 * 2).to_bytes(4, 'little'))  # Data size
            
            # Generate a simple sine wave
            for i in range(16000):  # 1 second at 16kHz
                # Generate sine wave values (amplitude of 16000, frequency of 440Hz)
                value = int(16000 * math.sin(2 * math.pi * 440 * i / 16000))
                f.write(value.to_bytes(2, 'little', signed=True))
        
        file_size = os.path.getsize(local_path)
        logger.info(f"Created WAV file of size {file_size} bytes at {local_path}")
        
        return local_path
    except Exception as e:
        logger.error(f"Error creating valid test audio file: {str(e)}")
        return None

async def test_deepgram_with_sdk():
    """Test Deepgram using the official SDK"""
    try:
        # Get Deepgram API key from environment
        deepgram_api_key = os.environ.get("DEEPGRAM_API_KEY", "d6290865c35bddd50928c5d26983769682fca987")
        if not deepgram_api_key:
            logger.error("DEEPGRAM_API_KEY environment variable is not set!")
            return {"error": "No API key provided"}
        
        # Initialize Deepgram SDK
        deepgram = Deepgram(deepgram_api_key)
        
        # Create a valid test file
        audio_path = create_valid_wav()
        if not audio_path:
            return {"error": "Failed to create test audio file"}
        
        # Read the file
        with open(audio_path, 'rb') as audio:
            source = {'buffer': audio, 'mimetype': 'audio/wav'}
            logger.info(f"Sending file to Deepgram with size {os.path.getsize(audio_path)} bytes")
            
            # Set options
            options = {
                "smart_format": True,
                "model": "nova-2",
                "language": "en",
                "punctuate": True,
                "diarize": True,
                "detect_language": True,
                "summarize": True
            }
            
            try:
                # Send to Deepgram using SDK
                logger.info("Sending request to Deepgram via SDK...")
                response = await deepgram.transcription.prerecorded(source, options)
                
                # Log full response
                logger.info(f"Deepgram SDK response: {json.dumps(response, indent=2)}")
                
                # Check if the response has a transcript
                if 'results' in response and 'channels' in response['results']:
                    for channel in response['results']['channels']:
                        if 'alternatives' in channel and len(channel['alternatives']) > 0:
                            transcript = channel['alternatives'][0].get('transcript', '')
                            logger.info(f"Extracted transcript ({len(transcript)} chars): {transcript[:100]}...")
                            return {"success": True, "transcript": transcript}
                
                return {"success": True, "response": response}
            except Exception as e:
                logger.error(f"Error in Deepgram SDK request: {str(e)}")
                return {"error": str(e)}
    except Exception as e:
        logger.error(f"Error in test: {str(e)}")
        return {"error": str(e)}
    finally:
        # Clean up
        if audio_path and os.path.exists(audio_path):
            os.remove(audio_path)
            logger.info(f"Cleaned up test file: {audio_path}")

# This function uses requests directly instead of the SDK
async def test_deepgram_with_requests():
    """Test Deepgram using direct requests"""
    import requests
    
    try:
        # Get Deepgram API key from environment
        deepgram_api_key = os.environ.get("DEEPGRAM_API_KEY", "d6290865c35bddd50928c5d26983769682fca987")
        if not deepgram_api_key:
            logger.error("DEEPGRAM_API_KEY environment variable is not set!")
            return {"error": "No API key provided"}
        
        # Create a valid test file
        audio_path = create_valid_wav()
        if not audio_path:
            return {"error": "Failed to create test audio file"}
            
        # URL for the Deepgram API
        url = "https://api.deepgram.com/v1/listen"
        
        # Parameters for transcription
        params = {
            "model": "nova-2",
            "smart_format": "true",
            "diarize": "true",
            "punctuate": "true",
            "detect_language": "true",
            "summarize": "true"
        }
        
        # Headers with API key
        headers = {
            "Authorization": f"Token {deepgram_api_key}",
            "Content-Type": "audio/wav"
        }
        
        # Read the audio file
        with open(audio_path, "rb") as f:
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
        logger.error(f"Error in test: {str(e)}")
        return {"error": str(e)}
    finally:
        # Clean up
        if audio_path and os.path.exists(audio_path):
            os.remove(audio_path)
            logger.info(f"Cleaned up test file: {audio_path}")

async def main():
    """Run both tests and compare results"""
    logger.info("Testing Deepgram with SDK...")
    sdk_result = await test_deepgram_with_sdk()
    
    logger.info("\n\nTesting Deepgram with direct requests...")
    requests_result = await test_deepgram_with_requests()
    
    logger.info("\n\nTest Results Summary:")
    logger.info(f"SDK Test: {'SUCCESS' if 'success' in sdk_result else 'FAILED'}")
    logger.info(f"Requests Test: {'SUCCESS' if 'success' in requests_result else 'FAILED'}")
    
    return {
        "sdk_test": sdk_result,
        "requests_test": requests_result
    }

if __name__ == "__main__":
    asyncio.run(main())