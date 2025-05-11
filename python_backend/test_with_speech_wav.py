#!/usr/bin/env python3
"""
Test Deepgram transcription with our speech-like WAV file
"""
import os
import logging
import json
import asyncio
import requests
from create_speech_wav import create_speech_wav
from deepgram import Deepgram

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_deepgram_with_speech():
    """Test Deepgram with our speech-like audio file"""
    try:
        # Get Deepgram API key
        deepgram_api_key = os.environ.get("DEEPGRAM_API_KEY", "d6290865c35bddd50928c5d26983769682fca987")
        if not deepgram_api_key:
            logger.error("DEEPGRAM_API_KEY environment variable is not set!")
            return {"error": "Missing API key"}
            
        # Create speech-like WAV file
        audio_path = create_speech_wav()
        if not audio_path:
            logger.error("Failed to create speech WAV file")
            return {"error": "Failed to create audio file"}
        
        try:
            # Initialize Deepgram SDK
            deepgram = Deepgram(deepgram_api_key)
            logger.info(f"Testing file {audio_path} with Deepgram SDK...")
            
            # Open the audio file
            with open(audio_path, 'rb') as audio:
                source = {'buffer': audio, 'mimetype': 'audio/wav'}
                
                # Set options with simpler configuration
                options = {
                    "punctuate": True,
                    "diarize": False,
                    "model": "nova-2",
                    "language": "en"
                }
                
                # Send to Deepgram
                response = await deepgram.transcription.prerecorded(source, options)
                logger.info(f"Deepgram response: {json.dumps(response, indent=2)}")
                
                # Look for transcript
                if 'results' in response and 'channels' in response['results']:
                    for channel in response['results']['channels']:
                        if 'alternatives' in channel and len(channel['alternatives']) > 0:
                            transcript = channel['alternatives'][0].get('transcript', '')
                            logger.info(f"Transcript: '{transcript}'")
                
                return {
                    "success": True, 
                    "response": response
                }
        
        except Exception as e:
            logger.error(f"Error with Deepgram SDK: {str(e)}")
            
        # Fallback to direct REST API
        logger.info(f"Testing file {audio_path} with direct REST API...")
        
        # Set up Deepgram API request
        url = "https://api.deepgram.com/v1/listen"
        
        # Parameters for transcription - simplified
        params = {
            "model": "nova-2",
            "punctuate": "true"
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
                
                logger.info(f"Extracted transcript ({len(transcript)} chars): '{transcript}'")
                return {"success": True, "response": response_json, "transcript": transcript}
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
    """Run the test"""
    logger.info("Testing Deepgram with speech-like WAV file...")
    result = await test_deepgram_with_speech()
    logger.info("Test complete")
    return result

if __name__ == "__main__":
    asyncio.run(main())