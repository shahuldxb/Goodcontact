#!/usr/bin/env python3
"""
Simple test for Deepgram using the REST API
"""
import os
import requests
import json
import tempfile
import math
import logging
import wave

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_simple_wav():
    """Create a simple but valid WAV file"""
    # Create a temp directory for our file
    temp_dir = tempfile.mkdtemp()
    output_path = os.path.join(temp_dir, "simple_test.wav")
    
    # Constants
    sample_rate = 16000
    duration = 1  # seconds
    
    # Create WAV file with just a sine wave
    with wave.open(output_path, 'wb') as wav_file:
        # Set parameters
        nchannels = 1  # mono
        sampwidth = 2  # 16-bit
        framerate = sample_rate
        nframes = duration * sample_rate
        
        # Set WAV parameters
        wav_file.setparams((nchannels, sampwidth, framerate, nframes, 'NONE', 'not compressed'))
        
        # Generate a simple 440 Hz sine wave
        for i in range(nframes):
            value = int(32767 * 0.5 * math.sin(2 * math.pi * 440 * i / sample_rate))
            wav_file.writeframes(value.to_bytes(2, byteorder='little', signed=True))
    
    logger.info(f"Created WAV file at {output_path} ({os.path.getsize(output_path)} bytes)")
    return output_path

def test_deepgram_direct():
    """Test Deepgram API with direct REST call"""
    try:
        # Get API key
        api_key = os.environ.get("DEEPGRAM_API_KEY", "d6290865c35bddd50928c5d26983769682fca987")
        
        # Create WAV file
        wav_path = create_simple_wav()
        
        # API endpoint
        url = "https://api.deepgram.com/v1/listen"
        
        # Simple parameters
        params = {
            "model": "nova-2",
            "smart_format": "true"
        }
        
        # Headers
        headers = {
            "Authorization": f"Token {api_key}",
            "Content-Type": "audio/wav"
        }
        
        # Read file
        with open(wav_path, 'rb') as f:
            audio_data = f.read()
            
            # Print first 20 bytes for debugging
            logger.info(f"First 20 bytes of WAV: {audio_data[:20].hex()}")
            logger.info(f"Sending {len(audio_data)} bytes to Deepgram")
            
            # Send to Deepgram
            response = requests.post(url, headers=headers, params=params, data=audio_data)
            
            logger.info(f"Response status: {response.status_code}")
            
            # Process response
            if response.status_code == 200:
                data = response.json()
                
                # Print full response for debugging
                logger.info(f"Response: {json.dumps(data, indent=2)}")
                
                # Try to extract transcript
                if "results" in data and "channels" in data["results"]:
                    for channel in data["results"]["channels"]:
                        if "alternatives" in channel and len(channel["alternatives"]) > 0:
                            transcript = channel["alternatives"][0].get("transcript", "")
                            logger.info(f"Transcript: '{transcript}'")
                
                return {"success": True, "response": data}
            else:
                logger.error(f"Error: {response.text}")
                return {"error": response.text}
    
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {"error": str(e)}
    finally:
        # Clean up
        if 'wav_path' in locals() and os.path.exists(wav_path):
            os.remove(wav_path)
            logger.info(f"Removed {wav_path}")

if __name__ == "__main__":
    test_deepgram_direct()