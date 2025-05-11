#!/usr/bin/env python3
"""
Quick test script to verify Deepgram API functionality
"""
import os
import aiohttp
import asyncio
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("deepgram_test")

# Test with a direct HTTP request
async def test_deepgram_api():
    """Test Deepgram API with a direct HTTP request"""
    # Sample WAV file (create or use existing)
    test_file_path = "test_audio.wav"
    
    # Create a simple test WAV file if needed
    if not os.path.exists(test_file_path):
        # Generate a basic WAV header (44 bytes) + 1 second of silence
        with open(test_file_path, "wb") as f:
            # RIFF header
            f.write(b'RIFF')
            f.write((36 + 1 * 16000).to_bytes(4, 'little'))  # File size
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
            f.write((1 * 16000).to_bytes(4, 'little'))  # Data size
            
            # One second of silence (16000 samples at 16 bit)
            f.write(b'\x00\x00' * 16000)
    
    # Get API key from environment
    deepgram_api_key = os.environ.get("DEEPGRAM_API_KEY")
    if not deepgram_api_key:
        logger.error("DEEPGRAM_API_KEY environment variable is not set!")
        return
    
    # API endpoint
    url = "https://api.deepgram.com/v1/listen"
    
    # Parameters
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
    
    logger.info(f"Reading file: {test_file_path}")
    file_size = os.path.getsize(test_file_path)
    logger.info(f"File size: {file_size} bytes")
    
    # Read the audio file
    with open(test_file_path, "rb") as f:
        data = f.read()
        logger.info(f"First 44 bytes (WAV header): {data[:44].hex()}")
    
    # Make API request
    async with aiohttp.ClientSession() as session:
        try:
            logger.info(f"Making request to Deepgram API with audio file of size {len(data)} bytes")
            async with session.post(url, params=params, headers=headers, data=data) as response:
                logger.info(f"Deepgram API response status: {response.status}")
                
                if response.status == 200:
                    response_json = await response.json()
                    logger.info(f"Deepgram API response: {json.dumps(response_json, indent=2)}")
                    return response_json
                else:
                    error_text = await response.text()
                    logger.error(f"Deepgram API error: {response.status}, {error_text}")
                    return {"error": error_text}
        except Exception as e:
            logger.error(f"Error making request to Deepgram API: {str(e)}")
            return {"error": str(e)}

# Main function
async def main():
    """Main function"""
    logger.info("Testing Deepgram API integration")
    result = await test_deepgram_api()
    logger.info("Test complete")
    return result

# Run the test script
if __name__ == "__main__":
    asyncio.run(main())