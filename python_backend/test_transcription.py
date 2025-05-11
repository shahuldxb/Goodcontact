"""
Test audio transcription with Deepgram API
"""
import os
import logging
from direct_transcribe import DirectTranscriber

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Deepgram API key
DEEPGRAM_API_KEY = os.environ.get("DEEPGRAM_API_KEY", "ba94baf7840441c378c58ccd1d5202c38ddc42d8")

def test_transcription():
    """Test transcription with Deepgram API using a sample audio URL"""
    
    # Create a sample SAS URL for testing
    # This is a mock SAS URL - in production, we would generate a real SAS URL for the blob
    sample_url = "https://infolder.blob.core.windows.net/shahulin/agricultural_finance_(murabaha)_2.mp3"
    
    logger.info(f"Testing transcription with sample URL: {sample_url}")
    
    try:
        # Create a DirectTranscriber instance
        transcriber = DirectTranscriber(DEEPGRAM_API_KEY)
        
        # Transcribe the audio
        result = transcriber.transcribe_url(sample_url, model="nova-3", diarize=True)
        
        if result:
            # Check if the transcription was successful
            logger.info("Transcription successful")
            
            # Extract some basic information from the transcription result
            if "results" in result and "channels" in result["results"]:
                channels = result["results"]["channels"]
                if channels and "alternatives" in channels[0]:
                    alternatives = channels[0]["alternatives"]
                    if alternatives:
                        transcript = alternatives[0].get("transcript", "")
                        confidence = alternatives[0].get("confidence", 0)
                        logger.info(f"Transcript confidence: {confidence:.4f}")
                        logger.info(f"Transcript preview: {transcript[:100]}...")
            
            return True
        else:
            logger.error("Transcription failed - no result returned")
            return False
    except Exception as e:
        logger.error(f"Transcription failed with error: {str(e)}")
        return False

if __name__ == "__main__":
    test_transcription()