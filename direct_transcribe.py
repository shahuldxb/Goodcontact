#!/usr/bin/env python3
"""
DirectTranscribe Class
This class implements direct transcription with Azure Storage SAS URLs and Deepgram REST API.

Important features:
1. Uses direct REST API calls to Deepgram (not SDK)
2. Sends Azure Blob SAS URLs directly to Deepgram (no download needed)
3. Handles errors properly with detailed error messages
4. Verifies transcription content before returning success
5. Designed for integration with the main workflow

Usage:
    transcriber = DirectTranscribe(deepgram_api_key)
    result = transcriber.transcribe_audio(sas_url)
    
    if result["success"]:
        transcription_json = result["result"]
        # Process the transcription...
    else:
        error_message = result["error"]["message"]
        # Handle the error...
"""

import json
import requests
import logging
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DirectTranscribe:
    """
    DirectTranscribe class for audio transcription using Deepgram REST API with SAS URLs
    """
    
    def __init__(self, deepgram_api_key: str):
        """
        Initialize the DirectTranscribe class
        
        Args:
            deepgram_api_key (str): The Deepgram API key
        """
        self.deepgram_api_key = deepgram_api_key
        self.api_endpoint = "https://api.deepgram.com/v1/listen"
        
    def transcribe_audio(self, audio_url: str, **kwargs) -> Dict[str, Any]:
        """
        Transcribe audio using Deepgram REST API with SAS URL
        
        Args:
            audio_url (str): The SAS URL for the audio file
            **kwargs: Additional parameters to pass to Deepgram API
            
        Returns:
            Dict with keys:
                success (bool): Whether the transcription was successful
                result (Dict): The Deepgram API response (if successful)
                error (Dict): Error details (if unsuccessful)
                transcript (str): The extracted transcript (if successful)
        """
        # Set up request headers
        headers = {
            "Authorization": f"Token {self.deepgram_api_key}",
            "Content-Type": "application/json"
        }
        
        # Default parameters
        payload = {
            "url": audio_url,
            "model": kwargs.get("model", "nova-3"),
            "smart_format": kwargs.get("smart_format", True),
            "diarize": kwargs.get("diarize", True),
            "punctuate": kwargs.get("punctuate", True),
            "utterances": kwargs.get("utterances", True),
            "paragraphs": True,
            "detect_language": kwargs.get("detect_language", True),
            "filler_words": kwargs.get("filler_words", True),
            "alternatives": kwargs.get("alternatives", 1),
            "sentiment": kwargs.get("sentiment", False),
            "search": kwargs.get("search", [])
        }
        
        # Log the request (exclude sensitive parts)
        logger.info(f"Transcribing audio with Deepgram REST API")
        logger.info(f"URL: {audio_url[:50]}...{audio_url[-10:] if len(audio_url) > 60 else ''}")
        
        try:
            # Send the request to Deepgram
            response = requests.post(self.api_endpoint, headers=headers, json=payload, timeout=300)
            
            # Check if the request was successful
            if response.status_code == 200:
                result = response.json()
                
                # Extract transcript to verify content
                transcript = self._extract_transcript(result)
                
                # Verify that we have content
                if not transcript:
                    logger.warning("Transcription succeeded but no transcript content found")
                    return {
                        "success": False,
                        "error": {
                            "message": "Transcription succeeded but no transcript content found",
                            "status": response.status_code
                        },
                        "result": result,
                        "transcript": ""
                    }
                
                # Log success
                logger.info(f"Transcription successful (length: {len(transcript)} characters)")
                
                # Return success response
                return {
                    "success": True,
                    "result": result,
                    "transcript": transcript,
                    "error": None
                }
            else:
                # Log failure
                logger.error(f"Transcription failed with status {response.status_code}: {response.text}")
                
                # Return error response
                return {
                    "success": False,
                    "error": {
                        "message": response.text,
                        "status": response.status_code
                    },
                    "result": None,
                    "transcript": ""
                }
                
        except Exception as e:
            # Log exception
            logger.exception(f"Exception during transcription: {str(e)}")
            
            # Return error response
            return {
                "success": False,
                "error": {
                    "message": str(e),
                    "status": None
                },
                "result": None,
                "transcript": ""
            }
    
    def _extract_transcript(self, result: Dict[str, Any]) -> str:
        """
        Extract transcript from Deepgram API response with paragraphs and sentences if available
        
        Args:
            result (Dict): The Deepgram API response
            
        Returns:
            str: The extracted transcript
        """
        try:
            if "results" in result and "channels" in result["results"]:
                channels = result["results"]["channels"]
                if channels and "alternatives" in channels[0]:
                    alternatives = channels[0]["alternatives"]
                    
                    # First check if there's a standard transcript
                    if alternatives and "transcript" in alternatives[0]:
                        transcript = alternatives[0]["transcript"]
                        
                        # Now check if we have paragraphs
                        if "paragraphs" in alternatives[0] and "paragraphs" in alternatives[0]["paragraphs"]:
                            paragraphs = alternatives[0]["paragraphs"]["paragraphs"]
                            if paragraphs:
                                # Return structured paragraphs if available
                                formatted_paragraphs = []
                                for paragraph in paragraphs:
                                    if "sentences" in paragraph:
                                        sentences = [s.get("text", "") for s in paragraph.get("sentences", [])]
                                        formatted_paragraphs.append(" ".join(sentences))
                                    elif "text" in paragraph:
                                        formatted_paragraphs.append(paragraph["text"])
                                
                                if formatted_paragraphs:
                                    return "\n\n".join(formatted_paragraphs)
                        
                        # Return the standard transcript if paragraphs aren't available or failed
                        return transcript
            
            # If we couldn't find a transcript in the standard way, look for it in utterances
            if "results" in result and "utterances" in result["results"]:
                utterances = result["results"]["utterances"]
                if utterances:
                    return " ".join([u.get("transcript", "") for u in utterances])
                    
            return ""
            
        except Exception as e:
            logger.error(f"Error extracting transcript: {str(e)}")
            return ""

# Example usage
if __name__ == "__main__":
    import os
    from datetime import datetime, timedelta
    from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
    
    # Constants
    DEEPGRAM_API_KEY = "ba94baf7840441c378c58ccd1d5202c38ddc42d8"
    AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=infolder;AccountKey=NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==;EndpointSuffix=core.windows.net"
    
    def generate_sas_url(blob_name, container_name="shahulin", expiry_hours=240):
        """Generate SAS URL with long expiry time"""
        try:
            # Extract account info from connection string
            conn_parts = {p.split('=')[0]: p.split('=', 1)[1] for p in AZURE_STORAGE_CONNECTION_STRING.split(';') if '=' in p}
            account_name = conn_parts.get('AccountName')
            account_key = conn_parts.get('AccountKey')
            
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
            url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
            return url
        except Exception as e:
            logger.error(f"Error generating SAS URL: {str(e)}")
            return None
    
    # Example file
    blob_name = "agricultural_leasing_(ijarah)_normal.mp3"
    
    # Generate SAS URL
    sas_url = generate_sas_url(blob_name)
    if not sas_url:
        logger.error("Failed to generate SAS URL")
        exit(1)
    
    # Create DirectTranscribe instance
    transcriber = DirectTranscribe(DEEPGRAM_API_KEY)
    
    # Transcribe audio
    result = transcriber.transcribe_audio(sas_url)
    
    # Check result
    if result["success"]:
        logger.info("Transcription successful!")
        logger.info(f"Transcript preview: {result['transcript'][:100]}...")
        
        # Save result to file
        with open("transcription_result.json", "w") as f:
            json.dump(result["result"], f, indent=2)
        logger.info("Result saved to transcription_result.json")
    else:
        logger.error(f"Transcription failed: {result['error']['message']}")