#!/usr/bin/env python3
"""
DirectTranscribe Class
This class implements direct transcription with Azure Storage SAS URLs and Deepgram REST API.

Key features:
1. Uses direct REST API calls to Deepgram (not SDK)
2. Sends Azure Blob SAS URLs directly to Deepgram (no download needed)
3. Handles errors properly with detailed error messages
4. Verifies transcription content before returning success
"""

import json
import requests
import logging
import os
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
            "paragraphs": kwargs.get("paragraphs", True),
            "detect_language": kwargs.get("detect_language", True)
        }
        
        # Log the request (exclude sensitive parts)
        logger.info(f"Transcribing audio with Deepgram REST API")
        logger.info(f"URL: {audio_url[:50]}...{audio_url[-10:] if len(audio_url) > 60 else ''}")
        
        try:
            # Send the request to Deepgram
            response = requests.post(self.api_endpoint, headers=headers, json=payload, timeout=300)
            
            # Log raw response for debugging
            logger.info(f"Deepgram API Response Status: {response.status_code}")
            
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
                logger.info(f"Transcript preview: {transcript[:100]}...")
                
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
        Extract transcript from Deepgram API response
        
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
                    if alternatives and "transcript" in alternatives[0]:
                        return alternatives[0]["transcript"]
            
            # Log structure of result for debugging
            logger.warning(f"Could not extract transcript, result structure: {json.dumps(result, default=str)[:500]}...")
            return ""
        except Exception as e:
            logger.error(f"Error extracting transcript: {str(e)}")
            return ""