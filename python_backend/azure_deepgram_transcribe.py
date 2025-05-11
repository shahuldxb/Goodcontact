#!/usr/bin/env python3
"""
Transcribe Azure Storage Audio Files using Deepgram's API

An adapted script to transcribe audio files from Azure Blob Storage using Deepgram's REST API directly.
This script demonstrates the speaker diarization functionality to identify
different speakers in an audio recording.

This is built as a module that can be imported and used by the main application.
"""

import os
import requests
import logging
import tempfile
import json
from datetime import datetime
from azure_storage_service import AzureStorageService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def transcribe_azure_audio(blob_name, api_key=None, model="nova-2", diarize=True, container_name="shahulin"):
    """
    Transcribe an audio file from Azure Blob Storage using Deepgram API
    
    Args:
        blob_name: Name of the blob in Azure Storage
        api_key: Deepgram API key (if None, will use environment variable)
        model: Deepgram model to use
        diarize: Whether to enable speaker diarization
        container_name: Azure Storage container name
        
    Returns:
        dict: Result of the transcription
    """
    if api_key is None:
        api_key = os.environ.get('DEEPGRAM_API_KEY')
        if not api_key:
            return {
                'success': False,
                'error': "DEEPGRAM_API_KEY environment variable not set."
            }
    
    # Initialize Azure Storage Service
    try:
        azure_storage = AzureStorageService()
    except Exception as e:
        return {
            'success': False,
            'error': f"Failed to initialize Azure Storage: {str(e)}"
        }
    
    # Determine file type from extension
    file_type = blob_name.split('.')[-1].lower()
    
    # Configure API parameters
    api_url = "https://api.deepgram.com/v1/listen"
    params = {
        "model": model,
        "smart_format": "true",
        "punctuate": "true"
    }
    
    # Add diarization if requested
    if diarize:
        params["diarize"] = "true"
    
    # Set up headers with API key
    headers = {
        "Authorization": f"Token {api_key}",
        "Content-Type": f"audio/{file_type}"
    }
    
    logger.info(f"Transcribing {blob_name} from container {container_name} using Deepgram API...")
    logger.info(f"Model: {model}, Speaker Diarization: {'Enabled' if diarize else 'Disabled'}")
    
    try:
        # Create a temporary file for the downloaded audio
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_type}") as temp_file:
            temp_path = temp_file.name
        
        try:
            # Download the blob
            logger.info(f"Downloading {blob_name} from {container_name}...")
            azure_storage.download_blob(blob_name, temp_path, container_name)
            
            # Read the audio file
            with open(temp_path, 'rb') as audio_file:
                audio_data = audio_file.read()
            
            # Send the request to Deepgram
            logger.info("Sending audio to Deepgram, please wait...")
            response = requests.post(api_url, params=params, headers=headers, data=audio_data)
            
            # Check if the request was successful
            if response.status_code == 200:
                logger.info("Transcription successful!")
                response_data = response.json()
                
                # Save the full response for debugging
                response_json = json.dumps(response_data, indent=2)
                
                # Extract basic transcript
                basic_transcript = ""
                if 'results' in response_data and 'channels' in response_data['results']:
                    basic_transcript = response_data['results']['channels'][0]['alternatives'][0]['transcript']
                
                # Process speaker information
                has_speakers = False
                speaker_transcript = ""
                
                # Try to extract utterances with speaker info first
                if diarize and 'results' in response_data and 'utterances' in response_data['results']:
                    has_speakers = True
                    utterances = response_data['results']['utterances']
                    
                    for utterance in utterances:
                        if 'speaker' in utterance and 'text' in utterance:
                            speaker = utterance['speaker']
                            text = utterance['text']
                            speaker_transcript += f"Speaker {speaker}: {text}\n\n"
                
                # If no utterances but paragraphs with speaker info are available
                elif diarize and 'results' in response_data and 'paragraphs' in response_data['results'] and 'paragraphs' in response_data['results']['paragraphs']:
                    has_speakers = True
                    paragraphs = response_data['results']['paragraphs']['paragraphs']
                    
                    current_speaker = None
                    for paragraph in paragraphs:
                        if 'speaker' in paragraph:
                            speaker_num = paragraph.get('speaker', 0)
                            
                            # Add speaker change
                            if current_speaker != speaker_num:
                                current_speaker = speaker_num
                                if speaker_transcript:
                                    speaker_transcript += "\n\n"
                                speaker_transcript += f"Speaker {speaker_num}: "
                            
                            # Add paragraph text
                            if 'text' in paragraph:
                                speaker_transcript += paragraph['text'] + " "
                
                return {
                    'success': True,
                    'has_speakers': has_speakers,
                    'basic_transcript': basic_transcript,
                    'speaker_transcript': speaker_transcript.strip() if has_speakers else None,
                    'full_response': response_data  # Return the full response for advanced processing
                }
            else:
                error_msg = f"Deepgram API returned status code {response.status_code}: {response.text}"
                logger.error(error_msg)
                return {
                    'success': False,
                    'error': error_msg
                }
        
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_path):
                os.remove(temp_path)
                logger.info(f"Removed temporary file: {temp_path}")
    
    except Exception as e:
        logger.error(f"Error during transcription: {str(e)}")
        return {
            'success': False,
            'error': f"Error: {str(e)}"
        }

# Function to handle the transcription and move file to output container
def process_audio_file(blob_name, fileid=None, output_container="shahulout"):
    """
    Process an audio file from Azure Blob Storage:
    1. Transcribe it using Deepgram
    2. Copy it to the output container
    3. Return the transcription results
    
    Args:
        blob_name: Name of the blob in Azure Storage
        fileid: Optional ID for the file (for database tracking)
        output_container: Container to move the processed file to
        
    Returns:
        dict: Processing results including transcription
    """
    try:
        # Initialize Azure Storage Service
        azure_storage = AzureStorageService()
        
        # Step 1: Transcribe the audio
        transcription_result = transcribe_azure_audio(blob_name)
        
        # Step 2: Copy the blob to the output container
        if transcription_result['success']:
            try:
                logger.info(f"Moving {blob_name} to {output_container}...")
                destination_url = azure_storage.copy_blob_to_destination(blob_name)
                logger.info(f"File moved successfully to {destination_url}")
            except Exception as e:
                logger.error(f"Error moving file to output container: {str(e)}")
                # Don't fail the whole process if just the move fails
        
        # Step 3: Return the combined results
        return {
            'transcription': transcription_result,
            'filename': blob_name,
            'fileid': fileid,
            'processed_timestamp': datetime.now().isoformat(),
            'success': transcription_result['success']
        }
    
    except Exception as e:
        logger.error(f"Error processing audio file: {str(e)}")
        return {
            'success': False,
            'error': f"Processing error: {str(e)}",
            'filename': blob_name,
            'fileid': fileid
        }

# Test function for command-line usage
def main():
    """Command-line test function"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python azure_deepgram_transcribe.py <blob_name> [container_name]")
        sys.exit(1)
    
    blob_name = sys.argv[1]
    container_name = sys.argv[2] if len(sys.argv) > 2 else "shahulin"
    
    result = transcribe_azure_audio(blob_name, container_name=container_name)
    
    if result['success']:
        print("\n" + "=" * 50)
        print("TRANSCRIPTION RESULT")
        print("=" * 50)
        
        if result['has_speakers']:
            print("\nWith Speaker Diarization:")
            print("-" * 25)
            print(result['speaker_transcript'])
        else:
            print("\nBasic Transcript:")
            print("-" * 25)
            print(result['basic_transcript'])
    else:
        print(f"\nTranscription failed: {result['error']}")

if __name__ == '__main__':
    main()