#!/usr/bin/env python3
"""
Using the original transcription code exactly as provided
- Downloads audio file from Azure Storage using SAS URL
- Uses the exact original transcription function
- Stores results in SQL database
"""

import os
import sys
import logging
import uuid
import json
import requests
import tempfile
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
import pymssql

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Azure Storage settings
STORAGE_ACCOUNT_NAME = os.environ.get("AZURE_STORAGE_ACCOUNT", "infolder")
STORAGE_ACCOUNT_KEY = os.environ.get("AZURE_STORAGE_KEY", "NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==")
STORAGE_CONNECTION_STRING = os.environ.get(
    "AZURE_STORAGE_CONNECTION_STRING", 
    f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={STORAGE_ACCOUNT_KEY};EndpointSuffix=core.windows.net"
)
SOURCE_CONTAINER = os.environ.get("AZURE_SOURCE_CONTAINER", "shahulin")

# Azure SQL settings
SQL_SERVER = os.environ.get("AZURE_SQL_SERVER", "callcenter1.database.windows.net")
SQL_DATABASE = os.environ.get("AZURE_SQL_DATABASE", "call")
SQL_USER = os.environ.get("AZURE_SQL_USER", "shahul")
SQL_PASSWORD = os.environ.get("AZURE_SQL_PASSWORD", "apple123!@#")
SQL_PORT = int(os.environ.get("AZURE_SQL_PORT", "1433"))

# Deepgram API key - keeping this explicit rather than using environment variable
DEEPGRAM_API_KEY = "ba94baf7840441c378c58ccd1d5202c38ddc42d8"

def get_sql_connection():
    """Get a connection to Azure SQL Database"""
    return pymssql.connect(
        server=SQL_SERVER,
        port=SQL_PORT,
        database=SQL_DATABASE,
        user=SQL_USER,
        password=SQL_PASSWORD,
        tds_version='7.4',
        as_dict=True
    )

def list_audio_blobs(container_name=SOURCE_CONTAINER, limit=5):
    """List audio blobs in the container"""
    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(container_name)
        
        # List all blobs in the container
        all_blobs = list(container_client.list_blobs())
        
        # Filter for audio files
        audio_extensions = ('.mp3', '.wav', '.m4a', '.ogg', '.aac', '.flac')
        audio_blobs = [blob.name for blob in all_blobs if blob.name.lower().endswith(audio_extensions)]
        
        return audio_blobs[:limit]
    except Exception as e:
        logger.error(f"Error listing audio blobs: {str(e)}")
        return []

def generate_blob_sas_url(blob_name, container_name=SOURCE_CONTAINER, expiry_hours=240):
    """Generate a Blob SAS URL for a specific blob in Azure Storage"""
    try:
        # Generate SAS token
        sas_token = generate_blob_sas(
            account_name=STORAGE_ACCOUNT_NAME,
            container_name=container_name,
            blob_name=blob_name,
            account_key=STORAGE_ACCOUNT_KEY,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=expiry_hours)
        )
        
        # Build the full SAS URL specifically for this blob
        blob_sas_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
        
        logger.info(f"Generated Blob SAS URL for {blob_name}")
        return blob_sas_url
    except Exception as e:
        logger.error(f"Error generating Blob SAS URL: {str(e)}")
        return None

def download_blob_to_temp_file(blob_sas_url):
    """Download a blob to a temporary file"""
    try:
        response = requests.get(blob_sas_url, stream=True)
        if response.status_code == 200:
            # Create a temporary file
            file_extension = blob_sas_url.split('?')[0].split('.')[-1]
            temp_file = tempfile.NamedTemporaryFile(suffix=f".{file_extension}", delete=False)
            
            # Write the content to the temporary file
            for chunk in response.iter_content(chunk_size=8192):
                temp_file.write(chunk)
            
            temp_file.close()
            logger.info(f"Downloaded blob to {temp_file.name}")
            return temp_file.name
        else:
            logger.error(f"Failed to download blob: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Error downloading blob: {str(e)}")
        return None

# ============================
# EXACTLY AS PROVIDED IN THE ATTACHED FILE
# ============================

def transcribe_audio_file(file_path, api_key, model="nova-2", diarize=True):
    """
    Transcribe an audio file using Deepgram API
    
    Args:
        file_path: Path to the audio file
        api_key: Deepgram API key
        model: Deepgram model to use
        diarize: Whether to enable speaker diarization
        
    Returns:
        dict: Result of the transcription
    """
    # Determine file type from extension
    file_type = file_path.split('.')[-1].lower()
    
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
    
    print(f"Transcribing {file_path} using Deepgram API...")
    print(f"Model: {model}, Speaker Diarization: {'Enabled' if diarize else 'Disabled'}")
    
    try:
        # Read the audio file
        with open(file_path, 'rb') as audio_file:
            audio_data = audio_file.read()
        
        # Send the request to Deepgram
        print("Sending audio to Deepgram, please wait...")
        response = requests.post(api_url, params=params, headers=headers, data=audio_data)
        
        # Check if the request was successful
        if response.status_code == 200:
            print("Transcription successful!")
            response_data = response.json()
            
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
                'response_data': response_data  # Include the full response for further processing
            }
        else:
            print(f"Error: Deepgram API returned status code {response.status_code}")
            print(f"Error details: {response.text}")
            return {
                'success': False,
                'error': f"API Error: {response.status_code} - {response.text}"
            }
    
    except FileNotFoundError:
        return {
            'success': False,
            'error': f"File not found: {file_path}"
        }
    except Exception as e:
        return {
            'success': False,
            'error': f"Error: {str(e)}"
        }

# ============================
# DATABASE STORAGE FUNCTION
# ============================

def store_in_sql_database(fileid, blob_name, transcription_result):
    """Store transcription results in Azure SQL database"""
    try:
        if not transcription_result['success']:
            logger.error(f"Cannot store unsuccessful transcription: {transcription_result['error']}")
            return False, {"error": transcription_result['error']}
        
        # Extract data from transcription result
        basic_transcript = transcription_result['basic_transcript']
        speaker_transcript = transcription_result['speaker_transcript']
        response_data = transcription_result['response_data']
        
        # Connect to database
        conn = get_sql_connection()
        cursor = conn.cursor()
        
        # Insert audio metadata
        logger.info("Inserting audio metadata")
        cursor.execute("""
            EXEC RDS_InsertAudioMetadata
            @fileid = %s,
            @filename = %s,
            @file_size = %s,
            @upload_date = %s,
            @language_detected = %s,
            @transcription = %s,
            @status = %s,
            @processing_duration = %s
        """, (
            fileid,
            blob_name,
            0,  # File size
            datetime.now(),
            "English",  # Default language
            basic_transcript[:4000] if basic_transcript else "",
            "completed",
            0  # Processing duration
        ))
        
        # Extract and insert paragraphs/utterances
        para_count = 0
        sent_count = 0
        
        # Process as paragraphs
        if transcription_result['has_speakers'] and 'results' in response_data:
            # Try to get utterances first
            if 'utterances' in response_data['results']:
                utterances = response_data['results']['utterances']
                logger.info(f"Found {len(utterances)} utterances")
                
                for i, utterance in enumerate(utterances):
                    if 'speaker' in utterance and 'text' in utterance:
                        para_count += 1
                        cursor.execute("""
                            EXEC RDS_InsertParagraph
                            @fileid = %s,
                            @para_num = %s,
                            @para_text = %s,
                            @start_time = %s,
                            @end_time = %s,
                            @speaker = %s
                        """, (
                            fileid,
                            i + 1,
                            utterance['text'][:4000],
                            int(utterance.get('start', 0) * 1000),  # Convert to milliseconds
                            int(utterance.get('end', 0) * 1000),    # Convert to milliseconds
                            utterance.get('speaker', 0)
                        ))
                        
                        # Insert same text as a sentence (one per utterance)
                        sent_count += 1
                        cursor.execute("""
                            EXEC RDS_InsertSentence
                            @fileid = %s,
                            @para_num = %s,
                            @sent_num = %s,
                            @sent_text = %s,
                            @start_time = %s,
                            @end_time = %s,
                            @speaker = %s
                        """, (
                            fileid,
                            i + 1,  # Paragraph number
                            1,      # Sentence number (one per utterance)
                            utterance['text'][:4000],
                            int(utterance.get('start', 0) * 1000),
                            int(utterance.get('end', 0) * 1000),
                            utterance.get('speaker', 0)
                        ))
            
            # If no utterances, try paragraphs
            elif 'paragraphs' in response_data['results'] and 'paragraphs' in response_data['results']['paragraphs']:
                paragraphs = response_data['results']['paragraphs']['paragraphs']
                logger.info(f"Found {len(paragraphs)} paragraphs")
                
                for i, paragraph in enumerate(paragraphs):
                    if 'speaker' in paragraph and 'text' in paragraph:
                        para_count += 1
                        cursor.execute("""
                            EXEC RDS_InsertParagraph
                            @fileid = %s,
                            @para_num = %s,
                            @para_text = %s,
                            @start_time = %s,
                            @end_time = %s,
                            @speaker = %s
                        """, (
                            fileid,
                            i + 1,
                            paragraph['text'][:4000],
                            int(paragraph.get('start', 0) * 1000),
                            int(paragraph.get('end', 0) * 1000),
                            paragraph.get('speaker', 0)
                        ))
                        
                        # Insert same text as a sentence (one per paragraph)
                        sent_count += 1
                        cursor.execute("""
                            EXEC RDS_InsertSentence
                            @fileid = %s,
                            @para_num = %s,
                            @sent_num = %s,
                            @sent_text = %s,
                            @start_time = %s,
                            @end_time = %s,
                            @speaker = %s
                        """, (
                            fileid,
                            i + 1,  # Paragraph number
                            1,      # Sentence number (one per paragraph)
                            paragraph['text'][:4000],
                            int(paragraph.get('start', 0) * 1000),
                            int(paragraph.get('end', 0) * 1000),
                            paragraph.get('speaker', 0)
                        ))
        
        # If no structure, use the basic transcript
        elif basic_transcript:
            # Insert a single paragraph
            para_count = 1
            cursor.execute("""
                EXEC RDS_InsertParagraph
                @fileid = %s,
                @para_num = %s,
                @para_text = %s,
                @start_time = %s,
                @end_time = %s,
                @speaker = %s
            """, (
                fileid,
                1,
                basic_transcript[:4000],
                0,  # Start time
                0,  # End time
                0   # Speaker
            ))
            
            # Split transcript into sentences (very basic)
            sentences = [s.strip() + "." for s in basic_transcript.split('. ') if s.strip()]
            for i, sentence in enumerate(sentences):
                sent_count += 1
                cursor.execute("""
                    EXEC RDS_InsertSentence
                    @fileid = %s,
                    @para_num = %s,
                    @sent_num = %s,
                    @sent_text = %s,
                    @start_time = %s,
                    @end_time = %s,
                    @speaker = %s
                """, (
                    fileid,
                    1,
                    i + 1,
                    sentence[:4000],
                    0,  # Start time
                    0,  # End time
                    0   # Speaker
                ))
        
        # Commit transaction
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully stored transcription with {para_count} paragraphs and {sent_count} sentences")
        return True, {"para_count": para_count, "sent_count": sent_count}
    except Exception as e:
        logger.error(f"Error storing in database: {str(e)}")
        return False, {"error": str(e)}

def main():
    """Main function to run the test"""
    try:
        # Step 1: List audio blobs in container
        logger.info("Listing audio blobs...")
        audio_blobs = list_audio_blobs()
        
        if not audio_blobs:
            logger.error("No audio blobs found")
            return False
        
        # Print available blobs
        logger.info(f"Found {len(audio_blobs)} audio blobs:")
        for i, blob in enumerate(audio_blobs):
            logger.info(f"  {i+1}. {blob}")
        
        # Step 2: Select a blob and generate SAS URL
        blob_name = audio_blobs[0]
        logger.info(f"Selected blob: {blob_name}")
        
        # Generate a unique file ID
        fileid = f"test_{uuid.uuid4().hex[:8]}"
        logger.info(f"Generated file ID: {fileid}")
        
        # Generate SAS URL
        blob_sas_url = generate_blob_sas_url(blob_name)
        if not blob_sas_url:
            logger.error("Failed to generate SAS URL")
            return False
        
        logger.info(f"Generated SAS URL (first 60 chars): {blob_sas_url[:60]}...")
        
        # Step 3: Download the blob to a local temp file
        logger.info("Downloading audio file...")
        local_file_path = download_blob_to_temp_file(blob_sas_url)
        if not local_file_path:
            logger.error("Failed to download audio file")
            return False
        
        logger.info(f"Downloaded to local file: {local_file_path}")
        
        # Step 4: Transcribe using the original transcribe_audio_file function
        logger.info("Transcribing with Deepgram using original function...")
        transcription_result = transcribe_audio_file(
            file_path=local_file_path,
            api_key=DEEPGRAM_API_KEY,
            model="nova-3",  # Using nova-3 as it's more recent
            diarize=True
        )
        
        # Clean up the temporary file
        try:
            os.unlink(local_file_path)
            logger.info(f"Deleted temporary file: {local_file_path}")
        except Exception as e:
            logger.warning(f"Failed to delete temporary file: {str(e)}")
        
        if not transcription_result['success']:
            logger.error(f"Transcription failed: {transcription_result.get('error', 'Unknown error')}")
            return False
        
        # Save transcription to file for reference
        with open(f"transcription_{fileid}.json", "w") as f:
            json.dump(transcription_result['response_data'], f, indent=2)
        
        # Display preview of transcription
        logger.info("Transcription preview:")
        if transcription_result['has_speakers']:
            logger.info("With speaker diarization:")
            # Get the first 500 characters of the speaker transcript
            preview = transcription_result['speaker_transcript'][:500] + "..." if transcription_result['speaker_transcript'] else "None"
            logger.info(preview)
        else:
            logger.info("Basic transcript:")
            # Get the first 500 characters of the basic transcript
            preview = transcription_result['basic_transcript'][:500] + "..." if transcription_result['basic_transcript'] else "None"
            logger.info(preview)
        
        # Step 5: Store in database
        logger.info("Storing results in database...")
        db_success, db_details = store_in_sql_database(fileid, blob_name, transcription_result)
        
        if not db_success:
            logger.error(f"Failed to store in database: {db_details.get('error', 'Unknown error')}")
            return False
        
        logger.info(f"Successfully stored in database: {db_details}")
        return True
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("Starting original transcription test...")
    success = main()
    
    if success:
        logger.info("Test completed successfully!")
        sys.exit(0)
    else:
        logger.error("Test failed!")
        sys.exit(1)