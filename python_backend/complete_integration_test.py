#!/usr/bin/env python3
"""
Complete Integration Test for Deepgram-Azure integration
- Downloads audio file from Azure Blob Storage using SAS URL
- Transcribes it using Deepgram API 
- Stores results in SQL database correctly following the database constraints
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

# Deepgram API key
DEEPGRAM_API_KEY = os.environ.get("DEEPGRAM_API_KEY", "ba94baf7840441c378c58ccd1d5202c38ddc42d8")


# ============================
# AZURE SQL DATABASE 
# ============================

def get_sql_connection():
    """Get a connection to Azure SQL Database"""
    try:
        conn = pymssql.connect(
            server=SQL_SERVER,
            user=SQL_USER,
            password=SQL_PASSWORD,
            database=SQL_DATABASE,
            port=SQL_PORT
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to SQL database: {str(e)}")
        raise


# ============================
# AZURE BLOB STORAGE
# ============================

def list_audio_blobs(container_name=SOURCE_CONTAINER, limit=5):
    """List audio blobs in the container"""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(container_name)
        
        blobs = []
        for blob in container_client.list_blobs():
            if blob.name.lower().endswith(('.mp3', '.wav', '.ogg', '.flac', '.m4a')):
                blobs.append(blob.name)
                if len(blobs) >= limit:
                    break
        
        return blobs
    except Exception as e:
        logger.error(f"Error listing blobs: {str(e)}")
        return []


def generate_blob_sas_url(blob_name, container_name=SOURCE_CONTAINER, expiry_hours=240):
    """Generate a Blob SAS URL for a specific blob in Azure Storage"""
    try:
        # Create SAS token with read permission that expires in specified hours
        sas_token = generate_blob_sas(
            account_name=STORAGE_ACCOUNT_NAME,
            account_key=STORAGE_ACCOUNT_KEY,
            container_name=container_name,
            blob_name=blob_name,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=expiry_hours)
        )
        
        # Construct the URL with SAS token
        blob_sas_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
        logger.info(f"Generated SAS URL for {blob_name} (expires in {expiry_hours} hours)")
        
        return blob_sas_url
    except Exception as e:
        logger.error(f"Error generating SAS URL: {str(e)}")
        return None


def download_blob_to_temp_file(blob_sas_url):
    """Download a blob to a temporary file"""
    try:
        # Create a temporary file
        fd, temp_path = tempfile.mkstemp(suffix='.mp3')
        os.close(fd)  # Close the file descriptor
        
        # Download the blob
        logger.info(f"Downloading blob to {temp_path}")
        response = requests.get(blob_sas_url, stream=True)
        response.raise_for_status()
        
        with open(temp_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        
        return temp_path
    except Exception as e:
        logger.error(f"Error downloading blob: {str(e)}")
        return None


# ============================
# DEEPGRAM TRANSCRIPTION
# ============================

def transcribe_audio_file(file_path, api_key=DEEPGRAM_API_KEY, model="nova-2", diarize=True):
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
    try:
        logger.info(f"Transcribing audio file: {file_path}")
        
        # Construct the request URL
        url = "https://api.deepgram.com/v1/listen"
        
        # Prepare parameters
        params = {
            "model": model,
            "diarize": "true" if diarize else "false",
            "punctuate": "true",
            "utterances": "true",
            "paragraphs": "true",
            "filler_words": "true",
            "detect_language": "true",
            "smart_format": "true"
        }
        
        # Prepare headers
        headers = {
            "Authorization": f"Token {api_key}",
            "Content-Type": "audio/mpeg"  # Assuming MP3 format
        }
        
        # Open the file and send the request
        with open(file_path, 'rb') as file:
            response = requests.post(url, params=params, headers=headers, data=file)
        
        # Check if the request was successful
        response.raise_for_status()
        
        # Parse the response
        result = response.json()
        
        # Check if we have valid results
        if 'results' not in result:
            return {
                'success': False,
                'error': 'No results in Deepgram response',
                'response_data': result
            }
        
        # Extract the basic transcript
        basic_transcript = ""
        if 'results' in result and 'channels' in result['results'] and len(result['results']['channels']) > 0:
            if 'alternatives' in result['results']['channels'][0] and len(result['results']['channels'][0]['alternatives']) > 0:
                basic_transcript = result['results']['channels'][0]['alternatives'][0].get('transcript', '')
        
        # Extract speaker transcript if diarization is enabled
        speaker_transcript = ""
        if diarize and 'results' in result and 'utterances' in result['results']:
            utterances = result['results']['utterances']
            speaker_segments = []
            
            for utterance in utterances:
                speaker = utterance.get('speaker', 'unknown')
                text = utterance.get('transcript', '')
                speaker_segments.append(f"Speaker {speaker}: {text}")
            
            speaker_transcript = "\n".join(speaker_segments)
        
        return {
            'success': True,
            'basic_transcript': basic_transcript,
            'speaker_transcript': speaker_transcript,
            'response_data': result
        }
    except Exception as e:
        logger.error(f"Error transcribing audio: {str(e)}")
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
        
        # First, insert a record into rdt_assets
        logger.info("Inserting asset record")
        cursor.execute("""
            INSERT INTO rdt_assets (
                fileid, 
                filename, 
                source_path, 
                destination_path, 
                file_size,
                upload_date,
                status,
                created_dt
            )
            VALUES (
                %s, 
                %s, 
                %s, 
                %s, 
                %s,
                %s,
                %s,
                %s
            )
        """, (
            fileid,
            blob_name,
            SOURCE_CONTAINER,  # Source container - use global variable
            None,              # Destination path
            0,                 # File size (not available)
            datetime.now(),    # Upload date
            "completed",       # Status
            datetime.now()     # Created date
        ))
        logger.info("Asset record inserted successfully")
        
        # Insert audio metadata
        logger.info("Inserting audio metadata")
        cursor.execute("""
            EXEC RDS_InsertAudioMetadata
            @fileid = %s,
            @request_id = %s,
            @sha256 = %s,
            @created_timestamp = %s,
            @audio_duration = %s,
            @confidence = %s,
            @status = %s
        """, (
            fileid,
            f"request_{uuid.uuid4().hex[:8]}",  # Generate a request ID
            "0000000000000000000000000000000000000000000000000000000000000000",  # Placeholder for SHA256
            datetime.now().isoformat(),
            0.0,  # Audio duration (not available)
            0.9,  # Default confidence
            "completed"
        ))
        
        # Extract and insert paragraphs/utterances
        para_count = 0
        sent_count = 0
        
        try:
            # Process paragraphs - adding more detailed debugging
            logger.info(f"Checking paragraphs structure: 'results' in response_data: {'results' in response_data}")
            logger.info(f"Available keys in results: {list(response_data['results'].keys()) if 'results' in response_data else []}")
            
            # Write the entire response to a file for inspection
            with open(f"deepgram_response_{fileid}.json", "w") as f:
                json.dump(response_data, f, indent=2)
            logger.info(f"Wrote complete response to deepgram_response_{fileid}.json")
            
            if 'results' in response_data:
                logger.info(f"'paragraphs' in response_data['results']: {'paragraphs' in response_data['results']}")
                if 'paragraphs' in response_data['results']:
                    paragraphs_data = response_data['results']['paragraphs']
                    logger.info(f"Paragraphs data structure: {json.dumps(paragraphs_data, indent=2)[:500]}")
                
                # Check for utterances which we can use as an alternative
                if 'utterances' in response_data['results']:
                    utterances = response_data['results']['utterances']
                    logger.info(f"Found {len(utterances)} utterances that can be used instead of paragraphs")
                    logger.info(f"First utterance sample: {json.dumps(utterances[0], indent=2) if utterances else 'No utterances'}")
                    
            # Process paragraphs if available, otherwise use utterances
            paragraphs = []
            
            if 'results' in response_data and 'paragraphs' in response_data['results']:
                # If paragraphs feature is available
                paragraphs = response_data['results']['paragraphs'].get('paragraphs', [])
                logger.info(f"Found {len(paragraphs)} paragraphs from paragraphs feature")
                
            elif 'results' in response_data and 'utterances' in response_data['results']:
                # Use utterances as paragraphs if paragraphs feature is not available
                utterances = response_data['results']['utterances']
                logger.info(f"Using {len(utterances)} utterances as paragraphs")
                # Convert utterances to paragraph format
                for i, utterance in enumerate(utterances):
                    paragraphs.append({
                        'text': utterance.get('transcript', ''),
                        'start': utterance.get('start', 0.0),
                        'end': utterance.get('end', 0.0),
                        'speaker': utterance.get('speaker', 0)
                    })
            
            # Process the paragraphs (either from paragraphs feature or converted from utterances)
            for idx, paragraph in enumerate(paragraphs):
                para_text = paragraph.get('text', '')
                para_start = paragraph.get('start', 0.0)
                para_end = paragraph.get('end', 0.0)
                para_speaker = paragraph.get('speaker', 0)
                logger.info(f"Processing paragraph {idx}: speaker={para_speaker}, length={len(para_text)}, start={para_start}, end={para_end}")
                
                # Insert paragraph and get paragraph_id
                cursor.execute("""
                    DECLARE @paragraph_id INT;
                    EXEC RDS_InsertParagraph
                    @fileid = %s,
                    @paragraph_idx = %s,
                    @text = %s,
                    @start_time = %s,
                    @end_time = %s,
                    @speaker = %s,
                    @num_words = %s,
                    @paragraph_id = @paragraph_id OUTPUT;
                    SELECT @paragraph_id;
                    """, (
                        fileid,
                        idx,
                        para_text,
                        para_start,
                        para_end,
                        str(para_speaker),
                        len(para_text.split())
                    ))
                    
                # Get the paragraph_id returned from the stored procedure
                paragraph_id = cursor.fetchone()[0]
                para_count += 1
                
                # Process sentences in this paragraph
                # Here we just split by period as a simple approach,
                # a real implementation would use the actual sentences from Deepgram
                sentences = para_text.split('.')
                for sent_idx, sentence in enumerate(sentences):
                    if not sentence.strip():
                        continue
                        
                    # For simplicity, distribute time evenly across sentences
                    sent_duration = (para_end - para_start) / max(1, len(sentences))
                    sent_start = para_start + (sent_idx * sent_duration)
                    sent_end = sent_start + sent_duration
                    
                    cursor.execute("""
                    EXEC RDS_InsertSentence
                    @fileid = %s,
                    @paragraph_id = %s,
                    @sentence_idx = %s,
                    @text = %s,
                    @start_time = %s,
                    @end_time = %s
                    """, (
                        fileid,
                        paragraph_id,
                        sent_idx,
                        sentence.strip() + '.',
                        sent_start,
                        sent_end
                    ))
                    sent_count += 1
        except Exception as e:
            logger.error(f"Error processing paragraphs/sentences: {str(e)}")
            # Continue with the rest of the function, don't throw exception
        
        # Commit transaction and close connection
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully stored transcription results in SQL database. Paragraphs: {para_count}, Sentences: {sent_count}")
        return True, {"paragraphs": para_count, "sentences": sent_count}
    
    except Exception as e:
        logger.error(f"Error storing in SQL database: {str(e)}")
        return False, {"error": str(e)}


# ============================
# MAIN FUNCTION
# ============================

def main():
    """Main function to run the test"""
    
    try:
        # List available audio blobs
        logger.info("Listing audio blobs in the container...")
        blobs = list_audio_blobs()
        
        if not blobs:
            logger.error("No audio blobs found in the container.")
            return
        
        # Use the first audio blob for testing
        blob_name = blobs[0]
        logger.info(f"Using blob for testing: {blob_name}")
        
        # Generate SAS URL for the blob
        blob_sas_url = generate_blob_sas_url(blob_name)
        if not blob_sas_url:
            logger.error("Failed to generate SAS URL.")
            return
        
        # Create a unique file ID
        fileid = f"test_{uuid.uuid4().hex[:16]}"
        logger.info(f"Using file ID: {fileid}")
        
        # Download the blob to a temporary file
        temp_file = download_blob_to_temp_file(blob_sas_url)
        if not temp_file:
            logger.error("Failed to download blob to temporary file.")
            return
        
        try:
            # Transcribe the audio file
            transcription_result = transcribe_audio_file(temp_file)
            
            # Store the results in the SQL database
            success, storage_result = store_in_sql_database(fileid, blob_name, transcription_result)
            
            if success:
                logger.info(f"Integration test completed successfully! Storage result: {storage_result}")
            else:
                logger.error(f"Failed to store transcription results: {storage_result}")
        
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_file):
                os.remove(temp_file)
                logger.info(f"Temporary file removed: {temp_file}")
    
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")


if __name__ == "__main__":
    main()