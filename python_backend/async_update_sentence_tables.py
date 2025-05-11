"""
Async Update Sentence Tables

An asynchronous version of update_sentence_tables.py for better performance
in asyncio contexts. This stores detailed paragraph and sentence information
using async database connections.
"""

import os
import json
import traceback
import logging
import aiopg
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def store_transcription_details_async(fileid, transcription_json):
    """
    Store detailed transcription data in database tables asynchronously.
    
    This function stores:
    1. Metadata in rdt_audio_metadata
    2. Paragraphs in rdt_paragraphs
    3. Sentences in rdt_sentences
    
    Args:
        fileid (str): The unique identifier for the file
        transcription_json (dict): The transcription response from Deepgram
        
    Returns:
        dict: Result with operation status and counts
    """
    result = {
        'status': 'success',
        'metadata_stored': False,
        'paragraphs_stored': 0,
        'sentences_stored': 0
    }
    
    logger.info(f"Storing detailed transcription data for fileid: {fileid}")
    
    try:
        # Azure SQL connection string
        host = os.environ.get("PGHOST", "callcenter1.database.windows.net")
        user = os.environ.get("PGUSER", "shahul")
        password = os.environ.get("PGPASSWORD", "apple123!@#") 
        database = os.environ.get("PGDATABASE", "callcenter")
        port = os.environ.get("PGPORT", "5432")
        
        # Build DSN string
        dsn = f"dbname={database} user={user} password={password} host={host} port={port}"
        
        logger.info("Connecting to database...")
        
        async with aiopg.connect(dsn=dsn) as conn:
            async with conn.cursor() as cursor:
                # Extract metadata fields from transcription
                request_id = transcription_json.get('request_id', '')
                sha256 = transcription_json.get('sha256', '')
                created = transcription_json.get('created', datetime.now().isoformat())
                audio_duration = transcription_json.get('duration', 0)
                
                # Extract confidence if available
                confidence = 0.0
                if 'results' in transcription_json and 'channels' in transcription_json['results']:
                    channels = transcription_json['results']['channels']
                    if channels and len(channels) > 0:
                        alternatives = channels[0].get('alternatives', [])
                        if alternatives and len(alternatives) > 0:
                            confidence = alternatives[0].get('confidence', 0.0)
                
                # Insert metadata
                logger.info(f"Storing metadata for fileid: {fileid}")
                await cursor.execute(
                    "EXEC RDS_InsertAudioMetadata @fileid=%s, @request_id=%s, @sha256=%s, @created_timestamp=%s, @audio_duration=%s, @confidence=%s",
                    (fileid, request_id, sha256, created, audio_duration, confidence)
                )
                result['metadata_stored'] = True
                
                # Extract paragraphs and sentences
                paragraphs = []
                
                # Method 1: Try to get paragraphs directly
                if 'results' in transcription_json and 'paragraphs' in transcription_json['results']:
                    if 'paragraphs' in transcription_json['results']['paragraphs']:
                        paragraphs = transcription_json['results']['paragraphs']['paragraphs']
                        
                # If we extracted paragraphs, store them
                if paragraphs:
                    logger.info(f"Found {len(paragraphs)} paragraphs to store")
                    
                    for para_idx, para in enumerate(paragraphs):
                        paragraph_id = None
                        para_text = para.get('text', '')
                        para_start = para.get('start', 0)
                        para_end = para.get('end', 0)
                        speaker = para.get('speaker', 0)
                        num_words = len(para_text.split())
                        
                        # Insert paragraph and get its ID
                        logger.debug(f"Inserting paragraph {para_idx}: {para_text[:30]}...")
                        
                        # Use RDS_ prefixed stored procedure
                        await cursor.execute(
                            "DECLARE @new_para_id INT; EXEC RDS_InsertParagraph @fileid=%s, @paragraph_idx=%s, @text=%s, @start_time=%s, @end_time=%s, @speaker=%s, @num_words=%s, @paragraph_id=@new_para_id OUTPUT; SELECT @new_para_id;",
                            (fileid, para_idx, para_text, para_start, para_end, speaker, num_words)
                        )
                        
                        result_row = await cursor.fetchone()
                        if result_row:
                            paragraph_id = result_row[0]
                            result['paragraphs_stored'] += 1
                        
                        # If sentences are available, store them
                        if paragraph_id and 'sentences' in para:
                            for sent in para['sentences']:
                                sent_idx = sent.get('id', f"{para_idx}_0")
                                sent_text = sent.get('text', '')
                                sent_start = sent.get('start', 0)
                                sent_end = sent.get('end', 0)
                                
                                # Execute the stored procedure with RDS_ prefix
                                await cursor.execute(
                                    "EXEC RDS_InsertSentence @fileid=%s, @paragraph_id=%s, @sentence_idx=%s, @text=%s, @start_time=%s, @end_time=%s",
                                    (fileid, paragraph_id, sent_idx, sent_text, sent_start, sent_end)
                                )
                                result['sentences_stored'] += 1
                                
                    logger.info(f"Stored {result['paragraphs_stored']} paragraphs and {result['sentences_stored']} sentences")
                
                else:
                    logger.warning(f"No paragraphs found in transcription JSON for fileid: {fileid}")
                
                return result
                
    except Exception as e:
        error_message = f"Error storing transcription details: {str(e)}"
        logger.error(error_message)
        logger.error(traceback.format_exc())
        
        result = {
            'status': 'error',
            'message': error_message,
            'metadata_stored': result['metadata_stored'],
            'paragraphs_stored': result['paragraphs_stored'],
            'sentences_stored': result['sentences_stored']
        }
        
        return result