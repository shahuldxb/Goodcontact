#!/usr/bin/env python3
"""
Database integration for DirectTranscribe
Handles the database operations for the DirectTranscribe class,
storing transcription results in the Azure SQL database.
"""

import os
import json
import uuid
import time
import logging
import pymssql
from datetime import datetime
from direct_sql_connection import DirectSQLConnection

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DirectTranscribeDB:
    """
    Database integration for DirectTranscribe.
    Handles storing transcription results in the Azure SQL database.
    """
    
    def __init__(self, sql_conn_params=None):
        """
        Initialize the DirectTranscribeDB class.
        
        Args:
            sql_conn_params: Dictionary with SQL connection parameters.
                             If None, will use environment variables.
        """
        # Extract connection parameters from sql_conn_params or fall back to environment variables
        self.sql_conn_params = sql_conn_params or {}
        
        # Set default values from environment variables if not provided
        server = self.sql_conn_params.get('server') or os.environ.get('AZURE_SQL_SERVER', 'callcenter1.database.windows.net')
        database = self.sql_conn_params.get('database') or os.environ.get('AZURE_SQL_DATABASE', 'call')
        user = self.sql_conn_params.get('user') or self.sql_conn_params.get('username') or os.environ.get('AZURE_SQL_USER', 'shahul')
        password = self.sql_conn_params.get('password') or os.environ.get('AZURE_SQL_PASSWORD', 'apple123!@#')
        
        # Create DirectSQLConnection for reliable database access
        self.sql = DirectSQLConnection(
            server=server,
            database=database,
            user=user,
            password=password
        )
        
        logger.info(f"DirectTranscribeDB initialized with Azure SQL connection to {server}/{database}")
    
    def store_transcription_result(self, processing_result):
        """
        Store the complete transcription result in the database.
        This method assumes the processing_result contains:
        1. Transcription data
        2. File movement data (destination path)
        3. Processing time information
        
        Args:
            processing_result: The complete result from DirectTranscribe.process_file()
            
        Returns:
            dict: Result of the database operations
        """
        start_time = time.time()
        
        try:
            # Extract key information from the processing result
            blob_name = processing_result.get('blob_name', '')
            source_container = processing_result.get('source_container', 'shahulin')
            destination_container = processing_result.get('destination_container', 'shahulout')
            
            # Extract transcription result
            transcription = processing_result.get('transcription', {})
            transcription_result = transcription.get('result')
            transcription_error = transcription.get('error')
            
            # Extract file movement result
            file_movement = processing_result.get('file_movement', {})
            destination_url = file_movement.get('destination_url', '')
            
            # Extract processing times
            processing_times = processing_result.get('processing_times', {})
            total_processing_time = processing_times.get('total_processing_time', 0)
            
            # Generate a file ID if not present
            fileid = processing_result.get('process_id') or processing_result.get('fileid') or f"{int(time.time())}_{blob_name}"
            
            # CRITICAL DATA INTEGRITY CHECK: Do not insert records with failed or empty transcriptions
            # Properly evaluate if transcription was successful by checking both error state and content
            transcription_is_valid = False
            
            # If there's an error, we definitely don't have a valid transcription
            if transcription_error:
                error_message = transcription_error.get('message') if isinstance(transcription_error, dict) else str(transcription_error)
                logger.error(f"Cannot store transcription result - transcription failed with error: {error_message}")
                return {
                    "status": "error",
                    "message": f"Transcription failed: {error_message}",
                    "db_operations_performed": 0,
                    "fileid": fileid
                }
                
            # Even if no error, verify transcription has actual content
            if not transcription_result:
                logger.error(f"Cannot store transcription result - transcription result is empty or null")
                return {
                    "status": "error", 
                    "message": "Transcription failed: Empty or null result without explicit error",
                    "db_operations_performed": 0,
                    "fileid": fileid
                }
                
            # Verify transcription has actual content (not just an empty structure)
            if isinstance(transcription_result, dict):
                # Check for transcript content in common Deepgram response patterns
                transcript_text = ""
                
                # Check standard results path for transcript content
                if "results" in transcription_result:
                    results = transcription_result["results"]
                    
                    # Check channels path
                    if "channels" in results and results["channels"]:
                        channels = results["channels"]
                        for channel in channels:
                            if "alternatives" in channel and channel["alternatives"]:
                                for alt in channel["alternatives"]:
                                    if "transcript" in alt and alt["transcript"]:
                                        transcript_text += alt["transcript"] + " "
                
                # Check direct transcript field as fallback
                if not transcript_text and "transcript" in transcription_result:
                    transcript_text = transcription_result["transcript"]
                
                # If we still have no transcript text, reject the record
                if not transcript_text.strip():
                    logger.error(f"Cannot store transcription result - no transcript text found in result structure")
                    return {
                        "status": "error", 
                        "message": "Transcription failed: No transcript text found in result structure",
                        "db_operations_performed": 0,
                        "fileid": fileid
                    }
                    
                # We have verified there is actual transcript content
                transcription_is_valid = True
                logger.info(f"Verified transcription has content: {len(transcript_text)} characters")
            
            # 1. Insert into rdt_assets with full data
            logger.info(f"Storing complete transcription result for file {fileid}")
            
            # Serialize transcription result to JSON
            transcription_json = json.dumps(transcription_result)
            
            # Prepare parameters
            source_path = f"{source_container}/{blob_name}"
            destination_path = f"{destination_container}/{blob_name}"
            
            # Current timestamp
            now = datetime.utcnow()
            
            # Insert the record
            query = """
                INSERT INTO rdt_assets 
                (fileid, filename, source_path, destination_path, 
                processing_duration, transcription, transcription_json, 
                status, created_dt, created_by, file_size, upload_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            params = (
                fileid, 
                blob_name, 
                source_path,
                destination_path,
                total_processing_time,
                transcript_text,  # Plain text for easy searching
                transcription_json,  # Full JSON for detailed analysis
                'completed',
                now,
                1,  # Created by user ID 1 (default system user)
                processing_result.get("file_size", 0),  # File size in bytes, use 0 if not available
                now  # Upload date is now since we're processing it now
            )
            
            try:
                self.sql.execute_non_query(query, params)
                logger.info(f"Inserted record into rdt_assets for file {fileid}")
            except Exception as e:
                logger.error(f"Error inserting into rdt_assets: {str(e)}")
                return {
                    "status": "error",
                    "message": f"Database error: {str(e)}",
                    "fileid": fileid,
                    "db_operations_performed": 0
                }
            
            # Process paragraphs and sentences
            paragraphs_processed = 0
            sentences_processed = 0
            
            # Process paragraphs and sentences
            if transcription_result and 'results' in transcription_result:
                try:
                    # Build a list of paragraphs with their sentences
                    paragraphs = []
                    
                    # Start with utterances if available
                    if 'utterances' in transcription_result['results']:
                        for i, utterance in enumerate(transcription_result['results']['utterances']):
                            paragraphs.append({
                                'text': utterance.get('transcript', ''),
                                'start': utterance.get('start', 0),
                                'end': utterance.get('end', 0),
                                'speaker': utterance.get('speaker', f"speaker_{utterance.get('speaker_id', i)}"),
                                'num_words': len(utterance.get('transcript', '').split()),
                                'sentences': [{
                                    'id': f"{i}_0",
                                    'text': utterance.get('transcript', ''),
                                    'start': utterance.get('start', 0),
                                    'end': utterance.get('end', 0)
                                }]
                            })
                    
                    # Check for paragraphs
                    if 'paragraphs' in transcription_result['results']:
                        # API structure has changed over time, so we need to handle different structures
                        paragraph_data = transcription_result['results']['paragraphs']
                        
                        # Different Deepgram API versions return different structures
                        if isinstance(paragraph_data, dict) and 'paragraphs' in paragraph_data:
                            # New structure: results.paragraphs.paragraphs[]
                            paragraph_list = paragraph_data['paragraphs']
                        else:
                            # Old structure: results.paragraphs[]
                            paragraph_list = paragraph_data
                            
                        paragraphs = []
                        for i, paragraph in enumerate(paragraph_list):
                            # Process individual sentences if available
                            sentences = []
                            if 'sentences' in paragraph:
                                for j, sentence in enumerate(paragraph['sentences']):
                                    sentences.append({
                                        'id': f"{i}_{j}",
                                        'text': sentence.get('text', ''),
                                        'start': sentence.get('start', 0),
                                        'end': sentence.get('end', 0)
                                    })
                            
                            # Add complete paragraph info
                            paragraphs.append({
                                'text': paragraph.get('text', ''),
                                'start': paragraph.get('start', 0),
                                'end': paragraph.get('end', 0),
                                'speaker': paragraph.get('speaker', f"speaker_{paragraph.get('speaker_id', i)}"),
                                'num_words': len(paragraph.get('text', '').split()),
                                'sentences': sentences
                            })
                    
                    # If we have no paragraphs yet, try one last source
                    if not paragraphs and 'channels' in transcription_result['results']:
                        # Get transcript from first channel and break it into paragraphs by empty lines
                        channel = transcription_result['results']['channels'][0]
                        if 'alternatives' in channel and channel['alternatives']:
                            alt = channel['alternatives'][0]
                            if 'paragraphs' in alt:
                                para_data = alt['paragraphs']
                                if isinstance(para_data, dict) and 'paragraphs' in para_data:
                                    paragraph_list = para_data['paragraphs']
                                    
                                    for i, paragraph in enumerate(paragraph_list):
                                        sentences = []
                                        if 'sentences' in paragraph:
                                            for j, sentence in enumerate(paragraph['sentences']):
                                                sentences.append({
                                                    'id': f"{i}_{j}",
                                                    'text': sentence.get('text', ''),
                                                    'start': sentence.get('start', 0),
                                                    'end': sentence.get('end', 0)
                                                })
                                        
                                        # Add paragraph info
                                        paragraphs.append({
                                            'text': paragraph.get('text', ''),
                                            'start': paragraph.get('start', 0),
                                            'end': paragraph.get('end', 0),
                                            'speaker': paragraph.get('speaker', 'unknown'),
                                            'num_words': len(paragraph.get('text', '').split()),
                                            'sentences': sentences
                                        })
                    
                    # If we still have no paragraphs, create a single paragraph
                    if not paragraphs:
                        logger.warning(f"No paragraphs found in API response, creating single paragraph")
                        channel = transcription_result['results']['channels'][0]
                        if 'alternatives' in channel and channel['alternatives']:
                            alt = channel['alternatives'][0]
                            if 'transcript' in alt:
                                transcript = alt['transcript']
                                paragraphs.append({
                                    'text': transcript,
                                    'start': 0,
                                    'end': transcription_result.get('duration', 0),
                                    'speaker': 'unknown',
                                    'num_words': len(transcript.split()),
                                    'sentences': [{
                                        'id': "0_0",
                                        'text': transcript,
                                        'start': 0,
                                        'end': transcription_result.get('duration', 0)
                                    }]
                                })
                    
                    # Process and store paragraphs
                    for para_idx, paragraph in enumerate(paragraphs):
                        # Insert paragraph
                        para_query = """
                        INSERT INTO rdt_paragraphs 
                        (fileid, paragraph_idx, text, start_time, end_time, speaker, created_dt)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        # Include speaker information if available
                        speaker = paragraph.get('speaker', 'unknown')
                        
                        para_params = (
                            fileid,
                            para_idx,
                            paragraph.get('text', ''),
                            paragraph.get('start', 0),
                            paragraph.get('end', 0),
                            speaker,
                            now
                        )
                        
                        try:
                            # Insert paragraph
                            self.sql.execute_non_query(para_query, para_params)
                            paragraphs_processed += 1
                            
                            # Get the paragraph ID for sentence relationships
                            get_para_id_query = """
                            SELECT id FROM rdt_paragraphs 
                            WHERE fileid = %s AND paragraph_idx = %s
                            """
                            para_id_result = self.sql.execute_query(get_para_id_query, (fileid, para_idx))
                            paragraph_id = para_id_result[0][0] if para_id_result and len(para_id_result) > 0 else None
                            
                            # Process sentences if we got a valid paragraph ID
                            if paragraph_id:
                                for sent_idx, sentence in enumerate(paragraph.get('sentences', [])):
                                    sent_query = """
                                    INSERT INTO rdt_sentences 
                                    (fileid, paragraph_id, sentence_idx, text, start_time, end_time, created_dt)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    """
                                    
                                    sent_params = (
                                        fileid,
                                        paragraph_id,
                                        sent_idx,
                                        sentence.get('text', ''),
                                        sentence.get('start', 0),
                                        sentence.get('end', 0),
                                        now
                                    )
                                    
                                    try:
                                        self.sql.execute_non_query(sent_query, sent_params)
                                        sentences_processed += 1
                                    except Exception as e:
                                        logger.error(f"Error inserting sentence {sent_idx}: {str(e)}")
                            else:
                                logger.warning(f"Could not retrieve paragraph_id for paragraph {para_idx}")
                        except Exception as e:
                            logger.error(f"Error inserting paragraph {para_idx}: {str(e)}")
                            
                except Exception as e:
                    logger.error(f"Error processing paragraphs and sentences: {str(e)}")
                    # Continue processing - we still have the main record
            
            # Return success results
            elapsed_time = time.time() - start_time
            logger.info(f"Database operations completed in {elapsed_time:.2f} seconds")
            return {
                "status": "success",
                "message": f"Successfully stored transcription with {paragraphs_processed} paragraphs and {sentences_processed} sentences",
                "fileid": fileid,
                "db_operations_performed": 1 + paragraphs_processed + sentences_processed,
                "paragraphs_processed": paragraphs_processed,
                "sentences_processed": sentences_processed,
                "processing_time": elapsed_time
            }
        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(f"Error in store_transcription_result: {str(e)}")
            return {
                "status": "error",
                "message": f"Database error: {str(e)}",
                "db_operations_performed": 0,
                "fileid": fileid if 'fileid' in locals() else None,
                "processing_time": elapsed_time
            }
    
    def test_connection(self):
        """Test the connection to the database"""
        return self.sql.test_connection()