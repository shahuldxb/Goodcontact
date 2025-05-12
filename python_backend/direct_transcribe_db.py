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
        # SQL connection parameters
        self.sql_conn_params = sql_conn_params or {}
        
        # Set default values from environment variables if not provided
        server = self.sql_conn_params.get('server') or os.environ.get('AZURE_SQL_SERVER', 'callcenter1.database.windows.net')
        database = self.sql_conn_params.get('database') or os.environ.get('AZURE_SQL_DATABASE', 'call')
        username = self.sql_conn_params.get('username') or os.environ.get('AZURE_SQL_USER', 'shahul')
        password = self.sql_conn_params.get('password') or os.environ.get('AZURE_SQL_PASSWORD', 'apple123!@#')
        
        # Check for connection string (for backward compatibility)
        self.conn_string = os.environ.get('AZURE_SQL_CONNECTION_STRING') or os.environ.get('DATABASE_URL')
        
        if self.conn_string:
            logger.info("Using connection string for Azure SQL database")
        else:
            # Use individual connection parameters
            self.sql_conn_params = {
                'server': server,
                'database': database,
                'user': username,
                'password': password
            }
            logger.info(f"Using explicit parameters for Azure SQL database: {server}/{database}")
    
    def _get_connection(self):
        """
        Get an SQL connection.
        
        Returns:
            pymssql.Connection: An active database connection
        """
        try:
            if self.conn_string:
                # Parse the connection string
                parts = self.conn_string.split(';')
                params = {}
                for part in parts:
                    if '=' in part:
                        key, value = part.split('=', 1)
                        params[key.strip()] = value.strip()
                
                # Extract connection parameters
                server = params.get('Server', '')
                if ',' in server:
                    server = server.split(',')[0]  # Remove port if present
                
                database = params.get('Database', '')
                user = params.get('User ID', '')
                password = params.get('Password', '')
                
                # Create connection
                conn = pymssql.connect(server=server, database=database, user=user, password=password)
            else:
                # Use explicit parameters
                conn = pymssql.connect(**self.sql_conn_params)
            
            return conn
        except Exception as e:
            logger.error(f"Error connecting to SQL database: {str(e)}")
            raise
    
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
            
            # This line is no longer needed because we generate the fileid earlier
            # fileid = processing_result.get('process_id') or f"{int(time.time())}_{blob_name}"
            
            # Get database connection
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # 1. Insert into rdt_asset with full data
            logger.info(f"Storing complete transcription result for file {fileid}")
            
            # Serialize transcription result to JSON
            transcription_json = json.dumps(transcription_result)
            
            # Prepare parameters
            source_path = f"{source_container}/{blob_name}"
            destination_path = f"{destination_container}/{blob_name}"
            
            # Current timestamp
            now = datetime.utcnow().isoformat()
            
            # Insert the record
            cursor.execute("""
                INSERT INTO rdt_asset 
                (fileid, filename, source_container, source_path, destination_container, 
                destination_path, processing_time, transcription, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                fileid, 
                blob_name, 
                source_container,
                source_path,
                destination_container,
                destination_path,
                total_processing_time,
                transcription_json,
                'COMPLETED',
                now
            ))
            
            # Commit the transaction
            conn.commit()
            logger.info(f"Inserted record into rdt_asset for file {fileid}")
            
            # 2. Process audio metadata
            if transcription_result:
                try:
                    # Extract metadata from transcription result
                    request_id = transcription_result.get('request_id', '')
                    sha256 = transcription_result.get('sha256', '')
                    created = transcription_result.get('created', '')
                    duration = transcription_result.get('duration', 0)
                    
                    # Get confidence from channels if available
                    confidence = 0
                    if 'results' in transcription_result and 'channels' in transcription_result['results']:
                        channels = transcription_result['results']['channels']
                        if channels and 'alternatives' in channels[0]:
                            alternatives = channels[0]['alternatives']
                            if alternatives and 'confidence' in alternatives[0]:
                                confidence = alternatives[0]['confidence']
                    
                    # Execute the stored procedure
                    cursor.execute(
                        "EXEC RDS_InsertAudioMetadata @fileid=%s, @request_id=%s, @sha256=%s, @created_timestamp=%s, @audio_duration=%s, @confidence=%s",
                        (fileid, request_id, sha256, created, duration, confidence)
                    )
                    
                    # Commit after audio metadata
                    conn.commit()
                    logger.info(f"Inserted audio metadata for file {fileid}")
                    
                except Exception as e:
                    logger.error(f"Error storing audio metadata for {fileid}: {str(e)}")
                    # Continue processing - don't stop if metadata storage fails
            
            # 3. Process paragraphs and sentences
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
                    
                    # Use the paragraph-level data if available (more accurate)
                    if 'paragraphs' in transcription_result['results']:
                        paragraphs = []
                        for i, paragraph in enumerate(transcription_result['results']['paragraphs']):
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
                            else:
                                # If no sentences, use the paragraph as a single sentence
                                sentences.append({
                                    'id': f"{i}_0",
                                    'text': paragraph.get('text', ''),
                                    'start': paragraph.get('start', 0),
                                    'end': paragraph.get('end', 0)
                                })
                            
                            paragraphs.append({
                                'text': paragraph.get('text', ''),
                                'start': paragraph.get('start', 0),
                                'end': paragraph.get('end', 0),
                                'speaker': paragraph.get('speaker', f"speaker_{paragraph.get('speaker_id', i)}"),
                                'num_words': len(paragraph.get('text', '').split()),
                                'sentences': sentences
                            })
                    
                    # If no structured data is available, extract from the transcript
                    if not paragraphs and 'channels' in transcription_result['results']:
                        full_transcript = transcription_result['results']['channels'][0]['alternatives'][0]['transcript']
                        paragraphs.append({
                            'text': full_transcript,
                            'start': 0,
                            'end': transcription_result.get('duration', 0),
                            'speaker': 'unknown',
                            'num_words': len(full_transcript.split()),
                            'sentences': [{
                                'id': "0_0",
                                'text': full_transcript,
                                'start': 0,
                                'end': transcription_result.get('duration', 0)
                            }]
                        })
                    
                    # Process and store all paragraphs and sentences
                    for para_idx, paragraph in enumerate(paragraphs):
                        para_text = paragraph.get('text', '')
                        para_start = paragraph.get('start', 0)
                        para_end = paragraph.get('end', 0)
                        para_speaker = paragraph.get('speaker', 'unknown')
                        para_num_words = paragraph.get('num_words', 0)
                        
                        # Execute the stored procedure and get the new paragraph ID
                        cursor.execute(
                            "DECLARE @new_para_id INT; EXEC RDS_InsertParagraph @fileid=%s, @paragraph_idx=%s, @text=%s, @start_time=%s, @end_time=%s, @speaker=%s, @num_words=%s, @paragraph_id=@new_para_id OUTPUT; SELECT @new_para_id;",
                            (fileid, para_idx, para_text, para_start, para_end, para_speaker, para_num_words)
                        )
                        
                        # Get the paragraph ID
                        result = cursor.fetchone()
                        paragraph_id = result[0] if result else None
                        
                        # Commit after each paragraph
                        conn.commit()
                        
                        if not paragraph_id:
                            logger.warning(f"Failed to get paragraph ID for paragraph {para_idx} in file {fileid}")
                            continue
                        
                        # Insert sentences for this paragraph
                        for sent in paragraph.get('sentences', []):
                            sent_idx = sent.get('id', f"{para_idx}_0")
                            sent_text = sent.get('text', '')
                            sent_start = sent.get('start', 0)
                            sent_end = sent.get('end', 0)
                            
                            # Execute the stored procedure
                            cursor.execute(
                                "EXEC RDS_InsertSentence @fileid=%s, @paragraph_id=%s, @sentence_idx=%s, @text=%s, @start_time=%s, @end_time=%s",
                                (fileid, paragraph_id, sent_idx, sent_text, sent_start, sent_end)
                            )
                            
                            # Commit after each sentence
                            conn.commit()
                    
                    logger.info(f"Stored {len(paragraphs)} paragraphs for file {fileid}")
                    
                except Exception as e:
                    logger.error(f"Error processing paragraphs for {fileid}: {str(e)}")
                    import traceback
                    logger.error(traceback.format_exc())
            
            # Close the connection
            cursor.close()
            conn.close()
            
            # Calculate database operation time
            db_operation_time = time.time() - start_time
            
            return {
                "status": "success",
                "message": f"Successfully stored transcription result for file {fileid}",
                "fileid": fileid,
                "blob_name": blob_name,
                "paragraphs_processed": len(paragraphs) if 'paragraphs' in locals() else 0,
                "db_operation_time": db_operation_time
            }
            
        except Exception as e:
            logger.error(f"Error storing transcription result: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            
            # Analyze transcription result to log details about paragraphs and sentences
            # This will help us verify the data is correctly extracted even if DB storage fails
            try:
                # Extract transcription result
                transcription_result = processing_result.get('transcription', {}).get('result', {})
                fileid = processing_result.get('fileid', 'unknown')
                
                # Try to find paragraphs
                paragraphs = []
                if 'results' in transcription_result and 'paragraphs' in transcription_result['results']:
                    if 'paragraphs' in transcription_result['results']['paragraphs']:
                        paragraphs = transcription_result['results']['paragraphs']['paragraphs']
                
                # Log number of paragraphs found
                logger.info(f"Found {len(paragraphs)} paragraphs in transcription for fileid {fileid}")
                
                # Log first paragraph details if available
                if paragraphs:
                    first_para = paragraphs[0]
                    logger.info(f"First paragraph text: {first_para.get('text', '')[:100]}...")
                    
                    # Check for sentences
                    sentences = first_para.get('sentences', [])
                    logger.info(f"Found {len(sentences)} sentences in first paragraph")
                    
                    # Log first sentence if available
                    if sentences:
                        logger.info(f"First sentence text: {sentences[0].get('text', '')}")
            except Exception as parse_err:
                logger.error(f"Error analyzing transcription content: {str(parse_err)}")
            
            return {
                "status": "error",
                "message": f"Failed to store transcription result: {str(e)}",
                "db_operations_performed": 0
            }

# Example usage
if __name__ == "__main__":
    # Get a test result
    import json
    from direct_transcribe import DirectTranscribe
    
    # Create a DirectTranscribe instance
    transcriber = DirectTranscribe()
    
    # Process a file
    result = transcriber.process_file("agricultural_leasing_(ijarah)_normal.mp3")
    
    # Create a database instance
    db = DirectTranscribeDB()
    
    # Store the result
    db_result = db.store_transcription_result(result)
    
    # Print the result
    print(json.dumps(db_result, indent=2))