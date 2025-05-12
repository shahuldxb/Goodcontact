#!/usr/bin/env python3
"""
Enhanced DirectTranscribeDB Class
This class handles storing transcription results in the Azure SQL database using
DirectSQLConnection for reliable database access.
"""
import os
import json
import time
import logging
from datetime import datetime
from direct_sql_connection import DirectSQLConnection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DirectTranscribeDBEnhanced:
    """
    Enhanced database integration for DirectTranscribe.
    Uses DirectSQLConnection for reliable database access.
    """
    
    def __init__(self, server=None, database=None, user=None, password=None):
        """
        Initialize the DirectTranscribeDBEnhanced class with Direct SQL Connection
        
        Args:
            server (str): SQL Server hostname
            database (str): Database name
            user (str): Database username
            password (str): Database password
        """
        # Create DirectSQLConnection for reliable database access
        self.sql = DirectSQLConnection(
            server=server or os.environ.get("AZURE_SQL_SERVER", "callcenter1.database.windows.net"),
            database=database or os.environ.get("AZURE_SQL_DATABASE", "call"),
            user=user or os.environ.get("AZURE_SQL_USER", "shahul"),
            password=password or os.environ.get("AZURE_SQL_PASSWORD", "apple123!@#")
        )
        
        logger.info(f"Enhanced DirectTranscribeDB initialized with reliable SQL connection")
    
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
                logger.error(f"Transcription failed for {blob_name}: {error_message}")
                return {
                    "status": "error",
                    "message": f"Transcription failed: {error_message}",
                    "fileid": fileid
                }
            
            # Check if transcription_result is present and has expected structure
            if not transcription_result:
                logger.error(f"No transcription result for {blob_name}")
                return {
                    "status": "error",
                    "message": "No transcription result",
                    "fileid": fileid
                }
            
            # Get transcript text
            transcript_text = transcription.get('transcript', '')
            if not transcript_text or len(transcript_text) < 10:
                logger.error(f"Empty or very short transcript for {blob_name}: {transcript_text}")
                return {
                    "status": "error",
                    "message": "Empty or very short transcript",
                    "fileid": fileid
                }
            
            # If we get here, we have a valid transcription
            transcription_is_valid = True
            
            # Store the main asset record first
            transcription_json_str = json.dumps(transcription_result)
            
            # Insert into rdt_asset table using our reliable connection
            query = """
            INSERT INTO rdt_asset 
            (fileid, orig_filename, transcription, source_container, dest_container, insert_time, file_size, transcription_duration)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            params = (
                fileid, 
                blob_name, 
                transcription_json_str, 
                source_container, 
                destination_container,
                datetime.now(),
                0,  # file_size (not tracked)
                total_processing_time or 0
            )
            
            try:
                self.sql.execute_non_query(query, params)
                logger.info(f"Successfully inserted record into rdt_asset for {fileid}")
            except Exception as e:
                logger.error(f"Error inserting into rdt_asset: {str(e)}")
                return {
                    "status": "error",
                    "message": f"Database error: {str(e)}",
                    "fileid": fileid
                }
            
            # Process paragraphs and sentences
            paragraphs_processed = 0
            sentences_processed = 0
            
            # Extract paragraphs from the transcription result
            # This logic assumes the structure we've seen in successful responses
            paragraphs = []
            
            # Try different structures for where paragraphs might be located
            try:
                if "results" in transcription_result and "paragraphs" in transcription_result["results"]:
                    if "paragraphs" in transcription_result["results"]["paragraphs"]:
                        paragraphs = transcription_result["results"]["paragraphs"]["paragraphs"]
                        logger.info(f"Found {len(paragraphs)} paragraphs in structure 1")
                        
                # Structure 2: In channels > alternatives
                if not paragraphs and "results" in transcription_result and "channels" in transcription_result["results"]:
                    channels = transcription_result["results"]["channels"]
                    for channel in channels:
                        if "alternatives" in channel:
                            alternatives = channel["alternatives"]
                            for alternative in alternatives:
                                if "paragraphs" in alternative:
                                    if "paragraphs" in alternative["paragraphs"]:
                                        paragraphs = alternative["paragraphs"]["paragraphs"]
                                        logger.info(f"Found {len(paragraphs)} paragraphs in structure 2")
                                        break
                            if paragraphs:
                                break
                        if paragraphs:
                            break
            except Exception as e:
                logger.error(f"Error extracting paragraphs: {str(e)}")
                # Continue anyway - we'll still have the main transcription stored
            
            # Process and store paragraphs and sentences
            if paragraphs:
                for para_idx, paragraph in enumerate(paragraphs):
                    try:
                        paragraph_text = paragraph.get("text", "")
                        paragraph_start = paragraph.get("start", 0)
                        paragraph_end = paragraph.get("end", 0)
                        
                        # Insert paragraph
                        para_query = """
                        INSERT INTO rdt_paragraphs 
                        (fileid, paragraph_index, paragraph_text, start_time, end_time)
                        VALUES (%s, %s, %s, %s, %s)
                        """
                        
                        para_params = (
                            fileid,
                            para_idx,
                            paragraph_text,
                            paragraph_start,
                            paragraph_end
                        )
                        
                        self.sql.execute_non_query(para_query, para_params)
                        paragraphs_processed += 1
                        
                        # Process sentences for this paragraph
                        sentences = paragraph.get("sentences", [])
                        for sent_idx, sentence in enumerate(sentences):
                            try:
                                sentence_text = sentence.get("text", "")
                                sentence_start = sentence.get("start", 0)
                                sentence_end = sentence.get("end", 0)
                                
                                # Insert sentence
                                sent_query = """
                                INSERT INTO rdt_sentences 
                                (fileid, paragraph_index, sentence_index, sentence_text, start_time, end_time)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                """
                                
                                sent_params = (
                                    fileid,
                                    para_idx,
                                    sent_idx,
                                    sentence_text,
                                    sentence_start,
                                    sentence_end
                                )
                                
                                self.sql.execute_non_query(sent_query, sent_params)
                                sentences_processed += 1
                            except Exception as e:
                                logger.error(f"Error inserting sentence {sent_idx} in paragraph {para_idx}: {str(e)}")
                    except Exception as e:
                        logger.error(f"Error inserting paragraph {para_idx}: {str(e)}")
            
            elapsed_time = time.time() - start_time
            logger.info(f"Database operations completed in {elapsed_time:.2f} seconds")
            logger.info(f"Processed {paragraphs_processed} paragraphs and {sentences_processed} sentences")
            
            return {
                "status": "success",
                "message": "Successfully stored transcription in database",
                "fileid": fileid,
                "paragraphs_processed": paragraphs_processed,
                "sentences_processed": sentences_processed,
                "elapsed_time": elapsed_time
            }
        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(f"Error in store_transcription_result: {str(e)}")
            return {
                "status": "error",
                "message": f"Database error: {str(e)}",
                "elapsed_time": elapsed_time
            }
        
    def test_connection(self):
        """Test the connection to the database"""
        return self.sql.test_connection()