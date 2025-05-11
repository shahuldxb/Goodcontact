#!/usr/bin/env python3
"""
Create new SQL tables for storing paragraph and sentence-level data from Deepgram transcriptions
"""
import os
import logging
import pymssql
from azure_sql_service import AzureSQLService

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def update_sentence_tables():
    """Create tables for storing paragraph and sentence data"""
    try:
        # Read SQL script from the repository
        script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'server', 'alter-tables-for-sentences.sql')
        
        with open(script_path, 'r') as sql_file:
            sql_script = sql_file.read()
        
        # Get SQL connection
        sql_service = AzureSQLService()
        conn = sql_service._get_connection()
        
        # Create a cursor and execute the SQL script
        cursor = conn.cursor()
        cursor.execute(sql_script)
        
        # Commit the changes
        conn.commit()
        
        # Close the connection
        cursor.close()
        conn.close()
        
        logger.info("Successfully created paragraph and sentence tables")
        return {"status": "success", "message": "Successfully created paragraph and sentence tables"}
    except Exception as e:
        logger.error(f"Error creating paragraph and sentence tables: {str(e)}")
        return {"status": "error", "message": str(e)}

def store_transcription_details(fileid, transcription_response):
    """
    Store detailed transcription data including metadata, paragraphs, and sentences.
    
    Args:
        fileid (str): The unique file identifier
        transcription_response (dict): The full Deepgram transcription response with metadata
    
    Returns:
        dict: Status information about the storage operation
    """
    try:
        # Get SQL connection
        sql_service = AzureSQLService()
        conn = sql_service._get_connection()
        cursor = conn.cursor()
        
        # 1. Store metadata
        request_id = transcription_response.get('request_id', '')
        sha256 = transcription_response.get('sha256', '')
        created = transcription_response.get('created', '')
        duration = transcription_response.get('duration', 0)
        confidence = transcription_response.get('confidence', 0)
        
        # Execute the stored procedure
        cursor.execute(
            "EXEC RDS_InsertAudioMetadata @fileid=%s, @request_id=%s, @sha256=%s, @created_timestamp=%s, @audio_duration=%s, @confidence=%s",
            (fileid, request_id, sha256, created, duration, confidence)
        )
        
        # 2. Store paragraphs and sentences
        if 'paragraphs' in transcription_response and transcription_response['paragraphs']:
            for para_idx, paragraph in enumerate(transcription_response['paragraphs']):
                # Insert paragraph
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
                
                if not paragraph_id:
                    logger.warning(f"Failed to get paragraph ID for paragraph {para_idx} in file {fileid}")
                    continue
                
                # Insert sentences for this paragraph
                if 'sentences' in paragraph and paragraph['sentences']:
                    for sent in paragraph['sentences']:
                        sent_idx = sent.get('id', f"{para_idx}_0")
                        sent_text = sent.get('text', '')
                        sent_start = sent.get('start', 0)
                        sent_end = sent.get('end', 0)
                        
                        # Execute the stored procedure
                        cursor.execute(
                            "EXEC RDS_InsertSentence @fileid=%s, @paragraph_id=%s, @sentence_idx=%s, @text=%s, @start_time=%s, @end_time=%s",
                            (fileid, paragraph_id, sent_idx, sent_text, sent_start, sent_end)
                        )
        
        # Commit the changes
        conn.commit()
        
        # Close the connection
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully stored transcription details for file {fileid}")
        return {
            "status": "success", 
            "message": f"Successfully stored transcription details for file {fileid}",
            "metadata_stored": True,
            "paragraphs_stored": len(transcription_response.get('paragraphs', [])),
            "sentences_stored": sum(len(p.get('sentences', [])) for p in transcription_response.get('paragraphs', []))
        }
    except Exception as e:
        logger.error(f"Error storing transcription details for file {fileid}: {str(e)}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    # Create the tables
    result = update_sentence_tables()
    print(result)