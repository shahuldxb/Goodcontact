#!/usr/bin/env python3
"""
Get transcription from database
"""
import logging
from azure_sql_service import AzureSQLService

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_transcription(fileid):
    """Get transcription from database"""
    try:
        # Get SQL connection
        sql_service = AzureSQLService()
        conn = sql_service._get_connection()
        cursor = conn.cursor(as_dict=False)
        
        # Get paragraphs for the file
        cursor.execute(
            "SELECT paragraph_idx, text, speaker FROM rdt_paragraphs WHERE fileid=%s ORDER BY paragraph_idx",
            (fileid,)
        )
        paragraphs = cursor.fetchall()
        
        # Print paragraphs
        print(f"\nTranscription for file: {fileid}")
        print("=" * 50)
        
        if not paragraphs:
            print("No paragraphs found for this file.")
        else:
            for para in paragraphs:
                para_idx, text, speaker = para
                print(f"[Speaker {speaker}] {text}")
        
        print("=" * 50)
        
        # Close the connection
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error retrieving transcription: {str(e)}")
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        fileid = sys.argv[1]
    else:
        fileid = "test_agricultural_finance_(murabaha)_neutral.mp3"
    
    get_transcription(fileid)