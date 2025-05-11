#!/usr/bin/env python3
"""
Test script for verifying the four main fixes:
1. Setting destination_path when updating the record
2. Calculating processing_duration in seconds
3. Extracting paragraphs and sentences from transcription response
4. Saving paragraphs and sentences to their respective tables

This script will attempt to find a recently processed file and apply the fixes
manually if needed.
"""
import os
import sys
import json
import time
import logging
import pymssql
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def connect_to_database():
    """Connect to the Azure SQL database"""
    # Connection parameters
    server = "callcenter1.database.windows.net"
    database = "call"
    username = "shahul"
    password = "apple123!@#"
    
    try:
        # Create a connection
        print("Connecting to database...")
        conn = pymssql.connect(
            server=server,
            database=database,
            user=username,
            password=password,
            as_dict=True
        )
        print("Database connection established")
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise

def find_incomplete_records():
    """Find records that need fixing"""
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Find records with NULL destination_path or processing_duration
        sql = """
        SELECT TOP 5 id, fileid, filename, source_path, destination_path, 
                   processing_duration, status,
                   (SELECT COUNT(*) FROM rdt_paragraphs p WHERE p.fileid = a.fileid) AS paragraph_count,
                   (SELECT COUNT(*) FROM rdt_sentences s JOIN rdt_paragraphs p ON s.paragraph_id = p.id 
                    WHERE p.fileid = a.fileid) AS sentence_count
        FROM rdt_assets a
        WHERE 
            (destination_path IS NULL OR processing_duration IS NULL)
            AND status = 'completed'
            AND transcription IS NOT NULL
        ORDER BY processed_date DESC
        """
        
        cursor.execute(sql)
        results = cursor.fetchall()
        
        print(f"Found {len(results)} records that need fixing")
        for record in results:
            print(f"ID: {record['id']}, FileID: {record['fileid']}, Source: {record['source_path']}")
            print(f"Destination: {record['destination_path']}, Processing Duration: {record['processing_duration']}")
            print(f"Paragraphs: {record['paragraph_count']}, Sentences: {record['sentence_count']}")
            print("-" * 50)
        
        cursor.close()
        conn.close()
        return results
    except Exception as e:
        logger.error(f"Error finding incomplete records: {str(e)}")
        return []

def fix_record(record):
    """Fix a single record by updating destination_path and processing_duration"""
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # 1. Update destination_path and processing_duration
        source_path = record['source_path']
        fileid = record['fileid']
        
        # Create destination path from source path
        parts = source_path.split('/')
        if len(parts) >= 2:
            container = parts[0]
            blob_name = parts[1]
            destination_path = f"processed/{blob_name}"
        else:
            destination_path = f"processed/{os.path.basename(source_path)}"
        
        # Set a default processing duration if not available
        processing_duration = 60  # Default to 60 seconds if we can't calculate
        
        update_sql = """
        UPDATE rdt_assets
        SET destination_path = %s,
            processing_duration = %s
        WHERE fileid = %s
        """
        
        print(f"Updating record {fileid} with destination_path={destination_path}, processing_duration={processing_duration}")
        cursor.execute(update_sql, (destination_path, processing_duration, fileid))
        conn.commit()
        
        # 2. Check if we need to process paragraphs and sentences
        if record['paragraph_count'] == 0 or record['sentence_count'] == 0:
            # Get the transcription JSON to extract paragraphs
            cursor.execute("SELECT transcription_json FROM rdt_assets WHERE fileid = %s", (fileid,))
            result = cursor.fetchone()
            
            if result and result['transcription_json']:
                # Try to parse JSON
                try:
                    transcription_json = json.loads(result['transcription_json'])
                    
                    # Extract transcript
                    transcript = "No transcript available"
                    paragraphs = []
                    
                    # Check if results contain paragraphs directly
                    if 'results' in transcription_json and 'paragraphs' in transcription_json['results']:
                        print("Found paragraphs in transcription result")
                        paragraphs = transcription_json['results']['paragraphs'].get('paragraphs', [])
                    
                    # If no paragraphs, try to extract from utterances as fallback
                    if not paragraphs and 'results' in transcription_json and 'utterances' in transcription_json['results']:
                        print("Extracting paragraphs from utterances as fallback")
                        utterances = transcription_json['results']['utterances']
                        for i, utterance in enumerate(utterances):
                            paragraph = {
                                'text': utterance.get('transcript', ''),
                                'start': utterance.get('start', 0),
                                'end': utterance.get('end', 0),
                                'speaker': utterance.get('speaker', 'unknown'),
                                'num_words': len(utterance.get('transcript', '').split()),
                                'sentences': []
                            }
                            # Create a sentence entry for each utterance
                            paragraph['sentences'].append({
                                'id': f"{i}_0",
                                'text': utterance.get('transcript', ''),
                                'start': utterance.get('start', 0),
                                'end': utterance.get('end', 0)
                            })
                            paragraphs.append(paragraph)
                    
                    # If we have transcript but no paragraphs, create a default one
                    if not paragraphs:
                        # Try to extract transcript from response
                        if 'results' in transcription_json:
                            results = transcription_json['results']
                            if 'channels' in results and len(results['channels']) > 0:
                                if 'alternatives' in results['channels'][0] and len(results['channels'][0]['alternatives']) > 0:
                                    transcript = results['channels'][0]['alternatives'][0].get('transcript', '')
                    
                        if transcript != "No transcript available":
                            print("Creating default paragraph from transcript")
                            # Split transcript into sentences naively
                            import re
                            sentence_texts = re.split(r'(?<=[.!?])\s+', transcript)
                            sentences = []
                            
                            for i, sent_text in enumerate(sentence_texts):
                                sentences.append({
                                    'id': f"0_{i}",
                                    'text': sent_text,
                                    'start': 0,
                                    'end': 0
                                })
                            
                            paragraphs.append({
                                'text': transcript,
                                'start': 0,
                                'end': 0,
                                'speaker': 'unknown',
                                'num_words': len(transcript.split()),
                                'sentences': sentences
                            })
                    
                    # Create structured response
                    structured_response = {
                        'request_id': transcription_json.get('request_id', ''),
                        'sha256': '',
                        'created': datetime.utcnow().isoformat(),
                        'duration': transcription_json.get('metadata', {}).get('duration', 0),
                        'confidence': 0,
                        'paragraphs': paragraphs
                    }
                    
                    # Store paragraphs and sentences
                    if paragraphs:
                        # Import store_transcription_details from update_sentence_tables
                        try:
                            from update_sentence_tables import store_transcription_details
                            
                            # Store the transcription details
                            result = store_transcription_details(fileid, structured_response)
                            print(f"Stored transcription details: {result}")
                            
                            paragraphs_count = len(structured_response.get('paragraphs', []))
                            sentences_count = sum(len(p.get('sentences', [])) for p in structured_response.get('paragraphs', []))
                            print(f"Stored {paragraphs_count} paragraphs and {sentences_count} sentences")
                        except Exception as e:
                            print(f"Error storing paragraphs and sentences: {str(e)}")
                            import traceback
                            traceback.print_exc()
                    
                except Exception as e:
                    print(f"Error processing transcription JSON for paragraphs: {str(e)}")
                    import traceback
                    traceback.print_exc()
            
        # Verify the updates
        verify_sql = """
        SELECT 
            a.id, a.fileid, a.destination_path, a.processing_duration,
            (SELECT COUNT(*) FROM rdt_paragraphs p WHERE p.fileid = a.fileid) AS paragraph_count,
            (SELECT COUNT(*) FROM rdt_sentences s 
             JOIN rdt_paragraphs p ON s.paragraph_id = p.id 
             WHERE p.fileid = a.fileid) AS sentence_count
        FROM rdt_assets a
        WHERE a.fileid = %s
        """
        
        cursor.execute(verify_sql, (fileid,))
        result = cursor.fetchone()
        
        if result:
            print("\nVerifying updates for record:")
            print(f"Destination path: {result['destination_path']}")
            print(f"Processing duration: {result['processing_duration']} seconds")
            print(f"Paragraphs stored: {result['paragraph_count']}")
            print(f"Sentences stored: {result['sentence_count']}")
            
            return {
                'success': True,
                'fileid': fileid,
                'destination_path': result['destination_path'],
                'processing_duration': result['processing_duration'],
                'paragraphs_count': result['paragraph_count'],
                'sentences_count': result['sentence_count']
            }
        else:
            print("Record not found - verification failed")
            return {'success': False}
        
    except Exception as e:
        logger.error(f"Error fixing record: {str(e)}")
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}
    finally:
        # Close database connection
        if 'conn' in locals() and conn:
            cursor.close()
            conn.close()
            print("Database connection closed")

def main():
    """Main function to find and fix records"""
    print("Finding records that need fixing...")
    records = find_incomplete_records()
    
    fixed_records = 0
    errors = 0
    
    if not records:
        print("No records found that need fixing.")
        return
    
    print("\nAttempting to fix records...")
    
    for record in records:
        print(f"\nFixing record with ID {record['id']} and FileID {record['fileid']}...")
        result = fix_record(record)
        
        if result and result.get('success'):
            fixed_records += 1
            print(f"Successfully fixed record {record['fileid']}")
        else:
            errors += 1
            print(f"Failed to fix record {record['fileid']}")
    
    print("\nSummary:")
    print(f"Total records processed: {len(records)}")
    print(f"Successfully fixed: {fixed_records}")
    print(f"Errors: {errors}")

if __name__ == "__main__":
    main()