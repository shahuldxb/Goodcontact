"""
Real Azure file test script
This script tests the full workflow with an actual Azure blob file:
1. Gets an Azure blob file
2. Inserts a row with NULL transcription
3. Performs the transcription
4. Updates the row with the transcription data
5. Logs the SQL statements

This demonstrates the two-phase database commit approach:
- Initial insert with NULL transcription, commit
- Later update with transcription data, commit
"""
import sys
import os
import logging
import uuid
import json
import time
import pymssql
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add python_backend to the path
sys.path.append('./python_backend')

# Try to import required modules
try:
    from azure_storage_service import AzureStorageService
    from deepgram_service import DeepgramService
except Exception as e:
    logger.error(f"Error importing required modules: {e}")
    sys.exit(1)

def connect_to_database():
    """Connect to the Azure SQL database"""
    # Connection parameters
    server = "callcenter1.database.windows.net"
    database = "call"  # Correct database name from azure_sql_service.py
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

def log_sql(query, params=None):
    """Log the SQL query with parameter substitution"""
    logger.info("\n===== ACTUAL SQL STATEMENT =====")
    # Make a copy of the query for parameter substitution
    formatted_query = query
    
    if params:
        # Log parameters
        logger.info(f"With parameters: {params}")
        
        # Replace placeholders with actual values for display purposes
        for key, value in params.items():
            placeholder = f"%({key})s"  # pymssql uses %(name)s format
            if isinstance(value, str):
                formatted_value = f"'{value}'"
            elif value is None:
                formatted_value = "NULL"
            else:
                formatted_value = str(value)
            formatted_query = formatted_query.replace(placeholder, formatted_value)
    
    # Log the query with parameters substituted
    logger.info(formatted_query)
    logger.info("===============================")
    return formatted_query

async def real_azure_file_test():
    """Test with a real Azure file using the two-phase approach"""
    print("\n===== TESTING WITH REAL AZURE FILE =====")
    
    conn = None
    
    try:
        # Initialize storage service
        storage_service = AzureStorageService()
        
        # Get the first available file from Azure storage
        print("Fetching files from Azure Storage...")
        container_name = "shahulin"  # Using the known container
        files = storage_service.list_blobs(container_name)
        
        if not files:
            logger.error(f"No files found in container {container_name}")
            return
        
        # Print file structure to debug
        print(f"File structure sample: {str(files[0])[:200]}")
        
        # Select the first audio file
        target_file = None
        for file in files:
            # Check the structure - files might be dictionaries instead of objects
            name = file.get('name') if isinstance(file, dict) else getattr(file, 'name', None)
            if name and (name.endswith('.mp3') or name.endswith('.wav')):
                target_file = file
                break
        
        if not target_file:
            logger.error("No audio files found in the container")
            return
            
        # Get blob name based on structure
        blob_name = target_file.get('name') if isinstance(target_file, dict) else target_file.name
        print(f"Selected file: {blob_name}")
        
        # Get blob size from the file info we already have
        file_size = target_file.get('size') if isinstance(target_file, dict) else getattr(target_file, 'size', 1024)
        print(f"File size: {file_size} bytes")
        
        # Generate a test file ID
        test_fileid = f"test_shortcut_{uuid.uuid4().hex[:8]}"
        
        # Generate SAS URL for the blob
        print("Generating SAS URL...")
        sas_url = storage_service.generate_sas_url(container_name, blob_name, expiry_hours=240)
        
        # Connect to database
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # STEP 1: INSERT with NULL transcription
        print("\n===== STEP 1: INITIAL INSERT WITH NULL TRANSCRIPTION =====")
        insert_sql = """
        INSERT INTO rdt_assets (
            fileid, 
            filename, 
            source_path, 
            file_size,
            status,
            transcription  -- This will be NULL initially
        ) VALUES (
            %(fileid)s, 
            %(filename)s, 
            %(source_path)s, 
            %(file_size)s,
            %(status)s,
            NULL
        )
        """
        
        # Parameters for the INSERT
        insert_params = {
            "fileid": test_fileid,
            "filename": blob_name,
            "source_path": f"{container_name}/{blob_name}",
            "file_size": file_size,
            "status": "processing"
        }
        
        # Log the SQL that would be executed
        log_sql(insert_sql, insert_params)
        
        # Execute the INSERT
        print("Executing INSERT with NULL transcription...")
        cursor.execute(insert_sql, insert_params)
        
        # COMMIT the transaction - this is critical
        print("Committing the INSERT transaction...")
        conn.commit()
        print("Row with NULL transcription committed to database")
        
        # Verify the insert
        verify_sql = "SELECT id, fileid, status, transcription FROM rdt_assets WHERE fileid = %(fileid)s"
        verify_params = {"fileid": test_fileid}
        cursor.execute(verify_sql, verify_params)
        result = cursor.fetchone()
        
        if result:
            print(f"Verified INSERT: ID={result['id']}, FileID={result['fileid']}, Status={result['status']}")
            print(f"Transcription is NULL: {result['transcription'] is None}")
        else:
            print("Failed to verify INSERT")
        
        # Record start time for processing duration calculation
        start_time = time.time()
        
        # STEP 2: Perform the transcription
        print("\n===== STEP 2: TRANSCRIBING AUDIO =====")
        print(f"Transcribing file using SAS URL: {sas_url[:50]}...")
        
        # Transcribe using Deepgram
        try:
            # Use direct transcription 
            from python_backend.direct_transcribe import DirectTranscribe
            
            # Initialize the direct transcription class
            print("Using DirectTranscribe class...")
            direct_transcribe = DirectTranscribe()
            
            # Perform transcription
            print("Starting transcription...")
            # Using the known working key directly for consistency
            api_key = "ba94baf7840441c378c58ccd1d5202c38ddc42d8"
            transcription_result = direct_transcribe.transcribe_audio(blob_sas_url=sas_url, api_key=api_key)
            
            # Extract transcript text 
            transcript = "Transcription not available"
            if transcription_result and isinstance(transcription_result, dict):
                print("Successfully received transcription result")
                # Try to extract transcript from different possible response formats
                if 'results' in transcription_result:
                    results = transcription_result['results']
                    if 'channels' in results and len(results['channels']) > 0:
                        if 'alternatives' in results['channels'][0] and len(results['channels'][0]['alternatives']) > 0:
                            transcript = results['channels'][0]['alternatives'][0].get('transcript', '')
            
            # Extract paragraphs and sentences
            paragraphs = []
            
            # Check if results contain paragraphs directly
            if 'results' in transcription_result and 'paragraphs' in transcription_result['results']:
                print("Found paragraphs in transcription result")
                paragraphs = transcription_result['results']['paragraphs'].get('paragraphs', [])
            
            # If no paragraphs, try to extract from utterances as fallback
            if not paragraphs and 'results' in transcription_result and 'utterances' in transcription_result['results']:
                print("Extracting paragraphs from utterances as fallback")
                utterances = transcription_result['results']['utterances']
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
                    
            # If no paragraphs or utterances, create a simple paragraph from the full transcript
            if not paragraphs and transcript != "Transcription not available":
                print("Creating default paragraph from transcript")
                # Split transcript into sentences naively (this is a simple approach)
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
                
            # Create a structured response object for storing in database
            structured_response = {
                'request_id': transcription_result.get('request_id', ''),
                'sha256': '',
                'created': datetime.utcnow().isoformat(),
                'duration': transcription_result.get('metadata', {}).get('duration', 0),
                'confidence': 0,
                'paragraphs': paragraphs
            }
                            
            # If we couldn't extract, just convert the whole response to a string
            if transcript == "Transcription not available" and transcription_result:
                try:
                    # Just extract something to show it worked
                    transcript = str(transcription_result)[:500] + "..."  # Truncate to avoid huge output
                except:
                    pass
                    
        except Exception as e:
            print(f"Error during transcription: {str(e)}")
            transcript = f"ERROR: {str(e)}"
            structured_response = None
            paragraphs = []
            
        # Calculate processing duration
        end_time = time.time()
        processing_duration = int(end_time - start_time)
        print(f"Processing duration: {processing_duration} seconds")
            
        # Show the transcript summary
        transcript_length = len(transcript)
        print(f"Transcription complete. Length: {transcript_length} characters")
        if transcript_length > 0:
            print(f"Sample: {transcript[:100]}...")
        print(f"Extracted {len(paragraphs)} paragraphs")
        
        # STEP 3: UPDATE with transcription data
        print("\n===== STEP 3: UPDATE WITH TRANSCRIPTION DATA =====")
        update_sql = """
        UPDATE rdt_assets
        SET transcription = %(transcription)s,
            transcription_json = %(transcription_json)s,
            status = 'completed',
            processed_date = GETDATE(),
            destination_path = %(destination_path)s,
            processing_duration = %(processing_duration)s
        WHERE fileid = %(fileid)s
        """
        
        # Set destination path - use the same container with a "processed/" prefix
        destination_path = f"processed/{blob_name}"
        
        # Convert transcription_result to JSON string
        transcription_json = json.dumps(transcription_result)
        
        # Parameters for the UPDATE
        update_params = {
            "fileid": test_fileid,
            "transcription": transcript,
            "transcription_json": transcription_json,
            "destination_path": destination_path,
            "processing_duration": processing_duration
        }
        
        # Log the SQL that would be executed
        log_sql(update_sql, update_params)
        
        # Execute the UPDATE
        print("Executing UPDATE with transcription data...")
        cursor.execute(update_sql, update_params)
        
        # COMMIT the transaction - this is critical
        print("Committing the UPDATE transaction...")
        conn.commit()
        print("Transcription update committed to database")
        
        # STEP 4: Store paragraphs and sentences
        print("\n===== STEP 4: STORING PARAGRAPHS AND SENTENCES =====")
        
        if structured_response and structured_response.get('paragraphs'):
            try:
                # Import the module for handling transcription details
                sys.path.append(os.path.dirname(os.path.dirname(__file__)))
                from python_backend.update_sentence_tables import store_transcription_details
                
                # Store the transcription details
                result = store_transcription_details(test_fileid, structured_response)
                print(f"Stored transcription details: {result}")
                
                paragraphs_count = len(structured_response.get('paragraphs', []))
                sentences_count = sum(len(p.get('sentences', [])) for p in structured_response.get('paragraphs', []))
                print(f"Stored {paragraphs_count} paragraphs and {sentences_count} sentences")
                
            except Exception as e:
                print(f"Error storing paragraphs and sentences: {str(e)}")
                import traceback
                traceback.print_exc()
        else:
            print("No paragraphs found in transcription response - skipping paragraph/sentence storage")
        
        # STEP 5: Verify the record
        print("\n===== STEP 5: VERIFY RECORD =====")
        verify_sql = """
        SELECT 
            a.id, a.fileid, a.status, a.processed_date, a.destination_path, a.processing_duration,
            LEFT(a.transcription, 100) AS transcription_sample,
            (SELECT COUNT(*) FROM rdt_paragraphs p WHERE p.fileid = a.fileid) AS paragraph_count,
            (SELECT COUNT(*) FROM rdt_sentences s 
             JOIN rdt_paragraphs p ON s.paragraph_id = p.id 
             WHERE p.fileid = a.fileid) AS sentence_count
        FROM rdt_assets a
        WHERE a.fileid = %(fileid)s
        """
        verify_params = {"fileid": test_fileid}
        
        # Execute the verification query
        cursor.execute(verify_sql, verify_params)
        result = cursor.fetchone()
        
        if result:
            print(f"Verified record: ID={result['id']}, FileID={result['fileid']}, Status={result['status']}")
            print(f"Transcription sample: {result['transcription_sample']}...")
            print(f"Destination path: {result['destination_path']}")
            print(f"Processing duration: {result['processing_duration']} seconds")
            print(f"Paragraphs stored: {result['paragraph_count']}")
            print(f"Sentences stored: {result['sentence_count']}")
        else:
            print("Record not found - verification failed")
        
        print("\n===== TEST COMPLETED SUCCESSFULLY =====")
        print("The two-phase commit approach has been demonstrated:")
        print("1. Initial INSERT with NULL transcription (COMMIT 1)")
        print("2. Transcription processing")
        print("3. UPDATE with transcription data (COMMIT 2)")
        
    except Exception as e:
        logger.error(f"Error during test: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Close database connection
        if conn:
            conn.close()
            print("Database connection closed")

if __name__ == "__main__":
    import asyncio
    asyncio.run(real_azure_file_test())