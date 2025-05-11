#!/usr/bin/env python3
"""
Full integration test script for Deepgram transcription with Azure Storage and SQL database

This script:
1. Fetches a real file from Azure blob storage
2. Generates a SAS URL for it
3. Transcribes it using Deepgram via DgClassCriticalTranscribeRest
4. Stores the results in the SQL database
5. Checks all logs for that blob
"""

import os
import sys
import json
import logging
import uuid
import traceback
from datetime import datetime, timedelta
from azure_storage_service import AzureStorageService
from azure_sql_service import AzureSQLService
from dg_class_critical_transcribe_rest import DgClassCriticalTranscribeRest
from update_sentence_tables import store_transcription_details

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Deepgram API key
DEEPGRAM_API_KEY = "ba94baf7840441c378c58ccd1d5202c38ddc42d8"

def run_full_integration_test(blob_name=None):
    """
    Run a full integration test with Azure Storage, Deepgram and SQL database
    
    Args:
        blob_name (str, optional): Specific blob to use. If None, the first available blob will be used.
        
    Returns:
        dict: Test results
    """
    results = {
        "test_id": str(uuid.uuid4()),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "steps": []
    }
    
    try:
        # Step 1: Connect to Azure Storage
        logger.info("Connecting to Azure Storage...")
        storage_service = AzureStorageService()
        results["steps"].append({"step": "connect_storage", "status": "success"})
        
        # Step 2: Get available blobs
        logger.info("Fetching available blobs...")
        blobs = storage_service.list_blobs("shahulin")
        if not blobs:
            error_msg = "No blobs found in container 'shahulin'"
            logger.error(error_msg)
            results["steps"].append({"step": "list_blobs", "status": "error", "message": error_msg})
            return results
            
        # If no specific blob was provided, use the first one
        if not blob_name:
            blob_name = blobs[0]["name"]
        
        logger.info(f"Using blob: {blob_name}")
        results["steps"].append({"step": "select_blob", "status": "success", "blob_name": blob_name})
        
        # Step 3: Generate SAS URL for the blob
        logger.info(f"Generating SAS URL for blob {blob_name}...")
        sas_url = storage_service.generate_sas_url("shahulin", blob_name, expiry_hours=240)
        
        if not sas_url:
            error_msg = f"Failed to generate SAS URL for blob {blob_name}"
            logger.error(error_msg)
            results["steps"].append({"step": "generate_sas", "status": "error", "message": error_msg})
            return results
            
        logger.info(f"SAS URL generated successfully (expires in 240 hours)")
        results["steps"].append({"step": "generate_sas", "status": "success"})
        
        # Generate a unique fileid for this test
        fileid = f"integration_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Step 4: Transcribe using Deepgram
        logger.info(f"Transcribing blob {blob_name} with Deepgram...")
        transcriber = DgClassCriticalTranscribeRest(DEEPGRAM_API_KEY)
        transcription_result = transcriber.transcribe_with_url(
            audio_url=sas_url,
            model="nova-3",
            diarize=True,
            debug_mode=True
        )
        
        if not transcription_result['success']:
            error_msg = f"Transcription failed: {transcription_result.get('error', 'Unknown error')}"
            logger.error(error_msg)
            results["steps"].append({"step": "transcribe", "status": "error", "message": error_msg})
            return results
            
        logger.info(f"Transcription successful! Duration: {transcription_result.get('duration', 0):.2f} seconds")
        results["steps"].append({
            "step": "transcribe", 
            "status": "success", 
            "request_id": transcription_result.get('request_id', 'unknown'),
            "duration": transcription_result.get('duration', 0)
        })
        
        # Step 5: Store transcription in database (both rdt_assets and sentence tables)
        logger.info(f"Storing transcription in database with fileid: {fileid}")
        
        # First update or insert into rdt_assets table
        try:
            logger.info("Updating rdt_assets table...")
            sql_service = AzureSQLService()
            
            # Extract essential info from transcription
            full_response = transcription_result['full_response']
            transcript_text = full_response.get('transcript', '')
            detected_language = full_response.get('language', 'en')
            transcription_json_str = json.dumps(full_response)
            
            # Using a more robust approach with one connection context
            with sql_service._get_connection() as conn:
                with conn.cursor() as cursor:
                    # Check if record already exists
                    cursor.execute("SELECT * FROM rdt_assets WHERE fileid = %s", (fileid,))
                    existing_asset = cursor.fetchone()
                    
                    if existing_asset:
                        # Update existing asset
                        logger.info(f"Updating existing record in rdt_assets for fileid {fileid}")
                        cursor.execute("""
                            UPDATE rdt_assets 
                            SET transcription = %s, 
                                transcription_json = %s, 
                                language_detected = %s,
                                status = 'completed',
                                processed_date = %s,
                                processing_duration = %s
                            WHERE fileid = %s
                        """, (
                            transcript_text,
                            transcription_json_str,
                            detected_language,
                            datetime.now(),
                            transcription_result.get('duration', 0),
                            fileid
                        ))
                    else:
                        # Create new asset record
                        logger.info(f"Inserting new record into rdt_assets for fileid {fileid}")
                        blob_name = blob_name or "unknown_blob"
                        source_path = f"shahulin/{blob_name}"
                        file_size = 0  # We don't have the file size readily available
                        
                        cursor.execute("""
                            INSERT INTO rdt_assets 
                            (fileid, filename, source_path, file_size, transcription, transcription_json, language_detected, status,
                             created_dt, processed_date, processing_duration) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            fileid,
                            blob_name,
                            source_path,
                            file_size,
                            transcript_text,
                            transcription_json_str,
                            detected_language,
                            'completed',
                            datetime.now(),
                            datetime.now(),
                            transcription_result.get('duration', 0)
                        ))
                    
                    # Commit the transaction
                    conn.commit()
            
            logger.info("Successfully updated rdt_assets table")
            assets_updated = True
        except Exception as e:
            logger.error(f"Error updating rdt_assets table: {str(e)}")
            logger.error(traceback.format_exc())
            assets_updated = False
        
        # Now store detailed paragraph and sentence data using the utility function
        logger.info("Storing detailed paragraph and sentence data...")
        storage_result = store_transcription_details(fileid, transcription_result['full_response'])
        
        if storage_result['status'] != 'success':
            error_msg = f"Detailed database storage failed: {storage_result.get('message', 'Unknown error')}"
            logger.error(error_msg)
            if not assets_updated:
                results["steps"].append({"step": "store_in_db", "status": "error", "message": error_msg})
                return results
        
        logger.info(f"Transcription stored successfully in database")
        results["steps"].append({
            "step": "store_in_db", 
            "status": "success",
            "rdt_assets_updated": assets_updated,
            "metadata_stored": storage_result.get('metadata_stored', False),
            "paragraphs_stored": storage_result.get('paragraphs_stored', 0),
            "sentences_stored": storage_result.get('sentences_stored', 0)
        })
        
        # Step 6: Verify database entry
        logger.info(f"Verifying database entry for fileid: {fileid}")
        sql_service = AzureSQLService()
        conn = sql_service._get_connection()
        cursor = conn.cursor(as_dict=True)
        
        # Check rdt_assets table first
        cursor.execute("SELECT * FROM rdt_assets WHERE fileid = %s", (fileid,))
        asset_row = cursor.fetchone()
        
        if asset_row:
            logger.info(f"Found record in rdt_assets for fileid {fileid}")
            asset_found = True
        else:
            logger.warning(f"No asset record found in rdt_assets for fileid {fileid}")
            asset_found = False
        
        # Check if metadata exists in database
        cursor.execute("SELECT * FROM rdt_audio_metadata WHERE fileid = %s", (fileid,))
        metadata_row = cursor.fetchone()
        
        if not metadata_row:
            error_msg = f"No metadata found in database for fileid {fileid}"
            logger.error(error_msg)
            if not asset_found:
                results["steps"].append({"step": "verify_db", "status": "error", "message": error_msg})
                return results
            else:
                metadata_found = False
        else:
            metadata_found = True
            
        # Check paragraphs
        cursor.execute("SELECT COUNT(*) as para_count FROM rdt_paragraphs WHERE fileid = %s", (fileid,))
        para_count = cursor.fetchone()
        para_count_val = para_count['para_count'] if para_count else 0
        
        # Check sentences
        cursor.execute("""
            SELECT COUNT(*) as sent_count 
            FROM rdt_sentences s 
            JOIN rdt_paragraphs p ON s.paragraph_id = p.id 
            WHERE p.fileid = %s
        """, (fileid,))
        sent_count = cursor.fetchone()
        sent_count_val = sent_count['sent_count'] if sent_count else 0
        
        logger.info(f"Database verification complete: Asset found: {asset_found}, Metadata found: {metadata_found}, Found {para_count_val} paragraphs and {sent_count_val} sentences")
        results["steps"].append({
            "step": "verify_db", 
            "status": "success",
            "asset_found": asset_found,
            "metadata_found": metadata_found,
            "paragraphs_count": para_count_val,
            "sentences_count": sent_count_val
        })
        
        # Step 7: Check logs for this blob and request
        logger.info(f"Checking logs for blob {blob_name}")
        # Get log files for this blob/request from logs directory
        log_files = []
        request_id = transcription_result.get('request_id', '')
        
        if os.path.exists('logs/requests'):
            request_logs = [f for f in os.listdir('logs/requests') if f.startswith(f'request_{request_id}')]
            for log_file in request_logs:
                log_files.append(f"logs/requests/{log_file}")
                
        if os.path.exists('logs/responses'):
            response_logs = [f for f in os.listdir('logs/responses') if f.startswith(f'response_{request_id}')]
            for log_file in response_logs:
                log_files.append(f"logs/responses/{log_file}")
        
        # Check main log file
        if os.path.exists('logs/deepgram_transcription.log'):
            log_files.append('logs/deepgram_transcription.log')
        
        logger.info(f"Found {len(log_files)} log files related to this request")
        results["steps"].append({
            "step": "check_logs", 
            "status": "success",
            "log_files_count": len(log_files),
            "log_files": log_files
        })
        
        # Step 8: Generate overall test summary
        results["overall_status"] = "success"
        results["summary"] = {
            "blob_name": blob_name,
            "fileid": fileid,
            "transcription_duration": transcription_result.get('duration', 0),
            "paragraphs_stored": para_count['para_count'],
            "sentences_stored": sent_count['sent_count'],
            "log_files_count": len(log_files)
        }
        
        logger.info("Integration test completed successfully!")
        return results
        
    except Exception as e:
        import traceback
        error_msg = f"Integration test failed with error: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        
        results["overall_status"] = "error"
        results["error"] = str(e)
        results["traceback"] = traceback.format_exc()
        
        return results

if __name__ == "__main__":
    # Get blob name from command line argument if provided
    blob_name = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Run the integration test
    print(f"Starting full integration test {'with blob ' + blob_name if blob_name else 'with first available blob'}...")
    test_results = run_full_integration_test(blob_name)
    
    # Save results to a file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"integration_test_results_{timestamp}.json", "w") as f:
        json.dump(test_results, f, indent=2)
    
    # Print summary
    print("\n=== Integration Test Summary ===")
    print(f"Status: {test_results['overall_status']}")
    
    if test_results['overall_status'] == 'success':
        summary = test_results['summary']
        print(f"Blob: {summary['blob_name']}")
        print(f"File ID: {summary['fileid']}")
        print(f"Transcription time: {summary['transcription_duration']:.2f} seconds")
        print(f"Paragraphs stored: {summary['paragraphs_stored']}")
        print(f"Sentences stored: {summary['sentences_stored']}")
        print(f"Log files: {summary['log_files_count']}")
    else:
        print(f"Error: {test_results.get('error', 'Unknown error')}")
    
    # Print step details
    print("\n--- Step Details ---")
    for i, step in enumerate(test_results['steps']):
        print(f"{i+1}. {step['step']}: {step['status']}")
        if step['status'] == 'error':
            print(f"   Error: {step.get('message', 'No error message')}")