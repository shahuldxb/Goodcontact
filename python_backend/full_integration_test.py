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
        
        # Step 5: Store transcription in database
        logger.info(f"Storing transcription in database with fileid: {fileid}")
        # Use the store_transcription_details function from update_sentence_tables module
        storage_result = store_transcription_details(fileid, transcription_result['full_response'])
        
        if storage_result['status'] != 'success':
            error_msg = f"Database storage failed: {storage_result.get('message', 'Unknown error')}"
            logger.error(error_msg)
            results["steps"].append({"step": "store_in_db", "status": "error", "message": error_msg})
            return results
            
        logger.info(f"Transcription stored successfully in database")
        results["steps"].append({
            "step": "store_in_db", 
            "status": "success",
            "metadata_stored": storage_result.get('metadata_stored', False),
            "paragraphs_stored": storage_result.get('paragraphs_stored', 0),
            "sentences_stored": storage_result.get('sentences_stored', 0)
        })
        
        # Step 6: Verify database entry
        logger.info(f"Verifying database entry for fileid: {fileid}")
        sql_service = AzureSQLService()
        conn = sql_service._get_connection()
        cursor = conn.cursor(as_dict=True)
        
        # Check if metadata exists in database
        cursor.execute("SELECT * FROM rdt_audio_metadata WHERE fileid = %s", (fileid,))
        metadata_row = cursor.fetchone()
        
        if not metadata_row:
            error_msg = f"No metadata found in database for fileid {fileid}"
            logger.error(error_msg)
            results["steps"].append({"step": "verify_db", "status": "error", "message": error_msg})
            return results
            
        # Check paragraphs
        cursor.execute("SELECT COUNT(*) as para_count FROM rdt_paragraphs WHERE fileid = %s", (fileid,))
        para_count = cursor.fetchone()
        
        # Check sentences
        cursor.execute("""
            SELECT COUNT(*) as sent_count 
            FROM rdt_sentences s 
            JOIN rdt_paragraphs p ON s.paragraph_id = p.id 
            WHERE p.fileid = %s
        """, (fileid,))
        sent_count = cursor.fetchone()
        
        logger.info(f"Database verification complete: Found {para_count['para_count']} paragraphs and {sent_count['sent_count']} sentences")
        results["steps"].append({
            "step": "verify_db", 
            "status": "success",
            "metadata_found": True,
            "paragraphs_count": para_count['para_count'],
            "sentences_count": sent_count['sent_count']
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