"""
Asynchronous Integration Test

This script tests the full Deepgram-Azure integration pipeline using async patterns:
1. Retrieves a blob from Azure storage
2. Generates a SAS URL with appropriate permissions 
3. Transcribes it using Deepgram's APIs
4. Stores the results asynchronously in the Azure SQL database
5. Verifies database entries
"""

import os
import sys
import json
import logging
import uuid
import asyncio
import traceback
from datetime import datetime, timedelta

# Import async services
from azure_storage_service import AzureStorageService
from azure_sql_service import AzureSQLService
from dg_class_critical_transcribe_rest import DgClassCriticalTranscribeRest
from async_sql_service import AsyncSQLService
from async_update_sentence_tables import store_transcription_details_async

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def run_async_integration_test(blob_name=None):
    """
    Run a full asynchronous integration test with Deepgram, Azure Storage, and SQL database
    
    Args:
        blob_name (str, optional): Specific blob to use. If None, the first available blob will be used.
        
    Returns:
        dict: Test results
    """
    results = {
        "status": "success",
        "steps": [],
        "start_time": datetime.now().isoformat(),
        "fileid": None,
        "blob_name": None,
        "transcript": None
    }
    
    storage_service = AzureStorageService()
    sql_service = AzureSQLService()
    async_sql_service = AsyncSQLService()
    
    try:
        # Step 1: Get a blob from Azure storage
        logger.info("Fetching blob from Azure Storage")
        
        if blob_name:
            blob_names = [blob_name]
        else:
            blob_list = storage_service.list_blobs("shahulin")
            blob_names = [blob.name for blob in blob_list]
            
            if not blob_names:
                error_msg = "No blobs found in the container"
                logger.error(error_msg)
                results["status"] = "error"
                results["steps"].append({"step": "get_blob", "status": "error", "message": error_msg})
                return results
            
            # Use the first blob
            blob_name = blob_names[0]
        
        results["blob_name"] = blob_name
        logger.info(f"Using blob: {blob_name}")
        results["steps"].append({"step": "get_blob", "status": "success", "blob_name": blob_name})
        
        # Step 2: Generate a SAS URL
        logger.info(f"Generating SAS URL for blob: {blob_name}")
        # Generate SAS URL with 10-day (240 hours) expiry
        sas_url = storage_service.generate_sas_url("shahulin", blob_name, expiry_hours=240)
        
        if not sas_url:
            error_msg = f"Failed to generate SAS URL for blob: {blob_name}"
            logger.error(error_msg)
            results["status"] = "error"
            results["steps"].append({"step": "generate_sas", "status": "error", "message": error_msg})
            return results
            
        logger.info(f"Successfully generated SAS URL (length: {len(sas_url)})")
        results["steps"].append({"step": "generate_sas", "status": "success", "url_length": len(sas_url)})
        
        # Step 3: Generate a unique file ID
        fileid = f"test_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        results["fileid"] = fileid
        logger.info(f"Using file ID: {fileid}")
        
        # Step 4: Transcribe with Deepgram
        logger.info(f"Transcribing blob {blob_name} using Deepgram")
        transcriber = DgClassCriticalTranscribeRest()
        
        transcription_result = transcriber.transcribe_with_url(sas_url, model="nova-2", diarize=True)
        
        if not transcription_result.get('success', False):
            error_msg = f"Transcription failed: {transcription_result.get('error', 'Unknown error')}"
            logger.error(error_msg)
            results["status"] = "error"
            results["steps"].append({"step": "transcribe", "status": "error", "message": error_msg})
            return results
            
        logger.info(f"Transcription successful")
        results["transcript"] = transcription_result.get('basic_transcript', '')[:100] + '...'
        results["steps"].append({
            "step": "transcribe", 
            "status": "success",
            "transcript_length": len(transcription_result.get('basic_transcript', '')),
            "has_speakers": transcription_result.get('has_speakers', False),
            "request_id": transcription_result.get('request_id', 'unknown'),
            "duration": transcription_result.get('duration', 0)
        })
        
        # Step 5: Store transcription in database asynchronously (both rdt_assets and sentence tables)
        logger.info(f"Storing transcription in database with fileid: {fileid}")
        
        # First update or insert into rdt_assets table
        try:
            logger.info("Updating rdt_assets table...")
            
            # Extract essential info from transcription
            full_response = transcription_result['full_response']
            transcript_text = full_response.get('transcript', '')
            detected_language = full_response.get('language', 'en')
            
            # Prepare data for upsert
            asset_data = {
                'filename': blob_name,
                'source_path': f"shahulin/{blob_name}",
                'transcription': transcript_text,
                'transcription_json': json.dumps(full_response),
                'language_detected': detected_language,
                'status': 'completed',
                'created_dt': datetime.now(),
                'processed_date': datetime.now(),
                'processing_duration': transcription_result.get('duration', 0)
            }
            
            # Use async SQL service to upsert
            assets_updated = await async_sql_service.upsert_assets_record(fileid, asset_data)
            logger.info(f"Assets record {'updated' if assets_updated else 'failed to update'}")
            
        except Exception as e:
            logger.error(f"Error updating rdt_assets table: {str(e)}")
            logger.error(traceback.format_exc())
            assets_updated = False
        
        # Now store detailed paragraph and sentence data using the async utility function
        logger.info("Storing detailed paragraph and sentence data...")
        storage_result = await store_transcription_details_async(fileid, transcription_result['full_response'])
        
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
        
        # Step 6: Verify database entry asynchronously
        logger.info(f"Verifying database entry for fileid: {fileid}")
        
        async with await async_sql_service.get_connection() as conn:
            async with conn.cursor() as cursor:
                # Check rdt_assets table first
                await cursor.execute("SELECT * FROM rdt_assets WHERE fileid = %s", (fileid,))
                asset_row = await cursor.fetchone()
                
                if asset_row:
                    logger.info(f"Found record in rdt_assets for fileid {fileid}")
                    asset_found = True
                else:
                    logger.warning(f"No asset record found in rdt_assets for fileid {fileid}")
                    asset_found = False
                
                # Check if metadata exists in database
                await cursor.execute("SELECT * FROM rdt_audio_metadata WHERE fileid = %s", (fileid,))
                metadata_row = await cursor.fetchone()
                
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
                await cursor.execute("SELECT COUNT(*) as para_count FROM rdt_paragraphs WHERE fileid = %s", (fileid,))
                para_count = await cursor.fetchone()
                para_count_val = para_count[0] if para_count else 0
                
                # Check sentences
                await cursor.execute("""
                    SELECT COUNT(*) as sent_count 
                    FROM rdt_sentences s 
                    JOIN rdt_paragraphs p ON s.paragraph_id = p.id 
                    WHERE p.fileid = %s
                """, (fileid,))
                sent_count = await cursor.fetchone()
                sent_count_val = sent_count[0] if sent_count else 0
        
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
        if os.path.exists("logs"):
            for subdir in ["requests", "responses", "errors"]:
                if os.path.exists(f"logs/{subdir}"):
                    log_files.extend([f"logs/{subdir}/{f}" for f in os.listdir(f"logs/{subdir}") if fileid in f or blob_name.replace('.', '_') in f])
        
        logger.info(f"Found {len(log_files)} log files related to this blob/request")
        results["steps"].append({
            "step": "check_logs", 
            "status": "success",
            "log_files_count": len(log_files),
            "log_files": log_files[:5]  # List up to 5 log files
        })
        
        # All steps completed successfully
        results["end_time"] = datetime.now().isoformat()
        results["duration"] = (datetime.now() - datetime.fromisoformat(results["start_time"])).total_seconds()
        logger.info(f"Integration test completed successfully in {results['duration']:.2f} seconds")
        
        return results
        
    except Exception as e:
        error_msg = f"Integration test failed with error: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        
        results["status"] = "error"
        results["error"] = str(e)
        results["traceback"] = traceback.format_exc()
        results["end_time"] = datetime.now().isoformat()
        results["duration"] = (datetime.now() - datetime.fromisoformat(results["start_time"])).total_seconds()
        
        return results

async def main():
    """Main function"""
    if len(sys.argv) > 1:
        blob_name = sys.argv[1]
        print(f"Testing with specific blob: {blob_name}")
    else:
        blob_name = None
        print("Testing with the first available blob")
    
    results = await run_async_integration_test(blob_name)
    
    # Print results
    print("\n" + "="*50)
    print(f"INTEGRATION TEST {'PASSED' if results['status'] == 'success' else 'FAILED'}")
    print("="*50)
    
    for step in results["steps"]:
        status_symbol = "✓" if step["status"] == "success" else "✗"
        print(f"{status_symbol} {step['step']}")
    
    if results["status"] == "success":
        print(f"\nSuccessfully processed blob: {results['blob_name']}")
        print(f"File ID: {results['fileid']}")
        print(f"Transcript preview: {results['transcript']}")
        print(f"\nTotal duration: {results.get('duration', 0):.2f} seconds")
    else:
        print(f"\nTest failed with error: {results.get('error', 'Unknown error')}")
    
    # Save results to file
    with open(f"direct_test_results/integration_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", "w") as f:
        json.dump(results, f, indent=2)
    
    return results["status"] == "success"

if __name__ == "__main__":
    # Create results directory if it doesn't exist
    os.makedirs("direct_test_results", exist_ok=True)
    
    # Run the async main function
    asyncio.run(main())