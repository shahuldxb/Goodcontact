"""
Complete Integration Test for Unified Contact Center Analytics

This test performs the full workflow:
1. Connect to Azure Storage and get a real audio file
2. Generate a proper Blob SAS URL for the file
3. Transcribe the audio with Deepgram API
4. Store the results in Azure SQL database using stored procedures
5. Verify the stored data

This represents the complete workflow that will be used in production.
"""
import os
import sys
import logging
import uuid
import json
import time
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
import pymssql
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Azure Storage settings
STORAGE_ACCOUNT_NAME = os.environ.get("AZURE_STORAGE_ACCOUNT", "infolder")
STORAGE_ACCOUNT_KEY = os.environ.get("AZURE_STORAGE_KEY", "NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==")
STORAGE_CONNECTION_STRING = os.environ.get(
    "AZURE_STORAGE_CONNECTION_STRING", 
    f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={STORAGE_ACCOUNT_KEY};EndpointSuffix=core.windows.net"
)
SOURCE_CONTAINER = os.environ.get("AZURE_SOURCE_CONTAINER", "shahulin")
DESTINATION_CONTAINER = os.environ.get("AZURE_DESTINATION_CONTAINER", "shahulout")

# Azure SQL settings
SQL_SERVER = os.environ.get("AZURE_SQL_SERVER", "callcenter1.database.windows.net")
SQL_DATABASE = os.environ.get("AZURE_SQL_DATABASE", "call")
SQL_USER = os.environ.get("AZURE_SQL_USER", "shahul")
SQL_PASSWORD = os.environ.get("AZURE_SQL_PASSWORD", "apple123!@#")
SQL_PORT = int(os.environ.get("AZURE_SQL_PORT", "1433"))

# Deepgram API key
DEEPGRAM_API_KEY = os.environ.get("DEEPGRAM_API_KEY", "ba94baf7840441c378c58ccd1d5202c38ddc42d8")

def get_sql_connection():
    """
    Get a connection to Azure SQL Database
    
    Returns:
        pymssql.Connection: SQL connection
    """
    return pymssql.connect(
        server=SQL_SERVER,
        port=SQL_PORT, 
        database=SQL_DATABASE,
        user=SQL_USER,
        password=SQL_PASSWORD,
        tds_version='7.4',
        as_dict=True
    )

def list_audio_blobs(container_name=SOURCE_CONTAINER, limit=5):
    """
    List audio blobs in the container
    
    Args:
        container_name: Name of the container
        limit: Maximum number of blobs to return
        
    Returns:
        list: List of audio blob names
    """
    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(container_name)
        
        # List all blobs in the container
        all_blobs = list(container_client.list_blobs())
        
        # Filter for audio files
        audio_extensions = ('.mp3', '.wav', '.m4a', '.ogg', '.aac', '.flac')
        audio_blobs = [blob.name for blob in all_blobs if blob.name.lower().endswith(audio_extensions)]
        
        return audio_blobs[:limit]
    except Exception as e:
        logger.error(f"Error listing audio blobs: {str(e)}")
        return []

def generate_blob_sas_url(blob_name, container_name=SOURCE_CONTAINER, expiry_hours=240):
    """
    Generate a Blob SAS URL for a specific blob in Azure Storage
    
    Args:
        blob_name: Name of the blob
        container_name: Name of the container
        expiry_hours: Number of hours until the SAS token expires
        
    Returns:
        str: SAS URL for the specific blob
    """
    try:
        # Generate SAS token
        sas_token = generate_blob_sas(
            account_name=STORAGE_ACCOUNT_NAME,
            container_name=container_name,
            blob_name=blob_name,
            account_key=STORAGE_ACCOUNT_KEY,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=expiry_hours)
        )
        
        # Build the full SAS URL specifically for this blob
        blob_sas_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
        
        logger.info(f"Generated Blob SAS URL for {blob_name}")
        return blob_sas_url
    except Exception as e:
        logger.error(f"Error generating Blob SAS URL: {str(e)}")
        return None

def transcribe_with_deepgram_api(blob_sas_url, model="nova-3", diarize=True):
    """
    Transcribe audio using Deepgram API directly
    
    Args:
        blob_sas_url: SAS URL to the blob
        model: Deepgram model to use
        diarize: Whether to enable speaker diarization
        
    Returns:
        dict: Transcription response
    """
    try:
        # Configure API endpoint and parameters
        api_url = "https://api.deepgram.com/v1/listen"
        params = {
            "model": model,
            "detect_language": "true",
            "punctuate": "true",
            "smart_format": "true",
            "utterances": "true"  # Enable utterances for better segmentation
        }
        
        # Add diarization if requested
        if diarize:
            params["diarize"] = "true"
        
        # Set up headers with API key
        headers = {
            "Authorization": f"Token {DEEPGRAM_API_KEY}",
            "Content-Type": "application/json"
        }
        
        # Prepare request body with URL
        body = {
            "url": blob_sas_url
        }
        
        logger.info(f"Sending request to Deepgram API with blob SAS URL")
        logger.info(f"Model: {model}, Diarization: {diarize}")
        
        # Send request to Deepgram
        response = requests.post(api_url, json=body, params=params, headers=headers)
        
        # Check if request was successful
        if response.status_code == 200:
            logger.info("Transcription successful")
            return response.json()
        else:
            logger.error(f"Transcription failed: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Error transcribing with Deepgram: {str(e)}")
        return None

def store_in_sql_database(fileid, blob_name, dg_response):
    """
    Store transcription results in Azure SQL Database using stored procedures
    
    Args:
        fileid: Unique ID for the file
        blob_name: Name of the blob
        dg_response: Deepgram transcription response
        
    Returns:
        tuple: (success, details)
    """
    try:
        logger.info(f"Storing results for file {fileid} in database")
        conn = get_sql_connection()
        cursor = conn.cursor()
        
        # Extract basic information from the response
        full_transcript = ""
        language_detected = "unknown"
        confidence = 0.0
        
        # Extract transcript
        if dg_response and "results" in dg_response:
            # Extract language information
            if "language" in dg_response["results"]:
                language_detected = dg_response["results"]["language"]
                confidence = 0.95  # Default if not provided
            
            # Extract transcript from channels
            if "channels" in dg_response["results"]:
                channels = dg_response["results"]["channels"]
                if channels and "alternatives" in channels[0]:
                    alternatives = channels[0]["alternatives"]
                    if alternatives and "transcript" in alternatives[0]:
                        full_transcript = alternatives[0]["transcript"]
        
        # 1. Insert audio metadata using the stored procedure
        logger.info("Inserting audio metadata")
        cursor.execute("""
            EXEC RDS_InsertAudioMetadata
            @fileid = %s,
            @filename = %s,
            @file_size = %s,
            @upload_date = %s,
            @language_detected = %s,
            @transcription = %s,
            @status = %s,
            @processing_duration = %s
        """, (
            fileid,
            blob_name,
            0,  # File size (we don't have this information readily)
            datetime.now(),
            language_detected,
            full_transcript[:4000] if full_transcript else "",  # Limit to prevent SQL errors
            "completed",
            0  # Processing duration
        ))
        
        # 2. Extract and insert paragraphs/utterances
        paragraphs = []
        utterances = []
        
        if dg_response and "results" in dg_response:
            # First try to get utterances
            if "utterances" in dg_response["results"]:
                utterances = dg_response["results"]["utterances"]
                logger.info(f"Found {len(utterances)} utterances")
                
            # If no utterances, try to get paragraphs
            elif "paragraphs" in dg_response["results"] and "paragraphs" in dg_response["results"]["paragraphs"]:
                paragraphs = dg_response["results"]["paragraphs"]["paragraphs"]
                logger.info(f"Found {len(paragraphs)} paragraphs")
        
        # Process utterances or paragraphs to insert into database
        para_count = 0
        sent_count = 0
        
        if utterances:
            logger.info(f"Inserting {len(utterances)} utterances as paragraphs")
            for i, utterance in enumerate(utterances):
                if "transcript" in utterance:
                    para_count += 1
                    
                    # Insert paragraph
                    cursor.execute("""
                        EXEC RDS_InsertParagraph
                        @fileid = %s,
                        @para_num = %s,
                        @para_text = %s,
                        @start_time = %s,
                        @end_time = %s,
                        @speaker = %s
                    """, (
                        fileid,
                        i + 1,
                        utterance["transcript"][:4000],
                        int(utterance.get("start", 0) * 1000),  # Convert to milliseconds
                        int(utterance.get("end", 0) * 1000),    # Convert to milliseconds
                        utterance.get("speaker", 0)
                    ))
                    
                    # Insert as sentence too (one sentence per utterance for simplicity)
                    sent_count += 1
                    cursor.execute("""
                        EXEC RDS_InsertSentence
                        @fileid = %s,
                        @para_num = %s,
                        @sent_num = %s,
                        @sent_text = %s,
                        @start_time = %s,
                        @end_time = %s,
                        @speaker = %s
                    """, (
                        fileid,
                        i + 1,  # Paragraph number
                        1,      # Sentence number (one per utterance)
                        utterance["transcript"][:4000],
                        int(utterance.get("start", 0) * 1000),  # Convert to milliseconds
                        int(utterance.get("end", 0) * 1000),    # Convert to milliseconds
                        utterance.get("speaker", 0)
                    ))
        elif paragraphs:
            logger.info(f"Inserting {len(paragraphs)} paragraphs")
            for i, paragraph in enumerate(paragraphs):
                if "text" in paragraph:
                    para_count += 1
                    
                    # Insert paragraph
                    cursor.execute("""
                        EXEC RDS_InsertParagraph
                        @fileid = %s,
                        @para_num = %s,
                        @para_text = %s,
                        @start_time = %s,
                        @end_time = %s,
                        @speaker = %s
                    """, (
                        fileid,
                        i + 1,
                        paragraph["text"][:4000],
                        int(paragraph.get("start", 0) * 1000),  # Convert to milliseconds
                        int(paragraph.get("end", 0) * 1000),    # Convert to milliseconds
                        paragraph.get("speaker", 0)
                    ))
                    
                    # Insert as sentence too (one sentence per paragraph for simplicity)
                    sent_count += 1
                    cursor.execute("""
                        EXEC RDS_InsertSentence
                        @fileid = %s,
                        @para_num = %s,
                        @sent_num = %s,
                        @sent_text = %s,
                        @start_time = %s,
                        @end_time = %s,
                        @speaker = %s
                    """, (
                        fileid,
                        i + 1,  # Paragraph number
                        1,      # Sentence number (one per paragraph)
                        paragraph["text"][:4000],
                        int(paragraph.get("start", 0) * 1000),  # Convert to milliseconds
                        int(paragraph.get("end", 0) * 1000),    # Convert to milliseconds
                        paragraph.get("speaker", 0)
                    ))
        elif full_transcript:
            # If no structured segments, create a single paragraph
            para_count = 1
            
            # Insert single paragraph
            cursor.execute("""
                EXEC RDS_InsertParagraph
                @fileid = %s,
                @para_num = %s,
                @para_text = %s,
                @start_time = %s,
                @end_time = %s,
                @speaker = %s
            """, (
                fileid,
                1,
                full_transcript[:4000],
                0,  # Start time
                0,  # End time
                0   # Speaker
            ))
            
            # Split transcript into sentences (very basic)
            sentences = [s.strip() + "." for s in full_transcript.split('. ') if s.strip()]
            for i, sentence in enumerate(sentences):
                sent_count += 1
                cursor.execute("""
                    EXEC RDS_InsertSentence
                    @fileid = %s,
                    @para_num = %s,
                    @sent_num = %s,
                    @sent_text = %s,
                    @start_time = %s,
                    @end_time = %s,
                    @speaker = %s
                """, (
                    fileid,
                    1,          # Paragraph number
                    i + 1,      # Sentence number
                    sentence[:4000],
                    0,  # Start time
                    0,  # End time
                    0   # Speaker
                ))
        
        # Commit the transaction
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully stored transcription in database with {para_count} paragraphs and {sent_count} sentences")
        return True, {"para_count": para_count, "sent_count": sent_count}
    except Exception as e:
        logger.error(f"Error storing results in database: {str(e)}")
        return False, {"error": str(e)}

def verify_database_records(fileid):
    """
    Verify that records were properly stored in the database
    
    Args:
        fileid: File ID to check
        
    Returns:
        dict: Verification results
    """
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()
        
        # Check audio metadata
        cursor.execute("SELECT * FROM rdt_audio_metadata WHERE fileid = %s", (fileid,))
        metadata = cursor.fetchone()
        
        # Check paragraphs
        cursor.execute("SELECT COUNT(*) as count FROM rdt_paragraphs WHERE fileid = %s", (fileid,))
        para_count = cursor.fetchone()["count"]
        
        # Check sentences
        cursor.execute("SELECT COUNT(*) as count FROM rdt_sentences WHERE fileid = %s", (fileid,))
        sent_count = cursor.fetchone()["count"]
        
        cursor.close()
        conn.close()
        
        return {
            "metadata_exists": metadata is not None,
            "paragraph_count": para_count,
            "sentence_count": sent_count
        }
    except Exception as e:
        logger.error(f"Error verifying database records: {str(e)}")
        return {"error": str(e)}

def copy_to_destination_container(blob_name, fileid):
    """
    Copy blob to destination container with new name
    
    Args:
        blob_name: Source blob name
        fileid: File ID for new blob name
        
    Returns:
        bool: Success or failure
    """
    try:
        # Connect to Azure Storage
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        source_container_client = blob_service_client.get_container_client(SOURCE_CONTAINER)
        dest_container_client = blob_service_client.get_container_client(DESTINATION_CONTAINER)
        
        # Get source blob
        source_blob = source_container_client.get_blob_client(blob_name)
        
        # Generate SAS URL for source blob
        source_sas_url = generate_blob_sas_url(blob_name)
        
        # Create destination blob with new name
        dest_blob_name = f"{fileid}_{blob_name}"
        dest_blob = dest_container_client.get_blob_client(dest_blob_name)
        
        # Copy blob
        logger.info(f"Copying blob from {blob_name} to {dest_blob_name}")
        copy_operation = dest_blob.start_copy_from_url(source_sas_url)
        
        # Check copy status
        props = dest_blob.get_blob_properties()
        copy_status = props.copy.status
        
        logger.info(f"Copy status: {copy_status}")
        return copy_status == "success"
    except Exception as e:
        logger.error(f"Error copying blob: {str(e)}")
        return False

def run_complete_test():
    """
    Run the complete integration test
    
    Returns:
        dict: Test results
    """
    start_time = time.time()
    results = {
        "overall_success": False,
        "steps": {},
        "details": {},
        "timestamp": datetime.now().isoformat()
    }
    
    try:
        # Step 1: List audio files in Azure Storage
        logger.info("Step 1: Listing audio files in Azure Storage")
        audio_blobs = list_audio_blobs()
        if not audio_blobs:
            logger.error("No audio files found")
            results["steps"]["list_blobs"] = False
            return results
        
        results["steps"]["list_blobs"] = True
        results["details"]["available_blobs"] = audio_blobs
        
        # Step 2: Select a blob to process
        logger.info("Step 2: Selecting a blob to process")
        blob_name = audio_blobs[0]
        logger.info(f"Selected blob: {blob_name}")
        
        # Generate a unique file ID
        fileid = f"test_{uuid.uuid4().hex[:12]}"
        logger.info(f"Generated file ID: {fileid}")
        
        results["steps"]["select_blob"] = True
        results["details"]["selected_blob"] = blob_name
        results["details"]["fileid"] = fileid
        
        # Step 3: Generate Blob SAS URL
        logger.info("Step 3: Generating Blob SAS URL")
        blob_sas_url = generate_blob_sas_url(blob_name)
        if not blob_sas_url:
            logger.error("Failed to generate Blob SAS URL")
            results["steps"]["generate_sas"] = False
            return results
        
        results["steps"]["generate_sas"] = True
        
        # Step 4: Transcribe with Deepgram API
        logger.info("Step 4: Transcribing with Deepgram API")
        transcription_start = time.time()
        transcription_result = transcribe_with_deepgram_api(blob_sas_url)
        transcription_duration = time.time() - transcription_start
        
        if not transcription_result:
            logger.error("Transcription failed")
            results["steps"]["transcribe"] = False
            return results
        
        results["steps"]["transcribe"] = True
        results["details"]["transcription_duration"] = f"{transcription_duration:.2f} seconds"
        
        # Extract basic transcript for verification
        basic_transcript = ""
        if "results" in transcription_result and "channels" in transcription_result["results"]:
            channels = transcription_result["results"]["channels"]
            if channels and "alternatives" in channels[0]:
                alternatives = channels[0]["alternatives"]
                if alternatives and "transcript" in alternatives[0]:
                    basic_transcript = alternatives[0]["transcript"]
                    results["details"]["transcript_preview"] = basic_transcript[:100] + "..."
        
        # Save transcription to file for reference
        transcription_file = f"transcription_{fileid}.json"
        with open(transcription_file, 'w') as f:
            json.dump(transcription_result, f, indent=2)
        
        results["details"]["transcription_file"] = transcription_file
        
        # Step 5: Store in SQL Database
        logger.info("Step 5: Storing in SQL Database")
        db_success, db_details = store_in_sql_database(fileid, blob_name, transcription_result)
        
        results["steps"]["store_in_db"] = db_success
        results["details"]["db_details"] = db_details
        
        if not db_success:
            logger.error("Failed to store in database")
            return results
        
        # Step 6: Verify database records
        logger.info("Step 6: Verifying database records")
        verification = verify_database_records(fileid)
        
        results["steps"]["verify_db"] = "error" not in verification
        results["details"]["verification"] = verification
        
        # Step 7: Copy to destination container
        logger.info("Step 7: Copying to destination container")
        copy_success = copy_to_destination_container(blob_name, fileid)
        
        results["steps"]["copy_blob"] = copy_success
        
        # Calculate overall success
        results["overall_success"] = all(results["steps"].values())
        results["total_duration"] = f"{time.time() - start_time:.2f} seconds"
        
        return results
    except Exception as e:
        logger.error(f"Integration test failed: {str(e)}")
        results["error"] = str(e)
        return results

if __name__ == "__main__":
    # Run the complete test
    logger.info("Starting complete integration test")
    
    test_results = run_complete_test()
    
    # Save results to file
    results_file = f"integration_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(results_file, 'w') as f:
        json.dump(test_results, f, indent=2)
    
    # Print summary
    logger.info("\n" + "=" * 50)
    logger.info("INTEGRATION TEST RESULTS")
    logger.info("=" * 50)
    
    logger.info(f"Overall Success: {'✅' if test_results['overall_success'] else '❌'}")
    logger.info("\nStep Results:")
    for step, success in test_results["steps"].items():
        icon = "✅" if success else "❌"
        logger.info(f"  {icon} {step}")
    
    logger.info(f"\nResults saved to: {results_file}")
    
    # Exit with success/failure code
    sys.exit(0 if test_results["overall_success"] else 1)