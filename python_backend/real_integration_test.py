"""
Full integration test using real Azure resources:
1. Connect to Azure Storage
2. Get a real audio blob with SAS URL
3. Transcribe with Deepgram
4. Store results in Azure SQL database
"""
import os
import sys
import logging
import uuid
import json
import time
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import pymssql

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Azure Storage settings
STORAGE_CONNECTION_STRING = os.environ.get(
    "AZURE_STORAGE_CONNECTION_STRING",
    "DefaultEndpointsProtocol=https;AccountName=infolder;AccountKey=NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==;EndpointSuffix=core.windows.net"
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
    """Get a connection to Azure SQL Database"""
    return pymssql.connect(
        server=SQL_SERVER,
        port=SQL_PORT,
        database=SQL_DATABASE,
        user=SQL_USER,
        password=SQL_PASSWORD,
        tds_version='7.4',
        as_dict=True
    )

def list_container_blobs(container_name=SOURCE_CONTAINER, limit=5):
    """
    List blobs in the container
    
    Args:
        container_name (str): Name of the container
        limit (int): Maximum number of blobs to list
        
    Returns:
        list: List of blob objects
    """
    try:
        # Connect to Azure Storage
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(container_name)
        
        # List all blobs in the container
        all_blobs = list(container_client.list_blobs())
        
        # Get just the audio blobs
        audio_blobs = [
            blob for blob in all_blobs 
            if blob.name.lower().endswith(('.mp3', '.wav', '.m4a', '.ogg', '.aac'))
        ]
        
        # Return limited number of blobs
        return audio_blobs[:limit]
    except Exception as e:
        logger.error(f"Error listing blobs: {str(e)}")
        return []

def generate_sas_url(blob_name, container_name=SOURCE_CONTAINER, expiry_hours=240):
    """
    Generate a SAS URL for the specified blob.
    
    Args:
        blob_name (str): Name of the blob
        container_name (str): Name of the container
        expiry_hours (int): Number of hours until the SAS URL expires
        
    Returns:
        str: SAS URL for the blob
    """
    try:
        # Extract account information from connection string
        account_name = None
        account_key = None
        
        parts = STORAGE_CONNECTION_STRING.split(';')
        for part in parts:
            if part.startswith('AccountName='):
                account_name = part.split('=', 1)[1]
            elif part.startswith('AccountKey='):
                account_key = part.split('=', 1)[1]
        
        if not account_name or not account_key:
            logger.error("Could not extract account name or key from connection string")
            return None
        
        # Generate SAS token
        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=expiry_hours)
        )
        
        # Build the full SAS URL
        sas_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
        return sas_url
    except Exception as e:
        logger.error(f"Error generating SAS URL: {str(e)}")
        return None

def transcribe_with_deepgram(audio_url):
    """
    Transcribe audio using Deepgram API
    
    Args:
        audio_url (str): URL of the audio file (with SAS token)
        
    Returns:
        dict: Transcription result
    """
    try:
        # Import here to avoid dependency issues
        from direct_transcribe import DirectTranscriber
        
        # Create a DirectTranscriber instance
        transcriber = DirectTranscriber(DEEPGRAM_API_KEY)
        
        # Transcribe the audio
        logger.info(f"Transcribing audio from URL (first 60 chars): {audio_url[:60]}...")
        start_time = time.time()
        response = transcriber.transcribe_url(audio_url, model="nova-3", diarize=True)
        elapsed_time = time.time() - start_time
        logger.info(f"Transcription completed in {elapsed_time:.2f} seconds")
        
        return response
    except Exception as e:
        logger.error(f"Error transcribing audio: {str(e)}")
        return None

def store_in_database(fileid, blob_name, dg_response):
    """
    Store transcription results in Azure SQL Database
    
    Args:
        fileid (str): Unique ID for the file
        blob_name (str): Name of the blob
        dg_response (dict): Deepgram transcription response
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logger.info(f"Storing results for file {fileid} in database")
        conn = get_sql_connection()
        
        # Insert into audio metadata table
        logger.info("Inserting audio metadata")
        cursor = conn.cursor()
        
        # Prepare transcription text
        transcript = ""
        if dg_response and "results" in dg_response and "channels" in dg_response["results"]:
            channels = dg_response["results"]["channels"]
            if channels and "alternatives" in channels[0]:
                alternatives = channels[0]["alternatives"]
                if alternatives and "transcript" in alternatives[0]:
                    transcript = alternatives[0]["transcript"]
        
        # Insert audio metadata
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
            "English",  # Language detected (assumed for now)
            transcript[:4000] if transcript else "",  # Limit to prevent SQL errors
            "completed",
            0  # Processing duration
        ))
        
        audio_id = cursor.fetchone()
        
        # Get utterances to create paragraph segments
        utterances = []
        paragraphs = []
        if dg_response and "results" in dg_response:
            # First try to get utterances
            if "utterances" in dg_response["results"]:
                utterances = dg_response["results"]["utterances"]
                
            # If no utterances, try to get paragraphs
            elif "paragraphs" in dg_response["results"]:
                paragraphs = dg_response["results"]["paragraphs"]["paragraphs"]
        
        # Insert paragraphs
        para_count = 0
        if utterances:
            logger.info(f"Inserting {len(utterances)} utterances as paragraphs")
            for i, utterance in enumerate(utterances):
                para_count += 1
                if "transcript" in utterance:
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
                        int(utterance.get("end", 0) * 1000),  # Convert to milliseconds
                        utterance.get("speaker", 0)
                    ))
        elif paragraphs:
            logger.info(f"Inserting {len(paragraphs)} paragraphs")
            for i, paragraph in enumerate(paragraphs):
                para_count += 1
                if "text" in paragraph:
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
                        int(paragraph.get("end", 0) * 1000),  # Convert to milliseconds
                        paragraph.get("speaker", 0)
                    ))
        elif "results" in dg_response and "channels" in dg_response["results"]:
            # If no utterances or paragraphs, create a single paragraph from the transcript
            channels = dg_response["results"]["channels"]
            if channels and "alternatives" in channels[0]:
                alternatives = channels[0]["alternatives"]
                if alternatives and "transcript" in alternatives[0]:
                    para_count = 1
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
                        alternatives[0]["transcript"][:4000],
                        0,  # Start time
                        0,  # End time
                        0   # Speaker
                    ))
        
        # Insert sentences
        # We'll derive sentences from paragraphs or utterances if available, or from the raw transcript
        sent_count = 0
        if utterances:
            # Use utterances as sentences for simplicity
            logger.info(f"Inserting {len(utterances)} utterances as sentences")
            for i, utterance in enumerate(utterances):
                sent_count += 1
                if "transcript" in utterance:
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
                        i + 1,  # Paragraph number (maps to utterance)
                        1,      # Sentence number (one sentence per utterance for simplicity)
                        utterance["transcript"][:4000],
                        int(utterance.get("start", 0) * 1000),  # Convert to milliseconds
                        int(utterance.get("end", 0) * 1000),    # Convert to milliseconds
                        utterance.get("speaker", 0)
                    ))
        elif paragraphs:
            # Use paragraphs as sentences for simplicity
            logger.info(f"Inserting {len(paragraphs)} paragraphs as sentences")
            for i, paragraph in enumerate(paragraphs):
                sent_count += 1
                if "text" in paragraph:
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
                        1,      # Sentence number (one sentence per paragraph for simplicity)
                        paragraph["text"][:4000],
                        int(paragraph.get("start", 0) * 1000),  # Convert to milliseconds
                        int(paragraph.get("end", 0) * 1000),    # Convert to milliseconds
                        paragraph.get("speaker", 0)
                    ))
        elif transcript:
            # Split transcript into sentences (very basic)
            basic_sentences = [s.strip() for s in transcript.split('. ') if s.strip()]
            logger.info(f"Inserting {len(basic_sentences)} basic sentences")
            for i, sentence in enumerate(basic_sentences):
                sent_count += 1
                if sentence:
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
                        1,          # Paragraph number (just one paragraph)
                        i + 1,      # Sentence number
                        sentence[:4000],
                        0,  # Start time (unknown)
                        0,  # End time (unknown)
                        0   # Speaker (unknown)
                    ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully stored transcription in database with {para_count} paragraphs and {sent_count} sentences")
        return True, {"para_count": para_count, "sent_count": sent_count}
    except Exception as e:
        logger.error(f"Error storing results in database: {str(e)}")
        return False, {"error": str(e)}

def copy_to_output_container(blob_name, fileid):
    """
    Copy blob to output container
    
    Args:
        blob_name (str): Name of the source blob
        fileid (str): File ID (used to rename the blob)
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Connect to Azure Storage
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        source_container_client = blob_service_client.get_container_client(SOURCE_CONTAINER)
        dest_container_client = blob_service_client.get_container_client(DESTINATION_CONTAINER)
        
        # Get source blob client
        source_blob = source_container_client.get_blob_client(blob_name)
        
        # Create a new name for the destination blob
        dest_blob_name = f"{fileid}_{blob_name}"
        dest_blob = dest_container_client.get_blob_client(dest_blob_name)
        
        # Start copy operation
        source_url = source_blob.url
        # Add SAS token for source
        source_sas_url = generate_sas_url(blob_name)
        
        # Copy blob
        logger.info(f"Copying blob from {blob_name} to {dest_blob_name}")
        dest_blob.start_copy_from_url(source_sas_url)
        
        # Wait for copy to complete
        props = dest_blob.get_blob_properties()
        copy_status = props.copy.status
        
        logger.info(f"Copy status: {copy_status}")
        return copy_status == "success"
    except Exception as e:
        logger.error(f"Error copying blob: {str(e)}")
        return False

def run_integration_test():
    """
    Run a full integration test
    
    Returns:
        dict: Test results
    """
    results = {
        "success": False,
        "steps": {
            "blob_listing": False,
            "blob_selection": False,
            "sas_generation": False,
            "transcription": False,
            "db_storage": False,
            "blob_copy": False
        },
        "details": {}
    }
    
    try:
        # Step 1: List blobs in container
        logger.info("Step 1: Listing audio blobs in container")
        audio_blobs = list_container_blobs()
        if not audio_blobs:
            logger.error("No audio blobs found in container")
            return results
        
        results["steps"]["blob_listing"] = True
        results["details"]["available_blobs"] = [blob.name for blob in audio_blobs]
        
        # Step 2: Select a blob to process
        logger.info("Step 2: Selecting an audio blob")
        selected_blob = audio_blobs[0]
        blob_name = selected_blob.name
        logger.info(f"Selected blob: {blob_name}")
        
        results["steps"]["blob_selection"] = True
        results["details"]["selected_blob"] = blob_name
        
        # Generate a unique file ID
        fileid = f"test_{uuid.uuid4().hex[:10]}"
        results["details"]["fileid"] = fileid
        
        # Step 3: Generate a SAS URL for the blob
        logger.info("Step 3: Generating SAS URL")
        sas_url = generate_sas_url(blob_name)
        if not sas_url:
            logger.error("Failed to generate SAS URL")
            return results
        
        results["steps"]["sas_generation"] = True
        results["details"]["sas_url_preview"] = sas_url[:60] + "..."
        
        # Step 4: Transcribe the audio
        logger.info("Step 4: Transcribing the audio")
        start_time = time.time()
        transcription_result = transcribe_with_deepgram(sas_url)
        elapsed_time = time.time() - start_time
        
        if not transcription_result:
            logger.error("Transcription failed")
            return results
        
        results["steps"]["transcription"] = True
        results["details"]["transcription_time"] = f"{elapsed_time:.2f} seconds"
        
        # Step 5: Store the results in the database
        logger.info("Step 5: Storing results in database")
        db_success, db_details = store_in_database(fileid, blob_name, transcription_result)
        
        results["steps"]["db_storage"] = db_success
        results["details"]["db_storage"] = db_details
        
        # Step 6: Copy the blob to the output container
        logger.info("Step 6: Copying blob to output container")
        copy_success = copy_to_output_container(blob_name, fileid)
        
        results["steps"]["blob_copy"] = copy_success
        
        # Overall success
        results["success"] = all(results["steps"].values())
        
    except Exception as e:
        logger.error(f"Integration test failed: {str(e)}")
        results["details"]["error"] = str(e)
    
    return results

if __name__ == "__main__":
    logger.info("Running full integration test")
    results = run_integration_test()
    
    # Print summary
    logger.info("Integration Test Results:")
    logger.info(f"Overall Success: {'✅' if results['success'] else '❌'}")
    logger.info("Step Results:")
    for step, success in results["steps"].items():
        status = "✅" if success else "❌"
        logger.info(f"  {status} {step}")
    
    # Print details
    logger.info("Details:")
    for key, value in results["details"].items():
        logger.info(f"  {key}: {value}")
    
    # Save results to file
    with open(f"integration_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", "w") as f:
        json.dump(results, f, indent=2)
    
    sys.exit(0 if results["success"] else 1)